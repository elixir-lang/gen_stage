alias Experimental.{DynamicSupervisor, Flow, GenStage}

defmodule Flow.Coordinator do
  @moduledoc false
  use GenServer

  def init({flow, type, consumers, options}) do
    {:ok, sup} = start_supervisor()
    start_link = &DynamicSupervisor.start_child(sup, [&1, &2, &3])
    type_options = Keyword.take(options, [:dispatcher])

    {producers, intermediary} =
      Flow.Materialize.materialize(flow, start_link, type, type_options)

    for {pid, _} <- intermediary do
      for consumer <- consumers do
        subscribe(consumer, pid)
      end
    end

    demand = Keyword.get(options, :demand, :forward)
    producers = Enum.map(producers, &elem(&1, 0))
    for producer <- producers, do: GenStage.demand(producer, demand)
    {:ok, %{supervisor: sup, producers: producers}}
  end

  def handle_cast({:"$demand", demand}, %{producers: producers} = state) do
    for producer <- producers, do: GenStage.demand(producer, demand)
    {:noreply, state}
  end

  defp subscribe({consumer, opts}, producer) when is_list(opts) do
    GenStage.sync_subscribe(consumer, [to: producer] ++ opts)
  end
  defp subscribe(consumer, producer) do
    GenStage.sync_subscribe(consumer, [to: producer])
  end

  defp start_supervisor() do
    children = [Supervisor.Spec.supervisor(GenStage, [])]
    DynamicSupervisor.start_link(children, strategy: :one_for_one)
  end
end