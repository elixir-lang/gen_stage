alias Experimental.{Flow, GenStage}

defmodule Flow.Coordinator do
  @moduledoc false
  use GenServer

  def init({flow, type, consumers, options}) do
    {:ok, sup} = start_supervisor()
    start_link = &Supervisor.start_child(sup, [&1, &2, &3])
    type_options = Keyword.take(options, [:dispatcher])

    {producers, intermediary} =
      Flow.Materialize.materialize(flow, start_link, type, type_options)

    refs =
      for {pid, _} <- intermediary do
        for consumer <- consumers do
          subscribe(consumer, pid)
        end
        Process.monitor(pid)
      end

    demand = Keyword.get(options, :demand, :forward)
    producers = Enum.map(producers, &elem(&1, 0))
    for producer <- producers, do: GenStage.demand(producer, demand)
    {:ok, %{supervisor: sup, producers: producers, refs: refs}}
  end

  def handle_cast({:"$demand", demand}, %{producers: producers} = state) do
    for producer <- producers, do: GenStage.demand(producer, demand)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, _, _, _}, %{refs: refs} = state) do
    case List.delete(refs, ref) do
      [] -> {:stop, :normal, state}
      refs -> {:noreply, %{state | refs: refs}}
    end
  end

  defp subscribe({consumer, opts}, producer) when is_list(opts) do
    GenStage.sync_subscribe(consumer, [to: producer] ++ opts)
  end
  defp subscribe(consumer, producer) do
    GenStage.sync_subscribe(consumer, [to: producer])
  end

  defp start_supervisor() do
    children = [Supervisor.Spec.worker(GenStage, [], restart: :transient)]
    Supervisor.start_link(children, strategy: :simple_one_for_one, max_restarts: 0)
  end
end
