alias Experimental.{DynamicSupervisor, Flow, GenStage}

defmodule Flow.Coordinator do
  @moduledoc false
  use GenServer

  def init({flow, type, options}) do
    {:ok, sup} = start_supervisor()
    start_link = &DynamicSupervisor.start_child(sup, [&1, &2, &3])
    type_options = Keyword.take(options, [:dispatcher])
    {producers, consumers} = Flow.Materialize.materialize(flow, start_link, type, type_options)

    demand = Keyword.get(options, :demand, :forward)
    for {producer, _} <- producers, do: GenStage.demand(producer, demand)
    {:ok, %{type: type, supervisor: sup, producers: producers, consumers: consumers}}
  end

  # TODO: call(stage, {:"$subscribe", to, opts}, timeout)
  # TODO: cast(stage, {:"$subscribe", to, opts})
  # TODO: cast(stage, {:"$demand", mode})
  # TODO: info({:"$gen_producer", from :: {consumer_pid, subscription_tag}, {:subscribe, options}})

  defp start_supervisor() do
    children = [Supervisor.Spec.supervisor(GenStage, [])]
    DynamicSupervisor.start_link(children, strategy: :one_for_one)
  end
end