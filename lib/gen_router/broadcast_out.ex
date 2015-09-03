defmodule GenRouter.BroadcastOut do
  # TODO: Implement pseudo-code

  # The broadcast router only sends demand upstream when are
  # sinks have asked for something. For this reason, there is
  # no buffering. BufferIn and BufferOut should provide buffering
  # alternatives.

  def init(_) do
    {:ok, %{}}
  end

  # TODO: Sometimes we may want to pass options when we send a demand.
  # For example, to customize what happens when the particular sink
  # gets behind. For such, should we add an opts to handle_demand/4?

  def handle_demand(demand, {_pid, ref}, sinks) do
    sinks = Map.update(sinks, ref, demand, & &1 + demand)

    # Get the common demand
    {_ref, min} =
      Enum.min_by sinks, fn {_ref, demand} -> demand end

    # Update the sinks by removing the common demand
    sinks =
      Enum.reduce sinks, sinks, fn {ref, demand}, acc ->
        Map.put acc, ref, demand - min
      end

    {:ok, min, sinks}
  end

  def handle_dispatch(_event, sinks) do
    {:ok, Map.keys(sinks), sinks}
  end

  def handle_down(_reason, {_pid, ref}, sinks) do
    {:ok, Map.delete(sinks, ref)}
  end
end
