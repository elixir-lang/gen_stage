defmodule GenRouter.BroadcastOut do
  # TODO: Implement pseudo-code

  # The broadcast router only sends demand upstream when all
  # consumers have asked for something. For this reason, there is
  # no buffering. BufferIn and BufferOut should provide buffering
  # alternatives.

  use GenRouter.Out

  def init(_) do
    {:ok, %{}}
  end

  def handle_demand(demand, {_pid, ref}, consumers) do
    consumers = Map.update(consumers, ref, demand, & &1 + demand)

    # Get the common demand
    {_ref, min} =
      Enum.min_by consumers, fn {_ref, demand} -> demand end

    # Update the consumers by removing the common demand
    consumers =
      Enum.reduce consumers, consumers, fn {ref, demand}, acc ->
        Map.put acc, ref, demand - min
      end

    {:ok, min, consumers}
  end

  def handle_dispatch(_event, consumers) do
    {:ok, Map.keys(consumers), consumers}
  end

  def handle_down(_reason, {_pid, ref}, consumers) do
    {:ok, Map.delete(consumers, ref)}
  end
end
