defmodule GenRouter.DynamicIn do
  # TODO: Set a buffer limit and what to do once it is reached

  use GenRouter.In

  def init(_) do
    queue = :queue.new()
    {:ok, {0, queue}}
  end

  # demand

  def handle_demand(demand, {0, queue}) do
    {demand, queue, entries} = take_demand_from_queue(demand, queue)
    events =
      for {from, event} <- entries do
        reply(from, :ok)
        event
      end

    {:dispatch, events, {demand, queue}}
  end

  def handle_demand(demand, {current, queue}) do
    {:noreply, {current+demand, queue}}
  end

  # info

  def handle_info({:"$gen_notify", from, event}, {0, queue}) do
    queue = put_event_in_queue(from, event, queue)
    {:noreply, {0, queue}}
  end

  def handle_info({:"$gen_notify", from, event}, {demand, queue}) do
    reply(from, :ok)
    {:dispatch, [event], {demand-1, queue}}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  defp reply({pid, ref}, reply) do
    send pid, {ref, reply}
  end

  defp take_demand_from_queue(demand, queue), do: take_demand_from_queue(demand, queue, [])
  defp take_demand_from_queue(0, queue, events), do: {0, queue, Enum.reverse(events)}
  defp take_demand_from_queue(demand, queue, events) do
      case :queue.out(queue) do
          {{:value, entry}, new_queue} -> take_demand_from_queue(demand-1, new_queue, [entry | events])
          {:empty, queue} -> {demand, queue, Enum.reverse(events)}
      end
  end

  defp put_event_in_queue(from, event, queue), do: :queue.in({from, event}, queue)
end
