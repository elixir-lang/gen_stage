defmodule GenRouter.DynamicIn do
  # TODO: Implement pseudo-code

  def init(_) do
    queue = :queue.new()
    {:ok, {demand, queue}}
  end

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
end
