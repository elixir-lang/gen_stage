defmodule GenStage.DemandDispatcher do
  @moduledoc """
  A dispatcher that sends batches to the highest demand.

  This is the default dispatcher used by `GenStage`. In order
  to avoid greedy consumers, it is recommended that all consumers
  have exactly the same maximum demand.
  """

  @behaviour GenStage.Dispatcher

  @doc false
  def init(_opts) do
    {:ok, {[], 0, nil}}
  end

  @doc false
  def info(msg, state) do
    send(self(), msg)
    {:ok, state}
  end

  @doc false
  def subscribe(_opts, {pid, ref}, {demands, pending, max}) do
    {:ok, 0, {demands ++ [{0, pid, ref}], pending, max}}
  end

  @doc false
  def cancel({_, ref}, {demands, pending, max}) do
    {current, demands} = pop_demand(ref, demands)
    {:ok, 0, {demands, current + pending, max}}
  end

  @doc false
  def ask(counter, {pid, ref}, {demands, pending, max}) do
    max = max || counter

    if counter > max do
      :error_logger.warning_msg('GenStage producer DemandDispatcher expects a maximum demand of ~p. ' ++
                                'Using different maximum demands will overload greedy consumers. ' ++
                                'Got demand for ~p events from ~p~n', [max, counter, pid])
    end

    {current, demands} = pop_demand(ref, demands)
    demands = add_demand(current + counter, pid, ref, demands)

    already_sent = min(pending, counter)
    {:ok, counter - already_sent, {demands, pending - already_sent, max}}
  end

  @doc false
  def dispatch(events, length, {demands, pending, max}) do
    {events, demands} = dispatch_demand(events, length, demands)
    {:ok, events, {demands, pending, max}}
  end

  defp dispatch_demand([], _length, demands) do
    {[], demands}
  end
  defp dispatch_demand(events, _length, [{0, _, _} | _] = demands) do
    {events, demands}
  end
  defp dispatch_demand(events, length, [{counter, pid, ref} | demands]) do
    {deliver_now, deliver_later, length, counter} =
      split_events(events, length, counter)
    Process.send(pid, {:"$gen_consumer", {self(), ref}, deliver_now}, [:noconnect])
    demands = add_demand(counter, pid, ref, demands)
    dispatch_demand(deliver_later, length, demands)
  end

  defp split_events(events, length, counter) when length <= counter do
    {events, [], 0, counter - length}
  end
  defp split_events(events, length, counter) do
    {now, later} = Enum.split(events, counter)
    {now, later, length - counter, 0}
  end

  defp add_demand(counter, pid, ref, [{c, _, _} | _] = demands) when counter > c,
    do: [{counter, pid, ref} | demands]
  defp add_demand(counter, pid, ref, [demand | demands]),
    do: [demand | add_demand(counter, pid, ref, demands)]
  defp add_demand(counter, pid, ref, []) when is_integer(counter),
    do: [{counter, pid, ref}]

  defp pop_demand(ref, demands) do
    {{current, _pid, ^ref}, rest} = List.keytake(demands, ref, 2)
    {current, rest}
  end
end
