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
    {:ok, {[], nil}}
  end

  @doc false
  def subscribe({_, _ref}, state) do
    {:ok, 0, state}
  end

  @doc false
  def cancel({_, ref}, {demands, max}) do
    {:ok, 0, {delete_demand(ref, demands), max}}
  end

  @doc false
  def ask(counter, {pid, ref}, {demands, max}) do
    max = max || counter

    if counter > max do
      :error_logger.warning_msg('GenStage producer DemandDispatcher expects a maximum demand of ~p. ' ++
                                'Using different maximum demands will overload greedy consumers. ' ++
                                'Got demand for ~p events from ~p~n', [max, counter, pid])
    end

    {current, demands} = pop_demand(ref, demands)
    demands = add_demand(current + counter, pid, ref, demands)
    {:ok, counter, {demands, max}}
  end

  @doc false
  def dispatch(events, {demands, max}) do
    {events, demands} = dispatch_demand(events, demands)
    {:ok, events, {demands, max}}
  end

  defp dispatch_demand([], demands) do
    {[], demands}
  end

  defp dispatch_demand(events, [{0, _, _} | _] = demands) do
    {events, demands}
  end

  defp dispatch_demand(events, [{counter, pid, ref} | demands]) do
    {deliver_now, deliver_later, counter} =
      split_events(events, counter, [])
    send(pid, {:"$gen_consumer", {self(), ref}, deliver_now})
    demands = add_demand(counter, pid, ref, demands)
    dispatch_demand(deliver_later, demands)
  end

  defp split_events(events, 0, acc),
    do: {Enum.reverse(acc), events, 0}
  defp split_events([], counter, acc),
    do: {Enum.reverse(acc), [], counter}
  defp split_events([event | events], counter, acc),
    do: split_events(events, counter - 1, [event | acc])

  defp add_demand(counter, pid, ref, [{c, _, _} | _] = demands) when counter > c,
    do: [{counter, pid, ref} | demands]
  defp add_demand(counter, pid, ref, [demand | demands]),
    do: [demand | add_demand(counter, pid, ref, demands)]
  defp add_demand(counter, pid, ref, []) when is_integer(counter),
    do: [{counter, pid, ref}]

  defp pop_demand(ref, demands) do
    case List.keytake(demands, ref, 2) do
      {{current, _pid, ^ref}, rest} -> {current, rest}
      nil -> {0, demands}
    end
  end

  defp delete_demand(ref, demands) do
    List.keydelete(demands, ref, 2)
  end
end
