defmodule GenStage.BroadcastDispatcher do
  @moduledoc """
  A dispatcher that accumulates demand from all consumers
  before broadcasting events to all of them.

  This dispatcher guarantees that events are dispatched to all
  consumers without exceeding the demand of any given consumer.

  If a producer uses `GenStage.BroadcastDispatcher`, its subscribers
  can specify an optional `:selector` function that receives the event
  and returns a boolean in the subscription options.

  Assume `producer` and `consumer` are stages exchanging events of type
  `%{:key => String.t, any => any}`, then by calling

      GenStage.sync_subscribe(consumer,
        to: producer,
        selector: fn %{key: key} -> String.starts_with?(key, "foo-") end)

  `consumer` will receive only the events broadcast from `producer`
  for which the selector function returns a truthy value.

  The `:selector` option can be specified in sync and async subscriptions,
  as well as in the `:subscribe_to` list in the return tuple of
  `c:GenStage.init/1`. For example:

      def init(:ok) do
        {:consumer, :ok, subscribe_to:
          [{producer, selector: fn %{key: key} -> String.starts_with?(key, "foo-") end}]}
      end

  """

  @behaviour GenStage.Dispatcher

  require Logger

  @doc false
  def init(_opts) do
    {:ok, {[], 0, MapSet.new()}}
  end

  @doc false
  def info(msg, state) do
    send(self(), msg)
    {:ok, state}
  end

  @doc false
  def subscribe(opts, {pid, ref}, {demands, waiting, subscribed_processes}) do
    selector = validate_selector(opts)

    if subscribed?(subscribed_processes, pid) do
      Logger.error(fn ->
        "#{inspect(pid)} is already registered with #{inspect(self())}. " <>
          "This subscription has been discarded."
      end)

      {:error, :already_subscribed}
    else
      subscribed_processes = add_subscriber(subscribed_processes, pid)
      {:ok, 0, {add_demand(-waiting, pid, ref, selector, demands), waiting, subscribed_processes}}
    end
  end

  @doc false
  def cancel({pid, ref}, {demands, waiting, subscribed_processes}) do
    # Since we may have removed the process we were waiting on,
    # cancellation may actually generate demand!
    demands = delete_demand(ref, demands)
    new_min = get_min(demands)
    demands = adjust_demand(new_min, demands)
    subscribed_processes = delete_subscriber(subscribed_processes, pid)
    {:ok, new_min, {demands, waiting + new_min, subscribed_processes}}
  end

  @doc false
  def ask(counter, {pid, ref}, {demands, waiting, subscribed_processes}) do
    {current, selector, demands} = pop_demand(ref, demands)
    demands = add_demand(current + counter, pid, ref, selector, demands)
    new_min = get_min(demands)
    demands = adjust_demand(new_min, demands)
    {:ok, new_min, {demands, waiting + new_min, subscribed_processes}}
  end

  @doc false
  def dispatch(events, _length, {demands, 0, subscribed_processes}) do
    {:ok, events, {demands, 0, subscribed_processes}}
  end

  def dispatch(events, length, {demands, waiting, subscribed_processes}) do
    {deliver_now, deliver_later, waiting} = split_events(events, length, waiting)

    for {_, pid, ref, selector} <- demands do
      selected =
        case filter_and_count(deliver_now, selector) do
          {selected, 0} ->
            selected

          {selected, discarded} ->
            send(self(), {:"$gen_producer", {pid, ref}, {:ask, discarded}})
            selected
        end

      Process.send(pid, {:"$gen_consumer", {self(), ref}, selected}, [:noconnect])
      :ok
    end

    {:ok, deliver_later, {demands, waiting, subscribed_processes}}
  end

  defp filter_and_count(messages, nil) do
    {messages, 0}
  end

  defp filter_and_count(messages, selector) do
    filter_and_count(messages, selector, [], 0)
  end

  defp filter_and_count([message | messages], selector, acc, count) do
    if selector.(message) do
      filter_and_count(messages, selector, [message | acc], count)
    else
      filter_and_count(messages, selector, acc, count + 1)
    end
  end

  defp filter_and_count([], _selector, acc, count) do
    {:lists.reverse(acc), count}
  end

  defp validate_selector(opts) do
    case Keyword.get(opts, :selector) do
      nil ->
        nil

      selector when is_function(selector, 1) ->
        selector

      other ->
        raise ArgumentError,
              ":selector option must be passed a unary function, got: #{inspect(other)}"
    end
  end

  defp get_min([]), do: 0

  defp get_min([{acc, _, _, _} | demands]),
    do: demands |> Enum.reduce(acc, fn {val, _, _, _}, acc -> min(val, acc) end) |> max(0)

  defp split_events(events, length, counter) when length <= counter do
    {events, [], counter - length}
  end

  defp split_events(events, _length, counter) do
    {now, later} = Enum.split(events, counter)
    {now, later, 0}
  end

  defp adjust_demand(0, demands) do
    demands
  end

  defp adjust_demand(min, demands) do
    Enum.map(demands, fn {counter, pid, key, selector} ->
      {counter - min, pid, key, selector}
    end)
  end

  defp add_demand(counter, pid, ref, selector, demands)
       when is_integer(counter) and is_pid(pid) and (is_nil(selector) or is_function(selector, 1)) do
    [{counter, pid, ref, selector} | demands]
  end

  defp pop_demand(ref, demands) do
    case List.keytake(demands, ref, 2) do
      {{current, _pid, ^ref, selector}, rest} -> {current, selector, rest}
      nil -> {0, nil, demands}
    end
  end

  defp delete_demand(ref, demands) do
    List.keydelete(demands, ref, 2)
  end

  defp add_subscriber(subscribed_processes, pid) do
    MapSet.put(subscribed_processes, pid)
  end

  defp delete_subscriber(subscribed_processes, pid) do
    MapSet.delete(subscribed_processes, pid)
  end

  defp subscribed?(subscribed_processes, pid) do
    MapSet.member?(subscribed_processes, pid)
  end
end
