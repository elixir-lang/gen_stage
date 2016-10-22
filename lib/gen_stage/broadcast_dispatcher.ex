alias Experimental.GenStage

defmodule GenStage.BroadcastDispatcher do
  @moduledoc """
  A dispatcher that accumulates demand from all consumers
  before broadcasting events to all of them.
  """

  @behaviour GenStage.Dispatcher

  @doc false
  def init(_opts) do
    {:ok, {[], 0}}
  end

  @doc false
  def notify(msg, {demands, _} = state) do
    Enum.each(demands, fn {_, pid, ref} ->
      Process.send(pid, {:"$gen_consumer", {self(), ref}, {:notification, msg}}, [:noconnect])
    end)
    {:ok, state}
  end

  @doc false
  def subscribe(_opts, {pid, ref}, {demands, waiting}) do
    {:ok, 0, {add_demand(-waiting, pid, ref, demands), waiting}}
  end

  @doc false
  def cancel({_, ref}, {demands, waiting}) do
    # Since we may have removed the process we were waiting on,
    # cancellation may actually generate demand!
    demands = delete_demand(ref, demands)
    new_min = get_min(demands)
    demands = adjust_demand(new_min, demands)
    {:ok, new_min, {demands, waiting + new_min}}
  end

  @doc false
  def ask(counter, {pid, ref}, {demands, waiting}) do
    {current, demands} = pop_demand(ref, demands)
    demands = add_demand(current + counter, pid, ref, demands)
    new_min = get_min(demands)
    demands = adjust_demand(new_min, demands)
    {:ok, new_min, {demands, waiting + new_min}}
  end

  @doc false
  def dispatch(events, _length, {demands, 0}) do
    {:ok, events, {demands, 0}}
  end

  def dispatch(events, length, {demands, waiting}) do
    {deliver_now, deliver_later, waiting} =
      split_events(events, length, waiting)

    Enum.each(demands, fn {_, pid, ref} ->
      Process.send(pid, {:"$gen_consumer", {self(), ref}, deliver_now}, [:noconnect])
    end)

    {:ok, deliver_later, {demands, waiting}}
  end

  defp get_min([]),
    do: 0
  defp get_min([{acc, _, _} | demands]),
    do: demands |> Enum.reduce(acc, fn {val, _, _}, acc -> min(val, acc) end) |> max(0)

  defp split_events(events, length, counter) when length <= counter do
    {events, [], counter - length}
  end
  defp split_events(events, _length, counter) do
    {now, later} = Enum.split(events, counter)
    {now, later, 0}
  end

  defp adjust_demand(0, demands),
    do: demands
  defp adjust_demand(min, demands),
    do: Enum.map(demands, fn {counter, pid, key} -> {counter - min, pid, key} end)

  defp add_demand(counter, pid, ref, demands) when is_integer(counter) and is_pid(pid) do
    [{counter, pid, ref} | demands]
  end

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
