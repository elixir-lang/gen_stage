alias Experimental.GenStage

defmodule GenStage.BroadcastDispatcher do
  @moduledoc """
  A dispatcher that accumulates demand from all consumer
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
      Process.send(pid, {ref, msg}, [:noconnect])
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
  def dispatch(events, {demands, 0}) do
    {:ok, events, {demands, 0}}
  end

  def dispatch(events, {demands, waiting}) do
    {deliver_now, deliver_later, waiting} =
      split_events(events, waiting, [])

    Enum.each(demands, fn {_, pid, ref} ->
      Process.send(pid, {:"$gen_consumer", {self(), ref}, deliver_now}, [:noconnect])
    end)

    {:ok, deliver_later, {demands, waiting}}
  end

  defp get_min([]),
    do: 0
  defp get_min([{acc, _, _} | demands]),
    do: demands |> Enum.reduce(acc, fn {val, _, _}, acc -> min(val, acc) end) |> max(0)

  defp split_events(events, 0, acc),
    do: {Enum.reverse(acc), events, 0}
  defp split_events([], counter, acc),
    do: {Enum.reverse(acc), [], counter}
  defp split_events([event | events], counter, acc),
    do: split_events(events, counter - 1, [event | acc])

  defp adjust_demand(0, demands),
    do: demands
  defp adjust_demand(min, demands),
    do: Enum.map(demands, fn {counter, pid, key} -> {counter - min, pid, key} end)

  defp add_demand(counter, pid, ref, demands)
       when is_integer(counter) and is_pid(pid) and is_reference(ref) do
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
