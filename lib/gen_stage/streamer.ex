alias Experimental.GenStage

defmodule GenStage.Streamer do
  @moduledoc false
  use GenStage

  def init({stream, opts}) do
    consumers = case Keyword.get(opts, :consumers, :temporary) do
      :temporary -> :temporary
      :permanent -> []
    end

    continuation = &Enumerable.reduce(stream, &1, fn
      x, {acc, 1} -> {:suspend, {[x | acc], 0}}
      x, {acc, counter} -> {:cont, {[x | acc], counter - 1}}
    end)

    partitioned? = match?({GenStage.PartitionDispatcher, _}, opts[:dispatcher])
    {:producer, {partitioned?, consumers, continuation}, Keyword.take(opts, [:dispatcher])}
  end

  def handle_subscribe(_, _, {pid, ref}, {partitioned?, consumers, continuation}) do
    if not partitioned? and is_atom(continuation) do
      msg = {:producer, continuation}
      Process.send(pid, {:"$gen_consumer", {self(), ref}, {:notification, msg}}, [:noconnect])
    end
    consumers = case consumers do
      :temporary -> :temporary
      _ -> [ref | consumers]
    end
    {:automatic, {partitioned?, consumers, continuation}}
  end

  def handle_cancel(_, _, {_partitioned?, :temporary, _} = state) do
    {:noreply, [], state}
  end
  def handle_cancel(_, {_, ref}, {partitioned?, consumers, continuation}) do
    case List.delete(consumers, ref) do
      [] -> {:stop, :normal, {partitioned?, [], continuation}}
      consumers -> {:noreply, [], {partitioned?, consumers, continuation}}
    end
  end

  def handle_demand(_demand, {_, _, status} = state) when is_atom(status) do
    {:noreply, [], state}
  end
  def handle_demand(demand, {partitioned?, consumers, continuation}) when demand > 0 do
    case continuation.({:cont, {[], demand}}) do
      {:suspended, {list, 0}, continuation} ->
        {:noreply, Enum.reverse(list), {partitioned?, consumers, continuation}}
      {status, {list, _}} ->
        GenStage.async_notify(self(), {:producer, status})
        {:noreply, Enum.reverse(list), {partitioned?, consumers, status}}
    end
  end
end
