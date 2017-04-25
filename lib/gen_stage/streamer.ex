defmodule GenStage.Streamer do
  @moduledoc false
  use GenStage

  def init({stream, opts}) do
    continuation = &Enumerable.reduce(stream, &1, fn
      x, {acc, 1} -> {:suspend, {[x | acc], 0}}
      x, {acc, counter} -> {:cont, {[x | acc], counter - 1}}
    end)

    {:producer, continuation, Keyword.take(opts, [:dispatcher, :demand])}
  end

  def handle_demand(_demand, continuation) when is_atom(continuation) do
    {:noreply, [], continuation}
  end
  def handle_demand(demand, continuation) when demand > 0 do
    case continuation.({:cont, {[], demand}}) do
      {:suspended, {list, 0}, continuation} ->
        {:noreply, :lists.reverse(list), continuation}
      {status, {list, _}} ->
        GenStage.async_info(self(), :stop)
        {:noreply, :lists.reverse(list), status}
    end
  end

  def handle_info(:stop, state) do
    {:stop, :normal, state}
  end
end
