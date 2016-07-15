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

    {:producer, {consumers, continuation}, Keyword.take(opts, [:dispatcher])}
  end

  def handle_subscribe(_, _, _, {:temporary, _} = state) do
    {:automatic, state}
  end
  def handle_subscribe(_, _, {pid, ref}, {consumers, continuation}) do
    if is_atom(continuation) do
      msg = {:producer, continuation}
      Process.send(pid, {:"$gen_consumer", {self(), ref}, {:notification, msg}}, [:noconnect])
    end
    {:automatic, {[ref | consumers], continuation}}
  end

  def handle_cancel(_, _, {:temporary, _} = state) do
    {:noreply, [], state}
  end
  def handle_cancel(_, {_, ref}, {consumers, continuation}) do
    case List.delete(consumers, ref) do
      [] -> {:stop, :normal, {[], continuation}}
      consumers -> {:noreply, [], {consumers, continuation}}
    end
  end

  def handle_demand(_demand, {_, status} = state) when is_atom(status) do
    {:noreply, [], state}
  end
  def handle_demand(demand, {consumers, continuation}) when demand > 0 do
    case continuation.({:cont, {[], demand}}) do
      {:suspended, {list, 0}, continuation} ->
        {:noreply, Enum.reverse(list), {consumers, continuation}}
      {status, {list, _}} ->
        GenStage.async_notify(self(), {:producer, status})
        {:noreply, Enum.reverse(list), {consumers, status}}
    end
  end
end
