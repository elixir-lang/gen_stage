alias Experimental.GenStage

defmodule GenStage.Streamer do
  @moduledoc false
  use GenStage

  def init({stream, opts}) do
    consumers = Keyword.get(opts, :consumers, :temporary)

    continuation = &Enumerable.reduce(stream, &1, fn
      x, {acc, 1} -> {:suspend, {[x | acc], 0}}
      x, {acc, counter} -> {:cont, {[x | acc], counter - 1}}
    end)

    {:producer, {consumers, continuation}, Keyword.take(opts, [:dispatcher])}
  end

  def handle_cancel(_, _, {:temporary, _} = state) do
    {:noreply, [], state}
  end
  def handle_cancel({_, reason}, _, {:permanent, _} = state) do
    {:stop, reason, state}
  end

  def handle_demand(_demand, {_, :nofun} = state) do
    {:noreply, [], state}
  end
  def handle_demand(demand, {consumers, continuation}) when demand > 0 do
    case continuation.({:cont, {[], demand}}) do
      {:suspended, {list, 0}, continuation} ->
        {:noreply, Enum.reverse(list), {consumers, continuation}}
      {state, {list, _}} ->
        GenStage.async_notify(self(), {:producer, state})
        {:noreply, Enum.reverse(list), {consumers, :nofun}}
    end
  end
end
