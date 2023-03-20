defmodule GenStage.Streamer do
  @moduledoc false
  use GenStage

  def start_link({stream, opts}) do
    {:current_stacktrace, [_info_call | stack]} = Process.info(self(), :current_stacktrace)
    GenStage.start_link(__MODULE__, {stream, stack, opts}, opts)
  end

  def init({stream, stack, opts}) do
    continuation =
      &Enumerable.reduce(stream, &1, fn
        x, {acc, 1} -> {:suspend, {[x | acc], 0}}
        x, {acc, counter} -> {:cont, {[x | acc], counter - 1}}
      end)

    on_cancel =
      case Keyword.get(opts, :on_cancel, :continue) do
        :continue -> nil
        :stop -> %{}
      end

    {:producer, {stack, continuation, on_cancel}, Keyword.take(opts, [:dispatcher, :demand])}
  end

  def handle_subscribe(:consumer, _opts, {pid, ref}, {stack, continuation, on_cancel}) do
    if on_cancel do
      {:automatic, {stack, continuation, Map.put(on_cancel, ref, pid)}}
    else
      {:automatic, {stack, continuation, on_cancel}}
    end
  end

  def handle_cancel(_reason, {_, ref}, {stack, continuation, on_cancel}) do
    case on_cancel do
      %{^ref => _} when map_size(on_cancel) == 1 ->
        {:stop, :normal, {stack, continuation, Map.delete(on_cancel, ref)}}

      %{^ref => _} ->
        {:noreply, [], {stack, continuation, Map.delete(on_cancel, ref)}}

      _ ->
        {:noreply, [], {stack, continuation, on_cancel}}
    end
  end

  def handle_demand(_demand, {stack, continuation, on_cancel}) when is_atom(continuation) do
    {:noreply, [], {stack, continuation, on_cancel}}
  end

  def handle_demand(demand, {stack, continuation, on_cancel}) when demand > 0 do
    case continuation.({:cont, {[], demand}}) do
      {:suspended, {list, 0}, continuation} ->
        {:noreply, :lists.reverse(list), {stack, continuation, on_cancel}}

      {status, {list, _}} ->
        GenStage.async_info(self(), :stop)
        {:noreply, :lists.reverse(list), {stack, status, on_cancel}}
    end
  end

  def handle_info(:stop, state) do
    {:stop, :normal, state}
  end

  def handle_info(msg, {stack, continuation, on_cancel}) do
    log =
      ~c"** Undefined handle_info in ~tp~n** Unhandled message: ~tp~n** Stream started at:~n~ts"

    :error_logger.warning_msg(log, [inspect(__MODULE__), msg, Exception.format_stacktrace(stack)])
    {:noreply, [], {stack, continuation, on_cancel}}
  end
end
