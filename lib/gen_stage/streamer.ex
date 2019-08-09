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

    {:producer, {stack, continuation}, Keyword.take(opts, [:dispatcher, :demand])}
  end

  def handle_demand(_demand, {stack, continuation}) when is_atom(continuation) do
    {:noreply, [], {stack, continuation}}
  end

  def handle_demand(demand, {stack, continuation}) when demand > 0 do
    case continuation.({:cont, {[], demand}}) do
      {:suspended, {list, 0}, continuation} ->
        {:noreply, :lists.reverse(list), {stack, continuation}}

      {status, {list, _}} ->
        GenStage.async_info(self(), :stop)
        {:noreply, :lists.reverse(list), {stack, status}}
    end
  end

  def handle_info(:stop, state) do
    {:stop, :normal, state}
  end

  def handle_info(msg, {stack, continuation}) do
    log = '** Undefined handle_info in ~tp~n** Unhandled message: ~tp~n** Stream started at:~n~ts'
    :error_logger.warning_msg(log, [inspect(__MODULE__), msg, Exception.format_stacktrace(stack)])
    {:noreply, [], {stack, continuation}}
  end
end
