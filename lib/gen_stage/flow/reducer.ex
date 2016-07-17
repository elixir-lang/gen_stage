alias Experimental.GenStage

defmodule GenStage.Flow.Reducer do
  @moduledoc false
  use GenStage

  def init({reducer, opts}) do
    {:producer_consumer, {[], [], [], :ets.new(:reducer, [:private, :set]), reducer}, opts}
  end

  def handle_subscribe(:producer, _, {_, ref}, {producers, consumers, done, acc, reducer}) do
    {:automatic, {[ref | producers], consumers, [ref | done], acc, reducer}}
  end
  def handle_subscribe(:consumer, _, {pid, ref}, {producers, consumers, done, acc, reducer}) do
    # if is_atom(reducer) do
    #   # TODO: should not send this message for partition dispatcher as it buffers them (test me)
    #   msg = {:producer, reducer}
    #   Process.send(pid, {:"$gen_consumer", {self(), ref}, {:notification, msg}}, [:noconnect])
    # end
    {:automatic, {producers, [ref | consumers], done, acc, reducer}}
  end

  def handle_cancel(_, {_, ref}, {producers, consumers, done, acc, reducer} = state) do
    cond do
      ref in producers ->
        {done, reducer} = maybe_notify(done, acc, reducer, ref)
        {:noreply, [], {List.delete(producers, ref), consumers, done, acc, reducer}}
      consumers == [ref] ->
        {:stop, :normal, state}
      true ->
        {:noreply, [], {producers, List.delete(consumers, ref), done, acc, reducer}}
    end
  end

  def handle_info({{_, ref}, {:producer, _}}, {producers, consumers, done, acc, reducer}) do
    {done, reducer} = maybe_notify(done, acc, reducer, ref)
    {:noreply, [], {producers, consumers, done, acc, reducer}}
  end
  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  def handle_events(_events, _from, {_, _, _, _, :done} = state) do
    {:noreply, [], state}
  end
  def handle_events(events, _from, {producers, consumers, done, acc, reducer}) do
    _ = for event <- events do
      {key, new} = event
      case :ets.lookup(acc, key) do
        [{^key, old}] -> :ets.insert(acc, {key, reducer.(old, new)})
        [] -> :ets.insert(acc, event)
      end
    end
    {:noreply, [], {producers, consumers, done, acc, reducer}}
  end

  defp maybe_notify(done, _, :done, _ref) do
    {done, :done}
  end

  defp maybe_notify(done, acc, reducer, ref) do
    case List.delete(done, ref) do
      [] when done != [] ->
        GenStage.async_notify(self(), {:stream, :ets.tab2list(acc)})
        GenStage.async_notify(self(), {:producer, :done})
        {[], :done}
      done ->
        {done, reducer}
    end
  end
end
