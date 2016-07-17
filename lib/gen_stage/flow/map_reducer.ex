alias Experimental.GenStage

defmodule GenStage.Flow.MapReducer do
  @moduledoc false
  use GenStage

  def init({type, opts, index, change, acc, reducer}) do
    partitioned? = match?({GenStage.PartitionDispatcher, _}, opts[:dispatcher])
    status = %{producers: [], consumers: [], done: [], index: index,
               change: change, partitioned?: partitioned?, current: {:window, 0}}
    {type, {status, acc.(), reducer}, opts}
  end

  def handle_subscribe(:producer, _, {_, ref}, {status, acc, reducer}) do
    %{producers: producers, done: done} = status
    status = %{status | producers: [ref | producers], done: [ref | done]}
    {:automatic, {status, acc, reducer}}
  end

  def handle_subscribe(:consumer, _, {pid, ref}, {status, acc, reducer}) do
    %{consumers: consumers} = status

    # If partitioned we do not deliver the current status
    # because the partition dispatcher can buffer those.
    case status do
      %{partitioned?: false, current: current} ->
        Process.send(pid, {:"$gen_consumer", {self(), ref}, {:notification, current}}, [:noconnect])
      %{} ->
        :ok
    end

    status = %{status | consumers: [ref | consumers]}
    {:automatic, {status, acc, reducer}}
  end

  def handle_cancel(_, {_, ref}, {status, acc, reducer}) do
    %{producers: producers, consumers: consumers} = status

    cond do
      ref in producers ->
        {done, current, events, acc} = maybe_notify(status, acc, ref)
        status = %{status | producers: List.delete(producers, ref), done: done, current: current}
        {:noreply, events, {status, acc, reducer}}
      consumers == [ref] ->
        {:stop, :normal, {status, acc, reducer}}
      true ->
        status = %{status | consumers: List.delete(consumers, ref)}
        {:noreply, [], {status, acc, reducer}}
    end
  end

  def handle_info({{_, ref}, {:producer, _}}, {status, acc, reducer}) do
    {done, current, events, acc} = maybe_notify(status, acc, ref)
    status = %{status | done: done, current: current}
    {:noreply, events, {status, acc, reducer}}
  end
  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  def handle_events(events, _from, {status, acc, reducer}) do
    {events, acc} = reducer.(events, acc)
    {:noreply, events, {status, acc, reducer}}
  end

  defp maybe_notify(%{done: done, current: {:producer, _} = current}, acc, _ref) do
    {done, current, [], acc}
  end
  defp maybe_notify(%{done: done, current: current, change: change, index: index}, acc, ref) do
    case List.delete(done, ref) do
      [] when done != [] ->
        current = {:producer, :done}
        {events, acc} = change.(current, acc, index)
        {[], current, events, acc}
      done ->
        {done, current, [], acc}
    end
  end
end
