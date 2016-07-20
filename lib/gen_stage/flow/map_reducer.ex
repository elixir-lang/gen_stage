alias Experimental.GenStage

defmodule GenStage.Flow.MapReducer do
  @moduledoc false
  use GenStage

  def init({type, opts, index, trigger, acc, reducer}) do
    partitioned? = match?({GenStage.PartitionDispatcher, _}, opts[:dispatcher])
    consumers = if type == :consumer, do: :none, else: []
    status = %{producers: [], consumers: consumers, done: [], done?: false,
               trigger: trigger, partitioned?: partitioned?, index: index}
    {type, {status, acc.(), reducer}, opts}
  end

  def handle_subscribe(:producer, _, {_, ref}, {status, acc, reducer}) do
    %{producers: producers, done: done} = status
    status = %{status | producers: [ref | producers], done: [ref | done]}
    {:automatic, {status, acc, reducer}}
  end

  def handle_subscribe(:consumer, _, {pid, ref}, {status, acc, reducer}) do
    %{consumers: consumers} = status

    # If partitioned we do not deliver the notification
    # because the partition dispatcher can buffer those.
    case status do
      %{partitioned?: false, done?: true} ->
        Process.send(pid, {:"$gen_consumer", {self(), ref},
                           {:notification, {:producer, :done}}}, [:noconnect])
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
        {done, done?, events, acc} = maybe_notify(status, acc, ref)
        status = %{status | producers: List.delete(producers, ref), done: done, done?: done?}
        {:noreply, events, {status, acc, reducer}}
      consumers == [ref] ->
        {:stop, :normal, {status, acc, reducer}}
      true ->
        status = %{status | consumers: List.delete(consumers, ref)}
        {:noreply, [], {status, acc, reducer}}
    end
  end

  def handle_info({{_, ref}, {:producer, state}}, {status, acc, reducer}) when state in [:halt, :done] do
    {done, done?, events, acc} = maybe_notify(status, acc, ref)
    status = %{status | done: done, done?: done?}
    {:noreply, events, {status, acc, reducer}}
  end
  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  def handle_events(events, _from, {status, acc, reducer}) do
    {events, acc} = reducer.(events, acc)
    {:noreply, events, {status, acc, reducer}}
  end

  defp maybe_notify(%{done: [], done?: true}, acc, _ref) do
    {[], true, [], acc}
  end
  defp maybe_notify(%{done: done, done?: false, trigger: trigger,
                      index: index, consumers: consumers}, acc, ref) do
    case List.delete(done, ref) do
      [] when done != [] ->
        {events, acc} = trigger.(acc, index)
        if is_list(consumers) do
          GenStage.async_notify(self(), {:producer, :done})
        end
        {[], true, events, acc}
      done ->
        {done, false, [], acc}
    end
  end
end
