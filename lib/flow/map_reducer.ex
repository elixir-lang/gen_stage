alias Experimental.{GenStage, Flow}

defmodule Flow.MapReducer do
  @moduledoc false
  use GenStage

  def init({type, opts, index, trigger, acc, reducer}) do
    consumers = if type == :consumer, do: :none, else: []
    status = %{consumers: consumers, done: [], done?: false, trigger: trigger}
    {type, {%{}, status, index, acc.(), reducer}, opts}
  end

  def handle_subscribe(:producer, opts, {_, ref}, {tags, status, index, acc, reducer}) do
    tags = Map.put(tags, ref, opts[:tag])
    status = update_in status.done, &[ref | &1]
    {:automatic, {tags, status, index, acc, reducer}}
  end

  def handle_subscribe(:consumer, _, {_, ref}, {tags, status, index, acc, reducer}) do
    %{consumers: consumers} = status
    status = %{status | consumers: [ref | consumers]}
    {:automatic, {tags, status, index, acc, reducer}}
  end

  def handle_cancel(_, {_, ref}, {tags, status, index, acc, reducer}) do
    %{consumers: consumers} = status

    cond do
      Map.has_key?(tags, ref) ->
        {events, acc, done, done?} = maybe_done(status, index, acc, ref)
        status = %{status | done: done, done?: done?}
        {:noreply, events, {Map.delete(tags, ref), status, index, acc, reducer}}
      consumers == [ref] ->
        {:stop, :normal, {tags, status, index, acc, reducer}}
      true ->
        status = %{status | consumers: List.delete(consumers, ref)}
        {:noreply, [], {tags, status, index, acc, reducer}}
    end
  end

  def handle_info({:trigger, keep_or_reset, name}, {tags, status, index, acc, reducer}) do
    %{trigger: trigger} = status
    {events, acc} = trigger.(acc, index, keep_or_reset, name)
    {:noreply, events, {tags, status, index, acc, reducer}}
  end
  def handle_info({{_, ref}, {:producer, state}}, {tags, status, index, acc, reducer}) when state in [:halted, :done] do
    {events, acc, done, done?} = maybe_done(status, index, acc, ref)
    status = %{status | done: done, done?: done?}
    {:noreply, events, {tags, status, index, acc, reducer}}
  end
  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  def handle_events(events, _from, {tags, status, index, acc, reducer}) when is_function(reducer, 3) do
    {events, acc} = reducer.(events, acc, index)
    {:noreply, events, {tags, status, index, acc, reducer}}
  end
  def handle_events(events, {_, ref}, {tags, status, index, acc, reducer}) when is_function(reducer, 4) do
    {events, acc} = reducer.(Map.get(tags, ref), events, acc, index)
    {:noreply, events, {tags, status, index, acc, reducer}}
  end

  defp maybe_done(%{done: [], done?: true}, _index, acc, _ref) do
    {[], acc, [], true}
  end
  defp maybe_done(%{done: done, done?: false, trigger: trigger, consumers: consumers},
                    index, acc, ref) do
    case List.delete(done, ref) do
      [] when done != [] ->
        {events, acc} = trigger.(acc, index, :keep, :done)
        if is_list(consumers) do
          GenStage.async_notify(self(), {:producer, :done})
        end
        {events, acc, [], true}
      done ->
        {[], acc, done, false}
    end
  end
end
