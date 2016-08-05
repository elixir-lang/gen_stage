alias Experimental.Flow

defmodule Flow.Window.Fixed do
  @moduledoc false

  defstruct [:by, :duration, :trigger, lateness: 0, periodically: []]

  def materialize(%{by: by, duration: duration, lateness: lateness},
                  reducer_acc, reducer_fun, reducer_trigger) do
    ref = make_ref()
    acc = fn -> {nil, %{}} end
    lateness_fun = lateness_fun(lateness, ref, reducer_acc, reducer_trigger)

    fun =
      fn events, old_acc, index ->
        {reducer_emit, new_acc} =
          split_events(events, [], nil, by, duration, old_acc, index, reducer_acc, reducer_fun, [])
        {trigger_emit, new_acc} =
          emit_trigger_messages(ref, old_acc, new_acc, index, lateness_fun)
        {reducer_emit ++ trigger_emit, new_acc}
      end

    trigger =
      fn acc, index, op, name ->
        handle_trigger(ref, acc, index, op, name, reducer_trigger)
      end

    {acc, fun, trigger}
  end

  ## Reducer

  defp split_events([event | events], buffer, current, by, duration,
                    acc, index, reducer_acc, reducer_fun, emit) do
    window = div(by!(by, event), duration)
    if is_nil(current) or window === current do
      split_events(events, [event | buffer], window, by, duration,
                   acc, index, reducer_acc, reducer_fun, emit)
    else
      {emit, acc} = reduce_events(buffer, current, acc, index, reducer_acc, reducer_fun, emit)
      split_events(events, [event], window, by, duration,
                   acc, index, reducer_acc, reducer_fun, emit)
    end
  end
  defp split_events([], buffer, window, _by, _duration, acc, index, reducer_acc, reducer_fun, emit) do
    reduce_events(buffer, window, acc, index, reducer_acc, reducer_fun, emit)
  end

  defp reduce_events([], _window, acc, _index, _reducer_acc, _reducer_fun, emit) do
    {emit, acc}
  end
  defp reduce_events(buffer, window, {current, windows}, index, reducer_acc, reducer_fun, emit) do
    events = :lists.reverse(buffer)

    case recent_window(window, current, windows, reducer_acc) do
      {:ok, window_acc, current} ->
        {new_emit, window_acc} =
          if is_function(reducer_fun, 3) do
            reducer_fun.(events, window_acc, index)
          else
            reducer_fun.(events, window_acc, index, {:fixed, window, :placeholder})
          end

        {emit ++ new_emit, {current, Map.put(windows, window, window_acc)}}
      :error ->
        {emit, {current, windows}}
    end
  end

  defp recent_window(window, current, windows, reducer_acc) do
    case windows do
      %{^window => acc} -> {:ok, acc, max(window, current)}
      %{} when is_nil(current) or window >= current -> {:ok, reducer_acc.(), window}
      %{} -> :error
    end
  end

  defp by!(by, event) do
    case by.(event) do
      x when is_integer(x) -> x
      x -> raise "Flow.Window.fixed/3 expects `by` function to return an integer, " <>
                 "got #{inspect x} from #{inspect by}"
    end
  end

  ## Trigger emission

  defp emit_trigger_messages(ref, {nil, _}, {new, windows}, index, lateness) do
    emit_trigger_messages(ref, Enum.min(Map.keys(windows)), new, windows, index, lateness, [])
  end
  defp emit_trigger_messages(ref, {old, _}, {new, windows}, index, lateness) do
    emit_trigger_messages(ref, old, new, windows, index, lateness, [])
  end

  defp emit_trigger_messages(_ref, new, new, windows, _index, _lateness, emit) do
    {emit, {new, windows}}
  end
  defp emit_trigger_messages(ref, old, new, windows, index, lateness, emit) do
    {new_emit, windows} = lateness.(old, windows, index)
    emit_trigger_messages(ref, old + 1, new, windows, index, lateness, emit ++ new_emit)
  end

  defp lateness_fun(0, _ref, reducer_acc, reducer_trigger) do
    fn window, windows, index ->
      acc = Map.get_lazy(windows, window, reducer_acc)
      {emit, _} = reducer_trigger.(acc, index, :keep, {:fixed, window, :done})
      {emit, Map.delete(windows, window)}
    end
  end
  defp lateness_fun(lateness, ref, reducer_acc, _reducer_trigger) do
    fn window, windows, _index ->
      Process.send_after(self(), {:trigger, :keep, {ref, window}}, lateness)
      {[], Map.put_new_lazy(windows, window, reducer_acc)}
    end
  end

  ## Trigger handling

  # A particular window was triggered for termination.
  def handle_trigger(ref, {current, windows}, index, op, {ref, window}, trigger) do
    case windows do
      %{^window => acc} ->
        {emit, _window_acc} = trigger.(acc, index, op, {:fixed, window, :done})
        {emit, {current, Map.delete(windows, window)}}
      %{} ->
        {[], {current, windows}}
    end
  end

  # Trigger all windows
  def handle_trigger(_ref, {current, windows}, index, op, name, trigger) do
    {emit, windows} = trigger_all(Enum.sort(Map.to_list(windows)), %{}, index, op, name, trigger, [])
    {emit, {current, windows}}
  end

  defp trigger_all([{window, acc} | pairs], windows, index, op, name, trigger, emit) do
    {new_emit, acc} = trigger.(acc, index, op, {:fixed, window, name})
    trigger_all(pairs, Map.put(windows, window, acc), index, op, name, trigger, emit ++ new_emit)
  end
  defp trigger_all([], windows, _index, _op, _name, _trigger, emit) do
    {emit, windows}
  end
end
