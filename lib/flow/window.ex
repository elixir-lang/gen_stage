alias Experimental.Flow

defmodule Flow.Window do
  @moduledoc """
  Splits a flow into windows that are materialized at certain triggers.

  Windows allow developers to split unbounded data so we can understand
  incoming data as time progresses. Once a window is created, we can specify
  triggers that allows us to customize when the data accumulated on every
  window is materialized.

  There is currently one window type:

    * Global windows - that's the default window which means all data
      belongs to one single window. In other words, the data is not
      split in any way. The window finishes when all producers notify
      there is no more data

  Windows can be added to a flow via the `Flow.window/2` function.

  ## Message-based triggers

  TODO: Document it will trigger for all windows that have
  changed since the last trigger.

  TODO: Make sure we only emit triggers if data has been added (except
  for done which is always emitted).

  It is also possible to dispatch a trigger by sending a message to
  `self()` with the format of `{:trigger, :keep | :reset, name}`.
  This is useful for custom triggers and timers. One example is to
  send the message when building the accumulator for `reduce/3`.
  If `:reset` is used, every time the accumulator is rebuilt, a new
  message will be sent. If `:keep` is used and a new timer is necessary,
  then `each_state/2` can called after `reduce/3` to resend it.
  """

  defstruct [:type, :trigger, periodically: []]
  @type t :: %Flow.Window{type: type,
                          trigger: {fun(), fun()} | nil,
                          periodically: []}

  @typedoc "The supported window types."
  @type type :: :global

  @typedoc """
  The window indentifier.

  It is `:global` for `:global` windows. An integer for fixed
  and sliding windows and a custom value for session windows.
  """
  @type id :: :global | non_neg_integer() | term()

  @typedoc "The name of the trigger."
  @type trigger :: term

  @typedoc "The operation to perform on the accumulator."
  @type accumulator :: :keep | :reset

  @trigger_operation [:keep, :reset]

  @doc """
  Returns a new global window.
  """
  @spec global :: t
  def global do
    %Flow.Window{type: :global}
  end

  @doc """
  Calculates when to emit a trigger.

  Triggers are calculated per window and are used to temporarily
  halt the window `reduce/3` step allowing the next operations
  to execute before reducing is resumed.

  This function expects the accumulator function, which will be
  invoked at the beginning of every window, and a trigger function
  that receives the current batch of events and its own accumulator.
  The trigger function must return one of the two values:

    * `{:cont, acc}` - the reduce operation should continue as usual.
       `acc` is the trigger state.

    * `{:trigger, name, pre, operation, pos, acc}` - where `name` is the
      trigger `name`, `pre` are the events to be consumed before the trigger,
      the `operation` configures the stage should `:keep` the reduce accumulator
      or `:reset` it. `pos` controls events to be processed after the trigger
      with the `acc` as the new trigger accumulator.

  We recommend looking at the implementation of `trigger_every/3` as
  an example of a custom trigger.
  """
  @spec trigger(t, (() -> acc), ([event], acc -> cont_tuple | trigger_tuple)) :: t
        when cont_tuple: {:cont, acc},
             trigger_tuple: {:trigger, trigger(), pre, accumulator(), pos, acc},
             pre: [event], pos: [event], acc: term(), event: term()
  def trigger(window, acc_fun, trigger_fun) do
    if is_function(acc_fun, 0) do
      add_trigger(window, {acc_fun, trigger_fun})
    else
      raise ArgumentError, "Flow.Window.trigger/3 expects the accumulator to be given as a function"
    end
  end

  @doc """
  A trigger emitted every `count` elements in a window.

  The `keep_or_reset` argument must be one of `:keep` or `:reset`.
  If `:keep`, the state accumulated so far on `reduce/3` will be kept,
  otherwise discarded.

  The trigger will be named `{:every, count}`.

  ## Examples

  Below is an example that checkpoints the sum from 1 to 100, emitting
  a trigger with the state every 10 items. The extra 5050 value at the
  end is the trigger emitted because processing is done.

      iex> window = Flow.Window.global |> Flow.Window.trigger_every(10)
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(stages: 1) |> Flow.window(window)
      iex> flow |> Flow.reduce(fn -> 0 end, & &1 + &2) |> Flow.emit(:state) |> Enum.to_list()
      [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]

  Now let's see an example similar to above except we reset the counter
  on every trigger. At the end, the sum of all values is still 5050:

      iex> window = Flow.Window.global |> Flow.Window.trigger_every(10, :reset)
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(stages: 1) |> Flow.window(window)
      iex> flow |> Flow.reduce(fn -> 0 end, & &1 + &2) |> Flow.emit(:state) |> Enum.to_list()
      [55, 155, 255, 355, 455, 555, 655, 755, 855, 955, 0]

  """
  def trigger_every(window, count, keep_or_reset \\ :keep)
      when count > 0 and keep_or_reset in @trigger_operation do
    name = {:every, count}

    trigger(window, fn -> count end, fn events, acc ->
      length = length(events)
      if length(events) >= acc do
        {pre, pos} = Enum.split(events, acc)
        {:trigger, name, pre, keep_or_reset, pos, count}
      else
        {:cont, acc - length}
      end
    end)
  end

  @doc """
  Emits a trigger periodically every `count` `unit`.

  Such trigger will apply to every window that has changed since the last
  periodic trigger.

  `count` must be a positive integer and `unit` is one of `:microseconds`,
  `:seconds`, `:minutes`, `:hours`. Notice such times are an estimate and
  intrinsically inaccurate as they are based on the processing time.

  The `keep_or_reset` argument must be one of `:keep` or `:reset`. If
  `:keep`, the state accumulate so far on `reduce/3` will be kept, otherwise
  discarded.

  The trigger will be named `{:periodically, count, unit}`.
  """
  def trigger_periodically(%Flow.Window{periodically: periodically} = window,
                           count, unit, keep_or_reset \\ :keep) do
    periodically = [{to_ms(count, unit), keep_or_reset, {:periodically, count, unit}} | periodically]
    %{window | periodically: periodically}
  end

  defp to_ms(count, :microseconds), do: count
  defp to_ms(count, :seconds), do: count * 1000
  defp to_ms(count, :minutes), do: count * 1000 * 60
  defp to_ms(count, :hours), do: count * 1000 * 60 * 60
  defp to_ms(_count, unit), do: raise ArgumentError, "unknown unit #{inspect unit}"

  defp add_trigger(%Flow.Window{trigger: nil} = window, trigger) do
    %{window | trigger: trigger}
  end
  defp add_trigger(%Flow.Window{}, _trigger) do
    raise ArgumentError, "Flow.Window.trigger/3 or Flow.Window.trigger_every/3 " <>
                         "can only be called once per window"
  end
end
