alias Experimental.Flow

defmodule Flow.Window do
  @moduledoc """
  Splits a flow into windows that are materialized at certain triggers.

  Windows allow developers to split data so we can understand incoming
  data as time progresses. Once a window is created, we can specify
  triggers that allow us to customize when the data accumulated on every
  window is materialized.

  Windows must be created by calling one of the window type functions.
  There is currently one window type:

    * Global windows - that's the default window which means all data
      belongs to one single window. In other words, the data is not
      split in any way. The window finishes when all producers notify
      there is no more data

  We discuss all types and include examples below. In the first section,
  "Global windows", we build the basic intuition about windows and triggers
  and explore more complex types afterwards.

  ## Global windows

  By default, all events belongs to the global window. The global window
  is automatically attached to a partition if no window is specified.
  The flow below:

      Flow.from_stage(some_producer)
      |> Flow.partition()
      |> Flow.reduce(fn -> 0 end, & &1 + 2)

  is equivalent to:

      Flow.from_stage(some_producer)
      |> Flow.partition()
      |> Flow.window(Flow.Window.global())
      |> Flow.reduce(fn -> 0 end, & &1 + 2)

  Even though the global window does not split the data in any way, it
  already provides conveniences for working with both bounded (finite)
  and unbounded (infinite) via triggers.

  For example, the flow below uses a global window with a count-based
  trigger to emit the values being summed as we sum them:

      iex> window = Flow.Window.global |> Flow.Window.trigger_every(10)
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(stages: 1) |> Flow.window(window)
      iex> flow |> Flow.reduce(fn -> 0 end, & &1 + &2) |> Flow.emit(:state) |> Enum.to_list()
      [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]

  Let's explore the types of triggers available next.

  ### Triggers

  Triggers allow us to check point the data processed so far. There
  are different triggers we can use:

    * Event count triggers - compute state operations every X events

    * Processing time triggers - compute state operations every X time
      units for every stage

    * Punctuation - hand-written triggers based on the data

  Flow supports the triggers above via the `trigger_every/3`,
  `trigger_periodically/4` and `trigger/3` respectively.

  Once a trigger is emitted, the `reduce/3` step halts and invokes
  the remaining steps for that flow such as `map_state/2` or any other
  call after `reduce/3`. Triggers are also named and the trigger names
  will be sent alongside the window name as third argument to the callback
  given to `map_state/2` and `each_state/2`.

  For every emitted trigger, developers have the choice of either
  reseting the reducer accumulator (`:reset`) or keeping it as is (`:keep`).
  The resetting option is useful when you are interested only on intermediate
  results, usually because another step is aggregating the data. Keeping the
  accumulator is the default and used to checkpoint the values while still
  working towards an end result.

  Before we move to other window types, it is important to discuss
  the distinction between event time and processing time. In particular,
  triggers created with the `trigger_periodically/4` function are
  intrinsically innacurate and therefore should not be used to split the
  data. Periodic triggers are established per partition, which means
  partitions will emit the triggers at different times. However, it is
  exactly this lack of precision which makes them efficient for checkpointing
  data.

  Flow provides other window types exactly to address the issues with
  processing time. Windows use the event time which is based on the data
  itself. When working with event time, we can assign the data into proper
  windows even when late or out of order. Such windows can be used to gather
  time-based insight from the data (for example, the most popular hashtags
  in the last 10 minutes) as well as for checkpointing data.

  ## More window types

  TODO.
  """

  defstruct [:type, :trigger, periodically: []]
  @type t :: %Flow.Window{type: type,
                          trigger: {fun(), fun()} | nil,
                          periodically: []}

  @typedoc "The supported window types."
  @type type :: :global

  # TODO: Make sure message-based triggers will trigger all windows
  # that have changed since the last message-based trigger
  # (except :done that always triggers).

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

  ## Message-based triggers (timers)

  It is also possible to dispatch a trigger by sending a message to
  `self()` with the format of `{:trigger, :keep | :reset, name}`.
  This is useful for custom triggers and timers. One example is to
  send the message when building the accumulator for `reduce/3`.
  If `:reset` is used, every time the accumulator is rebuilt, a new
  message will be sent. If `:keep` is used and a new timer is necessary,
  then `each_state/2` can be called after `reduce/3` to resend it.

  Similar to periodic triggers, message-based triggers will also be
  invoked to all windows that have changed since the last trigger.
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
