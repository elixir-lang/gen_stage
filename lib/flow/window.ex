alias Experimental.Flow

defmodule Flow.Window do
  @moduledoc """
  Splits a flow into windows that are materialized at certain triggers.

  Windows allow developers to split data so we can understand incoming
  data as time progresses. Once a window is created, we can specify
  triggers that allow us to customize when the data accumulated on every
  window is materialized.

  Windows must be created by calling one of the window type functions.
  There are currently two window types:

    * Global windows - that's the default window which means all data
      belongs to one single window. In other words, the data is not
      split in any way. The window finishes when all producers notify
      there is no more data

    * Fixed windows - splits the incoming events into periodic, non-
      overlaping windows. In other words, a given event belongs to a
      single window. If data arrives late, a configured lateness can
      be specified.

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
      |> Flow.partition(Flow.Window.global())
      |> Flow.reduce(fn -> 0 end, & &1 + 2)

  Even though the global window does not split the data in any way, it
  already provides conveniences for working with both bounded (finite)
  and unbounded (infinite) via triggers.

  For example, the flow below uses a global window with a count-based
  trigger to emit the values being summed as we sum them:

      iex> window = Flow.Window.global |> Flow.Window.trigger_every(10)
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(window: window, stages: 1)
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
  resetting the reducer accumulator (`:reset`) or keeping it as is (`:keep`).
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

  ## Fixed windows

  Non-global windows allow us to group the data based on the event times.
  Regardless if the data is bounded or not, fixed windows allows us to
  gather time-based insight about the data.

  Fixed windows are created via the `fixed/3` function which specified
  the duration of the window and a function that retrieves the event time
  from each event:

      Flow.Window.fixed(1, :hours, fn {word, timestamp} -> timestamp end)

  Let's see example that will use the window above to count the frequency
  of words based on windows that are 1 hour long. The timestamps used by
  Flow are integers in milliseconds. For now we will also set the concurrency
  down 1 and max demand down to 5 as it is simpler to reason about the results:

      iex> data = [{"elixir", 0}, {"elixir", 1_000}, {"erlang", 60_000},
      ...>         {"concurrency", 3_200_000}, {"elixir", 4_000_000},
      ...>         {"erlang", 5_000_000}, {"erlang", 6_000_000}]
      iex> window = Flow.Window.fixed(1, :hours, fn {_word, timestamp} -> timestamp end)
      iex> flow = Flow.from_enumerable(data, max_demand: 5, stages: 1)
      iex> flow = Flow.partition(flow, window: window, stages: 1)
      iex> flow = Flow.reduce(flow, fn -> %{} end, fn {word, _}, acc ->
      ...>   Map.update(acc, word, 1, & &1 + 1)
      ...> end)
      iex> flow |> Flow.emit(:state) |> Enum.to_list
      [%{"elixir" => 2, "erlang" => 1, "concurrency" => 1},
       %{"elixir" => 1, "erlang" => 2}]

  Since the data has been broken in two windows, the first four events belong
  to the same window while the last 3 belongs to the second one. Notice that
  `reduce/3` is executed per window and that each event belongs to a single
  window exclusively.

  Similar to global windows, fixed windows can also have triggers, allowing
  us to checkpoint the data as the computation happens.

  ### Data ordering, watermarks and lateness

  When working with event time, Flow assumes by default that events are time
  ordered. This means that, when we move from one window to another, for
  example when we received the entry `{"elixir", 4_000_000}` in the example
  above, we assume the previous window has completed. We call this the
  **watermark trigger**. Let's change the events above to be out of order.
  We will get the first event, put it last, and see which results will be
  emitted:

      iex> data = [{"elixir", 1_000}, {"erlang", 60_000},
      ...>         {"concurrency", 3_200_000}, {"elixir", 4_000_000},
      ...>         {"erlang", 5_000_000}, {"erlang", 6_000_000}, {"elixir", 0}]
      iex> window = Flow.Window.fixed(1, :hours, fn {_word, timestamp} -> timestamp end)
      iex> flow = Flow.from_enumerable(data) |> Flow.partition(window: window, stages: 1, max_demand: 5)
      iex> flow = Flow.reduce(flow, fn -> %{} end, fn {word, _}, acc ->
      ...>   Map.update(acc, word, 1, & &1 + 1)
      ...> end)
      iex> flow |> Flow.emit(:state) |> Enum.to_list
      [%{"elixir" => 1, "erlang" => 1, "concurrency" => 1},
       %{"elixir" => 1, "erlang" => 2}]

  Notice that now the first map did not count the "elixir" word twice.
  Since the event arrived late, it was marked as lost. However, in many
  flows we actually expect data to arrive late, especially when talking
  about concurrent data processing.

  Luckily fixed windows include the concept of lateness, which is a
  processing time base period we would wait to receive late events.
  Let's change the example above once more but now change the window
  to include the allowed_lateness parameter:

      iex> data = [{"elixir", 1_000}, {"erlang", 60_000},
      ...>         {"concurrency", 3_200_000}, {"elixir", 4_000_000},
      ...>         {"erlang", 5_000_000}, {"erlang", 6_000_000}, {"elixir", 0}]
      iex> window = Flow.Window.fixed(1, :hours, fn {_word, timestamp} -> timestamp end)
      iex> window = Flow.Window.allowed_lateness(window, 5, :minutes)
      iex> flow = Flow.from_enumerable(data) |> Flow.partition(window: window, stages: 1, max_demand: 5)
      iex> flow = Flow.reduce(flow, fn -> %{} end, fn {word, _}, acc ->
      ...>   Map.update(acc, word, 1, & &1 + 1)
      ...> end)
      iex> flow |> Flow.emit(:state) |> Enum.to_list
      [%{"concurrency" => 1, "elixir" => 1, "erlang" => 1},
       %{"concurrency" => 1, "elixir" => 2, "erlang" => 1},
       %{"elixir" => 1, "erlang" => 2}]

  Now that we allow late events, we can see the first window emitted
  twice: once at watermark and another when the collection is effectively
  done. If desired, we can use `Flow.map_state/2` to get more information
  about each particular window. Replace the last line above by the following:

      flow = flow |> Flow.map_state(fn state, _index, trigger -> {state, trigger} end)
      flow = flow |> Flow.emit(:state) |> Enum.to_list()

  The trigger parameter will include the type of window, the current
  window and what caused the window to be emitted (`:watermark` or
  `:done`).
  """

  @type t :: %{required(:trigger) => {fun(), fun()} | nil,
               required(:periodically) => []}

  @typedoc "The supported window types."
  @type type :: :global | :fixed | any()

  @typedoc """
  A function that retrieves the field to window by.

  It must be an integer representing the time in milliseconds.
  Flow does not care if the integer is using the UNIX epoch,
  Gregorian epoch or any other as long as it is consistent.
  """
  @type by :: (term -> non_neg_integer)

  @typedoc """
  The window indentifier.

  It is `:global` for `:global` windows. An integer for fixed
  windows and a custom value for session windows.
  """
  @type id :: :global | non_neg_integer() | term()

  @typedoc "The name of the trigger."
  @type trigger :: term

  @typedoc "The operation to perform on the accumulator."
  @type accumulator :: :keep | :reset

  @trigger_operation [:keep, :reset]

  @doc """
  Returns a global window.
  """
  @spec global :: t
  def global do
    %Flow.Window.Global{}
  end

  @doc """
  Returns a fixed window of duration `count` `unit` where the
  event time is calculated by the given function `by`.
  """
  def fixed(count, unit, by) when is_function(by, 1) do
    %Flow.Window.Fixed{duration: to_ms(count, unit), by: by}
  end

  @doc """
  Sets a duration, in processing time, of how long we will
  wait for late events for a given window.
  """
  def allowed_lateness(%{lateness: _} = window, count, unit) do
    %{window | lateness: to_ms(count, unit)}
  end
  def allowed_lateness(window, _, _) do
    raise ArgumentError, "allowed_lateness/3 not supported for window type #{inspect window}"
  end

  @doc """
  Calculates when to emit a trigger.

  Triggers are calculated per window and are used to temporarily
  halt the window accumulation, typically done with `Flow.reduce/3`,
  allowing the next operations to execute before accumulation is
  resumed.

  This function expects the trigger accumulator function, which will
  be invoked at the beginning of every window, and a trigger function
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
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(window: window, stages: 1)
      iex> flow |> Flow.reduce(fn -> 0 end, & &1 + &2) |> Flow.emit(:state) |> Enum.to_list()
      [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]

  Now let's see an example similar to above except we reset the counter
  on every trigger. At the end, the sum of all values is still 5050:

      iex> window = Flow.Window.global |> Flow.Window.trigger_every(10, :reset)
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(window: window, stages: 1)
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

  `count` must be a positive integer and `unit` is one of `:milliseconds`,
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
  def trigger_periodically(%{periodically: periodically} = window,
                           count, unit, keep_or_reset \\ :keep) do
    periodically = [{to_ms(count, unit), keep_or_reset, {:periodically, count, unit}} | periodically]
    %{window | periodically: periodically}
  end

  defp to_ms(count, :milliseconds), do: count
  defp to_ms(count, :seconds), do: count * 1000
  defp to_ms(count, :minutes), do: count * 1000 * 60
  defp to_ms(count, :hours), do: count * 1000 * 60 * 60
  defp to_ms(_count, unit), do: raise ArgumentError, "unknown unit #{inspect unit}"

  defp add_trigger(%{trigger: nil} = window, trigger) do
    %{window | trigger: trigger}
  end
  defp add_trigger(%{}, _trigger) do
    raise ArgumentError, "Flow.Window.trigger/3 or Flow.Window.trigger_every/3 " <>
                         "can only be called once per window"
  end
end
