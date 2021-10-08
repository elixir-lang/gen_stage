# Changelog

## v1.1.2 (2021-10-08)

### Enhancements

  * Support `:shuffle_demands_on_first_dispatch` on `GenStage.DemandDispatcher`

## v1.1.1 (2021-08-13)

### Enhancements

  * Log a clear error messages when stage terminates due to noproc on non-temporary subscription

## v1.1.0 (2021-02-05)

v1.1 requires Elixir v1.7+.

### Enhancements

  * Fix warnings on latest Elixir versions
  * Support more process specifications in the `:subscribe_to` option
  * Add callback to allow instrumenting discarded count and function to get estimated buffer size

## v1.0.0 (2020-02-03)

### Enhancements

  * Allow events to be discarded in PartitionDispatcher by returning `:none`
  * Raise for unknown partitionis in PartitionDispatcher

## v0.14.3 (2019-10-28)

### Enhancements

  * Improvements to typespecs and error messages

## v0.14.2 (2019-06-12)

### Enhancements

  * Add `GenStage.demand/1`

### Bug fixes

  * Fix code_change callback implementation

## v0.14.1 (2018-10-08)

### Bug fixes

  * Fix warnings on Elixir v1.8

## v0.14.0 (2018-06-10)

This version requires Elixir v1.5+.

### Bug fixes

  * Ensure a `:producer_consumer` stops asking the producer if downstream demand is 0
  * Enforce the :hash option for non-int partitions in PartitionDispatcher

## v0.13.1 (2018-01-28)

Note: this is the last version to support Elixir v1.3 and v1.4.

### Enhancements

  * Log error on unknown partitions

### Bug fixes

  * Do not allow restart: :permanent in `ConsumerSupervisor` to avoid common pitfalls
  * Improve and fix types to avoid dialyzer warnings
  * Avoid conflict with user specified `@opts`

## v0.13.0 (2018-01-13)

### Enhancements

  * Mirror `ConsumerSupervisor.init/2` after `Supervisor.init/2`
  * No longer define default implementations for callbacks, instead declare them as `@optional_callbacks`

### Bug fixes

  * Ensure `ConsumerSupervisor` does not send demand when restarting a child

## v0.12.2

### Enhancements

  * Support Elixir v1.5 supervisor childspecs in ConsumerSupervisor
  * Mark `GenStage.child_spec/1` overridable

## v0.12.1

### Enhancements

  * Define Elixir v1.5 childspecs in GenStage and ConsumerSupervisor

### Bug fixes

  * Fix a bug where info messages would be sent out of order in producer consumers
  * Fix a bug where handle_cancel would be invoked out of order in producer consumers

## v0.12.0

### Enhancements

  * Add `cancel: :transient` to subscription options which does terminate if the exit is `:normal`, `:shutdown`, or `{:shutdown, _}`
  * Add `GenStage.sync_info/3` and `GenStage.async_info/2` which queues an information message to be delivered once the current queue is consumed

### Backwards incompatible changes

  * Remove `:max_dynamic` from ConsumerSupervisor
  * The notification mechanism has been removed from GenStage. For termination, GenStage now uses proper exit signals and `cancel: :transient` has been added as a subscription option.

## v0.11.0

### Backwards incompatible changes

  * Remove the Experimental namespace
  * Rename DynamicSupervisor to ConsumerSupervisor
  * Move Flow to a separate project: https://github.com/elixir-lang/flow

Except by the module name changes, all APIs remain exactly the same.

### Bug fixes

  * Accumulate demands but don't sum them together. This provides a better ramp up time for producers with multiple consumers

## v0.10.0

### Enhancements

  * Add `Flow.group_by/3` and `Flow.group_by_key/3` as conveniences around `Flow.reduce/3`
  * Add `Flow.map_values/2` for mapping over the values in a key-value based state
  * Add `Flow.take_sort/3` that efficiently sorts and takes the top N entries

### Bug fixes

  * Ensure BroadcastDispatcher sends demand to itself when custom selector discards events
  * Ensure flows started with `Flow.start_link/2` properly terminate if producers terminate
  * Ensure flows exit locally instead of relying on linked processes exits. With this change, `Flow.run(flow)` and `Enum.to_list(flow)` no longer start stages linked directly to the caller but does so through a supervisor

## v0.9.0

### Enhancements

  * Add `GenStage.sync_resubscribe/4` and `GenStage.async_resubscribe/4`
  * Improve logs, specs and docs

### Bug fixes

  * Ensure `Flow.departition/4` works on `Flow.start_link/1`
  * Make sure no lingering monitors or messages on the inbox after GenStage.stream/1

## v0.8.0

### Enhancements

  * Support a `:selector` option in the `BroadcastDispatcher`

### Bug fix

  * Ensure PartitionDispatcher does not create more partitions than necessary

### Backwards incompatible changes

  * Pass the events `length` to dispatchers for more performant dispatching

## v0.7.0

### Enhancements

  * Introduce count-based windows, process-time windows and session-based windows on Flow
  * Support resetting or keeping buffer on Flow watermarks

### Backwards incompatible changes

  * Remove `:milliseconds`, `:seconds`, `:minutes` and `:hours` for units in favor of `:millisecond`, `:second`, `:minute` and `:hour`. You will get an error if you use the previous values.
  * Specifying shortcuts to `:hash` has been removed in favor of the `:key` option. You will get an error if you use the previous values.

### Bug fixes

  * Ensure uneven partitions emit all windows on `Flow.departition/4`
  * Properly emit the beginning of the window time on triggers for fixed windows

## v0.6.1 (2016-10-05)

### Bug fixes

  * Properly count the most recent entry for each fixed window

## v0.6.0 (2016-10-04)

### Enhancements

  * Introduce `Flow.departition/5`
  * Include examples of broadcasters and rate limiters in the documentation
  * Allow custom-named, non-integer partitions

### Bug fixes

  * Ensure consumer supervisor respects `min_demand` and does not send demand too soon

### Backwards incompatible changes

  * Remove `Flow.new/0`, `Flow.new/1` and `Flow.new/2` in favor of passing options to `from_enumerable/2` and `from_stage/2`
  * Remove `Flow.partition/3` and `Flow.merge/3` in favor of passing the `:window` option to `Flow.partition/2` and `Flow.merge/2`

## v0.5.0 (2016-08-09)

This release moves `Flow` from under the `GenStage` namespace and into `Experimental.Flow`.

### Enhancements

  * Add `Flow.uniq/2` and `Flow.uniq_by/2`
  * Add `Flow.start_link/2` and `Flow.into_stages/3`
  * Add `Flow.window_join/8`
  * Unify window and partition APIs
  * Support `Flow.Window.global/0` and `Flow.Window.fixed/3`

## v0.4.3 (2016-07-28)

### Enhancements

  * Add `Flow.inner_join/6`
  * Add `GenStage.demand/2` that allows a producer to accumulate demand as a synchronization mechanism
  * Improve performance for the partition dispatcher and allow it to change the partitioned event

## v0.4.2 (2016-07-25)

### Bug fixes

  * Fix a bug where a flow wouldn't terminate if a source stream halts

## v0.4.1 (2016-07-21)

### Enhancements

  * Add `Flow.trigger/3` and `Flow.trigger_every/4` supporting custom, count and processing-time triggers. Event-time triggers can be implemented via `trigger/3`. Event-time triggers will be added once windows support is included

## v0.4.0 (2016-07-19)

### Enhancements

  * Introduce `Flow` with enumerable/stream based operations
  * Include more information on `:sys.get_status/1` calls for GenStage

### Bug fixes

  * Fix a bug where a `:producer_consumer` stage which filtered events would eventually halt
  * Fix `format_status/2` results when inspecting GenStage in `:observer`

## v0.3.0 (2016-07-12)

### Enhancements

  * Support notifications
  * Introduce `GenStage.stream/1` to stream events as a consumer from a stage
  * Introduce `GenStage.from_enumerable/2` to start a producer stage that emits events from an enumerable (or a stream)

## v0.2.1 (2016-07-08)

### Enhancements

  * Add `GenStage.PartitionDispatcher`
  * Set default `:max_demand` to 1000
  * Use buffer based `:producer_consumer` to avoid escalating demand

## v0.2.0 (2016-07-05)

### Enhancements

  * Support `:producer_consumer` type
  * Support `:infinity` as `:buffer_size` (useful for `:producer_consumer`)

### Backwards incompatible changes

  * Namespace all modules under `Experimental`
  * Ensure `:cancel` reason does not cascade through the pipeline

## v0.1.0 (2016-07-03)

### Enhancements

  * Include GenStage with `:producer` and `:consumer` types
  * Include ConsumerSupervisor implemented as a `GenStage` consumer and that provides the `:simple_one_for_one` functionality
