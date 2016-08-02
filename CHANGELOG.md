# Changelog

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
  * Include DynamicSupervisor implemented as a `GenStage` consumer and that provides the `:simple_one_for_one` functionality
