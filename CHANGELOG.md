# Changelog

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
  * Require subscriptions to be explicitly acked by producers
  * Ensure `:cancel` reason does not cascade through the pipeline

## v0.1.0 (2016-07-03)

### Enhancements

  * Include GenStage with `:producer` and `:consumer` types
  * Include DynamicSupervisor implemented as a `GenStage` consumer and that provides the `:simple_one_for_one` functionality
