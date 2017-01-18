# GenStage

GenStage is a specification for exchanging events between producers and consumers.

This project currently provides the following functionality:

  * `GenStage` ([docs](https://hexdocs.pm/gen_stage/GenStage.html)) - a behaviour for implementing producer and consumer stages

  * `ConsumerSupervisor` ([docs](https://hexdocs.pm/gen_stage/ConsumerSupervisor.html)) - a supervisor designed for consuming events from GenStage and starting a child process per event

You may also be interested in [the Flow project](https://github.com/elixir-lang/flow) for building computational flows using regular `map`, `reduce` and more that run in parallel on top of GenStage. See documentation for [Flow](https://hexdocs.pm/flow) or [José Valim's keynote at ElixirConf 2016](https://youtu.be/srtMWzyqdp8?t=244) introducing the main concepts behind GenStage and Flow.

## Examples

Examples for using GenStage and ConsumerSupervisor can be found [examples](examples) directory:

  * [ProducerConsumer](examples/producer_consumer.exs) - a simple example of setting up a pipeline of `A -> B -> C` stages and having events flowing through

  * [ConsumerSupervisor](examples/consumer_supervisor.exs) - an example of how to use one or more `ConsumerSupervisor` as a consumer to a producer that works as a counter

  * [GenEvent](examples/gen_event.exs) - an example of how to use `GenStage` to implement a `GenEvent` replacement that leverages concurrency and provides more flexibility regarding buffer size and back-pressure

  * [RateLimiter](examples/rate_limiter.exs) - an example of performing rate limiting in a GenStage pipeline

## Installation

GenStage requires Elixir v1.3. Just add `:gen_stage` to your list of dependencies in mix.exs:

```elixir
def deps do
  [{:gen_stage, "~> 0.11"}]
end
```

## License

Same as Elixir.
