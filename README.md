# GenStage

GenStage is a specification for exchanging events between producers and consumers.

This project currently provides the following functionality:

  * `Experimental.GenStage` ([docs](https://hexdocs.pm/gen_stage/Experimental.GenStage.html)) - a behaviour for implementing producer and consumer stages

  * `Experimental.DynamicSupervisor` ([docs](https://hexdocs.pm/gen_stage/Experimental.DynamicSupervisor.html)) - a supervisor designed for starting children dynamically. Besides being a replacement for the `:simple_one_for_one` strategy in the regular `Supervisor`, a `DynamicSupervisor` can also
  be used as a stage consumer, making it straight-forward to spawn a new process for every
  event in a stage pipeline

The module names are marked as `Experimental` to avoid conflicts as they are meant to be included in future Elixir releases. In your code, you may add `alias Experimental.{DynamicSupervisor, GenStage}` to the top of your files and use the relative names from then on.

You can find examples on how to use the modules above in the [examples](examples) directory:

  * [DymamicSupervisor consumer](examples/dynamic_supervisor_consumer.exs) - an example of how to use one or more `DynamicSupervisor` as a consumer to a producer that works as a counter

  * [GenEvent](examples/gen_event.exs) - an example of how to use `GenStage` to implement a `GenEvent` replacement that leverages concurrency and provides more flexibility regarding buffer size and back-pressure

## Installation

GenStage requires Elixir v1.3.

  1. Add `:gen_stage` to your list of dependencies in mix.exs:

        def deps do
          [{:gen_stage, "~> 0.1"}]
        end

  2. Ensure `:gen_stage` is started before your application:

        def application do
          [applications: [:gen_stage]]
        end

## Future research

Here is a list of potential topics to be explored by this project (in no particular order or guarantee):

  * Provide examples with delivery guarantees and/or checkpoints

  * Consider using DynamicSupervisor to implement Task.Supervisor (as a consumer)

  * TCP and UDP acceptors as producers

  * Ability to attach filtering to producers - today, if we need to filter events from a producer, we need an intermediary process which incurs extra copying

  * Connecting stages across nodes - because there is no guarantee demand is delivered, the demand driven approach in `GenStage` won't perform well in a distributed setup

  * Integration with streams - how the `GenStage` foundation integrates with Elixir streams? In particular, streams composition is still purely functional while stages introduce asynchronicity.

  * Exploit parallelism - how the GenStage foundation can help us implement conveniences like pmap, chunked map, farming and so on. See Patterns for Parallel Programming (1), the Eden project (2) and the skel project (3, 4, 5)

  * Explore different windowing strategies - the ideas behind the Apache Beam project are interesting, specially the mechanism that divides operations between what/where/when/how (6, 7) as well as windowing from the perspective of aggregation (8)

  * Introduce key-based functions - after a `group_by` is performed, there are many operations that can be inlined like `map_by_key`, `reduce_by_key` and so on. The Spark project, for example, provides many functions for key-based functionality (9)

Other research topics include the Naiad's Differential Dataflow engine (10) and Lasp (11).

### Links

  1.  https://www.amazon.com/Patterns-Parallel-Programming-paperback-Paperback/dp/0321940784
  2.  http://www.mathematik.uni-marburg.de/~eden/
  3.  https://github.com/ParaPhrase/skel
  4.  http://paraphrase-ict.eu/Deliverables/d2.5/at_download/file
  5.  http://paraphrase-ict.eu/Deliverables/d2-10-pattern-amenability/at_download/file
  6.  https://cloud.google.com/blog/big-data/2016/05/why-apache-beam-a-google-perspective
  7.  http://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf
  8.  http://www.vldb.org/pvldb/vol8/p702-tangwongsan.pdf
  9.  http://spark.apache.org/docs/latest/programming-guide.html#working-with-key-value-pairs
  10. http://research-srv.microsoft.com/pubs/176693/differentialdataflow.pdf
  11. https://lasp-lang.org/

## License

Same as Elixir.
