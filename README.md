# GenBroker

Coming soon.

## Installation

GenBroker requires Erlang 18 and Elixir v1.2.

  1. Add `:gen_stage` to your list of dependencies in mix.exs:

        def deps do
          [{:gen_stage, "~> 0.0.1"}]
        end

  2. Ensure `:gen_stage` is started before your application:

        def application do
          [applications: [:gen_stage]]
        end

## Further topics

Here is a list of potential topics to be explored by this project (in no particular order or guarantee):

  * Provide examples with delivery guarantees

  * TCP and UDP acceptors producers

  * Ability to attach filtering to producers - today, if we need to filter events from a producer, we need an intermediary process which incurs extra copying

  * Connecting stages across nodes - because there is no guarantee messages are delivered, the demand driven approach in `GenStage` won't perform well in a distributed setup

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
