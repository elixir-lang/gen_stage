defmodule GenStage.Flow do
  @moduledoc ~S"""
  Computational flows with stages.

  `GenStage.Flow` allows developers to express computations
  on collections, similar to the `Enum` and `Stream` modules
  although computations will be executed in parallel using
  multiple `GenStage`s.

  Flows are based on the map reduce programming model which
  we will explore with detail below.

  ## MapReduce

  The MapReduce programming model forces us to break our
  computations in two stages: map and reduce. The map stage
  is often quite easy to parallellize because items are processed
  individually. The reduce stages need to group the data either
  partially or completely.

  As an example, let's implement the classical word counting
  algorithm using flow. The word counting program will receive
  one or more files and find the words that occur more frequently.

  We can write it with flow as shown below:

      alias Experimental.GenStage.Flow

      # Let's compile common patterns for performance
      empty_space = :binary.compile_pattern(" ")

      # Words for us to ignore from the text
      stop_words = ~w(a an and of the)

      # This is our source file
      stream = File.stream!("path/to/some/file", read_ahead: 100_000)

      Flow.new
      |> Flow.from_enumerable(stream) # All streams are enumerable
      |> Flow.flat_map(fn line ->
        for word <- String.split(empty_space),
            not word in stop_words,
            do: {word, 1}
      end)
      |> Flow.reduce_by_key(& &1 + &2)
      |> Enum.to_list()

  In the example above, we start with the path to a given file,
  convert it to a stream so we can read the file line by line
  (alongside a read ahead buffer).

  We starting building the flow with `new/0` and then invoke
  `from_enumerable/2` which sets the given as file stream as the
  source of our flow of data. The next function is `flat_map/2`
  which receives every line and convert it into a list of words
  into key-value pairs. For example "the quick brown fox" will
  become `[{"quick", 1}, {"brown", 1}, {"fox", 1}]`. Breaking
  the input into key-value pairs allows us to call `reduce_by_key/2`
  with a callback which will be invoked every time the flow sees
  a word more than once. For example, if `{"quick", 1}` appears
  twice, `reduce_by_key/2` will be invoked and it will become
  `{"quick", 2}` and so on.

  The `flow` itself is also an stream. Once `Enum.to_list/1` is
  called, the whole flow is *materialized* and data starts to flow
  through. `Enum.to_list/1` will effectively load the whole data
  set into memory. Loading the whole data into memory is OK for
  short examples although we would like to avoid such for large
  data.

  ### Materializing the flow

  Materializing the flow is the act of converting the flow we
  have defined above effectively into stages (processes).
  Imagine we executed the example above in a machine with
  two cores, it would become the following processes:

       [file stream]  # Flow.from_enumerable/2 (producer)
          |    |
        [M1]  [M2]    # Flow.flat_map/2 (producer-consumer)
          |\  /|
          | \/ |
          |/ \ |
        [R1]  [R2]    # Flow.reduce_by_key/2 (consumer)

  Where `M?` are the mapper stages and `R?` are the reducer
  stages. If our machine had four cores, we would have `M1`,
  `M2`, `M3` and `M4` and similar for reducers.

  The idea behind the mapper stages is that they are embarrassingly
  parallel. Because the `flat_map/2` function works line by line,
  we can have two, four, eight or more mapper processes that will
  break line by line into words without any need for coordination.
  Since the mapper stages receives data from the enumerable

  However, the reducing stage is a bit more complicated. Imagine
  the file we are processing has the "the quick brown fox" line
  twice. One line is sent to stage M1 and the other one is sent
  to stage M2. When counting words, however, we need to make sure
  both M1 and M2 send the words "quick" to the same reducer stage
  (for example R1). Similar should happen with the words "brown"
  and "fox" (which could have gone to R2).

  Luckily this is done automatically by flow when we break the line
  into key-value pairs. The mapper uses the hash of the key to figure
  out which reducer stage should receive a given word. Given both
  mappers will calculate the same hash for a given key, it is
  guaranteed a given word will reach the a reducer stage regardless
  of the mapper that it was on.

  Once all words have been processed, the reducer stages will
  send information to the process who called `Enum.to_list/1` so
  it can effectively build a list. It is important to emphasize
  that the reducer stages will only be done after it has processed
  the whole collection. After all, if the reducer stage is counting
  words, we can only know how many words we have after we have seen
  the whole dataset.

  ### Fusing stages

  Flow will do its best to ensure the minimum amount of stages are
  created to avoid unecessary copying of data. For example, imagine
  we rewrote the flow above to the following:

      Flow.new
      |> Flow.from_enumerable(stream)
      |> Flow.flat_map(fn line ->
        for word <- String.split(empty_space), do: {word, 1}
      end)
      |> Flow.filter(fn {word, _} -> not word in stop_words end) # NEW!
      |> Flow.reduce_by_key(& &1 + &2)
      |> Flow.map(&IO.inspect/1)                                 # NEW!
      |> Enum.to_list

  We have done two changes to the flow above. Instead of doing the
  word filtering inside `flat_map/2`, we are now performing a call
  to `filter/2` afterwards. We are now also calling `Flow.map/2`
  with `IO.inspect/1` to print every term before we convert it to
  list. For the example above, the following flow is generated:

       [file stream]  # Flow.from_enumerable/2
          |    |
        [M1]  [M2]    # Flow.flat_map/2 + Flow.filter/2
          |\  /|
          | \/ |
          |/ \ |
        [R1]  [R2]    # Flow.reduce_by_key/2
        [M1]  [M2]    # Flow.map/2

  Flow knew `flat_map/2` and `filter/2` can be merged
  together and has done so. Also flow did not create new mapper
  stages for the extra `map/2` called after `reduce_by_key/2`.
  Instead, when reducing is done, it will simply start mapping
  the items directly from the reducing stages to avoid copying.

  ## Long running-flows

  In the examples so far we have started a flow dynamically
  and consumed it using `Enum.to_list/1`. However, in many
  situations we want to start a flow as a separate process,
  possibly without producers or without consumers later on.

  TODO: Add an example with start_link/1. Talk about hot code
  swaps and anonymous functions.

  ## Performance discussions

  In this section we will discuss points related to performance
  with flows.

  ### ETS tables

  To increase performance, reducer stages will store the data in
  ETS tables by default. Once reducing is done, the ETS table will
  be sent to next process in the pipeline which can then start
  traversing it.

  On consequence of such design decision is that executing flows
  dynamically (for example, one flow per web server request) can
  be problematic as there is a limit of ETS tables that can be
  created by the runtime. If you really need to start flows
  dynamically and in large quantity, we recommend developers to
  pass `storage: :maps` option.

  TODO: Figure out if the `storage: :maps` option will be on
  `Flow.new` or on the first reducing function call (which can
  be brittle).

  ### Avoid single sources

  In the examples so far we have used a single file as our data
  source. In practice such should be avoided as the source could
  end-up being the bottleneck of our whole computation.

  In the file stream case above, instead of having one single
  large file, it is preferrable to break the file into smaller
  ones:

      streams = for file <- File.ls!("dir/with/files") do
        File.stream!("dir/with/files/#{file}", read_ahead: 100_000)
      end

      Flow.new
      |> Flow.from_enumerables(streams)
      |> Flow.flat_map(fn line ->
        for word <- String.split(empty_space),
            not word in stop_words,
            do: {word, 1}
      end)
      |> Flow.reduce_by_key(& &1 + &2)
      |> Enum.to_list()

  Instead of calling `from_enumerable/2`, we now called
  `from_enumerables/2` which expects a list of enumerables to
  be used as source. Notice every stream also uses the `:read_ahead`
  option which tells Elixir to buffer file data in memory to
  avoid multiple IO lookups.

  If the number of enumerables is equal to or more than the number of
  cores, flow will automatically fuse the enumerables with the mapper
  logic. For example, if three files are given to a machine with two
  cores, we will have the following topology:

      [F1][F2][F3]  # file stream
      [M1][M2][M3]  # Flow.flat_map/2 (producer-consumer)
       |\ /\ /|
       | /\/\ |
       |//  \\|
       [R1][R2]    # Flow.reduce_by_key/2

  """
end
