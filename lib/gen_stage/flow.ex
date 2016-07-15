alias Experimental.GenStage

defmodule GenStage.Flow do
  @moduledoc ~S"""
  Computational flows with stages.

  `GenStage.Flow` allows developers to express computations
  on collections, similar to the `Enum` and `Stream` modules
  although computations will be executed in parallel using
  multiple `GenStage`s.

  As an example, let's implement the classical word counting
  algorithm using flow. The word counting program will receive
  one file and count how many times each word appears in the
  document. Using the `Enum` module it could be implemented
  as follows:

      File.stream!("path/to/some/file")
      |> Enum.flat_map(fn line ->
          String.split(line, " ")
         end)
      |> Enum.reduce(%{}, fn word, acc ->
          Map.update(acc, word, 1, & &1 + 1)
         end)
      |> Enum.to_list()

  Unfortunately the implemenation above is not quite efficient
  as `Enum.flat_map/2` will build a list with all the words in
  the document before reducing it. If the document is, for example,
  2GB, we will load 2GB of data into memory.

  We can improve the solution above by using the Stream module:

      File.stream!("path/to/some/file")
      |> Stream.flat_map(fn line ->
          String.split(line, " ")
         end)
      |> Enum.reduce(%{}, fn word, acc ->
          Map.update(acc, word, 1, & &1 + 1)
         end)
      |> Enum.to_list()

  Now instead of loading the whole set into memory, we will only
  keep the current line in memory while we process it. While this
  allows us to process the whole data set efficiently, it does
  not leverage concurency. Flow solves that:

      alias Experimental.GenStage.Flow
      File.stream!("path/to/some/file")
      |> Flow.from_enumerable()
      |> Flow.flat_map(fn line ->
        for word <- String.split(" "), do: {word, 1}
      end)
      |> Flow.reduce_by_key(& &1 + &2)
      |> Enum.to_list()

  To convert from stream to flow, we have done two changes:

    1. `flat_map/2` now returns key-value pairs, using the
       word itself as key
    2. this allows us to invoke `reduce_by_key/2` which will
       invoke the given function for each key

  The example above will now use all cores available as well
  as keep an on going flow of data instead of traversing only
  line by line. We gain concurrency but we lose ordering and
  locality as the same function will be executed by different
  processes.

  We could further improve the flow above to achieve maximum
  performance with the following changes:

      alias Experimental.GenStage.Flow

      # Let's compile common patterns for performance
      empty_space = :binary.compile_pattern(" ") # NEW!

      File.stream!("path/to/some/file", read_ahead: 100_000) # NEW!
      |> Flow.from_enumerable()
      |> Flow.flat_map(fn line ->
        for word <- String.split(empty_space), do: {word, 1}
      end)
      |> Flow.partition_with(storage: :ets) # NEW!
      |> Flow.reduce_by_key(& &1 + &2)
      |> Enum.to_list()

  We first compiled the binary pattern we use to split lines into
  words and then changed the `File.stream!` to buffer lines, so we
  perform less IO operations. We also changed the partition storage
  to ETS, which will yield better performance in cases like the word
  counting example above. We will cover those operations in detail
  over the next sections when we talk about the Map Reduce programming
  model in which flow is built on top of and when discussing performance.

  ## MapReduce

  The MapReduce programming model forces us to break our computations
  in two stages: map and reduce. The map stage is often quite easy to
  parallellize because items are processed individually. The reduce
  stages need to group the data either partially or completely.

  Let's continue exploring the example above but let's add "stop words"
  which we don't want to count in our results, as shown below:

      alias Experimental.GenStage.Flow

      # Let's compile common patterns for performance
      empty_space = :binary.compile_pattern(" ")

      # Words for us to ignore from the text
      stop_words = ~w(a an and of the)

      File.stream!("path/to/some/file", read_ahead: 100_000)
      |> Flow.from_enumerable() # All streams are enumerable
      |> Flow.flat_map(fn line ->
        for word <- String.split(empty_space),
            not word in stop_words,
            do: {word, 1}
      end)
      |> Flow.partition_with(storage: :ets)
      |> Flow.reduce_by_key(& &1 + &2)
      |> Enum.to_list()

  In the example above, we start with the path to a given file,
  convert it to a stream so we can read the file line by line
  (alongside a read ahead buffer).

  We start building the flow with `new/0` and then invoke
  `from_enumerable/1` which sets the given file stream as the
  source of our flow of data. The next function is `flat_map/2`
  which receives every line and converts it into a list of words
  into key-value pairs. For example "the quick brown fox" will
  become `[{"quick", 1}, {"brown", 1}, {"fox", 1}]` (note "the" is
  removed as it is a stop word).

  Breaking the input into key-value pairs allows us to partition
  the data where `reduce_by_key/2` will be invoked every time the
  flow sees a word more than once. For example, if `{"quick", 1}`
  appears twice, `reduce_by_key/2` will be invoked and it will
  become `{"quick", 2}` and so forth. We have configured the
  partition to use ETS storage for performance.

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

       [file stream]  # Flow.from_enumerable/1 (producer)
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

  However, the reducing stage is a bit more complicated. Imagine
  the file we are processing has the "the quick brown fox" line
  twice. One line is sent to stage M1 and the other one is sent
  to stage M2. When counting words, however, we need to make sure
  both M1 and M2 send the words "quick" to the same reducer stage
  (for example R1). Similar should happen with the words "brown"
  and "fox" (which could have gone to R2).

  Luckily this is done automatically by flow since we broke each
  line into key-value pairs. The mapper uses the hash of the key
  to figure out which reducer stage should receive a given word.
  Given any mapper will calculate the same hash for a given key,
  it is guaranteed a given word will reach the a reducer stage
  regardless of the mapper that it was on. We call this process
  *partitioning*. The partitions can be customized with the
  `partition_with/2` call.

  Once all words have been processed, the reducer stages will
  send information to the process who called `Enum.to_list/1` so
  it can effectively build a list. It is important to emphasize
  that the reducer stages are only done after they have processed
  the whole collection. After all, if the reducer stage is counting
  words, we can only know how many words we have seen after we
  have analyzed the whole dataset.

  Generally speaking, the performance of our flow will be limited
  by the amount of work we can perform without having a need to
  look at the whole collection. Both `flat_map/2` and `reduce_by_key/2`
  functions work on an item-bases. Some operations like `each_partition/2`
  and `map_partition/2` are applied to whole partitions on every
  reducing stage. They are still parallel but must await for the data
  to be processed. Finally, all operations in `Enum` will necessarily
  work with the whole dataset.

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
      |> Flow.partition_with(storage: :ets)
      |> Flow.reduce_by_key(& &1 + &2)
      |> Flow.map(&IO.inspect/1)                                 # NEW!
      |> Enum.to_list

  We have done two changes to the flow above. Instead of doing the
  word filtering inside `flat_map/2`, we are now performing a call
  to `filter/2` afterwards. We are now also calling `Flow.map/2`
  with `IO.inspect/1` to print every term before we convert it to
  list. For the example above, the following flow is generated:

       [file stream]  # Flow.from_enumerable/1
          |    |
        [M1]  [M2]    # Flow.flat_map/2 + Flow.filter/2
          |\  /|
          | \/ |
          |/ \ |
        [R1]  [R2]    # Flow.reduce_by_key/2
        [M1]  [M2]    # Flow.map/2

  Flow knows `flat_map/2` and `filter/2` can be merged
  together and has done so. Also flow did not create new mapper
  stages for the extra `map/2` called after `reduce_by_key/2`.
  Instead, when reducing is done, it will simply start mapping
  the items directly from the reducing stages to avoid copying.

  Generally speaking, new stages are only created when we need
  to partition the data.

  ## Long running-flows

  In the examples so far we have started a flow dynamically
  and consumed it using `Enum.to_list/1`. However, in many
  situations we want to start a flow as a separate process,
  possibly without producers or without consumers later on.

  TODO: Add an example with start_link/1. Talk about hot code
  swaps and anonymous functions. Talk about attaching your own
  producer and your own consumer.

  ## Performance discussions

  In this section we will discuss points related to performance
  with flows.

  ### ETS-based partitions

  Since the reducer stages abstracts away the storage with functions
  like `reduce_by_key/2`, it is possible to configure the reducer
  to use ETS tables for some computations. This is particularly
  useful when the values in our key-value pairs are "atomic" values
  such as integers or atoms and when the number of keys is really
  high.

  For the word counting algorithm, ETS can considerably speed-up
  performance, as we typically have a wide range of keys with integer
  values, fitting both scenarios above. In the examples above, we have
  configured the partitions to use ETS with the `partition_with/2` call.

  Keep in mind it is important to benchmark before choosing a
  different storage as using ETS for complex key-value pairs will
  likely reduce performance due to the increasing cost in copying
  data to and from ETS tables.

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

      streams
      |> Flow.from_enumerables()
      |> Flow.flat_map(fn line ->
        for word <- String.split(empty_space),
            not word in stop_words,
            do: {word, 1}
      end)
      |> Flow.reduce_by_key(& &1 + &2)
      |> Enum.to_list()

  Instead of calling `from_enumerable/1`, we now called
  `from_enumerables/1` which expects a list of enumerables to
  be used as source. Notice every stream also uses the `:read_ahead`
  option which tells Elixir to buffer file data in memory to
  avoid multiple IO lookups.

  If the number of enumerables is equal to or more than the number of
  cores, flow will automatically fuse the enumerables with the mapper
  logic. For example, if three file streams are given as enumerables
  to a machine with two cores, we will have the following topology:

      [F1][F2][F3]  # file stream
      [M1][M2][M3]  # Flow.flat_map/2 (producer)
        |\ /\ /|
        | /\/\ |
        |//  \\|
        [R1][R2]    # Flow.reduce_by_key/2 (consumer)

  """

  defstruct producers: nil, stages: 0, mappers: 0, operations: []

  ## Building

  @doc """
  Starts a new flow.

  ## Options

    * `:stages` - the default number of stages to start per mapper and
      reducer layers throughout the flow. Defaults to `System.schedulers_online/0`.

    * `:mappers` - configures the mapper stages. It accepts the following options:

        * `:stages` - the number of mapper stages
        * `:buffer_keep` - how the buffer should behave, see `c:GenStage.init/1`
        * `:buffer_size` - how many events to buffer, see `c:GenStage.init/1`

      All remaining options are sent during subscription, allowing developers
      to customize `:min_demand`, `:max_demand` and others.
  """
  def new(opts \\ []) do
    stages = Keyword.get(opts, :stages, System.schedulers_online)
    mappers = Keyword.get(opts, :mappers, [])
    %GenStage.Flow{stages: stages, mappers: mappers}
  end

  @doc """
  Starts a flow with the given enumerable as producer.

  It is effectively a shortcut for:

      Flow.new |> Flow.from_enumerables([enumerable])

  ## Examples

      "some/file"
      |> File.stream!(read_ahead: 100_000)
      |> Flow.from_enumerable()

  """
  def from_enumerable(enumerable) do
    new() |> from_enumerables([enumerable])
  end

  @doc """
  Sets the given enumerable as a producer in the given flow.

  ## Examples

      file = File.stream!("some/file", read_ahead: 100_000)
      Flow.from_enumerable(Flow.new, file)

  """
  def from_enumerable(flow, enumerable) do
    flow |> from_enumerables([enumerable])
  end

  @doc """
  Starts a flow with the list of enumerables as producers.

  It is effectively a shortcut for:

      Flow.new |> Flow.from_enumerables(enumerables)

  ## Examples

      files = [File.stream!("some/file1", read_ahead: 100_000),
               File.stream!("some/file2", read_ahead: 100_000),
               File.stream!("some/file3", read_ahead: 100_000)]
      Flow.from_enumerable(files)

  """
  def from_enumerables(enumerables) do
    new() |> from_enumerables(enumerables)
  end

  @doc """
  Sets the given enumerables as producers in the given flow.

  ## Examples

      files = [File.stream!("some/file1", read_ahead: 100_000),
               File.stream!("some/file2", read_ahead: 100_000),
               File.stream!("some/file3", read_ahead: 100_000)]
      Flow.from_enumerable(Flow.new, files)
  """
  def from_enumerables(flow, [_ | _] = enumerables) do
    add_producers(flow, {:enumerables, enumerables})
  end
  def from_enumerables(_flow, enumerables) do
    raise ArgumentError, "from_enumerables/2 expects a non-empty list as argument, got: #{inspect enumerables}"
  end

  @doc """
  Starts a flow with the given stage as producer.

  It is effectively a shortcut for:

      Flow.new |> Flow.from_stages([stage])

  ## Examples

      Flow.from_stage(MyStage)

  """
  def from_stage(stage) do
    new() |> from_stages([stage])
  end

  @doc """
  Sets the given stage as a producer in the given flow.

  ## Examples

      Flow.from_stage(Flow.new, MyStage)

  """
  def from_stage(flow, stage) do
    flow |> from_stages([stage])
  end

  @doc """
  Starts a flow with the list of stages as producers.

  It is effectively a shortcut for:

      Flow.new |> Flow.from_stages(stages)

  ## Examples

      stages = [pid1, pid2, pid3]
      Flow.from_stage(stages)

  """
  def from_stages(stages) do
    new() |> from_stages(stages)
  end

  @doc """
  Sets the given stages as producers in the given flow.

  ## Examples

      stages = [pid1, pid2, pid3]
      Flow.from_stage(Flow.new, stages)

  """
  def from_stages(flow, [_ | _] = stages) do
    add_producers(flow, {:stages, stages})
  end
  def from_stages(_flow, stages) do
    raise ArgumentError, "from_stages/2 expects a non-empty list as argument, got: #{inspect stages}"
  end

  ## Mappers

  @doc """
  Applies the given function to each input without modifying it.

  ## Examples

      iex> parent = self()
      iex> [1, 2, 3] |> Flow.from_enumerable() |> Flow.each(&send(parent, &1)) |> Enum.sort()
      [1, 2, 3]
      iex> receive do
      ...>   1 -> :ok
      ...> end
      :ok

  """
  def each(flow, each) when is_function(each, 1) do
    add_operation(flow, {:mapper, :each, [each]})
  end

  @doc """
  Applies the given function filtering each input in parallel.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.filter(& rem(&1, 2) == 0)
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [2]

  """
  def filter(flow, filter) when is_function(filter, 1) do
    add_operation(flow, {:mapper, :filter, [filter]})
  end

  @doc """
  Applies the given function filtering and mapping each input in parallel.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.filter_map(& rem(&1, 2) == 0, & &1 * 2)
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [4]

  """
  def filter_map(flow, filter, mapper) when is_function(filter, 1) and is_function(mapper, 1) do
    add_operation(flow, {:mapper, :filter_map, [filter, mapper]})
  end

  @doc """
  Applies the given function mapping each input in parallel.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.map(& &1 * 2)
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [2, 4, 6]

      iex> flow = Flow.from_enumerables([[1, 2, 3], 1..3]) |> Flow.map(& &1 * 2)
      iex> Enum.sort(flow)
      [2, 2, 4, 4, 6, 6]

  """
  def map(flow, mapper) when is_function(mapper, 1) do
    add_operation(flow, {:mapper, :map, [mapper]})
  end

  @doc """
  Applies the given function mapping each input in parallel and
  flattening the result, but only one level deep.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.flat_map(fn(x) -> [x, x * 2] end)
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [1, 2, 2, 3, 4, 6]

  """
  def flat_map(flow, flat_mapper) when is_function(flat_mapper, 1) do
    add_operation(flow, {:mapper, :flat_map, [flat_mapper]})
  end

  @doc """
  Applies the given function rejecting each input in parallel.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.reject(& rem(&1, 2) == 0)
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [1, 3]

  """
  def reject(flow, filter) when is_function(filter, 1) do
    add_operation(flow, {:mapper, :reject, [filter]})
  end

  defimpl Enumerable do
    def reduce(flow, acc, fun) do
      GenStage.Flow.Materialize.to_stream(flow).(acc, fun)
    end

    def count(_flow) do
      {:error, __MODULE__}
    end

    def member?(_flow, _value) do
      {:error, __MODULE__}
    end
  end

  @compile {:inline, add_producers: 2, add_operation: 2}

  defp add_producers(%GenStage.Flow{producers: nil} = flow, producers) do
    %{flow | producers: producers}
  end
  defp add_producers(%GenStage.Flow{producers: producers}, _producers) do
    raise ArgumentError, "cannot set from_stages/from_enumerable because the flow already has set #{inspect producers} as producers"
  end
  defp add_producers(flow, _producers) do
    raise ArgumentError, "expected a flow as argument, got: #{inspect flow}"
  end

  defp add_operation(%GenStage.Flow{operations: operations} = flow, operation) do
    %{flow | operations: [operation | operations]}
  end
  defp add_operation(flow, _producers) do
    raise ArgumentError, "expected a flow as argument, got: #{inspect flow}"
  end
end
