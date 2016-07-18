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
      |> Enum.flat_map(&String.split(&1, " "))
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
      |> Stream.flat_map(&String.split(&1, " "))
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
      |> Flow.flat_map(&String.split(&1, " "))
      |> Flow.partition()
      |> Flow.reduce(fn -> %{} end, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  To convert from stream to flow, we have done two changes:

    1. We have replaced the calls to `Stream` by `Flow`
    2. We called `partition/1` so words are properly partitioned between stages

  The example above will now use all cores available as well
  as keep an on going flow of data instead of traversing them
  line by line. Once all data is computed, it is sent to the
  process which invoked `Enum.to_list/1`.

  While we gain concurrency by calling `Flow`, many of the
  benefits in using flow is in the partioning the data. We will
  discuss the need for data partioning next. Lastly, we will
  comment on possible optimizations to the example above.

  ## Partitioning

  To understand the need to partion the data, let's change the
  example above and remove the partition call:

      alias Experimental.GenStage.Flow
      File.stream!("path/to/some/file")
      |> Flow.from_enumerable()
      |> Flow.flat_map(&String.split(&1, " "))
      |> Flow.reduce(fn -> %{} end, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  The example above will execute the `flat_map` and `reduce`
  operations in parallel inside multiple stages. When running
  on a machine with two cores:

       [file stream]  # Flow.from_enumerable/1 (producer)
          |    |
        [M1]  [M2]    # Flow.flat_map/2 + Flow.reduce/3 (consumer)

  Now imagine that the `M1` and `M2` stages above receive the
  following lines:

      M1 - "roses are red"
      M2 - "violets are blue"

  `flat_map/2` will break them into:

      M1 - ["roses", "are", "red"]
      M2 - ["violets", "are", "blue"]

  Then `reduce/3` will make each stage have the following state:

      M1 - %{"roses" => 1, "are" => 1, "red" => 1}
      M2 - %{"violets" => 1, "are" => 1, "blue" => 1}

  Although both stages have performed word counting, we have words
  like "are" that appears on both stages. This means we would need
  to perform yet another pass on the data merging the duplicated
  words accross stages.

  Partioning solves this by introducing a new set of stages and
  making sure the same word is always mapped to the same stage
  with the help of a hash function. Let's introduce the call to
  `partition/1` back:

      alias Experimental.GenStage.Flow
      File.stream!("path/to/some/file")
      |> Flow.from_enumerable()
      |> Flow.flat_map(&String.split(&1, " "))
      |> Flow.partition()
      |> Flow.reduce(fn -> %{} end, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  Now we will have the following topology:

       [file stream]  # Flow.from_enumerable/1 (producer)
          |    |
        [M1]  [M2]    # Flow.flat_map/2 (producer-consumer)
          |\  /|
          | \/ |
          |/ \ |
        [R1]  [R2]    # Flow.reduce/3 (consumer)

  If the `M1` and `M2` stages receive the same lines and break
  them into words as before:

      M1 - ["roses", "are", "red"]
      M2 - ["violets", "are", "blue"]

  Now any given word will be consistently routed to `R1` or `R2`
  regardless of its origin. The default hashing function will route
  them such as:

      R1 - ["roses", "are", "red", "are"]
      R2 - ["violets", "blue"]

  Resulting in the reduced state of:

      R1 - %{"roses" => 1, "are" => 2, "red" => 1}
      R2 - %{"violets" => 1, "blue" => 1}

  In a way that each stage has a distinct subset of the data.
  This way, we know we don't need to merge the data later on
  as the word in each stage is guaranteed to be unique.

  Partioning the data is a very useful technique. For example,
  if we want to count the number of unique elements in a dataset,
  we could perform such count in each partition and then later
  sum their results as the partitioning guarantees the data on
  each partition won't overlap. A unique element would never
  be counted twice.

  The topology above alongside partitioning is very common in
  the MapReduce programming model which we will briefly discuss
  next.

  ## MapReduce

  The MapReduce programming model forces us to break our computations
  in two stages: map and reduce. The map stage is often quite easy to
  parallellize because events are processed individually and in isolation.
  The reduce stages need to group the data either partially or completely.

  In the example above, the stages executing `flat_map/2` are the
  mapper stages. Because the `flat_map/2` function works line by line,
  we can have two, four, eight or more mapper processes that will
  break line by line into words without any need for coordination.

  However, the reducing stage is a bit more complicated. Reducer
  stages typically compute some result based on its inputs. This
  implies reducer computations need to look at the whole data set
  and, in order to so efficiently, we want to partition the data
  to guarantee each reducer stage has a distinct subset of the data.

  Generally speaking, the performance of our flow will be limited
  by the amount of work we can perform without having a need to
  look at the whole collection. Both `flat_map/2` and `reduce/3`
  functions work on item-per-item. Some operations like `each_stage/2`
  and `map_stage/2` are applied to whole data in the stage. They are
  still parallel but must await for the data to be processed.

  Calling any function from `Enum` in a flow will start its
  execution and send the computed dataset to the caller process.

  ## Long running-flows

  In the examples so far we have started a flow dynamically
  and consumed it using `Enum.to_list/1`. Unfortunately calling
  a function from `Enum` will cause the computed dataset to be
  sent to a single process.

  In many situations, this is either too expensive or completely
  undesired. For example, in data-processing pipelines, it is
  common to constantly receive data from external sources. This
  data is either written to disk or to another storage after
  processed, without a need to be sent to a single process.

  Flow allows computations to be started as a group of processes
  which may run indefinitely.

  TODO: Add an example with start_link/1. Talk about hot code
  swaps and anonymous functions. Talk about attaching your own
  producer.

  ## Performance discussions

  In this section we will discuss points related to performance
  with flows.

  ### Know your code

  There are many optimizations we could perform in the flow above
  that are not necessarily related to flows themselves. Let's rewrite
  the flow above using some of them:

      alias Experimental.GenStage.Flow

      # The parent process which will own the table
      parent = self()

      # Let's compile common patterns for performance
      empty_space = :binary.compile_pattern(" ") # BINARY

      File.stream!("path/to/some/file", read_ahead: 100_000) # READ_AHEAD
      |> Flow.from_enumerable()
      |> Enum.flat_map(&String.split(&1, empty_space)) # BINARY
      |> Flow.partition()
      |> Flow.reduce(fn -> :ets.new(:words, []) end, fn word, ets -> # ETS
        :ets.update_counter(ets, word, {2, 1}, {word, 0})
        ets
      end)
      |> Flow.map_stage(fn ets ->         # ETS
        :ets.give_away(ets, parent, [])
        [ets]
      end)
      |> Enum.to_list()

  We have performed three optimizations:

    * BINARY - the first optimization is to compile the pattern we use
      to split the string on

    * READ_AHEAD - the second optimization is to use the `:read_ahead`
      option for file streams allowing us to do less IO operations by
      reading large chunks of data at once

    * ETS - the third stores the data in a ETS table and uses its counter
      operations. For counters and large dataset this provide a great
      performance benefit as it generates less garbage. At the end, we
      call `map_stage/2` to transger the ETS table to the parent process
      and wrap the table in a list so we can access it on `Enum.to_list/1`.
      Such step is not strictly required. For example, one could write the
      table to disk with `:ets.tab2file/2` at the end of the computation

  ### Configuration (demand and the number of stages)

  Both `new/1` and `partition/2` allows a set of options to configure
  how flows work. In particular, we recommend developers to play with
  the `:min_demand` and `:max_demand` options, which control the amount
  of data sent between stages.

  If the stages may perform IO computations, we also recommend increasing
  the number of stages. The default value is `System.schedulers_online/0`,
  which is a good default if the stages are CPU bound, however, if stages
  are waiting on external resources or other processes, increasing the
  number of stages may be helpful.

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
      |> Flow.flat_map(&String.split(&1, " "))
      |> Flow.reduce(fn -> %{} end, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
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

  defstruct producers: nil, options: [], operations: []

  ## Building

  @doc """
  Starts a new flow.

  ## Options

    * `:stages` - the number of stages before partitioning
    * `:buffer_keep` - how the buffer should behave, see `c:GenStage.init/1`
    * `:buffer_size` - how many events to buffer, see `c:GenStage.init/1`

  All remaining options are sent during subscription, allowing developers
  to customize `:min_demand`, `:max_demand` and others.
  """
  def new(options \\ []) do
    %GenStage.Flow{options: options}
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

  ## Reducers

  @doc """
  Partitions the flow with the given options.

  Every time this function is called, a new partition
  is created. It is typically recommended to invoke it
  before `reduce/3` so similar data is routed accordingly.

  ## Options

    * `:stages` - the number of partitions (reducer stages)
    * `:hash` - the hash to use when partitioning. It is a function
      that receives two arguments: the event to partition on and the
      maximum number of partitions. However, to facilitate customization,
      `:hash` also allows common values, such `{:elem, 0}`, to specify
      the hash should be calculated on the first element of a tuple.
      See more information on the "Hash shortcuts" section below.
      The default value hashing function `:erlang.phash2/2`.

  ## Hash shortcuts

  TODO: Implement this.
  """
  def partition(flow, opts \\ []) when is_list(opts) do
    add_operation(flow, {:partition, opts})
  end

  @doc """
  Reduces the given values with the given accumulator.

  The accumulator must be a function that receives no arguments
  and returns the actual accumulator. The accumulator function
  is executed inside per stage inside each stage.

  Once reducing is done, the returned accumulator will be
  the new state of the stage for the given window.

  ## Examples

      iex> flow = Flow.from_enumerable(["the quick brown fox"]) |> Flow.flat_map(fn word ->
      ...>    String.graphemes(word)
      ...> end)
      iex> flow = flow |> Flow.partition |> Flow.reduce(fn -> %{} end, fn grapheme, map ->
      ...>   Map.update(map, grapheme, 1, & &1 + 1)
      ...> end)
      iex> Enum.sort(flow)
      [{" ", 3}, {"b", 1}, {"c", 1}, {"e", 1}, {"f", 1},
       {"h", 1}, {"i", 1}, {"k", 1}, {"n", 1}, {"o", 2},
       {"q", 1}, {"r", 1}, {"t", 1}, {"u", 1}, {"w", 1},
       {"x", 1}]

  """
  def reduce(flow, acc, reducer) when is_function(reducer, 2) do
    if is_function(acc, 0) do
      add_operation(flow, {:reducer, :reduce, [acc, reducer]})
    else
      raise ArgumentError, "GenStage.Flow.reduce/3 expects the accumulator to be given as a function"
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
end
