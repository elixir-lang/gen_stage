alias Experimental.GenStage

defmodule GenStage.Flow do
  @moduledoc ~S"""
  Computational flows with stages.

  `GenStage.Flow` allows developers to express computations
  on collections, similar to the `Enum` and `Stream` modules,
  although computations will be executed in parallel using
  multiple `GenStage`s.

  Flow was also designed to work with both bounded (finite)
  and unbounded (infinite) data. Allowing the data to be
  partitioned into arbitrary windows which are materialized
  at different triggers.

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

  While we gain concurrency by using flow, many of the benefits
  in using flow is in the partioning the data. We will discuss
  the need for data partioning next.

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

  Which is converted to the list (in no particular ordering):

      [{"roses", 1},
       {"are", 1},
       {"red", 1},
       {"violets", 1},
       {"are", 1},
       {"blue", 1}]

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

  Which is converted to the list (in no particular ordering):

      [{"roses", 1},
       {"are", 2},
       {"red", 1},
       {"violets", 1},
       {"blue", 1}]

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

  ### MapReduce

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
  by the amount of work we can perform without having to look at
  the whole collection. At the moment we call `reduce/3`, it will
  group the whole data for that particular partition. Operations
  on the whole data are still parallel but must await until all
  data is collected. Finally, calling any function from `Enum`
  in a flow will start its execution and send the computed datasets
  to the caller process.

  While this approach works great for bound (finite) data, it
  is quite limited for unbounded (infinite) data. After all, if
  the reduce operation needs the whole data to complete, how can
  it do so if the data never finishes?

  This brings us to the aspects of data completion and data
  emission which we will discuss next.

  ## Data completion and triggers

  When working with an unbounded stream of data, there is no
  such thing as data completion. Therefore, when can we consider
  a reduce function to be "complete"?

  Our best option is to provide triggers that will allow us to
  check point the processed data so far.

  There are different triggers we can use:

    * Event count triggers - compute state operations every X events

    * Processing time triggers - compute state operations every X seconds

    * Event time triggers - compute state operations based on the times
      present in the events themselves

    * Punctuation - hand-written triggers based on the data

  Currently flow supports only explicit triggers via the `trigger/2`
  function (event count triggers can be emulated with such).

  Once a trigger is emitted, the `reduce` step halts and invokes
  the remaining steps for that stage, such as `map/2` and `filter/2`
  as well as `map_state/2` which will receive the whole reduction
  state. Triggers are also named and the trigger names will be sent
  as third argument to the function given in `map_state/2` and
  `each_state/2`.

  Once a trigger is emitted and the remaining steps in the stage are
  processed, developers have the choice of either reseting the
  accumulation stage or keeping it as is. The resetting option is
  useful when you are interested only on intermediate results. Keeping
  the accumulator is useful when you want to checkpoint the values
  but still work towards an end result.

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
      |> Flow.map_state(fn ets ->         # ETS
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
      call `map_state/2` to transfer the ETS table to the parent process
      and wrap the table in a list so we can access it on `Enum.to_list/1`.
      Such step is not strictly required. For example, one could write the
      table to disk with `:ets.tab2file/2` at the end of the computation

  ### Configuration (demand and the number of stages)

  Both `new/1` and `partition/2` allows a set of options to configure
  how flows work. In particular, we recommend developers to play with
  the `:min_demand` and `:max_demand` options, which control the amount
  of data sent between stages. The difference between max_demand and
  min_demand works as the batch size when the producer is full. If the
  producer has less events than the batch size, its current events are
  sent.

  If stages may perform IO computations, we also recommend increasing
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
  @type t :: %GenStage.Flow{producers: producers, operations: [operation], options: Keyword.t}

  @typep producers :: nil | {:stages, GenStage.stage} | {:enumerables, Enumerable.t}
  @typep operation :: {:mapper, atom(), [term()]} |
                      {:partition, Keyword.t} |
                      {:map_state, fun()} |
                      {:reduce, fun(), fun()}

  ## Building

  @doc """
  Starts a new flow.

  ## Options

  These options configure the stages before partitioning.

    * `:stages` - the number of stages
    * `:buffer_keep` - how the buffer should behave, see `c:GenStage.init/1`
    * `:buffer_size` - how many events to buffer, see `c:GenStage.init/1`

  All remaining options are sent during subscription, allowing developers
  to customize `:min_demand`, `:max_demand` and others.
  """
  @spec new(Keyword.t) :: t
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
  @spec from_enumerable(Enumerable.t) :: t
  def from_enumerable(enumerable) do
    new() |> from_enumerables([enumerable])
  end

  @doc """
  Sets the given enumerable as a producer in the given flow.

  ## Examples

      file = File.stream!("some/file", read_ahead: 100_000)
      Flow.from_enumerable(Flow.new, file)

  """
  @spec from_enumerable(t, Enumerable.t) :: t
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
  @spec from_enumerables([Enumerable.t]) :: t
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
  @spec from_enumerables(t, [Enumerable.t]) :: t
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
  @spec from_stage(GenStage.stage) :: t
  def from_stage(stage) do
    new() |> from_stages([stage])
  end

  @doc """
  Sets the given stage as a producer in the given flow.

  ## Examples

      Flow.from_stage(Flow.new, MyStage)

  """
  @spec from_stage(t, GenStage.stage) :: t
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
  @spec from_stages([GenStage.stage]) :: t
  def from_stages(stages) do
    new() |> from_stages(stages)
  end

  @doc """
  Sets the given stages as producers in the given flow.

  ## Examples

      stages = [pid1, pid2, pid3]
      Flow.from_stage(Flow.new, stages)

  """
  @spec from_stages(t, [GenStage.stage]) :: t
  def from_stages(flow, [_ | _] = stages) do
    add_producers(flow, {:stages, stages})
  end
  def from_stages(_flow, stages) do
    raise ArgumentError, "from_stages/2 expects a non-empty list as argument, got: #{inspect stages}"
  end

  @doc """
  Runs a given flow.

  This runs the given flow as a stream for its side-effects. No
  items are sent from the flow to the current process.

  ## Examples

      iex> parent = self()
      iex> [1, 2, 3] |> Flow.from_enumerable() |> Flow.each(&send(parent, &1)) |> Flow.run()
      :ok
      iex> receive do
      ...>   1 -> :ok
      ...> end
      :ok

  """
  @spec run(t) :: :ok
  def run(%{operations: operations} = flow) do
    case inject_to_run(operations) do
      :map_state ->
        [] = flow |> map_state(fn _, _ -> [] end) |> Enum.to_list()
      :reduce ->
        [] = flow |> reduce(fn -> [] end, fn _, acc -> acc end) |> Enum.to_list()
    end
    :ok
  end

  defp inject_to_run([{:reduce, _, _} | _]), do: :map_state
  defp inject_to_run([{:partition, _} | _]), do: :reduce
  defp inject_to_run([_ | ops]), do: inject_to_run(ops)
  defp inject_to_run([]), do: :reduce

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
  @spec each(t, (term -> term)) :: t
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
  @spec filter(t, (term -> term)) :: t
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
  @spec filter_map(t, (term -> term), (term -> term)) :: t
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
  @spec map(t, (term -> term)) :: t
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
  @spec flat_map(t, (term -> Enumerable.t)) :: t
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
  @spec reject(t, (term -> term)) :: t
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

  The following shortcuts can be given to the `:hash` option:

    * `{:elem, pos}` - apply the hash function to the element
      at position `pos` in the given tuple

    * `{:key, key}` - apply the hash function to the key of a given map

  """
  @spec partition(t, Keyword.t) :: t
  def partition(flow, opts \\ []) when is_list(opts) do
    hash =
      case Keyword.get(opts, :hash, &:erlang.phash2/2) do
        fun when is_function(fun, 2) ->
          fun
        {:elem, pos} when pos >= 0 ->
          pos = pos + 1
          &:erlang.phash2(:erlang.element(pos, &1), &2)
        {:key, key} ->
          &:erlang.phash2(Map.fetch!(&1, key), &2)
        other ->
          raise ArgumentError, """
          expected :hash to be one of:

            * a function expecting two arguments
            * {:elem, pos} when pos >= 0

          instead got: #{inspect other}
          """
      end
    opts = Keyword.put(opts, :hash, hash)
    add_operation(flow, {:partition, opts})
  end

  @doc """
  Reduces the given values with the given accumulator.

  `acc` is a function that receives no arguments and returns
  the actual accumulator. The `acc` function is executed per stage
  inside each stage.

  Once reducing is done, the returned accumulator will be
  the new state of the stage for the given batch.

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
  @spec reduce(t, (() -> acc), (term, acc -> acc)) :: t when acc: term()
  def reduce(flow, acc_fun, reducer_fun) when is_function(reducer_fun, 2) do
    if is_function(acc_fun, 0) do
      add_operation(flow, {:reduce, acc_fun, reducer_fun})
    else
      raise ArgumentError, "GenStage.Flow.reduce/3 expects the accumulator to be given as a function"
    end
  end

  @doc """
  Controls which values should be emitted from now.

  It can either be `:events` (the default)  or the current
  stage state as `:state`. This step must be called after
  the reduce operation and it will guarantee the state is
  a list that can be sent downstream.

  Most commonly, each partition will emit the events it has
  processed to the next stages. However, sometimes we want
  to emit counters or other data structures as a result of
  our computations. In such cases, the `:emit` option can be
  set to `:state`, to return the `:state` from `reduce/3`
  or `map_state/2` or even the processed collection as a whole.
  """
  def emit(flow, :events) do
    flow
  end
  def emit(flow, :state) do
    map_state(flow, fn acc, _, _ -> [acc] end)
  end
  def emit(_, emit) do
    raise ArgumentError, "unknown option for emit: #{inspect emit}"
  end

  @doc """
  Applies the given function over the stage state.

  This function must be called after `reduce/3`, as it
  maps over the state directly (or indirectly) returned
  by `reduce/3`.

  The `mapper` function may have arity 1 or 2 or 3:

    * when one, the state is given as argument
    * when two, the state and the stage indexes are given as arguments.
      The index is a tuple with the current stage index as first element
      and the total number of stages for this partition as second
    * when three, the state and the stage indexes as above are given
      as well as the trigger name that led for the `reduce/3` to stop.
      See `trigger/2` for custom triggers. If the producer halts because
      it does not have more events, the trigger name is `{:producer, :done}`

  The value returned by this function becomes the new state
  for the given window/stage pair.

  ## Examples

  We can use `map_state/2` to transform the collection after
  processing. For example, if we want to count the amount of
  unique letters in a sentence, we can partition the data,
  then reduce over the unique entries and finally return the
  size of each stage, summing it all:

      iex> flow = Flow.from_enumerable(["the quick brown fox"]) |> Flow.flat_map(fn word ->
      ...>    String.graphemes(word)
      ...> end)
      iex> flow = Flow.partition(flow)
      iex> flow = Flow.reduce(flow, fn -> %{} end, &Map.put(&2, &1, true))
      iex> flow |> Flow.map_state(fn map -> map_size(map) end) |> Flow.emit(:state) |> Enum.sum()
      16

  """
  @spec map_state(t, (term -> term) | (term, term -> term)) :: t
  def map_state(flow, mapper) when is_function(mapper, 3) do
    add_operation(flow, {:map_state, mapper})
  end
  def map_state(flow, mapper) when is_function(mapper, 2) do
    add_operation(flow, {:map_state, fn acc, index, _ -> mapper.(acc, index) end})
  end
  def map_state(flow, mapper) when is_function(mapper, 1) do
    add_operation(flow, {:map_state, fn acc, _, _ -> mapper.(acc) end})
  end

  @doc """
  Applies the given function over the stage state without changing its value.

  It is similar to `map_state/2` except that the value returned by `mapper`
  is ignored.

      iex> parent = self()
      iex> flow = Flow.from_enumerable(["the quick brown fox"]) |> Flow.flat_map(fn word ->
      ...>    String.graphemes(word)
      ...> end)
      iex> flow = flow |> Flow.partition(stages: 2) |> Flow.reduce(fn -> %{} end, &Map.put(&2, &1, true))
      iex> flow = flow |> Flow.each_state(fn map -> send(parent, map_size(map)) end)
      iex> Flow.run(flow)
      iex> receive do
      ...>   6 -> :ok
      ...> end
      :ok
      iex> receive do
      ...>   10 -> :ok
      ...> end
      :ok

  """
  @spec each_state(t, (term -> term) | (term, term -> term)) :: t
  def each_state(flow, mapper) when is_function(mapper, 3) do
    add_operation(flow, {:map_state, fn acc, index, trigger -> mapper.(acc, index, trigger); acc end})
  end
  def each_state(flow, mapper) when is_function(mapper, 2) do
    add_operation(flow, {:map_state, fn acc, index, _ -> mapper.(acc, index); acc end})
  end
  def each_state(flow, mapper) when is_function(mapper, 1) do
    add_operation(flow, {:map_state, fn acc, _, _ -> mapper.(acc); acc end})
  end

  @doc """
  Calculates when to emit a trigger.

  Triggers must be set after partitions and before the call to `reduce/3`.
  """
  @spec trigger(t, (() -> acc), ([term], acc -> trigger)) :: t
        when trigger: {:cont, acc} | {:trigger, name, pre, :keep | :reset, pos, acc},
             name: term(), pre: [event], pos: [event], acc: term(), event: term()
  def trigger(flow, acc_fun, trigger_fun) do
    if is_function(acc_fun, 0) do
      add_operation(flow, {:punctuation, acc_fun, trigger_fun})
    else
      raise ArgumentError, "GenStage.Flow.trigger/3 expects the accumulator to be given as a function"
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
