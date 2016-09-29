alias Experimental.{GenStage, Flow}

defmodule Flow do
  @moduledoc ~S"""
  Computational flows with stages.

  `Flow` allows developers to express computations
  on collections, similar to the `Enum` and `Stream` modules,
  although computations will be executed in parallel using
  multiple `GenStage`s.

  Flow was also designed to work with both bounded (finite)
  and unbounded (infinite) data. By default, Flow will work
  with batches of 500 items. This means Flow will only show
  improvements when working with larger collections. However,
  for certain cases, such as IO-bound flows, a smaller batch size
  can be configured through the `:min_demand` and `:max_demand`
  options supported by `new/2` or `partition/3`.

  Flow also provides the concepts of "windows" and "triggers",
  which allow developers to split the data into arbitrary
  windows according to event time. Triggers allow computations
  to be materialized at different intervals, allowing developers
  to peek at results as they are computed.

  **Note:** this module is currently namespaced under
  `Experimental.Flow`. You will need to `alias Experimental.Flow`
  before writing the examples below.

  ## Example

  As an example, let's implement the classic word counting
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

  Unfortunately, the implementation above is not very efficient,
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
  not leverage concurrency. Flow solves that:

      alias Experimental.Flow
      File.stream!("path/to/some/file")
      |> Flow.from_enumerable()
      |> Flow.flat_map(&String.split(&1, " "))
      |> Flow.partition()
      |> Flow.reduce(fn -> %{} end, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  To convert from Stream to Flow, we have made two changes:

    1. We have replaced the calls to `Stream` with `Flow`
    2. We call `partition/1` so words are properly partitioned between stages

  The example above will use all available cores and will
  keep an ongoing flow of data instead of traversing them
  line by line. Once all data is computed, it is sent to the
  process which invoked `Enum.to_list/1`.

  While we gain concurrency by using Flow, many of the benefits
  of Flow are in partioning the data. We will discuss
  the need for data partioning next.

  ## Partitioning

  To understand the need to partion the data, let's change the
  example above and remove the partition call:

      alias Experimental.Flow
      File.stream!("path/to/some/file")
      |> Flow.from_enumerable()
      |> Flow.flat_map(&String.split(&1, " "))
      |> Flow.reduce(fn -> %{} end, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  This will execute the `flat_map` and `reduce`
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

  Then `reduce/3` will result in each stage having the following state:

      M1 - %{"roses" => 1, "are" => 1, "red" => 1}
      M2 - %{"violets" => 1, "are" => 1, "blue" => 1}

  Which is converted to the list (in no particular order):

      [{"roses", 1},
       {"are", 1},
       {"red", 1},
       {"violets", 1},
       {"are", 1},
       {"blue", 1}]

  Although both stages have performed word counting, we have words
  like "are" that appear on both stages. This means we would need
  to perform yet another pass on the data merging the duplicated
  words across stages.

  Partioning solves this by introducing a new set of stages and
  making sure the same word is always mapped to the same stage
  with the help of a hash function. Let's introduce the call to
  `partition/1` back:

      alias Experimental.Flow
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

  Now, any given word will be consistently routed to `R1` or `R2`
  regardless of its origin. The default hashing function will route
  them like this:

      R1 - ["roses", "are", "red", "are"]
      R2 - ["violets", "blue"]

  Resulting in the reduced state of:

      R1 - %{"roses" => 1, "are" => 2, "red" => 1}
      R2 - %{"violets" => 1, "blue" => 1}

  Which is converted to the list (in no particular order):

      [{"roses", 1},
       {"are", 2},
       {"red", 1},
       {"violets", 1},
       {"blue", 1}]

  Each stage has a distinct subset of the data so we know
  that we don't need to merge the data later on, because a given
  word is guaranteed to have only been routed to one stage.

  Partioning the data is a very useful technique. For example,
  if we wanted to count the number of unique elements in a dataset,
  we could perform such a count in each partition and then sum
  their results, as the partitioning guarantees the data in
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
  stages typically aggregate some result based on their inputs, such
  as how many times a word has appeared. This implies reducer
  computations need to traverse the whole data set and, in order
  to do so in parallel, we partition the data into distinct
  datasets.

  The goal of the `reduce/3` operation is to accumulate a value
  which then becomes the partition state. Any operation that
  happens after `reduce/3` works on the whole state and is only
  executed after all the data for a partition is collected.

  While this approach works well for bounded (finite) data, it
  is quite limited for unbounded (infinite) data. After all, if
  the reduce operation needs to traverse the whole partition to
  complete, how can we do so if the data never finishes?

  To answer this question, we need to talk about data completion,
  triggers and windows.

  ## Data completion, windows and triggers

  By default, Flow uses GenStage's notification system to notify
  stages when a producer has emitted all events. This is done
  automatically by Flow when using `from_enumerable/2`. Custom
  producers can also send such notifications by calling
  `GenStage.async_notification/2` from themselves:

      # In case all the data is done
      GenStage.async_notification(self(), {:producer, :done})

      # In case the producer halted due to an external factor
      GenStage.async_notification(self(), {:producer, :halt})

  However, When working with an unbounded stream of data, there is
  no such thing as data completion. So when can we consider a reduce
  function to be "completed"?

  To handle such cases, Flow provides windows and triggers. Windows
  allow us to split the data based on the event time while triggers
  tells us when to write the results we have computed so far. By
  introducing windows, we no longer think the events are partitioned
  across stages. Instead each event belongs to a window and the window
  is partitioned across the stages.

  By default, all events belong to the same window (called the global
  window) which is partitioned across stages. However, different
  windowing strategies can be used by building a `Flow.Window`
  and passing it to the `Flow.partition/3` function.

  Once a window is specified, we can create triggers that tell us
  when to checkpoint the data, allowing us to report our progress
  while the data streams through the system, regardless if the data
  is bounded or unbounded.

  Windows and triggers effectively control how the `reduce/3` function
  works. `reduce/3` is invoked per window while a trigger configures
  when `reduce/3` halts so we can checkpoint the data before resuming
  the computation with an old or new accumulator. See `Flow.Window`
  for a complete introduction into windows and triggers.

  ## Supervisable flows

  In the examples so far we have started a flow dynamically
  and consumed it using `Enum.to_list/1`. Unfortunately calling
  a function from `Enum` will cause the whole computed dataset
  to be sent to a single process.

  In many situations, this is either too expensive or completely
  undesirable. For example, in data-processing pipelines, it is
  common to receive data continuously from external sources. At
  the end, this data is written to disk or another storage mechanism
  after being processed, rather than being sent to a single process.

  Flow allows computations to be started as a group of processes
  which may run indefinitely. This can be done by starting
  the flow as part of a supervision tree using `Flow.start_link/2`.
  `Flow.into_stages/3` can also be used to start the flow as a
  linked process which will send the events to the given consumers.

  ## Performance discussions

  In this section we will discuss points related to performance
  with flows.

  ### Know your code

  There are many optimizations we could perform in the flow above
  that are not necessarily related to flows themselves. Let's rewrite
  the flow using some of them:

      alias Experimental.Flow

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
      option for file streams allowing us to do fewer IO operations by
      reading large chunks of data at once

    * ETS - the third stores the data in a ETS table and uses its counter
      operations. For counters and a large dataset this provides a great
      performance benefit as it generates less garbage. At the end, we
      call `map_state/2` to transfer the ETS table to the parent process
      and wrap the table in a list so we can access it on `Enum.to_list/1`.
      This step is not strictly required. For example, one could write the
      table to disk with `:ets.tab2file/2` at the end of the computation

  ### Configuration (demand and the number of stages)

  Both `new/2` and `partition/3` allow a set of options to configure
  how flows work. In particular, we recommend that developers play with
  the `:min_demand` and `:max_demand` options, which control the amount
  of data sent between stages. The difference between `max_demand` and
  `min_demand` works as the batch size when the producer is full. If the
  producer has fewer events than requested by consumers, it usually sends the
  remaining events available.

  If stages perform IO, it may also be worth increasing
  the number of stages. The default value is `System.schedulers_online/0`,
  which is a good default if the stages are CPU bound, but if stages
  are waiting on external resources or other processes, increasing the
  number of stages may be helpful.

  ### Avoid single sources

  In the examples so far we have used a single file as our data
  source. In practice such should be avoided as the source could
  end up being the bottleneck of our whole computation.

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

  If the number of enumerables is equal to or greater than the number of
  cores, Flow will automatically fuse the enumerables with the mapper
  logic. For example, if three file streams are given as enumerables
  to a machine with two cores, we will have the following topology:

      [F1][F2][F3]  # file stream
      [M1][M2][M3]  # Flow.flat_map/2 (producer)
        |\ /\ /|
        | /\/\ |
        |//  \\|
        [R1][R2]    # Flow.reduce_by_key/2 (consumer)

  """

  defstruct producers: nil, window: nil, options: [], operations: []
  @type t :: %Flow{producers: producers, operations: [operation],
                   options: keyword(), window: Flow.Window.t}

  @typep producers :: nil |
                      {:stages, GenStage.stage} |
                      {:enumerables, Enumerable.t} |
                      {:join, t, t, fun(), fun(), fun()}

  @typep operation :: {:mapper, atom(), [term()]} |
                      {:partition, keyword()} |
                      {:map_state, fun()} |
                      {:reduce, fun(), fun()} |
                      {:window, Flow.Window.t}

  ## Building

  @doc """
  Starts a flow with the given enumerable as producer.

  Calling this function is equivalent to:

      Flow.from_enumerable([enumerable], options)

  ## Examples

      "some/file"
      |> File.stream!(read_ahead: 100_000)
      |> Flow.from_enumerable()

  """
  @spec from_enumerable(Enumerable.t, keyword) :: t
  def from_enumerable(enumerable, options \\ []) do
    from_enumerables([enumerable], options)
  end

  @doc """
  Starts a flow with the given enumerable as producer.

  See `GenStage.from_enumerable/2` for information and
  limitations on enumerable-based stages.

  ## Options

  These options configure the stages connected to producers before partitioning.

    * `:window` - a window to run the next stages on, see `Flow.Window`
    * `:stages` - the number of stages
    * `:buffer_keep` - how the buffer should behave, see `c:GenStage.init/1`
    * `:buffer_size` - how many events to buffer, see `c:GenStage.init/1`

  All remaining options are sent during subscription, allowing developers
  to customize `:min_demand`, `:max_demand` and others.

  ## Examples

      files = [File.stream!("some/file1", read_ahead: 100_000),
               File.stream!("some/file2", read_ahead: 100_000),
               File.stream!("some/file3", read_ahead: 100_000)]
      Flow.from_enumerables(files)
  """
  @spec from_enumerables([Enumerable.t], keyword) :: t
  def from_enumerables(enumerables, options \\ [])

  def from_enumerables([_ | _] = enumerables, options) do
    {window, options} = Keyword.pop(options, :window, Flow.Window.global)
    %Flow{options: options, window: window, producers: {:enumerables, enumerables}}
  end
  def from_enumerables(enumerables, _options) do
    raise ArgumentError, "from_enumerables/2 expects a non-empty list as argument, got: #{inspect enumerables}"
  end

  @doc """
  Starts a flow with the given stage as producer.

  Calling this function is equivalent to:

      Flow.from_stages([stage], options)

  See `from_stages/2` for more information.

  ## Examples

      Flow.from_stage(MyStage)

  """
  @spec from_stage(GenStage.stage, keyword) :: t
  def from_stage(stage, options \\ []) do
    from_stages([stage], options)
  end

  @doc """
  Starts a flow with the list of stages as producers.

  ## Options

  These options configure the stages connected to producers before partitioning.

    * `:window` - a window to run the next stages on, see `Flow.Window`
    * `:stages` - the number of stages
    * `:buffer_keep` - how the buffer should behave, see `c:GenStage.init/1`
    * `:buffer_size` - how many events to buffer, see `c:GenStage.init/1`

  All remaining options are sent during subscription, allowing developers
  to customize `:min_demand`, `:max_demand` and others.

  ## Examples

      stages = [pid1, pid2, pid3]
      Flow.from_stages(stages)

  ## Termination

  Producer stages can signal the flow that it has emitted all
  events by emitting a notication using `GenStage.async_notification/2`
  from themselves:

      # In case all the data is done
      GenStage.async_notification(self(), {:producer, :done})

      # In case the producer halted due to an external factor
      GenStage.async_notification(self(), {:producer, :halt})

  Your producer may also keep track of all consumers and automatically
  shut down when all consumers have exited.
  """
  @spec from_stages([GenStage.stage], keyword) :: t
  def from_stages(stages, options \\ [])

  def from_stages([_ | _] = stages, options) do
    {window, options} = Keyword.pop(options, :window, Flow.Window.global)
    %Flow{options: options, window: window, producers: {:stages, stages}}
  end
  def from_stages(stages, _options) do
    raise ArgumentError, "from_stages/2 expects a non-empty list as argument, got: #{inspect stages}"
  end

  @joins [:inner, :left_outer, :right_outer, :full_outer]

  @doc """
  Joins two bounded (finite) flows.

  It expects the `left` and `right` flow, the `left_key` and
  `right_key` to calculate the key for both flows and the `join`
  function which is invoked whenever there is a match.

  A join creates a new partitioned flow that subscribes to the
  two flows given as arguments.  The newly created partitions
  will accumulate the data received from both flows until there
  is no more data. Therefore, this function is useful for merging
  finite flows. If used for merging infinite flows, you will
  eventually run out of memory due to the accumulated data. See
  `window_join/8` for applying a window to a join, allowing the
  join data to be reset per window.

  The join has 4 modes:

    * `:inner` - data will only be emitted when there is a match
      between the keys in left and right side
    * `:left_outer` - similar to `:inner` plus all items given
      in the left that did not have a match will be emitted at the
      end with `nil` for the right value
    * `:right_outer` - similar to `:inner` plus all items given
      in the right that did not have a match will be emitted at the
      end with `nil` for the left value
    * `:full_outer` - similar to `:inner` plus all items given
      in the left and right that did not have a match will be emitted
      at the end with `nil` for the right and left value respectively

  The joined partitions can be configured via `options` with the
  same values as shown on `new/1`.

  ## Examples

      iex> posts = [%{id: 1, title: "hello"}, %{id: 2, title: "world"}]
      iex> comments = [{1, "excellent"}, {1, "outstanding"},
      ...>             {2, "great follow up"}, {3, "unknown"}]
      iex> flow = Flow.bounded_join(:inner,
      ...>                          Flow.from_enumerable(posts),
      ...>                          Flow.from_enumerable(comments),
      ...>                          & &1.id, # left key
      ...>                          & elem(&1, 0), # right key
      ...>                          fn post, {_post_id, comment} -> Map.put(post, :comment, comment) end)
      iex> Enum.sort(flow)
      [%{id: 1, title: "hello", comment: "excellent"},
       %{id: 2, title: "world", comment: "great follow up"},
       %{id: 1, title: "hello", comment: "outstanding"}]

  """
  @spec bounded_join(:inner | :left_outer | :right_outer | :outer, t, t,
                     fun(), fun(), fun(), keyword()) :: t
  def bounded_join(mode, %Flow{} = left, %Flow{} = right,
                   left_key, right_key, join, options \\ [])
      when is_function(left_key, 1) and is_function(right_key, 1) and
           is_function(join, 2) and mode in @joins do
    window_join(mode, left, right, Flow.Window.global, left_key, right_key, join, options)
  end

  @doc """
  Joins two flows with the given window.

  It is similar to `bounded_join/7` with the addition a window
  can be given. The window function applies to elements of both
  left and right side in isolation (and not the joined value). A
  trigger will cause the join state to be cleared.

  ## Examples

  As an example, let's expand the example given in `bounded_join/7`
  and apply a window to it. The example in `bounded_join/7` returned
  3 results but in this example, because we will split the posts
  and comments in two different windows, we will get only two results
  as the later comment for `post_id=1` won't have a matching comment for
  its window:

      iex> posts = [%{id: 1, title: "hello", timestamp: 0}, %{id: 2, title: "world", timestamp: 1000}]
      iex> comments = [{1, "excellent", 0}, {1, "outstanding", 1000},
      ...>             {2, "great follow up", 1000}, {3, "unknown", 1000}]
      iex> window = Flow.Window.fixed(1, :seconds, fn
      ...>   {_, _, timestamp} -> timestamp
      ...>   %{timestamp: timestamp} -> timestamp
      ...> end)
      iex> flow = Flow.window_join(:inner,
      ...>                         Flow.from_enumerable(posts),
      ...>                         Flow.from_enumerable(comments),
      ...>                         window,
      ...>                         & &1.id, # left key
      ...>                         & elem(&1, 0), # right key
      ...>                         fn post, {_post_id, comment, _ts} -> Map.put(post, :comment, comment) end,
      ...>                         stages: 1, max_demand: 1)
      iex> Enum.sort(flow)
      [%{id: 1, title: "hello", comment: "excellent", timestamp: 0},
       %{id: 2, title: "world", comment: "great follow up", timestamp: 1000}]

  """
  @spec window_join(:inner | :left_outer | :right_outer | :outer, t, t, Flow.Window.t,
                    fun(), fun(), fun(), keyword()) :: t
  def window_join(mode, %Flow{} = left, %Flow{} = right, %{} = window,
                  left_key, right_key, join, options \\ [])
      when is_function(left_key, 1) and is_function(right_key, 1) and
           is_function(join, 2) and mode in @joins do
    %Flow{producers: {:join, mode, left, right, left_key, right_key, join},
          options: options, window: window}
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
  def run(flow) do
    [] = flow |> emit(:nothing) |> Enum.to_list()
    :ok
  end

  @doc """
  Starts and runs the flow as a separate process.

  See `into_stages/3` in case you want the flow to
  work as a producer for another series of stages.

  ## Options

    * `:dispatcher` - the dispatcher responsible for handling demands.
      Defaults to `GenStage.DemandDispatch`. May be either an atom or
      a tuple with the dispatcher and the dispatcher options

    * `:demand` - configures the demand on the flow producers to `:forward`
      or `:accumulate`. The default is `:forward`. See `GenStage.demand/2`
      for more information.

  """
  @spec start_link(t, keyword()) :: GenServer.on_start
  def start_link(flow, options \\ []) do
    GenServer.start_link(Flow.Coordinator, {emit(flow, :nothing), :consumer, [], options}, options)
  end

  @doc """
  Starts and runs the flow as a separate process which
  will be a producer to the given `consumers`.

  It expects a list of consumers to subscribe to. Each element
  represents the consumer or a tuple with the consumer and the
  subscription options as defined in `GenStage.sync_subscribe/2`.

  Receives the same options as `start_link/2`.
  """
  @spec into_stages(t, consumers, keyword()) :: GenServer.on_start when
        consumers: [GenStage.stage | {GenStage.stage, keyword()}]
  def into_stages(flow, consumers, options \\ []) do
    GenServer.start_link(Flow.Coordinator, {flow, :producer_consumer, consumers, options}, options)
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
  Creates a new partition for the given flow with the given options

  Every time this function is called, a new partition
  is created. It is typically recommended to invoke it
  before a reducing function, such as `reduce/3`, so data
  belonging to the same partition can be kept together.

  ## Examples

      flow |> Flow.partition(window: Flow.Global.window)
      flow |> Flow.partition(stages: 4)

  ## Options

    * `:window` - a `Flow.Window` struct which controls how the
       reducing function behaves, see `Flow.Window` for more information.
    * `:stages` - the number of partitions (reducer stages)
    * `:hash` - the hash to use when partitioning. It is a function
      that receives a single argument: the event to partition on. However, to
      facilitate customization, `:hash` also allows common values, such
      `{:elem, 0}`, to specify the hash should be calculated on the first
      element of a tuple. See more information on the "Hash shortcuts" section
      below. The default value hashing function `&:erlang.phash2(&1, stages)`.
    * `:dispatcher` - by default, `partition/3` uses `GenStage.PartitionDispatcher`
      with the given hash function but any other dispatcher can be given

  ## Hash shortcuts

  The following shortcuts can be given to the `:hash` option:

    * `{:elem, pos}` - apply the hash function to the element
      at position `pos` in the given tuple

    * `{:key, key}` - apply the hash function to the key of a given map

  """
  @spec partition(t, keyword()) :: t
  def partition(flow, options \\ []) when is_list(options) do
    merge([flow], options)
  end

  @doc """
  Merges the given flow into a new partition with the given
  window and options.

  Every time this function is called, a new partition
  is created. It is typically recommended to invoke it
  before a reducing function, such as `reduce/3`, so data
  belonging to the same partition can be kept together.

  It accepts the same options and hash shortcuts as
  `partition/3`. See `partition/3` for more information.

  ## Examples

      Flow.merge([flow1, flow2], window: Flow.Global.window)
      Flow.merge([flow1, flow2], stages: 4)

  """
  @spec merge([t], keyword()) :: t
  def merge(flows, options \\ [])

  def merge([%Flow{} | _] = flows, options) when is_list(options) do
    {options, stages} = stages(options)
    options =
      case Keyword.fetch(options, :hash) do
        {:ok, hash} -> Keyword.put(options, :hash, hash(hash, stages))
        :error -> options
      end
    {window, options} = Keyword.pop(options, :window, Flow.Window.global)
    %Flow{producers: {:flows, flows}, options: options, window: window}
  end
  def merge(other, options) when is_list(options) do
    raise ArgumentError, "Flow.merge/3 expects a non-empty list of flows as first argument, got: #{inspect other}"
  end

  defp stages(options) do
    case Keyword.fetch(options, :stages) do
      {:ok, stages} ->
        {options, stages}
      :error ->
        stages = System.schedulers_online()
        {[stages: stages] ++ options, stages}
    end
  end

  defp hash(fun, _) when is_function(fun, 1) do
    fun
  end
  defp hash({:elem, pos}, stages) when pos >= 0 do
    pos = pos + 1
    &{&1, :erlang.phash2(:erlang.element(pos, &1), stages)}
  end
  defp hash({:key, key}, stages) do
    &{&1, :erlang.phash2(Map.fetch!(&1, key), stages)}
  end
  defp hash(other, _) do
    raise ArgumentError, """
    expected :hash to be one of:

      * a function expecting a single argument and returning
        the event and the partition in a tuple
      * {:elem, pos} when pos >= 0
      * {:key, key}

    instead got: #{inspect other}
    """
  end

  @doc """
  Reduces the given values with the given accumulator.

  `acc_fun` is a function that receives no arguments and returns
  the actual accumulator. The `acc_fun` function is invoked per window
  whenever a new window starts. If a trigger is emitted and it is
  configured to reset the accumulator, the `acc_fun` function will
  be invoked once again.

  Reducing will accumulate data until the a trigger is emitted
  or until a window completes. When that happens, the returned
  accumulator will be the new state of the stage and all functions
  after reduce will be invoked.

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
      raise ArgumentError, "Flow.reduce/3 expects the accumulator to be given as a function"
    end
  end

  @doc """
  Only emit unique events.

  Calling this function is equivalent to:

      Flow.uniq_by(flow, & &1)

  See `uniq_by/2` for more information.
  """
  def uniq(flow) do
    uniq_by(flow, & &1)
  end

  @doc """
  Only emit events that are unique according to the `by` function.

  In order to verify if an item is unique or not, `uniq_by/2`
  must store the value computed by `by/1` into a set. This means
  that, when working with unbounded data, it is recommended to
  wrap `uniq_by/2` in a window otherwise the data set will grow
  forever, eventually using all memory available.

  Also keep in mind that `uniq_by/2` is applied per partition.
  Therefore, if the data is not uniquely divided per partition,
  it won't be able to calculate the unique items properly.

  ## Examples

  To get started, let's create a flow that emits only the first
  odd and even number for a range:

      iex> flow = Flow.from_enumerable(1..100)
      iex> flow = Flow.partition(flow, stages: 1)
      iex> flow |> Flow.uniq_by(&rem(&1, 2)) |> Enum.sort()
      [1, 2]

  Since we have used only one stage when partitioning, we
  correctly calculate `[1, 2]` for the given partition. Let's see
  what happens when we increase the number of stages in the partition:

      iex> flow = Flow.from_enumerable(1..100)
      iex> flow = Flow.partition(flow, stages: 4)
      iex> flow |> Flow.uniq_by(&rem(&1, 2)) |> Enum.sort()
      [1, 2, 3, 4, 10, 16, 23, 39]

  Now we got 8 numbers, one odd and one even *per partition*. If
  we want to compute the unique items per partition, we must properly
  hash the events into two distinct partitions, one for odd numbers
  and another for even numbers:

      iex> flow = Flow.from_enumerable(1..100)
      iex> flow = Flow.partition(flow, stages: 2, hash: fn event -> {event, rem(event, 2)} end)
      iex> flow |> Flow.uniq_by(&rem(&1, 2)) |> Enum.sort()
      [1, 2]
  """
  @spec uniq_by(t, (term -> term)) :: t
  def uniq_by(flow, by) when is_function(by, 1) do
    add_operation(flow, {:uniq, by})
  end

  @doc """
  Controls which values should be emitted from now.

  It can either be `:events` (the default) or the current
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
  def emit(%{operations: operations} = flow, :nothing) do
    case inject_to_nothing(operations) do
      :map_state -> map_state(flow, fn _, _, _ -> [] end)
      :reduce -> reduce(flow, fn -> [] end, fn _, acc -> acc end)
    end
  end
  def emit(_, emit) do
    raise ArgumentError, "unknown option for emit: #{inspect emit}"
  end

  defp inject_to_nothing([{:reduce, _, _} | _]), do: :map_state
  defp inject_to_nothing([_ | ops]), do: inject_to_nothing(ops)
  defp inject_to_nothing([]), do: :reduce

  @doc """
  Applies the given function over the window state.

  This function must be called after `reduce/3` as it maps over
  the state accumulated by `reduce/3`. `map_state/2` is invoked
  per window on every stage whenever there is a trigger: this
  gives us an understanding of the window data while leveraging
  the parallelism between states.

  The `mapper` function may have arity 1, 2 or 3:

    * when one - the state is given as argument
    * when two - the state and the stage indexes are given as arguments.
      The index is a tuple with the current stage index as first element
      and the total number of stages for this partition as second
    * when three - the state, the stage indexes and a tuple with window-
      trigger parameters are given as argument. The tuple contains the
      window type, the window identifier and the trigger name. By default,
      the window is `:global`, which implies the `:global` identifier with
      a default trigger of `:done`, emitted when there is no more data to
      process.

  The value returned by this function is passed forward to the upcoming
  flow functions.

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
  @spec map_state(t, (term -> term) |
                     (term, term -> term) |
                     (term, term, {Flow.Window.type, Flow.Window.id, Flow.Window.trigger} -> term)) :: t
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
  @spec each_state(t, (term -> term) |
                      (term, term -> term) |
                      (term, term, {Flow.Window.type, Flow.Window.id, Flow.Window.trigger} -> term)) :: t
  def each_state(flow, mapper) when is_function(mapper, 3) do
    add_operation(flow, {:map_state, fn acc, index, trigger -> mapper.(acc, index, trigger); acc end})
  end
  def each_state(flow, mapper) when is_function(mapper, 2) do
    add_operation(flow, {:map_state, fn acc, index, _ -> mapper.(acc, index); acc end})
  end
  def each_state(flow, mapper) when is_function(mapper, 1) do
    add_operation(flow, {:map_state, fn acc, _, _ -> mapper.(acc); acc end})
  end

  defp add_operation(%Flow{operations: operations} = flow, operation) do
    %{flow | operations: [operation | operations]}
  end
  defp add_operation(flow, _producers) do
    raise ArgumentError, "expected a flow as argument, got: #{inspect flow}"
  end

  defimpl Enumerable do
    def reduce(flow, acc, fun) do
      {producers, consumers} =
        Flow.Materialize.materialize(flow, &GenStage.start_link/3, :producer_consumer, [])
      pids = for {pid, _} <- producers, do: pid
      GenStage.stream(consumers, producers: pids).(acc, fun)
    end

    def count(_flow) do
      {:error, __MODULE__}
    end

    def member?(_flow, _value) do
      {:error, __MODULE__}
    end
  end
end
