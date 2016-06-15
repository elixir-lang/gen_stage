defmodule GenStage do
  @moduledoc """
  Stages are computation steps that send and/or receive data
  from other stages.

  When a stage sends data, it acts as a producer. When it receives
  data, it acts as a consumer. Stages may take both producer and
  consumer roles at once.

  ## Stage types

  Besides taking both producer and consumer roles, a stage may be
  called "source" if it only produces items or called "sink" if it
  only consumes items.

  For example, imagine the stages below where A sends data to B
  that sends data to C:

      [A] -> [B] -> [C]

  we conclude that:

    * A is only a producer (and therefore a source)
    * B is both producer and consumer
    * C is only consumer (and therefore a sink)

  As we will see in the upcoming Examples section, we must
  specify the type of the stage when we implement each of them.

  To start the flow of events, we subscribe consumers to
  producers. Once the communication channel between them is
  established, consumers will ask the producers for events.
  We typically say the consumer is sending demand upstream.
  Once demand arrives, the producer will emit items, never
  emitting more items than the consumer asked for. This provides
  a back-pressure mechanism.

  Typically, a consumer may subscribe only to a single producer
  which by default establishes a one-to-one relationship.
  GenBroker lifts this restriction by allowing M producers to
  connect N consumers using different strategies.

  ## Example

  Let's define the simple pipeline below:

      [A] -> [B] -> [C]

  where A is a producer that will emit items starting from 0,
  B is a producer-consumer that will receive those items and
  multiply them by a given number and C will receive those events
  and print them to the terminal.

  Let's start with A. Since A is a producer, its main
  responsibility is to receive demand and generate events.
  Those events may be in memory or an external queue system.
  For simplicity, let's implement a simple counter starting
  from a given value of `counter` received on `init/1`:

      defmodule A do
        use GenStage

        def init(counter) do
          {:producer, counter}
        end

        def handle_demand(demand, counter) when demand > 0 do
          # If the counter is 3 and we ask for 2 items, we will
          # emit the items 3 and 4, and set the state to 5.
          events = Enum.to_list(counter..counter+demand-1)
          {:noreply, events, counter + demand}
        end
      end

  B is a producer-consumer. This means it does not explicitly
  handle the demand because the demand is always forwarded to
  its producer. Once A receives the demand from B, it will send
  events to B which will be transformed by B as desired. In
  our case, B will receive events and multiply them by a number
  giving on initialization and stored as the state:

      defmodule B do
        use GenStage

        def init(number) do
          {:producer_consumer, number}
        end

        def handle_events(events, from, number) do
          events = Enum.map(events, & &1 * number)
          {:noreply, events, number}
        end
      end

  C will finally receive those events and print them every second
  to the terminal:

      defmodule C do
        use GenStage

        def init(:ok) do
          {:consumer, :the_state_does_not_matter}
        end

        def handle_events(events, _from, state) do
          # Wait for a second.
          :timer.sleep(1000)

          # Inspect the events.
          IO.inspect(events)

          # We are a consumer, so we would never emit items.
          {:noreply, [], state}
        end
      end

  Now we can start and connect them:

      {:ok, a} = GenStage.start_link(A, 0)   # starting from zero
      {:ok, b} = GenStage.start_link(B, 2)   # multiply by 2
      {:ok, c} = GenStage.start_link(C, :ok) # state does not matter

      GenStage.sync_subscribe(c, to: b)
      GenStage.sync_subscribe(b, to: a)

  After you subscribe all of them, demand will start flowing
  upstream and events downstream. Because C blocks for one
  second, the demand will eventually be adjusted to C needs.
  When implementing consumers, we often set the `:max_demand` and
  `:min_demand` options on initialization. The `:max_demand`
  specifies the maximum amount of events that must be in flow
  while the `:min_demand` specifies the minimum threshold to
  trigger for more demand. For example, if `:max_demand` is 100
  and `:min_demand` is 50 (the default values), the consumer will
  ask for 100 events initially and ask for more only after it
  receives at least 50.

  When such values are applied to the stages above, it is easy
  to see the producer works in bursts. The producer A ends-up
  emitting batches of 50 items which will take approximately
  50 seconds to be consumed by C, which will then request another
  batch of 50 items.

  ## Overflown events

  Due to the concurrent nature of Elixir software, sometimes
  a producer may receive events without consumers to send those
  events to. For example, imagine a consumer C subscribes to
  producer B. Next, the consumer C sends demand to B, which sends
  the demand upstreams. Now, if the consumer C crashes, B may
  receive the events from upstream but it no longer has a consumer
  to send those events to. In such cases, B will buffer the events
  which have overflown from upstream.

  The overflow buffer can also be used in cases external sources
  only send events in batches larger than asked for. For example,
  if you are receiving events from an external source that only
  sends events in batches of 100 in 100 and the internal demand
  is smaller than that.

  In all of those cases, if the message cannot be sent immediately,
  it is stored and sent whenever there is an opportunity to. The
  number of overflown events that can be buffered is customized
  via the `:max_overflow` option returned by `init/1`. The default
  value is 1000.

  ## Streams

  After exploring the example above, you may be thinking that's
  a lot of code for something that could be expressed with streams.
  For example:

      Stream.iterate(0, fn i -> i + 1 end)
      |> Stream.map(fn i -> i * 2 end)
      |> Stream.each(&IO.inspect/1)
      |> Stream.run()

  The example above would print the same values as our stages with
  the difference the stream above is not leveraging concurrency.
  We can, however, break the stream into multiple stages too:

      Stream.iterate(0, fn i -> i + 1 end)
      |> Stream.async_stage()
      |> Stream.map(fn i -> i * 2 end)
      |> Stream.async_stage()
      |> Stream.each(&IO.inspect/1)
      |> Stream.run()

  Now each step runs into its own stage, using the same primitives
  as we have discussed above. While using streams may be helpful in
  many occasions, there are many reasons why someone would use
  GenStage, some being:

    * Stages provide a more structured approach by breaking into
      stage into a separate module
    * Stages provide all callbacks necessary for process management
      (init, terminate, etc)
    * Stages can be hot-code upgraded
    * Stages can be supervised individually

  ## Callbacks

  `GenStage` is implemented on top of a `GenServer` with two additions.
  Besides exposing all of the `GenServer` callbacks, it also provides
  `handle_demand/2` to be implemented by producers and `handle_events/3`
  to be implemented by consumers, as shown above. Futhermore, all the
  callback responses have been modified to potentially emit events.
  See the callbacks documentation for more information.

  By adding `use GenStage` to your module, Elixir will automatically
  define all callbacks for you except the following:

    * `init/1` - must be implemented to choose between `:producer`, `:consumer` or `:producer_consumer`
    * `handle_demand/2` - must be implemented by `:producer` types
    * `handle_events/3` - must be implemented by `:producer_consumer` and `:consumer` types

  Although this module exposes functions similar to the ones found in
  the `GenServer` API, like `call/3` and `cast/2`, developers can also
  rely directly on GenServer functions such as `GenServer.multi_call/4`
  and `GenServer.abcast/3` if they wish to.

  ### Name Registration

  `GenStage` is bound to the same name registration rules as a `GenServer`.
  Read more about it in the `GenServer` docs.

  ## Message-protocol overview

  This section will describe the message-protocol implemented
  by stages. By documenting these messages, we will allow
  developers to provide their own stage implementations.

  ### Back-pressure

  When data is sent between stages, it is done by a message
  protocol that provides back-pressure. The first step is
  for the consumer to subscribe to the producer. Each
  subscription has a unique reference.

  Once subscribed, the consumer may ask the producer for messages
  for the given subscription. The consumer may demand more items
  whenever it wants to. A consumer must never receive more data
  than it has asked for from any given producer stage.

  A consumer may have multiple producers, where each demand is
  managed invidually. A producer may have multiple consumers,
  where the demand and events are managed and delivered according
  to a `GenStage.Dispatcher` implementation.

  ### Producer messages

  The producer is responsible for sending events to consumers
  based on demand.

    * `{:"$gen_producer", from :: {consumer_pid, subscription_ref}, {:subscribe, options}}` -
      sent by the consumer to the producer to start a new subscription.

      Before sending, the consumer MUST monitor the producer for clean-up
      purposes in case of crashes. The `subscription_ref` is unique to
      identify the subscription (and may be the monitoring reference).

      Once sent, the consumer MAY immediately send demand to the producer.
      The `subscription_ref` is unique to identify the subscription.

      Once received, the producer MUST monitor the consumer and call
      call `dispatcher.subscribe(from, state)`. However, if the subscription
      reference is known, it must send a `:cancel` message to the consumer.

    * `{:"$gen_producer", from :: {consumer_pid, subscription_ref}, {:cancel, reason}}` -
      sent by the consumer to cancel a given subscription.

      Once received, the producer MUST call `dispatcher.cancel(from, state)`
      upon receival and discard the subscription.

    * `{:"$gen_producer", from :: {consumer_pid, subscription_ref}, {:ask, count}}` -
      sent by consumers to ask data in a given subscription.

      Once received, the producer MUST call `dispatcher.ask(count, from, state)`
      if one is available. The producer MUST send data up to the demand. If the
      pair is unknown, the producer MUST send an appropriate disconnect reply.

  ### Consumer messages

  The consumer is responsible for starting the subscription
  and sending demand to producers.

    * `{:"$gen_consumer", from :: {producer_pid, subscription_ref}, {:cancel, reason}}` -
      sent by producers to cancel a given subscription.

      It is used as a confirmation for client disconnects OR whenever
      the producer wants to cancel some upstream demand.

    * `{:"$gen_consumer", from :: {producer_pid, subscription_ref}, [event]}` -
      events sent by producers to consumers.

      `subscription_ref` identifies the subscription. The third argument
      is a non-empty list of events. If the subscription is unknown, the
      events must be ignored and a cancel message sent to the producer.

  """

  defstruct [:mod, :state, :type, :demand, :dispatcher_mod, :dispatcher_state, :overflow,
             monitors: %{}, producers: %{}, consumers: %{}]

  # TODO: Explore termination
  # TODO: Explore end-of-batch/end-of-window signaling
  # TODO: Explore filtering at the data source

  @typedoc "The supported stage types."
  @type type :: :producer | :consumer | :producer_consumer

  @typedoc "The supported init options"
  @type options :: []

  @typedoc "The stage reference"
  @type stage :: pid | atom | {:global, term} | {:via, module, term} | {atom, node}

  @doc """
  Invoked when the server is started.

  `start_link/3` (or `start/3`) will block until it returns. `args`
  is the argument term (second argument) passed to `start_link/3`.

  In case of successful start, this callback must return a tuple
  where the first element is the stage type, which is either
  a `:producer`, `:consumer` or `:producer_consumer` if it is
  taking both roles.

  For example:

      def init(args) do
        {:producer, some_state}
      end

  The returned tuple may also contain 3 or 4 elements. The third
  element may be a timeout value as integer, the `:hibernate` atom
  or a set of options defined below.

  Returning `:ignore` will cause `start_link/3` to return `:ignore`
  and the process will exit normally without entering the loop or
  calling `terminate/2`.

  Returning `{:stop, reason}` will cause `start_link/3` to return
  `{:error, reason}` and the process to exit with reason `reason`
  without entering the loop or calling `terminate/2`.

  ## Options

  This callback may return options. Some options are specific to
  the stage type while others are shared across all types.

  ### :producer options

    * `:max_overflow` - the size of the buffer to store overflown
      events. Check the "Overflown events" section on the module
      documentation (defaults to 1000)
    * `:dispatcher` - the dispatcher responsible for handling demands.
      Defaults to `GenStage.DemandDispatch`.

  ### :producer_consumer options

    * `:max_overflow` - the size of the buffer to store overflown
      events. Check the "Overflown events" section on the module
      documentation (defaults to 1000)
    * `:dispatcher` - the dispatcher responsible for handling demands.
      Defaults to `GenStage.DemandDispatch`.

  ### :consumer options

    * `:max_demand` - the maximum demand desired to send upstream
      (defaults to 100)
    * `:min_demand` - the minimum demand that when reached triggers
      more demand upstream (defaults to half of `:max_demand`)

  """
  @callback init(args :: term) ::
    {type, state} |
    {type, state, options} |
    {type, state, timeout | :hibernate} |
    {type, state, timeout | :hibernate, options} |
    :ignore |
    {:stop, reason :: any} when state: any

  @doc """
  Used by :producer types.
  """
  @callback handle_demand(demand :: pos_integer, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, timeout | :hibernate} |
    {:stop, reason, new_state} when new_state: term, reason: term, event: term

  @doc """
  Used by :producer_consumer and :consumer types.
  """
  @callback handle_events([event], GenServer.from, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, timeout | :hibernate} |
    {:stop, reason, new_state} when new_state: term, reason: term, event: term

  @callback handle_call(request :: term, GenServer.from, state :: term) ::
    {:reply, reply, [event], new_state} |
    {:reply, reply, [event], new_state, timeout | :hibernate} |
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term, event: term

  @callback handle_cast(request :: term, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, timeout | :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term, event: term

  @callback handle_info(msg :: :timeout | term, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, timeout | :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term, event: term

  @callback terminate(reason, state :: term) ::
    term when reason: :normal | :shutdown | {:shutdown, term} | term

  @callback code_change(old_vsn, state :: term, extra :: term) ::
    {:ok, new_state :: term} |
    {:error, reason :: term} when old_vsn: term | {:down, term}

  @optional_callbacks [handle_demand: 2, handle_events: 3]

  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour GenServer

      @doc false
      def handle_call(msg, _from, state) do
        # We do this to trick Dialyzer to not complain about non-local returns.
        reason = {:bad_call, msg}
        case :erlang.phash2(1, 1) do
          0 -> exit(reason)
          1 -> {:stop, reason, state}
        end
      end

      @doc false
      def handle_info(_msg, state) do
        {:noreply, [], state}
      end

      @doc false
      def handle_cast(msg, state) do
        # We do this to trick Dialyzer to not complain about non-local returns.
        reason = {:bad_cast, msg}
        case :erlang.phash2(1, 1) do
          0 -> exit(reason)
          1 -> {:stop, reason, state}
        end
      end

      @doc false
      def terminate(_reason, _state) do
        :ok
      end

      @doc false
      def code_change(_old, state, _extra) do
        {:ok, state}
      end

      defoverridable [handle_call: 3, handle_info: 2,
                      handle_cast: 2, terminate: 2, code_change: 3]
    end
  end

  @doc """
  Starts a `GenStage` process linked to the current process.

  This is often used to start the `GenStage` as part of a supervision tree.

  Once the server is started, the `init/1` function of the given `module` is
  called with `args` as its arguments to initialize the stage. To ensure a
  synchronized start-up procedure, this function does not return until `init/1`
  has returned.

  Note that a `GenStage` started with `start_link/3` is linked to the
  parent process and will exit in case of crashes from the parent. The GenStage
  will also exit due to the `:normal` reasons in case it is configured to trap
  exits in the `init/1` callback.

  ## Options

    * `:name` - used for name registration as described in the "Name
      registration" section of the module documentation

    * `:timeout` - if present, the server is allowed to spend the given amount of
      milliseconds initializing or it will be terminated and the start function
      will return `{:error, :timeout}`

    * `:debug` - if present, the corresponding function in the [`:sys`
      module](http://www.erlang.org/doc/man/sys.html) is invoked

    * `:spawn_opt` - if present, its value is passed as options to the
      underlying process as in `Process.spawn/4`

  ## Return values

  If the server is successfully created and initialized, this function returns
  `{:ok, pid}`, where `pid` is the pid of the server. If a process with the
  specified server name already exists, this function returns
  `{:error, {:already_started, pid}}` with the pid of that process.

  If the `init/1` callback fails with `reason`, this function returns
  `{:error, reason}`. Otherwise, if it returns `{:stop, reason}`
  or `:ignore`, the process is terminated and this function returns
  `{:error, reason}` or `:ignore`, respectively.
  """
  @spec start_link(module, any, options) :: GenServer.on_start
  def start_link(module, args, options \\ []) when is_atom(module) and is_list(options) do
    GenServer.start_link(__MODULE__, {module, args}, options)
  end

  @doc """
  Starts a `GenStage` process without links (outside of a supervision tree).

  See `start_link/3` for more information.
  """
  @spec start(module, any, options) :: GenServer.on_start
  def start(module, args, options \\ []) when is_atom(module) and is_list(options) do
    GenServer.start(__MODULE__, {module, args}, options)
  end

  @doc """
  Asks the stage to subscribe to the given producer stage synchronously.

  This call is synchronous and will return after the called stage
  sends the subscribe message to the producer. It does not, however,
  guarantee a subscription: for example, the producer stage may
  refuse the subscription or exit before or after receiving the
  message.

  This function will return `:ok` as long as the subscription message
  is sent. It may return `{:error, :not_a_consumer}` in case the stage
  is not a consumer or `{:error, :noproc}` in case the process we are
  trying to subscribe to no longer exists.

  ## Options

    * `:persistent` - when true, any cancel message or crash from the
      producer stage will lead the downstream stage to exit. In case
      of crashes, the same reason is used. In case of cancellations,
      the error is wrapped in a `:cancel` tuple.

  All other options are sent as is to the producer stage.
  """
  @spec sync_subscribe(stage, opts :: keyword(), timeout) ::
        :ok | {:error, :producer} | {:error, :noproc}
  def sync_subscribe(stage, opts, timeout \\ 5_000) do
    {to, opts} =
      Keyword.pop_lazy(opts, :to, fn ->
        raise ArgumentError, "expected :to argument in subscribe"
      end)
    GenServer.call(stage, {:"$subscribe", to, opts}, timeout)
  end

  @doc """
  Asks the stage to subscribe to the given producer stage asynchronously.

  This call returns `:ok` regardless if the subscription
  effectively happened or not. It is typically called from
  a stage own's `init/1` callback using the `persisted: true`
  option.

  ## Options

    * `:persistent` - when true, any cancel message or crash from the
      producer stage will lead the downstream stage to exit. In case
      of crashes, the same reason is used. In case of cancellations,
      the error is wrapped in a `:cancel` tuple.

  All other options are sent as is to the producer stage.

  ## Examples

      def init(producer) do
        GenStage.async_subscribe(self(), to: producer, persistent: true)
        {:consumer, []}
      end

  """
  @spec async_subscribe(stage, opts :: keyword()) :: :ok
  def async_subscribe(stage, opts) do
    {to, opts} =
      Keyword.pop_lazy(opts, :to, fn ->
        raise ArgumentError, "expected :to argument in subscribe"
      end)
    GenServer.cast(stage, {:"$subscribe", to, opts})
  end

  ## Callbacks

  @doc false
  def init({mod, args}) do
    case mod.init(args) do
      {:producer, state} ->
        init_producer(mod, [], state)
      {:producer, state, opts} when is_list(opts) ->
        init_producer(mod, opts, state)
      {:consumer, state} ->
        init_consumer(mod, [], state)
      {:consumer, state, opts} when is_list(opts) ->
        init_consumer(mod, opts, state)
      {:stop, _} = stop ->
        stop
      :ignore ->
        :ignore
      other ->
        {:stop, {:bad_return_value, other}}
    end
  end

  defp init_producer(mod, opts, state) do
    {dispatcher_mod, opts} = Keyword.pop(opts, :dispatcher, GenStage.DemandDispatcher)
    {:ok, dispatcher_state} = dispatcher_mod.init(opts)
    {:ok, %GenStage{mod: mod, state: state, type: :producer,
                    dispatcher_mod: dispatcher_mod, dispatcher_state: dispatcher_state}}
  end

  defp init_consumer(mod, opts, state) do
    with {:ok, max_demand} <- validate_integer(opts, :max_demand, 100, 1, :infinity),
         {:ok, min_demand} <- validate_integer(opts, :min_demand, div(max_demand, 2), 0, max_demand - 1) do
      {:ok, %GenStage{mod: mod, state: state, type: :consumer, demand: {min_demand, max_demand}}}
    else
      {:error, message} -> {:stop, {:bad_opts, message}}
    end
  end

  defp validate_integer(opts, key, default, min, max) do
    value = Keyword.get(opts, key, default)

    cond do
      not is_integer(value) ->
        {:error, "expected #{inspect key} to be an integer"}
      value < min ->
        {:error, "expected #{inspect key} to be equal to or greater than #{min}"}
      value > max ->
        {:error, "expected #{inspect key} to be equal to or less than #{max}"}
      true ->
        {:ok, value}
    end
  end

  @doc false
  def handle_call({:"$subscribe", to, opts}, _from, stage) do
    {reply, stage} = consumer_subscribe(to, opts, stage)
    {:reply, reply, stage}
  end

  @doc false
  def handle_cast({:"$subscribe", to, opts}, stage) do
    {_reply, stage} = consumer_subscribe(to, opts, stage)
    {:noreply, stage}
  end

  # TODO: handle cast/call/info/code_change/terminate
  # TODO: handle DOWN and cancel messages

  ## Producer messages

  @doc false
  def handle_info({:"$gen_producer", _, _} = msg, %{type: :consumer} = stage) do
    :error_logger.error_msg('GenStage consumer received $gen_producer message: ~p~n', [msg])
    {:noreply, stage}
  end

  def handle_info({:"$gen_producer", {consumer_pid, ref} = from, {:subscribe, _opts}},
                  %{consumers: consumers} = stage) do
    case consumers do
      %{^ref => _} ->
        :error_logger.error_msg('GenStage producer received duplicated subscription from: ~p~n', [from])
        send(consumer_pid, {:"$gen_consumer", {self(), ref}, {:cancel, :duplicated_subscription}})
        {:noreply, stage}
      %{} ->
        mon_ref = Process.monitor(consumer_pid)
        stage = put_in stage.monitors[mon_ref], ref
        stage = put_in stage.consumers[ref], consumer_pid
        %{dispatcher_state: dispatcher_state} = stage
        dispatcher_callback(:subscribe, [from, dispatcher_state], stage)
    end
  end

  def handle_info({:"$gen_producer", {consumer_pid, ref} = from, {:ask, counter}},
                  %{consumers: consumers} = stage) when is_integer(counter) do
    case consumers do
      %{^ref => _} ->
        %{dispatcher_state: dispatcher_state} = stage
        dispatcher_callback(:ask, [counter, from, dispatcher_state], stage)
      %{} ->
        send(consumer_pid, {:"$gen_consumer", {self(), ref}, {:cancel, :unknown_subscription}})
        {:noreply, stage}
    end
  end

  ## Consumer messages

  def handle_info({:"$gen_consumer", _, _} = msg, %{type: :producer} = stage) do
    :error_logger.error_msg('GenStage producer received $gen_consumer message: ~p~n', [msg])
    {:noreply, stage}
  end

  def handle_info({:"$gen_consumer", {producer_pid, ref} = from, events},
                  %{producers: producers, state: state} = stage) when is_list(events) do
    case producers do
      %{^ref => entry} ->
        case consumer_receive(ref, entry, events, stage) do
          {events, 0, stage} ->
            noreply_callback(:handle_events, [events, from, state], stage)
          {events, ask, stage} when is_integer(ask) and ask > 0 ->
            return = noreply_callback(:handle_events, [events, from, state], stage)
            send producer_pid, {:"$gen_producer", {self(), ref}, {:ask, ask}}
            return
        end
      _ ->
        send(producer_pid, {:"$gen_producer", {self(), ref}, {:cancel, :unknown_subscription}})
        {:noreply, stage}
    end
  end

  ## Helpers

  defp dispatcher_callback(callback, args, %{dispatcher_mod: dispatcher_mod} = stage) do
    case apply(dispatcher_mod, callback, args) do
      {:ok, 0, dispatcher_state} ->
        {:noreply, %{stage | dispatcher_state: dispatcher_state}}
      {:ok, counter, dispatcher_state} when is_integer(counter) and counter > 0 ->
        %{state: state} = stage = %{stage | dispatcher_state: dispatcher_state}
        # TODO: support producer_consumer
        noreply_callback(:handle_demand, [counter, state], stage)
    end
  end

  defp noreply_callback(callback, args, %{mod: mod} = stage) do
    case apply(mod, callback, args) do
      {:noreply, events, state} when is_list(events) ->
        stage = dispatch_events(events, stage)
        {:noreply, %{stage | state: state}}
      {:noreply, events, state, :hibernate} when is_list(events) ->
        stage = dispatch_events(events, stage)
        {:noreply, %{stage | state: state}, :hibernate}
      {:noreply, events, state, timeout} when is_list(events) and is_integer(timeout) and timeout >= 0 ->
        stage = dispatch_events(events, stage)
        {:noreply, %{stage | state: state}, timeout}
      {:stop, reason, state} ->
        {:stop, reason, %{stage | state: state}}
      other ->
        {:stop, {:bad_return_value, other}, stage}
    end
  end

  defp dispatch_events([], stage) do
    stage
  end
  defp dispatch_events(events, %{type: :consumer} = stage) do
    :error_logger.error_msg('GenStage consumer cannot dispatch events (an empty list must be returned): ~p~n', [events])
    stage
  end
  defp dispatch_events(events, %{consumers: consumers} = stage) when map_size(consumers) == 0 do
    overflow_events(events, stage)
  end
  defp dispatch_events(events, stage) do
    %{dispatcher_mod: dispatcher_mod, dispatcher_state: dispatcher_state} = stage
    {:ok, events, dispatcher_state} = dispatcher_mod.dispatch(events, dispatcher_state)
    overflow_events(events, %{stage | dispatcher_state: dispatcher_state})
  end

  # TODO: Support overflow
  defp overflow_events([], stage) do
    stage
  end
  defp overflow_events(_events, stage) do
    stage
  end

  defp consumer_receive(ref, {producer_id, persistent?, demand}, events, %{demand: {min, max}} = stage) do
    {demand, events} = consumer_check_excess(ref, producer_id, demand, events)
    new_demand = if demand <= min, do: max, else: demand
    stage = put_in stage.producers[ref], {producer_id, persistent?, new_demand}
    {events, new_demand - demand, stage}
  end

  defp consumer_check_excess(ref, producer_id, demand, events) do
    remaining = demand - length(events)

    if remaining < 0 do
      :error_logger.error_msg('GenStage consumer has discarded ~p events in excess from: ~p~n',
                              [abs(remaining), {producer_id, ref}])
      {0, Enum.take(events, demand)}
    else
      {remaining, events}
    end
  end

  defp consumer_subscribe(to, _opts, %{type: :producer} = stage) do
    :error_logger.error_msg('GenStage producer cannot be subscribed to another stage: ~p~n', [to])
    {{:error, :not_a_consumer}, stage}
  end

  defp consumer_subscribe(to, opts, %{demand: {_, max}} = stage) do
    {persistent?, opts} = Keyword.pop(opts, :persistent, true)

    if producer_pid = GenServer.whereis(to) do
      ref = Process.monitor(producer_pid)
      send producer_pid, {:"$gen_producer", {self(), ref}, {:subscribe, opts}}
      send producer_pid, {:"$gen_producer", {self(), ref}, {:ask, max}}
      stage = put_in stage.producers[ref], {producer_pid, persistent?, max}
      {:ok, stage}
    else
      {{:error, :noproc}, stage}
    end
  end
end
