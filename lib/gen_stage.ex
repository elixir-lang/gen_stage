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
          # emit the items "3" and "4", and set the state to 5.
          events = Enum.to_list(counter..counter+demand-1)
          {:noreply, events, counter + demand}
        end
      end

  B is a producer-consumer. This means it does not explicitly
  handle the demand because the demand is always forwarded to
  its producer. Once A receives the demand from B, it will send
  events to B which will be transformed by as B as desired. In
  our case, B will receive events and multiply them by a number
  giving on initialization and stored as the state:

      defmodule B do
        use GenStage

        def init(number) do
          {:producer_consumer, number}
        end

        def handle_event(event, number) do
          {:noreply, [event * number], number}
        end
      end

  C will finally receive those events and print them every second
  to the terminal:

      defmodule C do
        use GenStage

        def init(:ok) do
          {:consumer, :the_state_does_not_matter}
        end

        def handle_event(event, state) do
          # Wait for a second.
          :timer.sleep(1000)

          # Inspect the event.
          IO.inspect(event)

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

  ## Dynamic events

  In the example above, we have subscribed C to B and B to A,
  sent the demand upstream and received the events downstream.
  That's how we expect most data to flow in stages.

  However, sometimes we may need to directly inject data into
  a given stage. For example, in the example above, we may need
  to do at some point:

      GenStage.sync_notify(b, 42)

  The call above would send a message to B which would eventually
  be handled as an event and sent to C. However, once we start
  supporting dynamic events, one important question arises:
  what if we sent a message to B but C, its consumer, has not
  yet sent any demand (i.e. it has not asked for any items)?

  To handle such cases, all stages place dynamic events in an
  internal buffer. If the message cannot be sent immediately,
  it is stored and sent whenever there is an opportunity to.
  The number of dynamic events that can be buffered is customized
  via the `:dynamic_buffer_size` option returned by `init/1`.
  The default value is of 100.

  TODO: Is this a reasonable default? Whatever default we choose
  needs to be reflected in the source code and docs below.

  TODO: Discuss if the timeout is client-based, server-based
  or both.

  ## Overflown events

  Sometimes, a producer may produce more events than downstream
  has asked for. For example, you are receiving events from an
  external source that only sends events in batches of 10 in 10.
  However, according to the GenStage specification, a producer
  may never send more events than a consumer has asked for.

  To handle such cases, all producer stages place overflown events
  in an internal buffer. If the message cannot be sent immediately,
  it is stored and sent whenever there is an opportunity to.
  The number of overflown events that can be buffered is customized
  via the `:overflown_buffer_size` option returned by `init/1`.

  By default, the overflown buffer size is 0, which means an error
  will be logged. This is by design as an overflown event in most
  stages is likely an implementation error. The value can be
  increased though as needed.

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
  `handle_demand/2` to be implemented by producers and `handle_event/3`
  to be implemented by consumers, as shown above. Futhermore, all the
  callback responses have been modified to potentially emit events.
  See the callbacks documentation for more information.

  By adding `use GenStage` to your module, Elixir will automatically
  define all callbacks for you, leaving it up to you to implement the
  ones you want to customize.

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
  subscription has a unique reference. Once a subscription is
  done, many connections may be established.

  Once a connection is established, the consumer may ask the
  producer for messages for the given subscription-connection
  pair. The consumer may demand more items whenever it wants
  to. A consumer must never receive more data than it has asked
  for from any given producer stage.

  ### Producer messages

  The producer is responsible for sending events to consumers
  based on demand.

    * `{:"$gen_producer", from :: {consumer_pid, subscription_ref}, {:stage, options}}` -
      sent by the consumer to the producer to start a new subscription.

      Before sending, the consumer MUST monitor the producer for clean-up
      purposes in case of crashes. The `subscription_ref` is unique to
      identify the subscription (and may be the monitoring reference). 

      Once received, the producer MUST monitor the consumer and reply with a
      `:connect` message. If the producer already has a subscription, it MAY
      ignore future subscriptions by sending a disconnect reply (defined
      in the Consumer section) except for cases where the new subscription
      matches the `subscription_ref`. In such cases, the producer MUST crash.

    * `{:"$gen_producer", from :: {consumer_pid, subscription_ref}, {:broker, strategy, options}}` -
      sent by the consumer (a broker) to the producer to start a new subscription.

      The consumer MAY establish new connections by sending `:connect`
      messages defined below. The consumer MUST monitor the producer for
      clean-up purposes in case of crashes. The `subscription_ref` is
      unique to identify the subscription (and may be the monitoring
      reference). 

      Once received, the producer MUST monitor the consumer. The producer
      MUST initialize the `strategy` by calling `strategy.init(from, options)`.
      If the producer already has a subscription, it MAY ignore future
      subscriptions by sending a disconnect reply (defined in the Consumer
      section) except for cases where the new subscription matches the
      `subscription_ref`. In such cases, the producer MUST crash.

    * `{:"$gen_producer", from :: {pid, subscription_ref}, {:connect, consumers :: [pid]}}` -
      sent by the consumer (a broker) to producers to start new connections.

      Once sent, the consumer MAY immediately send demand to the producer.
      The `subscription_ref` is unique to identify the subscription.

      Once received, the producer MUST call `strategy.connect(consumers, from, state)`
      if one is available. If the `subscription_ref` is unknown, the
      producer MUST send an appropriate disconnect reply to each consumer.

    * `{:"$gen_producer", from :: {consumer_pid, subscription_ref}, {:disconnect, reason}}` -
      sent by the consumer to disconnect a given consumer-subscription pair.

      Once received, the producer MAY call `strategy.disconnect(reason, from, state)`
      if one is available. The strategy MUST send a disconnect message to the
      consumer pid. If the `consumer_pid` refers to the process that started
      the subscription, all connections MUST be disconnected. If the
      consumer-subscription is unknown, a disconnect MUST still be sent with
      proper reason. In all cases, however, there is no guarantee the message
      will be delivered (for example, the producer may crash just before sending
      the confirmation).

    * `{:"$gen_producer", from :: {consumer_pid, subscription_ref}, {:ask, count}}` -
      sent by consumers to ask data from a producer for a given consumer-subscription pair.

      Once received, the producer MUST call `strategy.ask(count, from, state)`
      if one is available. The producer MUST send data up to the demand. If the
      pair is unknown, the produder MUST send an appropriate disconnect reply.

  ### Consumer messages

  The consumer is responsible for starting the subscription
  and sending demand to producers.

    * `{:"$gen_consumer", from :: {producer_pid, subscription_ref}, {:connect, producers :: [pid]}}` -
      sent by producers to consumers to start new connections.

      Once received, the consumer MAY immediately send demand to
      the producer. The `subscription_ref` is unique to identify
      the subscription. If the subscription is not known, a
      disconnect message must be sent back to each producer.

    * `{:"$gen_consumer", from :: {producer_pid, subscription_ref}, {:disconnect, reason}}` -
      sent by producers to disconnect a given producer-subscription pair.

      It is used as a confirmation for client disconnects OR whenever
      the producer wants to cancel some upstream demand. Reason may be
      `:done`, `:halted` or `:unknown_subscription`.

    * `{:"$gen_consumer", from :: {producer_pid, subscription_ref}, [event]}` -
      events sent by producers to consumers.

      `subscription_ref` identifies the subscription. The third argument
      is a non-empty list of events. If the subscription is unknown, the
      events must be ignored.

  """

  # TODO: Make handle_demand and handle_event optional

  @typedoc "The supported stage types."
  @type type :: :producer | :consumer | :producer_consumer

  @typedoc "The supported init options"
  @type options :: []

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

  ### Shared options

    * `:dynamic_buffer_size` - the size of the buffer to store dynamic
      events. Check the "Dynamic events" section on the module
      documentation (defaults to 100)

  ### :producer options

    * `:overflown_buffer_size` - the size of the buffer to store overflown
      events. Check the "Overflown events" section on the module
      documentation (defaults to 0)

  ### :producer_consumer options

    * `:overflown_buffer_size` - the size of the buffer to store overflown
      events. Check the "Overflown events" section on the module
      documentation (defaults to 0)

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
  @callback handle_event(demand :: pos_integer, GenServer.from, state :: term) ::
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
end
