defmodule GenStage do
  @moduledoc ~S"""
  Stages are data-exchange steps that send and/or receive data
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
    * C is only a consumer (and therefore a sink)

  As we will see in the upcoming Examples section, we must
  specify the type of the stage when we implement each of them.

  To start the flow of events, we subscribe consumers to
  producers. Once the communication channel between them is
  established, consumers will ask the producers for events.
  We typically say the consumer is sending demand upstream.
  Once demand arrives, the producer will emit items, never
  emitting more items than the consumer asked for. This provides
  a back-pressure mechanism.

  A consumer may have multiple producers and a producer may have
  multiple consumers. When a consumer asks for data, each producer
  is handled separately, with its own demand. When a producer
  receives demand and sends data to multiple consumers, the demand
  is tracked and the events are sent by a dispatcher. This allows
  producers to send data using different "strategies". See
  `GenStage.Dispatcher` for more information.

  Many developers tend to create layers of stages, such as A, B and
  C, for achieving concurrency. If all you want is concurrency, starting
  multiple instances of the same stage is enough. Layers in GenStage must
  be created when there is a need for back-pressure or to route the data
  in different ways.

  For example, if you need the data to go over multiple steps but
  without a need for back-pressure or without a need to break the
  data apart, do not design it as such:

      [Producer] -> [Step 1] -> [Step 2] -> [Step 3]

  Instead it is better to design it as:

                   [Consumer]
                  /
      [Producer]-<-[Consumer]
                  \
                   [Consumer]

  where "Consumer" are multiple processes running the same code that
  subscribe to the same "Producer".

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

        def start_link(number) do
          GenStage.start_link(A, number)
        end

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
  given on initialization and stored as the state:

      defmodule B do
        use GenStage

        def start_link(number) do
          GenStage.start_link(B, number)
        end

        def init(number) do
          {:producer_consumer, number}
        end

        def handle_events(events, _from, number) do
          events = Enum.map(events, & &1 * number)
          {:noreply, events, number}
        end
      end

  C will finally receive those events and print them every second
  to the terminal:

      defmodule C do
        use GenStage

        def start_link() do
          GenStage.start_link(C, :ok)
        end

        def init(:ok) do
          {:consumer, :the_state_does_not_matter}
        end

        def handle_events(events, _from, state) do
          # Wait for a second.
          Process.sleep(1000)

          # Inspect the events.
          IO.inspect(events)

          # We are a consumer, so we would never emit items.
          {:noreply, [], state}
        end
      end

  Now we can start and connect them:

      {:ok, a} = A.start_link(0)  # starting from zero
      {:ok, b} = B.start_link(2)  # multiply by 2
      {:ok, c} = C.start_link()   # state does not matter

      GenStage.sync_subscribe(c, to: b)
      GenStage.sync_subscribe(b, to: a)

  Typically, we subscribe from bottom to top. Since A will
  start producing items only when B connects to it, we want this
  subscription to happen when the whole pipeline is ready. After
  you subscribe all of them, demand will start flowing upstream and
  events downstream.

  When implementing consumers, we often set the `:max_demand` and
  `:min_demand` on subscription. The `:max_demand` specifies the
  maximum amount of events that must be in flow while the `:min_demand`
  specifies the minimum threshold to trigger for more demand. For
  example, if `:max_demand` is 1000 and `:min_demand` is 750,
  the consumer will ask for 1000 events initially and ask for more
  only after it receives at least 250.

  In the example above, B is a `:producer_consumer` and therefore
  acts as a buffer. Getting the proper demand values in B is
  important: making the buffer too small may make the whole pipeline
  slower, making the buffer too big may unnecessarily consume
  memory.

  When such values are applied to the stages above, it is easy
  to see the producer works in batches. The producer A ends-up
  emitting batches of 50 items which will take approximately
  50 seconds to be consumed by C, which will then request another
  batch of 50 items.

  ## `init` and `:subscribe_to`

  In the example above, we have started the processes A, B, and C
  independently and subscribed them later on. But most often it is
  simpler to subscribe a consumer to its producer on its `c:init/1`
  callback. This way, if the consumer crashes, restarting the consumer
  will automatically re-invoke its `c:init/1` callback and resubscribe
  it to the producer.

  This approach works as long as the producer can be referenced when
  the consumer starts - such as by name for a named process. For example,
  assuming the process `A` and `B` are started as follows:

      # Let's call the stage in module A as A
      GenStage.start_link(A, 0, name: A)
      # Let's call the stage in module B as B
      GenStage.start_link(B, 2, name: B)
      # No need to name consumers as they won't be subscribed to
      GenStage.start_link(C, :ok)

  We can now change the `c:init/1` callback for C to the following:

      def init(:ok) do
        {:consumer, :the_state_does_not_matter, subscribe_to: [B]}
      end

  Subscription options as outlined in `sync_subscribe/3` can also be
  given by making each subscription a tuple, with the process name or
  pid as first element and the options as second:

      def init(:ok) do
        {:consumer, :the_state_does_not_matter, subscribe_to: [{B, options}]}
      end

  Similarly, we should change `B` to subscribe to `A` on `c:init/1`. Let's
  also set `:max_demand` to 10 when we do so:

      def init(number) do
        {:producer_consumer, number, subscribe_to: [{A, max_demand: 10}]}
     end

  And we will no longer need to call `sync_subscribe/2`.

  Another advantage of using `subscribe_to` is that it makes it straight-forward
  to leverage concurrency by simply starting multiple consumers that subscribe
  to their producer (or producer-consumer). This can be done in the example above
  by simply calling start link multiple times:

      # Start 4 consumers
      GenStage.start_link(C, :ok)
      GenStage.start_link(C, :ok)
      GenStage.start_link(C, :ok)
      GenStage.start_link(C, :ok)

  In a supervision tree, this is often done by starting multiple workers:

      children = [
        worker(A, [0]),
        worker(B, [2]),
        worker(C, []),
        worker(C, []),
        worker(C, []),
        worker(C, [])
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  Having multiple consumers is often the easiest and simplest way to
  leverage concurrency in a GenStage pipeline, especially if events can
  be processed out of order.

  The same guideline that applies to processes also applies to GenStage:
  use processes/stages to model runtime properties, such as concurrency and
  data-transfer, and not as code organization tools.

  Another alternative to the scenario above is to use a `ConsumerSupervisor`
  for consuming the events instead of N consumers. The `ConsumerSupervisor`
  will communicate with the producer respecting the back-pressure properties
  and start a separate supervised process per event. The number of children
  concurrently running in a `ConsumerSupervisor` is at most `max_demand` and
  the average amount of children is `(max_demand - min_demand) / 2`.

  If you don't need back-pressure at all, because the data is already in-memory,
  a simpler solution is available directly in Elixir via `Task.async_stream/2`.
  This function consumes a stream of data, with each entry running in a separate
  task. The maximum number of tasks is configurable via the `:max_concurrency`
  option.

  ## Buffering

  In many situations, producers may attempt to emit events while no consumers
  have yet subscribed. Similarly, consumers may ask producers for events
  that are not yet available. In such cases, it is necessary for producers
  to buffer events until a consumer is available or buffer the consumer
  demand until events arrive, respectively. As we will see next, buffering
  events can be done automatically by `GenStage`, while buffering the demand
  is a case that must be explicitly considered by developers implementing
  producers.

  ### Buffering events

  Due to the concurrent nature of Elixir software, sometimes a producer
  may dispatch events without consumers to send those events to. For example,
  imagine a `:consumer` B subscribes to `:producer` A. Next, the consumer B
  sends demand to A, which starts producing events to satisfy the demand.
  Now, if the consumer B crashes, the producer may attempt to dispatch the
  now produced events but it no longer has a consumer to send those events to.
  In such cases, the producer will automatically buffer the events until another
  consumer subscribes.

  The buffer can also be used in cases where external sources only send
  events in batches larger than asked for. For example, if you are
  receiving events from an external source that only sends events
  in batches of 1000 and the internal demand is smaller than
  that, the buffer allows you to always emit batches of 1000 events
  even when the consumer has asked for less.

  In all of those cases when an event cannot be sent immediately by
  a producer, the event will be automatically stored and sent the next
  time consumers ask for events. The size of the buffer is configured
  via the `:buffer_size` option returned by `init/1` and the default
  value is `10_000`. If the `buffer_size` is exceeded, an error is logged.
  See the documentation for `c:init/1` for more detailed information about
  the `:buffer_size` option.

  ### Buffering demand

  In case consumers send demand and the producer is not yet ready to
  fill in the demand, producers must buffer the demand until data arrives.

  As an example, let's implement a producer that broadcasts messages
  to consumers. For producers, we need to consider two scenarios:

    1. what if events arrive and there are no consumers?
    2. what if consumers send demand and there are not enough events?

  One way to implement such a broadcaster is to simply rely on the internal
  buffer available in `GenStage`, dispatching events as they arrive, as explained
  in the previous section:

      defmodule Broadcaster do
        use GenStage

        @doc "Starts the broadcaster."
        def start_link() do
          GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
        end

        @doc "Sends an event and returns only after the event is dispatched."
        def sync_notify(event, timeout \\ 5000) do
          GenStage.call(__MODULE__, {:notify, event}, timeout)
        end

        def init(:ok) do
          {:producer, :ok, dispatcher: GenStage.BroadcastDispatcher}
        end

        def handle_call({:notify, event}, _from, state) do
          {:reply, :ok, [event], state} # Dispatch immediately
        end

        def handle_demand(_demand, state) do
          {:noreply, [], state} # We don't care about the demand
        end
      end

  By always sending events as soon as they arrive, if there is any demand,
  we will serve the existing demand, otherwise the event will be queued in
  `GenStage`'s internal buffer. In case events are being queued and not being
  consumed, a log message will be emitted when we exceed the `:buffer_size`
  configuration.

  While the implementation above is enough to solve the constraints above,
  a more robust implementation would have tighter control over the events
  and demand by tracking this data locally, leaving the `GenStage` internal
  buffer only for cases where consumers crash without consuming all data.

  To handle such cases, we will use a two-element tuple as the broadcaster state
  where the first element is a queue and the second element is the pending
  demand.  When events arrive and there are no consumers, we will store the
  event in the queue alongside information about the process that broadcast
  the event. When consumers send demand and there are not enough events, we will
  increase the pending demand.  Once we have both data and demand, we
  acknowledge the process that has sent the event to the broadcaster and finally
  broadcast the event downstream.

      defmodule QueueBroadcaster do
        use GenStage

        @doc "Starts the broadcaster."
        def start_link() do
          GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
        end

        @doc "Sends an event and returns only after the event is dispatched."
        def sync_notify(event, timeout \\ 5000) do
          GenStage.call(__MODULE__, {:notify, event}, timeout)
        end

        ## Callbacks

        def init(:ok) do
          {:producer, {:queue.new, 0}, dispatcher: GenStage.BroadcastDispatcher}
        end

        def handle_call({:notify, event}, from, {queue, pending_demand}) do
          queue = :queue.in({from, event}, queue)
          dispatch_events(queue, pending_demand, [])
        end

        def handle_demand(incoming_demand, {queue, pending_demand}) do
          dispatch_events(queue, incoming_demand + pending_demand, [])
        end

        defp dispatch_events(queue, 0, events) do
          {:noreply, Enum.reverse(events), {queue, 0}}
        end

        defp dispatch_events(queue, demand, events) do
          case :queue.out(queue) do
            {{:value, {from, event}}, queue} ->
              GenStage.reply(from, :ok)
              dispatch_events(queue, demand - 1, [event | events])
            {:empty, queue} ->
              {:noreply, Enum.reverse(events), {queue, demand}}
          end
        end
      end

  Let's also implement a consumer that automatically subscribes to the
  broadcaster on `c:init/1`. The advantage of doing so on initialization
  is that, if the consumer crashes while it is supervised, the subscription
  is automatically re-established when the supervisor restarts it.

      defmodule Printer do
        use GenStage

        @doc "Starts the consumer."
        def start_link() do
          GenStage.start_link(__MODULE__, :ok)
        end

        def init(:ok) do
          # Starts a permanent subscription to the broadcaster
          # which will automatically start requesting items.
          {:consumer, :ok, subscribe_to: [QueueBroadcaster]}
        end

        def handle_events(events, _from, state) do
          for event <- events do
            IO.inspect {self(), event}
          end
          {:noreply, [], state}
        end
      end

  With the broadcaster in hand, now let's start the producer as well
  as multiple consumers:

      # Start the producer
      QueueBroadcaster.start_link()

      # Start multiple consumers
      Printer.start_link()
      Printer.start_link()
      Printer.start_link()
      Printer.start_link()

  At this point, all consumers must have sent their demand which we were not
  able to fulfill. Now by calling `QueueBroadcaster.sync_notify/1`, the event
  shall be broadcast to all consumers at once as we have buffered the demand
  in the producer:

      QueueBroadcaster.sync_notify(:hello_world)

  If we had called `QueueBroadcaster.sync_notify(:hello_world)` before any
  consumer was available, the event would also have been buffered in our own
  queue and served only when demand had been received.

  By having control over the demand and queue, the broadcaster has
  full control on how to behave when there are no consumers, when the
  queue grows too large, and so forth.

  ## Asynchronous work and `handle_subscribe`

  Both `:producer_consumer` and `:consumer` stages have been designed to do
  their work in the `c:handle_events/3` callback. This means that, after
  `c:handle_events/3` is invoked, both `:producer_consumer` and `:consumer`
  stages will immediately send demand upstream and ask for more items, as the
  stage that produced the events assumes events have been fully processed by
  `c:handle_events/3`.

  Such default behaviour makes `:producer_consumer` and `:consumer` stages
  unfeasible for doing asynchronous work. However, given `GenStage` was designed
  to run with multiple consumers, it is not a problem to perform synchronous or
  blocking actions inside `handle_events/3` as you can then start multiple
  consumers in order to max both CPU and IO usage as necessary.

  On the other hand, if you must perform some work asynchronously,
  `GenStage` comes with an option that manually controls how demand
  is sent upstream, avoiding the default behaviour where demand is
  sent after `c:handle_events/3`. Such can be done by implementing
  the `c:handle_subscribe/4` callback and returning `{:manual, state}`
  instead of the default `{:automatic, state}`. Once the consumer mode
  is set to `:manual`, developers must use `GenStage.ask/3` to send
  demand upstream when necessary.

  Note that `:max_demand` and `:min_demand` must be manually respected when
  asking for demand through `GenStage.ask/3`.

  For example, the `ConsumerSupervisor` module processes events
  asynchronously by starting a process for each event and this is achieved by
  manually sending demand to producers. `ConsumerSupervisor`
  can be used to distribute work to a limited amount of
  processes, behaving similar to a pool where a new process is
  started for each event. See the `ConsumerSupervisor` docs for more
  information.

  Setting the demand to `:manual` in `c:handle_subscribe/4` is not
  only useful for asynchronous work but also for setting up other
  mechanisms for back-pressure. As an example, let's implement a
  consumer that is allowed to process a limited number of events
  per time interval. Those are often called rate limiters:

      defmodule RateLimiter do
        use GenStage

        def init(_) do
          # Our state will keep all producers and their pending demand
          {:consumer, %{}}
        end

        def handle_subscribe(:producer, opts, from, producers) do
          # We will only allow max_demand events every 5000 milliseconds
          pending = opts[:max_demand] || 1000
          interval = opts[:interval] || 5000

          # Register the producer in the state
          producers = Map.put(producers, from, {pending, interval})
          # Ask for the pending events and schedule the next time around
          producers = ask_and_schedule(producers, from)

          # Returns manual as we want control over the demand
          {:manual, producers}
        end

        def handle_cancel(_, from, producers) do
          # Remove the producers from the map on unsubscribe
          {:noreply, [], Map.delete(producers, from)}
        end

        def handle_events(events, from, producers) do
          # Bump the amount of pending events for the given producer
          producers = Map.update!(producers, from, fn {pending, interval} ->
            {pending + length(events), interval}
          end)

          # Consume the events by printing them.
          IO.inspect(events)

          # A producer_consumer would return the processed events here.
          {:noreply, [], producers}
        end

        def handle_info({:ask, from}, producers) do
          # This callback is invoked by the Process.send_after/3 message below.
          {:noreply, [], ask_and_schedule(producers, from)}
        end

        defp ask_and_schedule(producers, from) do
          case producers do
            %{^from => {pending, interval}} ->
              # Ask for any pending events
              GenStage.ask(from, pending)
              # And let's check again after interval
              Process.send_after(self(), {:ask, from}, interval)
              # Finally, reset pending events to 0
              Map.put(producers, from, {0, interval})
            %{} ->
              producers
          end
        end
      end

  Let's subscribe the `RateLimiter` above to the
  producer we have implemented at the beginning of the module
  documentation:

      {:ok, a} = GenStage.start_link(A, 0)
      {:ok, b} = GenStage.start_link(RateLimiter, :ok)

      # Ask for 10 items every 2 seconds
      GenStage.sync_subscribe(b, to: a, max_demand: 10, interval: 2000)

  Although the rate limiter above is a consumer, it could be made a
  producer-consumer by changing `c:init/1` to return a `:producer_consumer`
  and then forwarding the events in `c:handle_events/3`.

  ## Callbacks

  `GenStage` is implemented on top of a `GenServer` with a few additions.
  Besides exposing all of the `GenServer` callbacks, it also provides
  `handle_demand/2` to be implemented by producers and `handle_events/3` to be
  implemented by consumers, as shown above, as well as subscription-related
  callbacks. Furthermore, all the callback responses have been modified to
  potentially emit events. See the callbacks documentation for more
  information.

  By adding `use GenStage` to your module, Elixir will automatically
  define all callbacks for you except for the following ones:

    * `init/1` - must be implemented to choose between `:producer`, `:consumer`, or `:producer_consumer` stages
    * `handle_demand/2` - must be implemented by `:producer` stages
    * `handle_events/3` - must be implemented by `:producer_consumer` and `:consumer` stages

  `use GenStage` also defines a `child_spec/1` function, allowing the
  defined module to be put under a supervision tree in Elixir v1.5+.
  The generated `child_spec/1` can be customized with the following options:

    * `:id` - the child specification id, defauts to the current module
    * `:start` - how to start the child process (defaults to calling `__MODULE__.start_link/1`)
    * `:restart` - when the child should be restarted, defaults to `:permanent`
    * `:shutdown` - how to shut down the child

  For example:

      use GenStage, restart: :transient, shutdown: 10_000

  See the `Supervisor` docs for more information.

  Although this module exposes functions similar to the ones found in
  the `GenServer` API, like `call/3` and `cast/2`, developers can also
  rely directly on GenServer functions such as `GenServer.multi_call/4`
  and `GenServer.abcast/3` if they wish to.

  ### Name registration

  `GenStage` is bound to the same name registration rules as a `GenServer`.
  Read more about it in the `GenServer` docs.

  ## Message protocol overview

  This section will describe the message protocol implemented
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

  A consumer may have multiple producers, where each demand is managed
  individually (on a per-subscription basis). A producer may have multiple
  consumers, where the demand and events are managed and delivered according to
  a `GenStage.Dispatcher` implementation.

  ### Producer messages

  The producer is responsible for sending events to consumers
  based on demand. These are the messages that consumers can
  send to producers:

    * `{:"$gen_producer", from :: {consumer_pid, subscription_tag}, {:subscribe, current, options}}` -
      sent by the consumer to the producer to start a new subscription.

      Before sending, the consumer MUST monitor the producer for clean-up
      purposes in case of crashes. The `subscription_tag` is unique to
      identify the subscription. It is typically the subscriber monitoring
      reference although it may be any term.

      Once sent, the consumer MAY immediately send demand to the producer.

      The `current` field, when not `nil`, is a two-item tuple containing a
      subscription that must be cancelled with the given reason before the
      current one is accepted.

      Once received, the producer MUST monitor the consumer. However, if
      the subscription reference is known, it MUST send a `:cancel` message
      to the consumer instead of monitoring and accepting the subscription.

    * `{:"$gen_producer", from :: {consumer_pid, subscription_tag}, {:cancel, reason}}` -
      sent by the consumer to cancel a given subscription.

      Once received, the producer MUST send a `:cancel` reply to the
      registered consumer (which may not necessarily be the one received
      in the tuple above). Keep in mind, however, there is no guarantee
      such messages can be delivered in case the producer crashes before.
      If the pair is unknown, the producer MUST send an appropriate cancel
      reply.

    * `{:"$gen_producer", from :: {consumer_pid, subscription_tag}, {:ask, demand}}` -
      sent by consumers to ask demand for a given subscription (identified
      by `subscription_tag`).

      Once received, the producer MUST send data up to the demand. If the
      pair is unknown, the producer MUST send an appropriate cancel reply.

  ### Consumer messages

  The consumer is responsible for starting the subscription
  and sending demand to producers. These are the messages that
  producers can send to consumers:

    * `{:"$gen_consumer", from :: {producer_pid, subscription_tag}, {:cancel, reason}}` -
      sent by producers to cancel a given subscription.

      It is used as a confirmation for client cancellations OR
      whenever the producer wants to cancel some upstream demand.

    * `{:"$gen_consumer", from :: {producer_pid, subscription_tag}, events :: [event, ...]}` -
      events sent by producers to consumers.

      `subscription_tag` identifies the subscription. The third argument
      is a non-empty list of events. If the subscription is unknown, the
      events must be ignored and a cancel message must be sent to the producer.

  """

  defstruct [:mod, :state, :type, :dispatcher_mod, :dispatcher_state, :buffer,
             :buffer_config, events: :forward, monitors: %{}, producers: %{}, consumers: %{}]

  @typedoc "The supported stage types."
  @type type :: :producer | :consumer | :producer_consumer

  @typedoc "The supported init options."
  @type options :: keyword()

  @typedoc "The stage."
  @type stage :: pid | atom | {:global, term} | {:via, module, term} | {atom, node}

  @typedoc "The term that identifies a subscription."
  @opaque subscription_tag :: reference

  @typedoc "The term that identifies a subscription associated with the corresponding producer/consumer."
  @type from :: {pid, subscription_tag}

  defmacrop is_transient_shutdown(value) do
    quote do
      unquote(value) == :normal or unquote(value) == :shutdown or
        (is_tuple(unquote(value)) and tuple_size(unquote(value)) == 2 and elem(unquote(value), 0) == :shutdown)
    end
  end

  @doc """
  Invoked when the server is started.

  `start_link/3` (or `start/3`) will block until this callback returns.
  `args` is the argument term (second argument) passed to `start_link/3`
  (or `start/3`).

  In case of successful start, this callback must return a tuple
  where the first element is the stage type, which is one of:

    * `:producer`
    * `:consumer`
    * `:producer_consumer` (if the stage is acting as both)

  For example:

      def init(args) do
        {:producer, some_state}
      end

  The returned tuple may also contain 3 or 4 elements. The third
  element may be the `:hibernate` atom or a set of options defined
  below.

  Returning `:ignore` will cause `start_link/3` to return `:ignore`
  and the process will exit normally without entering the loop or
  calling `terminate/2`.

  Returning `{:stop, reason}` will cause `start_link/3` to return
  `{:error, reason}` and the process to exit with reason `reason`
  without entering the loop or calling `terminate/2`.

  ## Options

  This callback may return options. Some options are specific to
  the chosen stage type while others are shared across all types.

  ### `:producer` options

    * `:demand` - when `:forward`, the demand is always forwarded to
      the `c:handle_demand/2` callback. When `:accumulate`, demand is
      accumulated until its mode is set to `:forward` via `demand/2`.
      This is useful as a synchronization mechanism, where the demand
      is accumulated until all consumers are subscribed. Defaults to
      `:forward`.

  ### `:producer` and `:producer_consumer` options

    * `:buffer_size` - the size of the buffer to store events without
      demand. Can be `:infinity` to signal no limit on the buffer size. Check
      the "Buffer events" section of the module documentation. Defaults to
      `10_000` for `:producer`, `:infinity` for `:producer_consumer`.

    * `:buffer_keep` - returns whether the `:first` or `:last` entries
      should be kept on the buffer in case the buffer size is exceeded.
      Defaults to `:last`.

    * `:dispatcher` - the dispatcher responsible for handling demands.
      Defaults to `GenStage.DemandDispatch`. May be either an atom
      representing a dispatcher module or a two-element tuple with
      the dispatcher module and the dispatcher options.

  ### `:consumer` and `:producer_consumer` options

    * `:subscribe_to` - a list of producers to subscribe to. Each element
      represents either the producer module or a tuple with the producer module
      and the subscription options (as defined in `sync_subscribe/2`).

  """
  @callback init(args :: term) ::
    {type, state} |
    {type, state, options} |
    :ignore |
    {:stop, reason :: any} when state: any

  @doc """
  Invoked on `:producer` stages.

  This callback is invoked on `:producer` stages with the demand from
  consumers/dispatcher. The producer that implements this callback must either
  store the demand, or return the amount of requested events.

  Must always be explicitly implemented by `:producer` stages.

  ## Examples

      def handle_demand(demand, state) do
        # We check if we're able to satisfy the demand and fetch
        # events if we aren't.
        events =
          if length(state.events) >= demand do
            state.events
          else
            # fetch_events()
          end

        # We dispatch only the requested number of events.
        {to_dispatch, remaining} = Enum.split(events, demand)

        {:noreply, to_dispatch, %{state | events: remaining}}
      end

  """
  @callback handle_demand(demand :: pos_integer, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason, new_state} when new_state: term, reason: term, event: term

  @doc """
  Invoked when a consumer subscribes to a producer.

  This callback is invoked in both producers and consumers.
  `producer_or_consumer` will be `:producer` when this callback is
  invoked on a consumer that subscribed to a producer, and `:consumer`
  if when this callback is invoked on producers a consumer subscribed to.

  For consumers, successful subscriptions must return one of:

    * `{:automatic, new_state}` - means the stage implementation will take care
      of automatically sending demand to producers. This is the default.

    * `{:manual, state}` - means that demand must be sent to producers
      explicitly via `ask/3`. `:manual` subscriptions must be cancelled when
      `c:handle_cancel/3` is called. `:manual` can be used when a special
      behaviour is desired (for example, `ConsumerSupervisor` uses `:manual`
      demand in its implementation).

  For producers, successful subscriptions must always return
  `{:automatic, new_state}`. `:manual` mode is not supported.

  If this callback is not implemented, the default implementation by
  `use GenStage` will return `{:automatic, state}`.

  ## Examples

  Let's see an example where we define this callback in a consumer that will use
  `:manual` mode. In this case, we'll store the subscription (`from`) in the
  state in order to be able to use it later on when asking demand via `ask/3`.

      def handle_subscribe(:producer, _options, from, state) do
        new_state = %{state | subscription: from}
        {:manual, new_state}
      end

  """
  @callback handle_subscribe(producer_or_consumer :: :producer | :consumer, options, from, state :: term) ::
    {:automatic | :manual, new_state} |
    {:stop, reason, new_state} when new_state: term, reason: term

  @doc """
  Invoked when a consumer is no longer subscribed to a producer.

  It receives the cancellation reason, the `from` tuple representing the
  cancelled subscription and the state.  The `cancel_reason` will be a
  `{:cancel, _}` tuple if the reason for cancellation was a `GenStage.cancel/2`
  call. Any other value means the cancellation reason was due to an EXIT.

  If this callback is not implemented, the default implementation by
  `use GenStage` will return `{:noreply, [], state}`.

  Return values are the same as `c:handle_cast/2`.
  """
  @callback handle_cancel(cancellation_reason :: {:cancel | :down, reason :: term}, from, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason, new_state} when event: term, new_state: term, reason: term

  @doc """
  Invoked on `:producer_consumer` and `:consumer` stages to handle events.

  Must always be explicitly implemented by such types.

  Return values are the same as `c:handle_cast/2`.
  """
  @callback handle_events(events :: [event], from, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason, new_state} when new_state: term, reason: term, event: term

  @doc """
  Invoked to handle synchronous `call/3` messages.

  `call/3` will block until a reply is received (unless the call times out or
  nodes are disconnected).

  `request` is the request message sent by a `call/3`, `from` is a two-element tuple
  containing the caller's PID and a term that uniquely identifies the call, and
  `state` is the current state of the `GenStage`.

  Returning `{:reply, reply, [events], new_state}` sends the response `reply`
  to the caller after events are dispatched (or buffered) and continues the
  loop with new state `new_state`. In case you want to deliver the reply before
  processing events, use `reply/2` and return `{:noreply, [event],
  state}`.

  Returning `{:noreply, [event], new_state}` does not send a response to the
  caller and processes the given events before continuing the loop with new
  state `new_state`. The response must be sent with `reply/2`.

  Hibernating is also supported as an atom to be returned from either
  `:reply` and `:noreply` tuples.

  Returning `{:stop, reason, reply, new_state}` stops the loop and `terminate/2`
  is called with reason `reason` and state `new_state`. Then the `reply` is sent
  as the response to the call and the process exits with reason `reason`.

  Returning `{:stop, reason, new_state}` is similar to
  `{:stop, reason, reply, new_state}` except that no reply is sent to the caller.

  If this callback is not implemented, the default implementation by
  `use GenStage` will return `{:stop, {:bad_call, request}, state}`.
  """
  @callback handle_call(request :: term, from :: GenServer.from, state :: term) ::
    {:reply, reply, [event], new_state} |
    {:reply, reply, [event], new_state, :hibernate} |
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term, event: term

  @doc """
  Invoked to handle asynchronous `cast/2` messages.

  `request` is the request message sent by a `cast/2` and `state` is the current
  state of the `GenStage`.

  Returning `{:noreply, [event], new_state}` dispatches the events and continues
  the loop with new state `new_state`.

  Returning `{:noreply, [event], new_state, :hibernate}` is similar to
  `{:noreply, new_state}` except the process is hibernated before continuing the
  loop. See the return values for `c:GenServer.handle_call/3` for more information
  on hibernation.

  Returning `{:stop, reason, new_state}` stops the loop and `terminate/2` is
  called with the reason `reason` and state `new_state`. The process exits with
  reason `reason`.

  If this callback is not implemented, the default implementation by
  `use GenStage` will return `{:stop, {:bad_cast, request}, state}`.
  """
  @callback handle_cast(request :: term, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term, event: term

  @doc """
  Invoked to handle all other messages.

  `message` is the message and `state` is the current state of the `GenStage`. When
  a timeout occurs the message is `:timeout`.

  If this callback is not implemented, the default implementation by
  `use GenStage` will return `{:noreply, [], state}`.

  Return values are the same as `c:handle_cast/2`.
  """
  @callback handle_info(message :: term, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term, event: term

  @doc """
  The same as `c:GenServer.terminate/2`.
  """
  @callback terminate(reason, state :: term) ::
    term when reason: :normal | :shutdown | {:shutdown, term} | term

  @doc """
  The same as `c:GenServer.code_change/3`.
  """
  @callback code_change(old_vsn, state :: term, extra :: term) ::
    {:ok, new_state :: term} |
    {:error, reason :: term} when old_vsn: term | {:down, term}

  @doc """
  The same as `c:GenServer.format_status/2`.
  """
  @callback format_status(:normal | :terminate, [pdict :: {term, term} | state :: term, ...]) ::
    status :: term

  @optional_callbacks [handle_demand: 2, handle_events: 3, format_status: 2]

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour GenStage

      spec = [
        id: opts[:id] || __MODULE__,
        start: Macro.escape(opts[:start]) || quote(do: {__MODULE__, :start_link, [arg]}),
        restart: opts[:restart] || :permanent,
        shutdown: opts[:shutdown] || 5000,
        type: :worker
      ]

      @doc false
      def child_spec(arg) do
        %{unquote_splicing(spec)}
      end

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
      def handle_info(msg, state) do
        proc =
          case Process.info(self(), :registered_name) do
            {_, []}   -> self()
            {_, name} -> name
          end
        :error_logger.error_msg('~p ~p received unexpected message in handle_info/2: ~p~n',
                                [__MODULE__, proc, msg])
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
      def handle_subscribe(_kind, _opts, _from, state) do
        {:automatic, state}
      end

      @doc false
      def handle_cancel(_reason, _from, state) do
        {:noreply, [], state}
      end

      @doc false
      def terminate(_reason, _state) do
        :ok
      end

      @doc false
      def code_change(_old, state, _extra) do
        {:ok, state}
      end

      defoverridable [handle_call: 3, handle_info: 2, handle_subscribe: 4,
                      handle_cancel: 3, handle_cast: 2, terminate: 2, code_change: 3,
                      child_spec: 1]
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
  parent process and will exit in case of crashes from the parent. The `GenStage`
  will also exit due to the `:normal` reason in case it is configured to trap
  exits in the `c:init/1` callback.

  ## Options

    * `:name` - used for name registration as described in the "Name
      registration" section of the module documentation

    * `:debug` - if present, the corresponding function in the [`:sys`
      module](http://www.erlang.org/doc/man/sys.html) is invoked

  This function also accepts all the options accepted by
  `GenServer.start_link/3`.

  ## Return values

  If the stage is successfully created and initialized, this function returns
  `{:ok, pid}`, where `pid` is the pid of the stage. If a process with the
  specified name already exists, this function returns
  `{:error, {:already_started, pid}}` with the pid of that process.

  If the `c:init/1` callback fails with `reason`, this function returns
  `{:error, reason}`. Otherwise, if `c:init/1` returns `{:stop, reason}`
  or `:ignore`, the process is terminated and this function returns
  `{:error, reason}` or `:ignore`, respectively.
  """
  @spec start_link(module, term, GenServer.options) :: GenServer.on_start
  def start_link(module, args, options \\ []) when is_atom(module) and is_list(options) do
    GenServer.start_link(__MODULE__, {module, args}, options)
  end

  @doc """
  Starts a `GenStage` process without links (outside of a supervision tree).

  See `start_link/3` for more information.
  """
  @spec start(module, term, GenServer.options) :: GenServer.on_start
  def start(module, args, options \\ []) when is_atom(module) and is_list(options) do
    GenServer.start(__MODULE__, {module, args}, options)
  end

  @doc """
  Queues an info message that is delivered after all currently buffered events.

  This call is synchronous and will return after the stage has queued
  the info message. The message will be eventually handled by the
  `handle_info/2` callback.

  If the stage is a consumer, it does not have buffered events, so the
  messaged is queued immediately.

  This function will return `:ok` if the info message is successfully queued.
  """
  @spec sync_info(stage, msg :: term(), timeout) :: :ok
  def sync_info(stage, msg, timeout \\ 5_000) do
    call(stage, {:"$info", msg}, timeout)
  end

  @doc """
  Asynchronously queues an info message that is delivered after all
  currently buffered events.

  If the stage is a consumer, it does not have buffered events, so the
  message is queued immediately.

  This call returns `:ok` regardless if the info has been successfully
  queued or not. It is typically called from the stage itself.
  """
  @spec async_info(stage, msg :: term()) :: :ok
  def async_info(stage, msg) do
    cast(stage, {:"$info", msg})
  end

  @doc """
  Sets the demand mode for a producer.

  When `:forward`, the demand is always forwarded to the `handle_demand`
  callback. When `:accumulate`, demand is accumulated until its mode is
  set to `:forward`. This is useful as a synchronization mechanism, where
  the demand is accumulated until all consumers are subscribed. Defaults
  to `:forward`.

  This command is asynchronous.
  """
  @spec demand(stage, :forward | :accumulate) :: :ok
  def demand(stage, mode) when mode in [:forward, :accumulate] do
    cast(stage, {:"$demand", mode})
  end

  @doc """
  Asks the consumer to subscribe to the given producer synchronously.

  This call is synchronous and will return after the called consumer
  sends the subscribe message to the producer. It does not, however,
  wait for the subscription confirmation. Therefore this function
  will return before `c:handle_subscribe/4` is called in the consumer.
  In other words, it guarantees the message was sent, but it does not
  guarantee a subscription has effectively been established.

  This function will return `{:ok, subscription_tag}` as long as the
  subscription message is sent. It will return `{:error, :not_a_consumer}`
  when the stage is not a consumer. `subscription_tag` is the second element
  of the two-element tuple that will be passed to `c:handle_subscribe/4`.

  ## Options

    * `:cancel` - `:permanent` (default), `:transient` or `:temporary`.
      When permanent, the consumer exits when the producer cancels or exits.
      When transient, the consumer exits only if reason is not `:normal`,
      `:shutdown`, or `{:shutdown, reason}`. When temporary, it never exits.
      In case of exits, the same reason is used to exit the consumer.
      In case of cancellations, the reason is wrapped in a `:cancel` tuple.

    * `:min_demand` - the minimum demand for this subscription. See the module
      documentation for more information.

    * `:max_demand` - the maximum demand for this subscription. See the module
      documentation for more information.

  Any other option is sent to the producer stage. This may be used by
  dispatchers for custom configuration. For example, if a producer uses
  a `GenStage.BroadcastDispatcher`, an optional `:selector` function
  that receives an event and returns a boolean limits this subscription to
  receiving only those events where the selector function returns a truthy
  value:

      GenStage.sync_subscribe(consumer,
        to: producer,
        selector: fn %{key: key} -> String.starts_with?(key, "foo-") end)

  """
  @spec sync_subscribe(stage, options, timeout) ::
        {:ok, subscription_tag} | {:error, :not_a_consumer} | {:error, {:bad_opts, String.t}}
  def sync_subscribe(stage, opts, timeout \\ 5_000) do
    sync_subscribe(stage, nil, opts, timeout)
  end

  @doc """
  Cancels `subscription_tag` with `reason` and subscribes synchronously in one step.

  `subscription_tag` is the subscription tag returned by `sync_subscribe/3`.

  See `sync_subscribe/3` for examples and options.
  """
  @spec sync_resubscribe(stage, subscription_tag, term, options, timeout) ::
        {:ok, subscription_tag} | {:error, :not_a_consumer} | {:error, {:bad_opts, String.t}}
  def sync_resubscribe(stage, subscription_tag, reason, opts, timeout \\ 5000) do
    sync_subscribe(stage, {subscription_tag, reason}, opts, timeout)
  end

  defp sync_subscribe(stage, cancel, opts, timeout) do
    {to, opts} =
      Keyword.pop_lazy(opts, :to, fn ->
        raise ArgumentError, "expected :to argument in sync_(re)subscribe"
      end)
    call(stage, {:"$subscribe", cancel, to, opts}, timeout)
  end

  @doc """
  Asks the consumer to subscribe to the given producer asynchronously.

  This call returns `:ok` regardless if the subscription
  effectively happened or not. It is typically called from
  a stage's `c:init/1` callback.

  ## Options

  This function accepts the same options as `sync_subscribe/4`.
  """
  @spec async_subscribe(stage, options) :: :ok
  def async_subscribe(stage, opts) do
    async_subscribe(stage, nil, opts)
  end

  @doc """
  Cancels `subscription_tag` with `reason` and subscribe asynchronously in one
  step.

  See `async_subscribe/2` for examples and options.
  """
  @spec async_resubscribe(stage, subscription_tag, reason :: term, options) :: :ok
  def async_resubscribe(stage, subscription_tag, reason, opts) do
    async_subscribe(stage, {subscription_tag, reason}, opts)
  end

  defp async_subscribe(stage, cancel, opts) do
    {to, opts} =
      Keyword.pop_lazy(opts, :to, fn ->
        raise ArgumentError, "expected :to argument in async_(re)subscribe"
      end)
    cast(stage, {:"$subscribe", cancel, to, opts})
  end

  @doc """
  Asks the given demand to the producer.

  `producer_subscription` is the subscription this demand will be asked on; this
  term could be for example stored in the stage when received in
  `c:handle_subscribe/4`.

  The demand is a non-negative integer with the amount of events to
  ask a producer for. If the demand is `0`, this function simply returns `:ok`
  without asking for data.

  This function must only be used in the cases when a consumer
  sets a subscription to `:manual` mode in the `c:handle_subscribe/4`
  callback.

  It accepts the same options as `Process.send/3`, and returns the same value as
  `Process.send/3`.
  """
  @spec ask(from, non_neg_integer, [:noconnect | :nosuspend]) ::
        :ok | :noconnect | :nosuspend
  def ask(producer_subscription, demand, opts \\ [])

  def ask({_pid, _ref}, 0, _opts) do
    :ok
  end

  def ask({pid, ref}, demand, opts) when is_integer(demand) and demand > 0 do
    Process.send(pid, {:"$gen_producer", {self(), ref}, {:ask, demand}}, opts)
  end

  @doc """
  Cancels the given subscription on the producer.

  Once the producer receives the request, a confirmation
  may be forwarded to the consumer (although there is no
  guarantee as the producer may crash for unrelated reasons
  before). This is an asynchronous request.

  It accepts the same options as `Process.send/3`, and returns the same value as
  `Process.send/3`.
  """
  @spec cancel(from, term, [:noconnect | :nosuspend]) ::
        :ok | :noconnect | :nosuspend
  def cancel({pid, ref} = _producer_subscription, reason, opts \\ []) do
    Process.send(pid, {:"$gen_producer", {self(), ref}, {:cancel, reason}}, opts)
  end

  @compile {:inline, send_noconnect: 2, ask: 3, cancel: 3}

  defp send_noconnect(pid, msg) do
    Process.send(pid, msg, [:noconnect])
  end

  @doc """
  Makes a synchronous call to the `stage` and waits for its reply.

  The client sends the given `request` to the stage and waits until a reply
  arrives or a timeout occurs. `c:handle_call/3` will be called on the stage
  to handle the request.

  `stage` can be any of the values described in the "Name registration"
  section of the documentation for this module.

  ## Timeouts

  `timeout` is an integer greater than zero which specifies how many
  milliseconds to wait for a reply, or the atom `:infinity` to wait
  indefinitely. The default value is `5000`. If no reply is received within
  the specified time, the function call fails and the caller exits. If the
  caller catches the failure and continues running, and the stage is just late
  with the reply, such reply may arrive at any time later into the caller's message
  queue. The caller must in this case be prepared for this and discard any such
  garbage messages that are two-element tuples with a reference as the first
  element.
  """
  @spec call(stage, term, timeout) :: term
  def call(stage, request, timeout \\ 5000) do
    GenServer.call(stage, request, timeout)
  end

  @doc """
  Sends an asynchronous request to the `stage`.

  This function always returns `:ok` regardless of whether
  the destination `stage` (or node) exists. Therefore it
  is unknown whether the destination stage successfully
  handled the message.

  `c:handle_cast/2` will be called on the stage to handle
  the request. In case the `stage` is on a node which is
  not yet connected to the caller one, the call is going to
  block until a connection happens.
  """
  @spec cast(stage, term) :: :ok
  def cast(stage, request) do
    GenServer.cast(stage, request)
  end

  @doc """
  Replies to a client.

  This function can be used to explicitly send a reply to a client that
  called `call/3` when the reply cannot be specified in the return value
  of `c:handle_call/3`.

  `client` must be the `from` argument (the second argument) accepted by
  `c:handle_call/3` callbacks. `reply` is an arbitrary term which will be given
  back to the client as the return value of the call.

  Note that `reply/2` can be called from any process, not just the `GenStage`
  that originally received the call (as long as that `GenStage` communicated the
  `from` argument somehow).

  This function always returns `:ok`.

  ## Examples

      def handle_call(:reply_in_one_second, from, state) do
        Process.send_after(self(), {:reply, from}, 1_000)
        {:noreply, [], state}
      end

      def handle_info({:reply, from}, state) do
        GenStage.reply(from, :one_second_has_passed)
      end

  """
  @spec reply(GenServer.from, term) :: :ok
  def reply(client, reply)

  def reply({to, tag}, reply) when is_pid(to) do
    try do
      send(to, {tag, reply})
      :ok
    catch
      _, _ -> :ok
    end
  end

  @doc """
  Stops the stage with the given `reason`.

  The `c:terminate/2` callback of the given `stage` will be invoked before
  exiting. This function returns `:ok` if the server terminates with the
  given reason; if it terminates with another reason, the call exits.

  This function keeps OTP semantics regarding error reporting.
  If the reason is any other than `:normal`, `:shutdown` or
  `{:shutdown, _}`, an error report is logged.
  """
  @spec stop(stage, term, timeout) :: :ok
  def stop(stage, reason \\ :normal, timeout \\ :infinity) do
    :gen.stop(stage, reason, timeout)
  end

  @doc """
  Starts a producer stage from an enumerable (or stream).

  This function will start a stage linked to the current process
  that will take items from the enumerable when there is demand.
  Since streams are enumerables, we can also pass streams as
  arguments (in fact, streams are the most common argument to
  this function).

  The enumerable is consumed in batches, retrieving `max_demand`
  items the first time and then `max_demand - min_demand` the
  next times. Therefore, for streams that cannot produce items
  that fast, it is recommended to pass a lower `:max_demand`
  value as an option.

  It is also expected the enumerable is able to produce the whole
  batch on demand or terminate. If the enumerable is a blocking one,
  for example, because it needs to wait for data from another source,
  it will block until the current batch is fully filled. GenStage and
  Flow were created exactly to address such issue. So if you have a
  blocking enumerable that you want to use in your Flow, then it must
  be implemented with GenStage and integrated with `from_stages/2`.

  When the enumerable finishes or halts, the stage will exit with
  `:normal` reason. This means that, if a consumer subscribes to
  the enumerable stage and the `:cancel` option is set to
  `:permanent`, which is the default, the consumer will also exit
  with `:normal` reason. This behaviour can be changed by setting
  setting the `:cancel` option to either `:transient` or `:temporary`
  at the moment of subscription as described in the `sync_subscribe/3`
  docs.

  Keep in mind that streams that require the use of the process
  inbox to work most likely won't behave as expected with this
  function since the mailbox is controlled by the stage process
  itself. As explained above, stateful or blocking enumerables
  are generally discouraged, as `GenStage` was designed precisely
  to support exchange of data in such cases.

  ## Options

    * `:link` - when false, does not link the stage to the current
      process. Defaults to `true`.

    * `:dispatcher` - the dispatcher responsible for handling demands.
      Defaults to `GenStage.DemandDispatch`. May be either an atom or
      a tuple with the dispatcher and the dispatcher options.

    * `:demand` - configures the demand to `:forward` or `:accumulate`
      mode. See `c:init/1` and `demand/2` for more information.

  All other options that would be given for `start_link/3` are
  also accepted.
  """
  @spec from_enumerable(Enumerable.t, keyword()) :: GenServer.on_start
  def from_enumerable(stream, opts \\ []) do
    case Keyword.pop(opts, :link, true) do
      {true, opts} -> start_link(GenStage.Streamer, {stream, opts}, opts)
      {false, opts} -> start(GenStage.Streamer, {stream, opts}, opts)
    end
  end

  @doc """
  Creates a stream that subscribes to the given producers
  and emits the appropriate messages.

  It expects a list of producers to subscribe to. Each element
  represents the producer or a tuple with the producer and the
  subscription options as defined in `sync_subscribe/2`:

      GenStage.stream([{producer, max_demand: 100}])

  If the producer process exits, the stream will exit with the same
  reason. If you want the stream to halt instead, set the cancel option
  to either `:transient` or `:temporary` as described in the
  `sync_subscribe/3` docs:

      GenStage.stream([{producer, max_demand: 100, cancel: :transient}])

  Once all producers are subscribed to, their demand is automatically
  set to `:forward` mode. See the `:demand` and `:producers`
  options below for more information.

  `GenStage.stream/1` will "hijack" the inbox of the process
  enumerating the stream to subscribe and receive messages
  from producers. However it guarantees it won't remove or
  leave unwanted messages in the mailbox after enumeration
  unless one of the producers comes from a remote node.
  For more information, read the "Known limitations" section
  below.

  ## Options

    * `:demand` - sets the demand in producers to `:forward` or
     `:accumulate` after subscription. Defaults to `:forward` so
       the stream can receive items.

    * `:producers` - the processes to set the demand to `:forward`
      on initialization. It defaults to the processes being subscribed
      to. Sometimes the stream is subscribing to a `:producer_consumer`
      instead of a `:producer`, in such cases, you can set this option
      to either an empty list or the list of actual producers so their
      demand is properly set.

  ## Known limitations

  ### `from_enumerable/2`

  This module also provides a function called `from_enumerable/2`
  which receives an enumerable (like a stream) and creates a stage
  that emits data from the enumerable.

  Given both `GenStage.from_enumerable/2` and `GenStage.stream/1`
  require the process inbox to send and receive messages, it is
  impossible to run a `stream/2` inside a `from_enumerable/2` as
  the `stream/2` will never receive the messages it expects.

  ### Remote nodes

  While it is possible to stream messages from remote nodes
  such should be done with care. In particular, in case of
  disconnections, there is a chance the producer will send
  messages after the consumer receives its DOWN messages and
  those will remain in the process inbox, violating the
  common scenario where `GenStage.stream/1` does not pollute
  the caller inbox. In such cases, it is recommended to
  consume such streams from a separate process which will be
  discarded after the stream is consumed.
  """
  @spec stream([stage | {stage, keyword}], keyword) :: Enumerable.t
  def stream(subscriptions, opts \\ [])

  def stream(subscriptions, options) when is_list(subscriptions) do
    subscriptions = :lists.map(&stream_validate_opts/1, subscriptions)
    Stream.resource(fn -> init_stream(subscriptions, options) end,
                    &consume_stream/1,
                    &close_stream/1)
  end

  def stream(subscriptions, _options) do
    raise ArgumentError, "GenStage.stream/1 expects a list of subscriptions, got: #{inspect subscriptions}"
  end

  defp stream_validate_opts({to, opts}) when is_list(opts) do
    with {:ok, cancel, _} <- validate_in(opts, :cancel, :permanent, [:temporary, :transient, :permanent]),
         {:ok, max, _} <- validate_integer(opts, :max_demand, 1000, 1, :infinity, false),
         {:ok, min, _} <- validate_integer(opts, :min_demand, div(max, 2), 0, max - 1, false) do
      {to, cancel, min, max, opts}
    else
      {:error, message} ->
        raise ArgumentError, "invalid options for #{inspect to} producer (#{message})"
    end
  end

  defp stream_validate_opts(to) do
    stream_validate_opts({to, []})
  end

  defp init_stream(subscriptions, options) do
    parent = self()
    demand = options[:demand] || :forward

    {monitor_pid, monitor_ref} =
      spawn_monitor(fn -> init_monitor(parent, demand) end)
    send(monitor_pid, {parent, monitor_ref})
    send(monitor_pid, {monitor_ref, {:subscribe, subscriptions}})

    receive do
      {:DOWN, ^monitor_ref, _, _, reason} ->
        exit(reason)
      {^monitor_ref, {:subscriptions, demand, subscriptions}} ->
        if producers = options[:producers] do
          for pid <- producers, do: demand(pid, demand)
        else
          demand_stream_subscriptions(demand, subscriptions)
        end
        {:receive, monitor_pid, monitor_ref, subscriptions}
    end
  end

  defp demand_stream_subscriptions(demand, subscriptions) do
    Enum.each(subscriptions, fn
      {_, {:subscribed, pid, _, _, _, _}} -> demand(pid, demand)
    end)
  end

  defp init_monitor(parent, demand) do
    parent_ref = Process.monitor(parent)

    receive do
      {:DOWN, ^parent_ref, _, _, reason} ->
        exit(reason)
      {^parent, monitor_ref} ->
        loop_monitor(parent, parent_ref, monitor_ref, demand, [])
    end
  end

  defp subscriptions_monitor(parent, monitor_ref, subscriptions) do
    :lists.foldl(fn {to, cancel, min, max, opts}, acc ->
      producer_pid = GenServer.whereis(to)

      cond do
        producer_pid != nil ->
          inner_ref = Process.monitor(producer_pid)
          from = {parent, {monitor_ref, inner_ref}}
          send_noconnect(producer_pid, {:"$gen_producer", from, {:subscribe, nil, opts}})
          send_noconnect(producer_pid, {:"$gen_producer", from, {:ask, max}})
          Map.put(acc, inner_ref, {:subscribed, producer_pid, cancel, min, max, max})
        cancel == :permanent or cancel == :transient ->
          exit({:noproc, {GenStage, :init_stream, [subscriptions]}})
        cancel == :temporary ->
          acc
      end
    end, %{}, subscriptions)
  end

  defp loop_monitor(parent, parent_ref, monitor_ref, demand, keys) do
    receive do
      {^monitor_ref, {:subscribe, pairs}} ->
        subscriptions = subscriptions_monitor(parent, monitor_ref, pairs)
        send(parent, {monitor_ref, {:subscriptions, demand, subscriptions}})
        loop_monitor(parent, parent_ref, monitor_ref, demand, Map.keys(subscriptions) ++ keys)
      {:DOWN, ^parent_ref, _, _, reason} ->
        exit(reason)
      {:DOWN, ref, _, _, reason} ->
        if ref in keys do
          send(parent, {monitor_ref, {:DOWN, ref, reason}})
        end
        loop_monitor(parent, parent_ref, monitor_ref, demand, keys -- [ref])
    end
  end

  defp cancel_monitor(monitor_pid, monitor_ref) do
    # Cancel the old ref and get a fresh one since
    # the monitor_ref may already have been received.
    Process.demonitor(monitor_ref, [:flush])

    ref = Process.monitor(monitor_pid)
    Process.exit(monitor_pid, :kill)

    receive do
      {:DOWN, ^ref, _, _, _} ->
        flush_monitor(monitor_ref)
    end
  end

  defp flush_monitor(monitor_ref) do
    receive do
      {^monitor_ref, _} ->
        flush_monitor(monitor_ref)
    after
      0 -> :ok
    end
  end

  defp consume_stream({:receive, monitor_pid, monitor_ref, subscriptions}) do
    receive_stream(monitor_pid, monitor_ref, subscriptions)
  end
  defp consume_stream({:ask, from, ask, batches, monitor_pid, monitor_ref, subscriptions}) do
    ask(from, ask, [:noconnect])
    deliver_stream(batches, from, monitor_pid, monitor_ref, subscriptions)
  end

  defp close_stream({:receive, monitor_pid, monitor_ref, subscriptions}) do
    request_to_cancel_stream(monitor_pid, monitor_ref, subscriptions)
    cancel_monitor(monitor_pid, monitor_ref)
  end
  defp close_stream({:ask, _, _, _, monitor_pid, monitor_ref, subscriptions}) do
    request_to_cancel_stream(monitor_pid, monitor_ref, subscriptions)
    cancel_monitor(monitor_pid, monitor_ref)
  end
  defp close_stream({:exit, reason, monitor_pid, monitor_ref, subscriptions}) do
    request_to_cancel_stream(monitor_pid, monitor_ref, subscriptions)
    cancel_monitor(monitor_pid, monitor_ref)
    exit({reason, {GenStage, :close_stream, [subscriptions]}})
  end

  defp receive_stream(monitor_pid, monitor_ref, subscriptions) when map_size(subscriptions) == 0 do
    {:halt, {:receive, monitor_pid, monitor_ref, subscriptions}}
  end
  defp receive_stream(monitor_pid, monitor_ref, subscriptions) do
    receive do
      {:"$gen_consumer", {producer_pid, {^monitor_ref, inner_ref} = ref}, events} when is_list(events) ->
        case subscriptions do
          %{^inner_ref => {:subscribed, producer_pid, cancel, min, max, demand}} ->
            from = {producer_pid, ref}
            {demand, batches} = split_batches(events, from, min, max, demand, demand, [])
            subscribed = {:subscribed, producer_pid, cancel, min, max, demand}
            deliver_stream(batches, from, monitor_pid, monitor_ref,
                           Map.put(subscriptions, inner_ref, subscribed))
          %{^inner_ref => {:cancel, _}} ->
            # We received this message before the cancellation was processed
            receive_stream(monitor_pid, monitor_ref, subscriptions)
          _ ->
            # Cancel if messages are out of order or unknown
            send_noconnect(producer_pid, {:"$gen_producer", {self(), ref}, {:cancel, :unknown_subscription}})
            receive_stream(monitor_pid, monitor_ref, Map.delete(subscriptions, inner_ref))
        end

      {:"$gen_consumer", {_, {^monitor_ref, inner_ref}}, {:cancel, reason}} ->
        cancel_stream(inner_ref, reason, monitor_pid, monitor_ref, subscriptions)

      {:"$gen_cast", {:"$subscribe", nil, to, opts}} ->
        send(monitor_pid, {monitor_ref, {:subscribe, [stream_validate_opts({to, opts})]}})
        receive do
          {^monitor_ref, {:subscriptions, demand, new_subscriptions}} ->
            demand_stream_subscriptions(demand, new_subscriptions)
            receive_stream(monitor_pid, monitor_ref, Map.merge(subscriptions, new_subscriptions))

          {^monitor_ref, {:DOWN, inner_ref, reason}} ->
            cancel_stream(inner_ref, reason, monitor_pid, monitor_ref, subscriptions)
        end

      {:DOWN, ^monitor_ref, _, _, reason} ->
        {:halt, {:exit, reason, monitor_pid, monitor_ref, subscriptions}}

      {^monitor_ref, {:DOWN, inner_ref, reason}} ->
        cancel_stream(inner_ref, reason, monitor_pid, monitor_ref, subscriptions)
    end
  end

  defp deliver_stream([], _from, monitor_pid, monitor_ref, subscriptions) do
    receive_stream(monitor_pid, monitor_ref, subscriptions)
  end
  defp deliver_stream([{events, ask} | batches], from, monitor_pid, monitor_ref, subscriptions) do
    {events, {:ask, from, ask, batches, monitor_pid, monitor_ref, subscriptions}}
  end

  defp request_to_cancel_stream(monitor_pid, monitor_ref, subscriptions) do
    subscriptions =
      :maps.fold(fn inner_ref, tuple, acc ->
        request_to_cancel_stream(inner_ref, tuple, monitor_ref, acc)
      end, subscriptions, subscriptions)
    receive_stream(monitor_pid, monitor_ref, subscriptions)
  end

  defp request_to_cancel_stream(_ref, {:cancel, _}, _monitor_ref, subscriptions) do
    subscriptions
  end
  defp request_to_cancel_stream(inner_ref, tuple, monitor_ref, subscriptions) do
    process_pid = elem(tuple, 1)
    cancel({process_pid, {monitor_ref, inner_ref}}, :normal, [:noconnect])
    Map.put(subscriptions, inner_ref, {:cancel, process_pid})
  end

  defp cancel_stream(inner_ref, reason, monitor_pid, monitor_ref, subscriptions) do
    case subscriptions do
      %{^inner_ref => {_, _, cancel, _, _, _}}
          when cancel == :permanent
          when cancel == :transient and not is_transient_shutdown(reason) ->
        Process.demonitor(inner_ref, [:flush])
        {:halt, {:exit, reason, monitor_pid, monitor_ref, Map.delete(subscriptions, inner_ref)}}
      %{^inner_ref => _} ->
        Process.demonitor(inner_ref, [:flush])
        receive_stream(monitor_pid, monitor_ref, Map.delete(subscriptions, inner_ref))
      %{} ->
        receive_stream(monitor_pid, monitor_ref, subscriptions)
    end
  end

  ## Callbacks

  @compile :inline_list_funcs

  @doc false
  def init({mod, args}) do
    case mod.init(args) do
      {:producer, state} ->
        init_producer(mod, [], state)
      {:producer, state, opts} when is_list(opts) ->
        init_producer(mod, opts, state)
      {:producer_consumer, state} ->
        init_producer_consumer(mod, [], state)
      {:producer_consumer, state, opts} when is_list(opts) ->
        init_producer_consumer(mod, opts, state)
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
    with {:ok, dispatcher_mod, dispatcher_state, opts} <- validate_dispatcher(opts),
         {:ok, buffer_size, opts} <- validate_integer(opts, :buffer_size, 10000, 0, :infinity, true),
         {:ok, buffer_keep, opts} <- validate_in(opts, :buffer_keep, :last, [:first, :last]),
         {:ok, demand, opts} <- validate_in(opts, :demand, :forward, [:accumulate, :forward]),
         :ok <- validate_no_opts(opts) do
      {:ok, %GenStage{mod: mod, state: state, type: :producer,
                      buffer: {:queue.new, 0, init_wheel(buffer_size)},
                      buffer_config: {buffer_size, buffer_keep},
                      events: if(demand == :accumulate, do: [], else: :forward),
                      dispatcher_mod: dispatcher_mod, dispatcher_state: dispatcher_state}}
    else
      {:error, message} -> {:stop, {:bad_opts, message}}
    end
  end

  defp validate_dispatcher(opts) do
    case Keyword.pop(opts, :dispatcher, GenStage.DemandDispatcher) do
      {dispatcher, opts} when is_atom(dispatcher) ->
        {:ok, dispatcher_state} = dispatcher.init([])
        {:ok, dispatcher, dispatcher_state, opts}
      {{dispatcher, dispatcher_opts}, opts} when is_atom(dispatcher) and is_list(dispatcher_opts) ->
        {:ok, dispatcher_state} = dispatcher.init(dispatcher_opts)
        {:ok, dispatcher, dispatcher_state, opts}
      {other, _opts} ->
        {:error, "expected :dispatcher to be an atom or a {atom, list}, got: #{inspect other}"}
    end
  end

  defp init_producer_consumer(mod, opts, state) do
    with {:ok, dispatcher_mod, dispatcher_state, opts} <- validate_dispatcher(opts),
         {:ok, subscribe_to, opts} <- validate_list(opts, :subscribe_to, []),
         {:ok, buffer_size, opts} <- validate_integer(opts, :buffer_size, :infinity, 0, :infinity, true),
         {:ok, buffer_keep, opts} <- validate_in(opts, :buffer_keep, :last, [:first, :last]),
         :ok <- validate_no_opts(opts) do
      stage = %GenStage{mod: mod, state: state, type: :producer_consumer,
                        buffer: {:queue.new, 0, init_wheel(buffer_size)},
                        buffer_config: {buffer_size, buffer_keep}, events: 0,
                        dispatcher_mod: dispatcher_mod, dispatcher_state: dispatcher_state}
      consumer_init_subscribe(subscribe_to, stage)
    else
      {:error, message} -> {:stop, {:bad_opts, message}}
    end
  end

  defp init_consumer(mod, opts, state) do
    with {:ok, subscribe_to, opts} <- validate_list(opts, :subscribe_to, []),
         :ok <- validate_no_opts(opts) do
      stage = %GenStage{mod: mod, state: state, type: :consumer}
      consumer_init_subscribe(subscribe_to, stage)
    else
      {:error, message} -> {:stop, {:bad_opts, message}}
    end
  end

  defp validate_list(opts, key, default) do
    {value, opts} = Keyword.pop(opts, key, default)

    if is_list(value) do
      {:ok, value, opts}
    else
      {:error, "expected #{inspect key} to be a list, got: #{inspect value}"}
    end
  end

  defp validate_in(opts, key, default, values) do
    {value, opts} = Keyword.pop(opts, key, default)

    if value in values do
      {:ok, value, opts}
    else
      {:error, "expected #{inspect key} to be one of #{inspect values}, got: #{inspect value}"}
    end
  end

  defp validate_integer(opts, key, default, min, max, infinity?) do
    {value, opts} = Keyword.pop(opts, key, default)

    cond do
      value == :infinity and infinity? ->
        {:ok, value, opts}
      not is_integer(value) ->
        {:error, "expected #{inspect key} to be an integer, got: #{inspect value}"}
      value < min ->
        {:error, "expected #{inspect key} to be equal to or greater than #{min}, got: #{inspect value}"}
      value > max ->
        {:error, "expected #{inspect key} to be equal to or less than #{max}, got: #{inspect value}"}
      true ->
        {:ok, value, opts}
    end
  end

  defp validate_no_opts(opts) do
    if opts == [] do
      :ok
    else
      {:error, "unknown options #{inspect opts}"}
    end
  end

  @doc false

  def handle_call({:"$info", msg}, _from, stage) do
    producer_info(msg, stage)
  end

  def handle_call({:"$subscribe", current, to, opts}, _from, stage) do
    consumer_subscribe(current, to, opts, stage)
  end

  def handle_call(msg, from, %{mod: mod, state: state} = stage) do
    case mod.handle_call(msg, from, state) do
      {:reply, reply, events, state} when is_list(events) ->
        stage = dispatch_events(events, length(events), stage)
        {:reply, reply, %{stage | state: state}}
      {:reply, reply, events, state, :hibernate} when is_list(events) ->
        stage = dispatch_events(events, length(events), stage)
        {:reply, reply, %{stage | state: state}, :hibernate}
      {:stop, reason, reply, state} ->
        {:stop, reason, reply, %{stage | state: state}}
      return ->
        handle_noreply_callback(return, stage)
    end
  end

  @doc false
  def handle_cast({:"$info", msg}, stage) do
    {:reply, _, stage} = producer_info(msg, stage)
    {:noreply, stage}
  end

  def handle_cast({:"$demand", mode}, stage) do
    producer_demand(mode, stage)
  end

  def handle_cast({:"$subscribe", current, to, opts}, stage) do
    case consumer_subscribe(current, to, opts, stage) do
      {:reply, _, stage}        -> {:noreply, stage}
      {:stop, reason, _, stage} -> {:stop, reason, stage}
      {:stop, _, _} = stop      -> stop
    end
  end

  def handle_cast(msg, %{state: state} = stage) do
    noreply_callback(:handle_cast, [msg, state], stage)
  end

  @doc false
  def handle_info({:DOWN, ref, _, _, reason} = msg,
                  %{producers: producers, monitors: monitors, state: state} = stage) do
    case producers do
      %{^ref => _} ->
        consumer_cancel(ref, :down, reason, stage)
      %{} ->
        case monitors do
          %{^ref => consumer_ref} ->
            producer_cancel(consumer_ref, :down, reason, stage)
          %{} ->
            noreply_callback(:handle_info, [msg, state], stage)
        end
    end
  end

  ## Producer messages

  def handle_info({:"$gen_producer", _, _} = msg, %{type: :consumer} = stage) do
    :error_logger.error_msg('GenStage consumer ~p received $gen_producer message: ~p~n', [name(), msg])
    {:noreply, stage}
  end

  def handle_info({:"$gen_producer", {consumer_pid, ref} = from, {:subscribe, cancel, opts}},
                  %{consumers: consumers} = stage) do
    case consumers do
      %{^ref => _} ->
        :error_logger.error_msg('GenStage producer ~p received duplicated subscription from: ~p~n', [name(), from])
        send_noconnect(consumer_pid, {:"$gen_consumer", {self(), ref}, {:cancel, :duplicated_subscription}})
        {:noreply, stage}
      %{} ->
        case maybe_producer_cancel(cancel, stage) do
          {:noreply, stage} ->
            mon_ref = Process.monitor(consumer_pid)
            stage = put_in stage.monitors[mon_ref], ref
            stage = put_in stage.consumers[ref], {consumer_pid, mon_ref}
            producer_subscribe(opts, from, stage)
          other ->
            other
        end
    end
  end

  def handle_info({:"$gen_producer", {consumer_pid, ref} = from, {:ask, counter}},
                  %{consumers: consumers} = stage) when is_integer(counter) do
    case consumers do
      %{^ref => _} ->
        %{dispatcher_state: dispatcher_state} = stage
        dispatcher_callback(:ask, [counter, from, dispatcher_state], stage)
      %{} ->
        send_noconnect(consumer_pid, {:"$gen_consumer", {self(), ref}, {:cancel, :unknown_subscription}})
        {:noreply, stage}
    end
  end

  def handle_info({:"$gen_producer", {_, ref}, {:cancel, reason}}, stage) do
    producer_cancel(ref, :cancel, reason, stage)
  end

  ## Consumer messages

  def handle_info({:"$gen_consumer", _, _} = msg, %{type: :producer} = stage) do
    :error_logger.error_msg('GenStage producer ~p received $gen_consumer message: ~p~n', [name(), msg])
    {:noreply, stage}
  end

  def handle_info({:"$gen_consumer", {producer_pid, ref}, events},
                  %{type: :producer_consumer, events: demand_or_queue, producers: producers} = stage) when is_list(events) do
    case producers do
      %{^ref => _entry} ->
        {counter, queue} =
          case demand_or_queue do
            demand when is_integer(demand)  ->
              {demand, :queue.new}
            queue ->
              {0, queue}
          end
        queue = put_pc_events(events, ref, queue)
        take_pc_events(queue, counter, stage)
      _ ->
        send_noconnect(producer_pid, {:"$gen_producer", {self(), ref}, {:cancel, :unknown_subscription}})
        {:noreply, stage}
    end
  end

  def handle_info({:"$gen_consumer", {producer_pid, ref} = from, events},
                  %{type: :consumer, producers: producers, mod: mod, state: state} = stage) when is_list(events) do
    case producers do
      %{^ref => entry} ->
        {batches, stage} = consumer_receive(from, entry, events, stage)
        {_, reply} = consumer_dispatch(batches, from, mod, state, stage, 0, false)
        reply
      _ ->
        send_noconnect(producer_pid, {:"$gen_producer", {self(), ref}, {:cancel, :unknown_subscription}})
        {:noreply, stage}
    end
  end

  def handle_info({:"$gen_consumer", {_, ref}, {:cancel, reason}}, stage) do
    consumer_cancel(ref, :cancel, reason, stage)
  end

  ## Catch-all messages

  def handle_info(msg, %{state: state} = stage) do
    noreply_callback(:handle_info, [msg, state], stage)
  end

  @doc false
  def terminate(reason, %{mod: mod, state: state}) do
    mod.terminate(reason, state)
  end

  @doc false
  def code_change(old_vsn, %{mod: mod, state: state} = stage, extra) do
    case mod.code_change(old_vsn, state, extra) do
      {:ok, state} -> {:ok, %{stage | state: state}}
      other -> other
    end
  end

  @doc false
  def format_status(opt, [pdict, %{mod: mod, state: state} = stage]) do
    case {function_exported?(mod, :format_status, 2), opt} do
      {true, :normal} ->
        data = [{~c(State), state}] ++ format_status_for_stage(stage)
        format_status(mod, opt, pdict, state, [data: data])
      {true, :terminate} ->
        format_status(mod, opt, pdict, state, state)
      {false, :normal} ->
        [data: [{~c(State), state}] ++ format_status_for_stage(stage)]
      {false, :terminate} ->
        state
    end
  end

  defp format_status(mod, opt, pdict, state, default) do
    try do
      mod.format_status(opt, [pdict, state])
    catch
      _, _ ->
        default
    end
  end

  defp format_status_for_stage(%{type: :producer, consumers: consumers,
                                 buffer: buffer, dispatcher_mod: dispatcher_mod}) do
    {_, counter, _} = buffer
    consumer_pids = for {_, {pid, _}} <- consumers, do: pid
    [{~c(Stage), :producer},
     {~c(Dispatcher), dispatcher_mod},
     {~c(Consumers), consumer_pids},
     {~c(Buffer size), counter}]
  end

  defp format_status_for_stage(%{type: :producer_consumer, producers: producers, consumers: consumers,
                                 buffer: buffer, dispatcher_mod: dispatcher_mod}) do
    {_, counter, _} = buffer
    producer_pids = for {_, {pid, _, _}} <- producers, do: pid
    consumer_pids = for {_, {pid, _}} <- consumers, do: pid
    [{~c(Stage), :producer_consumer},
     {~c(Dispatcher), dispatcher_mod},
     {~c(Producers), producer_pids},
     {~c(Consumers), consumer_pids},
     {~c(Buffer size), counter}]
  end

  defp format_status_for_stage(%{type: :consumer, producers: producers}) do
    producer_pids = for {_, {pid, _, _}} <- producers, do: pid
    [{~c(Stage), :consumer},
     {~c(Producers), producer_pids}]
  end

  ## Shared helpers

  defp noreply_callback(callback, args, %{mod: mod} = stage) do
    handle_noreply_callback apply(mod, callback, args), stage
  end

  defp handle_noreply_callback(return, stage) do
    case return do
      {:noreply, events, state} when is_list(events) ->
        stage = dispatch_events(events, length(events), stage)
        {:noreply, %{stage | state: state}}
      {:noreply, events, state, :hibernate} when is_list(events) ->
        stage = dispatch_events(events, length(events), stage)
        {:noreply, %{stage | state: state}, :hibernate}
      {:stop, reason, state} ->
        {:stop, reason, %{stage | state: state}}
      other ->
        {:stop, {:bad_return_value, other}, stage}
    end
  end

  defp name() do
    case :erlang.process_info(self(), :registered_name) do
      {:registered_name, name} when is_atom(name) -> name
      _ -> self()
    end
  end

  ## Producer helpers

  defp producer_demand(:forward, %{type: :producer_consumer} = stage) do
    # That's the only mode on producer consumers.
    {:noreply, stage}
  end
  defp producer_demand(_mode, %{type: type} = stage) when type != :producer do
    :error_logger.error_msg('Demand mode can only be set for producers, GenStage ~p is a ~ts', [name(), type])
    {:noreply, stage}
  end
  defp producer_demand(:forward, %{events: events} = stage) do
    stage = %{stage | events: :forward}

    if is_list(events) do
      :lists.foldl(fn
        d, {:noreply, %{state: state} = stage} ->
          noreply_callback(:handle_demand, [d, state], stage)
        d, {:noreply, %{state: state} = stage, _} ->
          noreply_callback(:handle_demand, [d, state], stage)
        _, {:stop, _, _} = acc ->
          acc
      end, {:noreply, stage}, :lists.reverse(events))
    else
      {:noreply, stage}
    end
  end
  defp producer_demand(:accumulate, %{events: events} = stage) do
    if is_list(events) do
      {:noreply, stage}
    else
      {:noreply, %{stage | events: []}}
    end
  end

  defp producer_subscribe(opts, from, stage) do
    %{mod: mod, state: state, dispatcher_state: dispatcher_state} = stage

    case apply(mod, :handle_subscribe, [:consumer, opts, from, state]) do
      {:automatic, state} ->
        # Call the dispatcher after since it may generate demand and the
        # main module must know the consumer is subscribed.
        dispatcher_callback(:subscribe, [opts, from, dispatcher_state], %{stage | state: state})
      {:stop, reason, state} ->
        {:stop, reason, %{stage | state: state}}
      other ->
        {:stop, {:bad_return_value, other}, stage}
    end
  end

  defp maybe_producer_cancel({ref, reason}, stage) do
    producer_cancel(ref, :cancel, reason, stage)
  end
  defp maybe_producer_cancel(nil, stage) do
    {:noreply, stage}
  end

  defp producer_cancel(ref, kind, reason, stage) do
    %{consumers: consumers, monitors: monitors, state: state} = stage

    case Map.pop(consumers, ref) do
      {nil, _consumers} ->
        {:noreply, stage}
      {{pid, mon_ref}, consumers} ->
        Process.demonitor(mon_ref, [:flush])
        send_noconnect(pid, {:"$gen_consumer", {self(), ref}, {:cancel, reason}})
        stage = %{stage | consumers: consumers, monitors: Map.delete(monitors, mon_ref)}

        case noreply_callback(:handle_cancel, [{kind, reason}, {pid, ref}, state], stage) do
          {:noreply, %{dispatcher_state: dispatcher_state} = stage} ->
            # Call the dispatcher after since it may generate demand and the
            # main module must know the consumer is no longer subscribed.
            dispatcher_callback(:cancel, [{pid, ref}, dispatcher_state], stage)
          {:stop, _, _} = stop ->
            stop
        end
    end
  end

  defp dispatcher_callback(callback, args, %{dispatcher_mod: dispatcher_mod} = stage) do
    {:ok, counter, dispatcher_state} = apply(dispatcher_mod, callback, args)
    stage = %{stage | dispatcher_state: dispatcher_state}

    case take_from_buffer(counter, stage) do
      {:ok, 0, stage} ->
        {:noreply, stage}
      {:ok, counter, stage} when is_integer(counter) and counter > 0 ->
        case stage do
          # producer
          %{events: :forward, state: state} ->
            noreply_callback(:handle_demand, [counter, state], stage)
          %{events: events} when is_list(events) ->
            {:noreply, %{stage | events: [counter | events]}}

          # producer_consumer
          %{events: events} when is_integer(events) ->
            {:noreply, %{stage | events: counter + events}}
          %{events: queue} ->
            take_pc_events(queue, counter, stage)
        end
    end
  end

  defp dispatch_events([], _length, stage) do
    stage
  end
  defp dispatch_events(events, _length, %{type: :consumer} = stage) do
    :error_logger.error_msg('GenStage consumer ~p cannot dispatch events (an empty list must be returned): ~p~n', [name(), events])
    stage
  end
  defp dispatch_events(events, _length, %{consumers: consumers} = stage) when map_size(consumers) == 0 do
    buffer_events(events, stage)
  end
  defp dispatch_events(events, length, stage) do
    %{dispatcher_mod: dispatcher_mod, dispatcher_state: dispatcher_state} = stage
    {:ok, events, dispatcher_state} = dispatcher_mod.dispatch(events, length, dispatcher_state)
    buffer_events(events, %{stage | dispatcher_state: dispatcher_state})
  end

  defp take_from_buffer(counter, %{buffer: {_, buffer, _}} = stage)
       when counter == 0 when buffer == 0 do
    {:ok, counter, stage}
  end

  defp take_from_buffer(counter, %{buffer: {queue, buffer, infos}} = stage) do
    {queue, events, new_counter, buffer, info, infos} =
      take_from_buffer(queue, [], counter, buffer, infos)
    # Update the buffer because dispatch events may
    # trigger more events to be buffered.
    stage = %{stage | buffer: {queue, buffer, infos}}
    stage = dispatch_events(events, counter - new_counter, stage)
    case info do
      {:ok, msg} ->
        take_from_buffer(new_counter, dispatch_info(msg, stage))
      :error ->
        take_from_buffer(new_counter, stage)
    end
  end

  defp take_from_buffer(queue, events, 0, buffer, infos) do
    {queue, :lists.reverse(events), 0, buffer, :error, infos}
  end

  defp take_from_buffer(queue, events, counter, 0, infos) do
    {queue, :lists.reverse(events), counter, 0, :error, infos}
  end

  defp take_from_buffer(queue, events, counter, buffer, infos) when is_reference(infos) do
    {{:value, value}, queue} = :queue.out(queue)

    case value do
      {^infos, msg} ->
        {queue, :lists.reverse(events), counter, buffer - 1, {:ok, msg}, infos}
      val ->
        take_from_buffer(queue, [val | events], counter - 1, buffer - 1, infos)
    end
  end

  defp take_from_buffer(queue, events, counter, buffer, wheel) do
    {{:value, value}, queue} = :queue.out(queue)

    case pop_and_increment_wheel(wheel) do
      {:ok, msg, wheel} ->
        {queue, :lists.reverse([value | events]), counter - 1, buffer - 1, {:ok, msg}, wheel}
      {:error, wheel} ->
        take_from_buffer(queue, [value | events], counter - 1, buffer - 1, wheel)
    end
  end

  defp buffer_events([], stage) do
    stage
  end
  defp buffer_events(events, %{buffer: {queue, counter, infos},
                               buffer_config: {max, keep}} = stage) do
    {{excess, queue, counter}, pending, infos} =
      queue_events(keep, events, queue, counter, max, infos)

    case excess do
      0 ->
        :ok
      excess ->
        :error_logger.warning_msg('GenStage producer ~p has discarded ~p events from buffer', [name(), excess])
    end

    stage = %{stage | buffer: {queue, counter, infos}}
    :lists.foldl(&dispatch_info/2, stage, pending)
  end

  defp queue_events(_keep, events, _queue, 0, :infinity, infos),
    do: {{0, :queue.from_list(events), length(events)}, [], infos}
  defp queue_events(_keep, events, queue, counter, :infinity, infos),
    do: {queue_infinity(events, queue, counter), [], infos}
  defp queue_events(:first, events, queue, counter, max, infos),
    do: {queue_first(events, queue, counter, max), [], infos}
  defp queue_events(:last, events, queue, counter, max, infos),
    do: queue_last(events, queue, 0, counter, max, [], infos)

  defp queue_infinity([], queue, counter),
    do: {0, queue, counter}
  defp queue_infinity([event | events], queue, counter),
    do: queue_infinity(events, :queue.in(event, queue), counter + 1)

  defp queue_first([], queue, counter, _max),
    do: {0, queue, counter}
  defp queue_first(events, queue, max, max),
    do: {length(events), queue, max}
  defp queue_first([event | events], queue, counter, max),
    do: queue_first(events, :queue.in(event, queue), counter + 1, max)

  defp queue_last([], queue, excess, counter, _max, pending, wheel),
    do: {{excess, queue, counter}, :lists.reverse(pending), wheel}
  defp queue_last([event | events], queue, excess, max, max, pending, wheel) do
    queue = :queue.in(event, :queue.drop(queue))
    case pop_and_increment_wheel(wheel) do
      {:ok, info, wheel} ->
        queue_last(events, queue, excess + 1, max, max, [info | pending], wheel)
      {:error, wheel} ->
        queue_last(events, queue, excess + 1, max, max, pending, wheel)
    end
  end
  defp queue_last([event | events], queue, excess, counter, max, pending, wheel),
    do: queue_last(events, :queue.in(event, queue), excess, counter + 1, max, pending, wheel)

  ## Info helpers

  # We use a wheel unless the queue is infinity.
  defp init_wheel(:infinity), do: make_ref()
  defp init_wheel(_), do: nil

  defp producer_info(msg, %{type: :consumer} = stage) do
    send(self(), msg)
    {:reply, :ok, stage}
  end
  defp producer_info(msg, %{type: :producer_consumer, events: demand_or_queue} = stage) do
    stage =
      case demand_or_queue do
        demand when is_integer(demand)  ->
          buffer_or_dispatch_info(msg, stage)
        queue ->
          %{stage | events: :queue.in({:info, msg}, queue)}
      end
    {:reply, :ok, stage}
  end
  defp producer_info(msg, %{type: :producer} = stage) do
    {:reply, :ok, buffer_or_dispatch_info(msg, stage)}
  end

  defp buffer_or_dispatch_info(msg, stage) do
    %{buffer: {queue, count, infos},
      buffer_config: {max, _keep}} = stage

    case count do
      0 ->
        dispatch_info(msg, stage)
      _ when is_reference(infos) ->
        queue = :queue.in({infos, msg}, queue)
        %{stage | buffer: {queue, count + 1, infos}}
      _ ->
        wheel = put_wheel(infos, count, max, msg)
        %{stage | buffer: {queue, count, wheel}}
    end
  end

  defp put_wheel(nil, count, max, contents),
    do: {0, max, %{count - 1 => contents}}
  defp put_wheel({pos, _, wheel}, count, max, contents),
    do: {pos, max, Map.put(wheel, rem(pos + count - 1, max), contents)}

  defp pop_and_increment_wheel(nil), do: {:error, nil}
  defp pop_and_increment_wheel({pos, limit, wheel}) do
    new_pos = rem(pos + 1, limit)

    # TODO: Use :maps.take/2
    case wheel do
      %{^pos => info} when map_size(wheel) == 1 ->
        {:ok, info, nil}
      %{^pos => info} ->
        {:ok, info, {new_pos, limit, Map.delete(wheel, pos)}}
      %{} ->
        {:error, {new_pos, limit, wheel}}
    end
  end

  defp dispatch_info(msg, stage) do
    %{dispatcher_mod: dispatcher_mod, dispatcher_state: dispatcher_state} = stage
    {:ok, dispatcher_state} = dispatcher_mod.info(msg, dispatcher_state)
    %{stage | dispatcher_state: dispatcher_state}
  end

  ## Consumer helpers

  defp consumer_init_subscribe(producers, stage) do
    :lists.foldl fn
      to, {:ok, stage} ->
        case consumer_subscribe(to, stage) do
          {:reply, _, stage}    -> {:ok, stage}
          {:stop, reason, _, _} -> {:stop, reason}
          {:stop, reason, _}    -> {:stop, reason}
        end
      _, {:stop, reason} ->
        {:stop, reason}
    end, {:ok, stage}, producers
  end

  defp consumer_receive({_, ref} = from, {producer_id, cancel, {demand, min, max}}, events, stage) do
    {demand, batches} = split_batches(events, from, min, max, demand, demand, [])
    stage = put_in stage.producers[ref], {producer_id, cancel, {demand, min, max}}
    {batches, stage}
  end
  defp consumer_receive(_, {_, _, :manual}, events, stage) do
    {[{events, 0}], stage}
  end

  defp split_batches([], _from, _min, _max, _old_demand, new_demand, batches) do
    {new_demand, :lists.reverse(batches)}
  end
  defp split_batches(events, from, min, max, old_demand, new_demand, batches) do
    {events, batch, batch_size} = split_events(events, max - min, 0, [])

    # Adjust the batch size to whatever is left of the demand in case of excess.
    {old_demand, batch_size} =
      case old_demand - batch_size do
        diff when diff < 0 ->
          :error_logger.error_msg('GenStage consumer ~p has received ~p events in excess from: ~p~n',
                                  [name(), abs(diff), from])
          {0, old_demand}
        diff ->
          {diff, batch_size}
      end

    # In case we've reached min, we will ask for more events.
    {new_demand, batch_size} =
      case new_demand - batch_size do
        diff when diff <= min ->
          {max, max - diff}
        diff ->
          {diff, 0}
      end

    split_batches(events, from, min, max, old_demand, new_demand, [{batch, batch_size} | batches])
  end

  defp split_events(events, limit, limit, acc),
    do: {events, :lists.reverse(acc), limit}
  defp split_events([], _limit, counter, acc),
    do: {[], :lists.reverse(acc), counter}
  defp split_events([event | events], limit, counter, acc),
    do: split_events(events, limit, counter + 1, [event | acc])

  defp consumer_dispatch([{batch, ask} | batches], from, mod, state, stage, count, _hibernate?) do
    case mod.handle_events(batch, from, state) do
      {:noreply, events, state} when is_list(events) ->
        stage = dispatch_events(events, length(events), stage)
        ask(from, ask, [:noconnect])
        consumer_dispatch(batches, from, mod, state, stage, count + length(events), false)
      {:noreply, events, state, :hibernate} when is_list(events) ->
        stage = dispatch_events(events, length(events), stage)
        ask(from, ask, [:noconnect])
        consumer_dispatch(batches, from, mod, state, stage, count + length(events), true)
      {:stop, reason, state} ->
        {count, {:stop, reason, %{stage | state: state}}}
      other ->
        {count, {:stop, {:bad_return_value, other}, %{stage | state: state}}}
    end
  end

  defp consumer_dispatch([], _from, _mod, state, stage, count, false) do
    {count, {:noreply, %{stage | state: state}}}
  end
  defp consumer_dispatch([], _from, _mod, state, stage, count, true) do
    {count, {:noreply, %{stage | state: state}, :hibernate}}
  end

  defp consumer_subscribe({to, opts}, stage) when is_list(opts),
    do: consumer_subscribe(nil, to, opts, stage)
  defp consumer_subscribe(to, stage),
    do: consumer_subscribe(nil, to, [], stage)

  defp consumer_subscribe(_cancel, to, _opts, %{type: :producer} = stage) do
    :error_logger.error_msg('GenStage producer ~p cannot be subscribed to another stage: ~p~n', [name(), to])
    {:reply, {:error, :not_a_consumer}, stage}
  end

  defp consumer_subscribe(current, to, opts, stage) do
    with {:ok, cancel, _} <- validate_in(opts, :cancel, :permanent, [:temporary, :transient, :permanent]),
         {:ok, max, _} <- validate_integer(opts, :max_demand, 1000, 1, :infinity, false),
         {:ok, min, _} <- validate_integer(opts, :min_demand, div(max, 2), 0, max - 1, false) do
      producer_pid = GenServer.whereis(to)
      cond do
        producer_pid != nil ->
          ref = Process.monitor(producer_pid)
          send_noconnect(producer_pid, {:"$gen_producer", {self(), ref}, {:subscribe, current, opts}})
          consumer_subscribe(opts, ref, producer_pid, cancel, min, max, stage)
        cancel == :permanent or cancel == :transient ->
          {:stop, :noproc, {:ok, make_ref()}, stage}
        cancel == :temporary ->
          {:reply, {:ok, make_ref()}, stage}
       end
    else
      {:error, message} ->
        :error_logger.error_msg('GenStage consumer ~p subscribe received invalid option: ~ts~n', [name(), message])
        {:reply, {:error, {:bad_opts, message}}, stage}
    end
  end

  defp consumer_subscribe(opts, ref, producer_pid, cancel, min, max, stage) do
    %{mod: mod, state: state} = stage
    to = {producer_pid, ref}

    case apply(mod, :handle_subscribe, [:producer, opts, to, state]) do
      {:automatic, state} ->
        ask(to, max, [:noconnect])
        stage = put_in stage.producers[ref], {producer_pid, cancel, {max, min, max}}
        {:reply, {:ok, ref}, %{stage | state: state}}
      {:manual, state} ->
        stage = put_in stage.producers[ref], {producer_pid, cancel, :manual}
        {:reply, {:ok, ref}, %{stage | state: state}}
      {:stop, reason, state} ->
        {:stop, reason, %{stage | state: state}}
      other ->
        {:stop, {:bad_return_value, other}, stage}
    end
  end

  defp consumer_cancel(ref, kind, reason, %{producers: producers} = stage) do
    case Map.pop(producers, ref) do
      {nil, _producers} ->
        {:noreply, stage}
      {{producer_pid, mode, _}, producers} ->
        Process.demonitor(ref, [:flush])
        stage = %{stage | producers: producers}
        schedule_cancel(mode, {kind, reason}, {producer_pid, ref}, stage)
    end
  end

  defp schedule_cancel(mode, kind_reason, pid_ref, %{type: :producer_consumer, events: demand_or_queue} = stage) do
    case demand_or_queue do
      demand when is_integer(demand)  ->
        invoke_cancel(mode, kind_reason, pid_ref, stage)
      queue ->
        {:noreply, %{stage | events: :queue.in({:cancel, mode, kind_reason, pid_ref}, queue)}}
    end
  end
  defp schedule_cancel(mode, kind_reason, pid_ref, stage) do
    invoke_cancel(mode, kind_reason, pid_ref, stage)
  end

  defp invoke_cancel(mode, {_, reason} = kind_reason, {pid, _} = pid_ref, %{state: state} = stage) do
    case noreply_callback(:handle_cancel, [kind_reason, pid_ref, state], stage) do
      {:noreply, stage}
          when mode == :permanent
          when mode == :transient and not is_transient_shutdown(reason) ->
        :error_logger.info_msg('GenStage consumer ~p is stopping after receiving cancel from producer ~p with reason: ~p~n', [name(), pid, reason])
        {:stop, reason, stage}
      other ->
        other
    end
  end

  ## Producer consumer helpers

  defp put_pc_events(events, ref, queue) do
    :queue.in({events, ref}, queue)
  end

  defp send_pc_events(events, ref,
                      %{mod: mod, state: state, producers: producers} = stage) do
    case producers do
      %{^ref => entry} ->
        {producer_id, _, _} = entry
        from = {producer_id, ref}
        {batches, stage} = consumer_receive(from, entry, events, stage)
        consumer_dispatch(batches, from, mod, state, stage, 0, false)
      %{} ->
        # We queued but producer was removed
        consumer_dispatch([{events, 0}], {:pid, ref}, mod, state, stage, 0, false)
    end
  end

  defp take_pc_events(queue, counter, stage) when counter > 0 do
    case :queue.out(queue) do
      {{:value, {:info, msg}}, queue} ->
        take_pc_events(queue, counter, buffer_or_dispatch_info(msg, stage))

      {{:value, {:cancel, mode, kind_reason, pid_ref}}, queue} ->
        case invoke_cancel(mode, kind_reason, pid_ref, stage) do
          {:noreply, stage} ->
            take_pc_events(queue, counter, stage)
          {:noreply, stage, :hibernate} ->
            take_pc_events(queue, counter, stage)
          {:stop, _, _} = stop ->
            stop
        end

      {{:value, {events, ref}}, queue} ->
        case send_pc_events(events, ref, stage) do
          {sent, {:noreply, stage}} ->
            take_pc_events(queue, counter - sent, stage)
          {sent, {:noreply, stage, :hibernate}} ->
            take_pc_events(queue, counter - sent, stage)
          {_, {:stop, _, _} = stop} ->
            stop
        end

      {:empty, _queue} ->
        {:noreply, %{stage | events: counter}}
    end
  end

  # It is OK to send more events than the consumer has
  # asked because those will always be buffered. Once
  # we have taken from the buffer, the event queue will
  # be adjusted again.
  defp take_pc_events(queue, _counter, stage) do
    {:noreply, %{stage | events: queue}}
  end
end
