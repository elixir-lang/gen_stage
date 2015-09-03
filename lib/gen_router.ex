defmodule GenRouter do
  @moduledoc ~S"""
  A behaviour module for routing events from multiple sources
  to multiple sinks.

  `GenRouter` allows developers to receive events from multiple
  sources and/or send events to multiple sinks. The relationship
  between sources (the one sending messages) and sinks (the one
  receiving them) is established when the sink explicitly asks
  for data.

  Since the same process can send and receive data, the same
  process can be both a source and sink, which is quite common.
  Sources that only send data and sinks that only receive data
  are called "definite source" and "definite sink", respectively.

  As an example, imagine the following processes A, B and C:

      [A] -> [B] -> [C]

  A is a source to B and B is a source to C. Similarly, C is a
  sink to B and B is a sink to A. A is a definite source, C is
  a definite sink.

  ## Incoming and outgoing

  The router is split in two parts, one for handling incoming
  messages and another to handle outgoing ones. Imagine `B` below
  is a router:

      [A] -> [B] -> [C]

  We can split it in two parts:

      [A] -> [incoming | outgoing] -> [C]

  Those parts are defined by different callback modules, allowing
  developers to compose functionality at will. For example, you
  can have a router that receives messages dynamically from multiple
  processes using `GenRouter.DynamicIn` composed with a router that
  relays those messages to multiple sinks at the same time
  (`GenRouter.BroadcastOut`). Or the same `GenRouter.DynamicIn`
  coupled with a custom `RoundRobinOut` mechanism.

  Let's see an example:

      # Starts a new router
      iex> {:ok, pid} = GenRouter.start_link(GenRouter.DynamicIn, [],
                                             GenRouter.BroadcastOut, [])

      # Let's ask the router for 10 messages, officialy making
      # the current process a sink for the current router
      iex> GenRouter.ask(pid, self(), Process.monitor(ref), 10)

      # Spawns a task that sends one event to the router
      iex> Task.start_link fn -> GenRouter.sync_notify(pid, :hello) end

      # Eventually the routed message arrives
      iex> flush()

  The documentation to implement the incoming and outgoing parts
  of the router are defined in `GenRouter.In` and `GenRouter.Out`
  respectively.

  ### Built-in incoming and outgoing

    * `GenRouter.In` - documents the callbacks required to implement
      the incoming part of a router (not an implementation)

    * `GenRouter.DynamicIn` - a special incoming router that receives
      events via `GenRouter.sync_notify/3` instead of receiving them
      from sources

    * `GenRouter.SingleIn` - a router that has a single source.
      The source can be given on start or by calling
      `GenRouter.subscribe/3`

    * `GenRouter.Out` - documents the callbacks required to implement
      the outgoing part of a router (not an implementation)

    * `GenRouter.BroadcastOut` - a router that has multiple sinks,
      synchronizing the demand between the sinks and broadcasting
      all incoming messages to all sinks

  ### Subscribing

  Sometimes the routing topology is defined dynamically. For example,
  developers may define multiple routers, so how to make the events
  flow between them?

  We have seen in the example above that the sink must explicitly
  ask the source for data. When we have multiple routers, we then
  need to tell a router (the sink) to ask another router (the
  source) for data. We call subscribe the process of telling one
  process to ask another process for data.

  Let's see an example:

      # Starts a new router that receives dynamic messages
      iex> {:ok, router1} = GenRouter.start_link(GenRouter.DynamicIn, [],
                                                 GenRouter.BroadcastOut, [])

      # Starts another router that will have a single source
      iex> {:ok, router2} = GenRouter.start_link(GenRouter.SingleIn, [],
                                                 GenRouter.BroadcastOut, []))

      # Let's ask the second router to subscribe to the first router
      iex> GenRouter.subscribe(router2, to: router1)

      # Finally ask the second router for data. The second router will
      # signal this demand to the first router (by calling `ask/4` internally)
      iex> GenRouter.ask(pid, self(), Process.monitor(ref), 10)

      # Spawns a task that sends one event to the first router
      iex> Task.start_link fn -> GenRouter.sync_notify(pid, :hello) end

      # The first router will send it to second the router which sends
      # it to the current process
      iex> flush()

  In other words, all subscribe does is to tell a router to ask another
  router for data. Imagine we have the following diagram:

      [router 1] - [router 2] - [the shell]

  Once the shell asks for data, the demand flows upstream:

      [router 1] <- [router 2] <- [the shell]

  Once events arrive to the router 1 (via `GenRouter.sync_notify/3`),
  it is sent downstream according to the demand:

      [router 1] -> [router 2] -> [the shell]

  One question is: when to use subscribe and when to use ask?

    * You use ask when you want the current process, the one you are
      currently in control of, to ask for events from a given source.
      In the example above, the shell process is asking for data

    * You use subscribe when you want another process, usually a
      router, to ask for events from a given resource. In the example
      above, the second router will ask the first router for data

  Note subscribing returns a pid and a reference. The reference can
  be given to ask the router to unsubscribe:

      GenRouter.unsubscribe(router2, ref)

  Or to cancel directly in the source:

      GenRouter.cancel(router1, ref, reason \\ :cancel)

  Finally, note it is not possible to ask a GenRouter with
  `GenRouter.DynamicIn` to subscribe to a source. That's because
  `GenRouter.DynamicIn` expects, by definition, to receive events
  dynamically and not from a single place.

  ### Custom sinks

  In the example above, the definite sink was the shell process.
  However, most of the times, we want to use a custom process as
  a sink too, which will also abstract the act of asking for data
  from us. This is done with `GenRouter.Sink`:

      defmodule MySink do
        use GenRouter.Sink

        def start_link do
          GenRouter.Sink.start_link(__MODULE__, [])
        end

        def handle_event(event, state) do
          IO.puts "Got #{inspect event}"
          {:ok, state}
        end
      end

  Now we can define a topology with our custom sink:

      # Starts a router that receives dynamic messages
      iex> {:ok, source} = GenRouter.start_link(GenRouter.DynamicIn, [],
                                                GenRouter.BroadcastOut, [])

      # Starts another router that will have a single source
      iex> {:ok, router} = GenRouter.start_link(GenRouter.SingleIn, [],
                                                GenRouter.BroadcastOut, []))

      # Our definitive sink
      iex> {:ok, sink} = MySink.start_link

      # Let's subscribe the proper elements
      iex> GenRouter.subscribe(router, to: source)
      iex> GenRouter.subscribe(sink, to: router)

      # Pushing an event now reaches the sink
      iex> GenRouter.sync_notify(source, :hello)

  ## Flow control

  In the examples above, we have used `GenRouter.ask/4` to ask
  the router for data. The reason why the sink must ask for data
  is to provide flow control and alternate between push and pull.

  One way to look at it as the communication between sources and
  sinks are demand-driven. The source won't send any data to the
  sink unless the sink first ask for it. Furthermore, the source
  must never send more data to the sink than the amount asked for.

  One workflow would look like:

    * The sink asks for 10 items
    * The source sends 3 items
    * The source sends 2 items
    * The sink asks for more 5 items (so it never has the buffer
      empty but always capping at some limit, in this case, 10)
    * The source sends 4 items
    * ...
    * The source sends EOS (end of stream) or the sink cancels subscription

  This allows proper back-pressure and flow control in different
  occasions. If the sink is faster than the source, it can ask for
  large amounts of data, which the source ends-up sending at will,
  working as if it was simply pushing data.

  However, if the sink is slower than the source, explicitly asking
  for data makes it a pull system, where the source needs to wait
  before sending more data to the sink. If the difference is large,
  it will force the definite source to either buffer messages up to
  some limit or to start discarding them.

  The messages between source and sink are as follows:

    * `{:"$gen_ask", {pid, ref}, {count, options}}` -
      used to ask data from a source. Once this message is
      received, the source MUST monitor the sink and emit
      data up to the counter. Following messages will
      increase the counter kept by the source.

    * `{:"$gen_route", {pid, ref}, [event]}` -
      used to send data to a sink. The third argument is a
      non-empty list of events. The `ref` is the same used
      when asked for the data.

    * `{:"$gen_ask", {pid, ref}, {:cancel, options}}` -
      cancels the current source/sink relationship. The source
      MUST send a reply (detailed below), even if it does not
      know the given reference. However there is no guarantee
      the message will be received (for example, the source
      may crash just before sending the confirmation). For
      such, it is recomended for the source to be monitored.

    * `{:"$gen_route", {pid, ref}, {:eos, reason}}` -
      signals the end of the "event stream". Reason may
      be `:done`, `:halted`, `:cancelled` or even `{:error,
      reason}` (in case `handle_up/2` returns error).

  Note those messages are not tied to GenRouter at all. The
  GenRouter is just one of the many processes that implement
  the message format defined above.

  ## Name Registration

  GenRouter processes are bound to the same name registration rules
  as `GenServer`. Read more about it in the `GenServer` docs.
  """

  # TODO: Provide @callback in GenServer (documentation purposes)
  # TODO: Provide GenServer.stop/1

  # TODO: Implement and provide format_status/2
  # TODO: Provide GenRouter.stop/1

  # TODO: GenRouter.Supervisor
  # TODO: GenRouter.TCPIn
  # TODO: GenRouter.Stream
  # TODO: ask with :via

  use GenServer
end
