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

  Let's see an example where we define all three components above:
  "a", "b" and "c" and connect them so events are routed through
  the system.

  The first step is to define a sink that will print events
  to terminal as they come:

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

  Now let's define a source, the router, and connect to the sink:

      # A is a router that receives events dynamically and broadcasts them
      iex> {:ok, source} = GenRouter.start_link(GenRouter.DynamicIn, [],
                                                GenRouter.BroadcastOut, [])

      # B is a router that receives events from one source and broadcasts them
      iex> {:ok, router} = GenRouter.start_link(GenRouter.SingleIn, [],
                                                GenRouter.BroadcastOut, [])

      # C is the sink (it doesn't care about outgoing events)
      iex> {:ok, sink} = MySink.start_link()

      # Now let's subscribe the sink to the router,
      # and the router to the source
      iex> GenRouter.subscribe(router, to: source)
      iex> GenRouter.subscribe(sink, to: router)

      # Finally spawn a task that sends one event to the router
      iex> Task.start_link fn -> GenRouter.sync_notify(pid, :hello) end

      # You will eventually see the event printed by the sink

  This short example shows a couple things:

    * Communicatation happens by subscribing sinks to sources.
      We will see why it happens from top to bottom later on.

    * `GenRouter` is a building block made of `*In` and `*Out` parts

    * Besides the composable blocks, there are built-in components
      like `GenRouter.Sink` when the building blocks are not enough
      or too much

  Over the next sections, we will explore those points. However,
  for specific on how to build incoming and outgoing parts of the
  router, please check `GenRouter.In` and `GenRouter.Out` respectively.

  ## Built-in components

  The following are incoming parts of a router:

    * `GenRouter.In` - documents the callbacks required to implement
      the incoming part of a router (not an implementation)

    * `GenRouter.DynamicIn` - a special incoming router that receives
      events via `GenRouter.sync_notify/3` instead of receiving them
      from sources

    * `GenRouter.SingleIn` - a router that has a single source.
      The source can be given on start or by calling
      `GenRouter.subscribe/3`

  The following are outgoing parts of a router:

    * `GenRouter.Out` - documents the callbacks required to implement
      the outgoing part of a router (not an implementation)

    * `GenRouter.BroadcastOut` - a router that has multiple sinks,
      synchronizing the demand between the sinks and broadcasting
      all incoming messages to all sinks

  The following are custom sources or sinks that are useful but
  cannot be broken into In and Out components (usually because
  the logic between the two parts is tightly coupled):

    * `GenRouter.TCPAcceptor` - a definite source that accepts TCP
      connections and dispatches the socket to its sink

    * `GenRouter.Sink` - a definite sink. Useful for tidying up
      and when there is nowhere else to route the event to

    * `GenRouter.Supervisor` - a definite sink that can receive
      routing messages and spawn children based on each message,
      with control of min and max amount of children (it is an
      improvement over simple_one_for_one supervisors)

  ## Subscribing

  In the example above, we have seen that we tell a sink to
  subscribe to a source. This is necessary because before
  the source sends any data to a sink, the sink must ask for
  data first. In other words, the sink initiates the whole process.

  All subscribe does is to tell a sink to ask the source for data.
  Let's go back to our previous diagram:

      [source] - [router] - [sink]

  Once the sink subscribes to the router, it asks the router for data,
  which then asks the source for data, so the demand flows upstream:

      [source] <- [router] <- [sink]

  Once events arrive to the source (via `GenRouter.sync_notify/3`),
  it is sent downstream according to the demand:

      [source] -> [router] -> [sink]

  Note subscribing returns a pid and a reference. The reference
  can be given to ask a process to unsubscribe:

      # will unsubscribe from source
      GenRouter.unsubscribe(router, router_source_ref)

      # will unsubscribe from router
      GenRouter.unsubscribe(sink, sink_source_ref)

  Or you can cancel directly in the source:

      GenRouter.cancel(source, router_source_ref, reason \\ :cancel)

  Finally, note it is not possible to ask a `GenRouter` with
  `GenRouter.DynamicIn` to subscribe to a source. That's because
  `GenRouter.DynamicIn` expects by definition to receive events
  dynamically and not from fixed sources. Another way to put it
  is that a router with `GenRouter.DynamicIn` is always a definite
  source.

  ## Flow control

  Now we know the sink must subscribe to the source so it asks
  the source for data. The reason why the sink must ask for data
  is to provide flow control and alternate between push and pull.

  In this section, we will document the messages used in the
  communication between sources and sinks. This communication is
  demand-driven. The source won't send any data to the sink unless
  the sink first ask for it. Furthermore, the source must never
  send more data to the sink than the amount asked for.

  One workflow would look like:

    * The sink asks for 10 items
    * The source sends 3 items
    * The source sends 2 items
    * The sink asks for more 5 items (so it never has the buffer
      empty but always capping at some limit, in this case, 10)
    * The source sends 4 items
    * ...
    * The source sends EOS (end of stream) or
      the sink cancels subscription

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
      increase the counter kept by the source. `GenRouter.ask/5`
      is a convenience function to send this message.

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
      `GenRouter.cancel/3` is a convenience function to send
      this message.

    * `{:"$gen_route", {pid, ref}, {:eos, reason}}` -
      signals the end of the "event stream". Reason may
      be `:done`, `:halted`, `:cancelled` or even `{:error,
      reason}` (in case `handle_up/2` returns error).

  Note those messages are not tied to GenRouter at all. The
  GenRouter is just one of the many processes that implement
  the message format defined above. Therefore, knowing the
  message format above is useful if you desire to write your
  own process.

  ## Name Registration

  `GenRouter` processes are bound to the same name registration rules
  as `GenServer`. Read more about it in the `GenServer` docs.
  """

  # TODO: Provide @callback in GenServer (documentation purposes)
  # TODO: Provide GenServer.stop/1

  # TODO: Implement and provide format_status/2
  # TODO: Provide GenRouter.stop/1

  # TODO: GenRouter.Supervisor
  # TODO: GenRouter.TCPAcceptor
  # TODO: GenRouter.Stream
  # TODO: ask with :via

  use GenServer
end
