defmodule GenRouter do
  @moduledoc """
  A behaviour module for routing data from multiple sources
  to multiple sinks.

  `GenRouter` allows developers to receive data from multiple
  sources and/or send data to multiple sinks. The relationship
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

  ## A broadcasting router

  As an example, let's implement a router that simplies
  broadcast all incoming events to all sinks. Its implementation
  is as follows:

      defmodule BroadcastRouter do
        use GenRouter

        def start_link do
          GenRouter.start_link(__MODULE__, %{})
        end

        def handle_up({pid, ref}, state) do
          {:ok, Map.put(state, ref, pid)}
        end

        def handle_down(_reason, {_pid, ref}, state) do
          {:ok, Map.delete(state, ref)}
        end

        def handle_dispatch(_event, state) do
          {:ok, Map.values(state), state}
        end
      end

  Our broadcast router implements 3 callbacks:

    * `handle_up/2` - it is invoked when a process asks the router
      to start receiving the data (i.e. become a sink). It receives
      the pid, the reference and the router state. The reference is
      the one sent by the sink and  `handle_up/3` is invoked only
      once per sink. In this case, all we do is to store the new
      `ref` and `pid` pair in the state

    * `handle_down/3` - if `handle_up/2` returns an `:ok` tuple, the
      sink then is monitored. `handle_down/3` is called when the
      sink process dies or explicitly cancels its subscription

    * `handle_dispatch/2` - invoked everytime an event comes. It
      must return `{:ok, pids, state}`, where `pids` is a list of
      pids to dispatch the event to

  Let's give it a try on terminal:

      # Start the broadcast router
      iex> {:ok, pid} = BroadcastRouter.start_link()

      # Create a reference and use it to ask for data
      iex> ref = Process.monitor(pid)
      iex> GenRouter.ask(pid, self(), ref, 10)

      # Finally broadcast something
      iex> GenRouter.sync_notify(BroadcastRouter, :hello)

      # We have received it
      iex> flush()

  The first time we invoke `ask/4` for that given reference,
  it is registered in the router which will now send data.
  We have initially asked the router to send us 10 events.
  Once the 10 events are delivered, the router will buffer
  messages until we ask for more or until a customizable
  threshold is reached. It is possible to opt-out by calling
  `GenRouter.cancel/2`.

  The underlying routing messages for routing, asking and
  cancelling are defined in later sections.

  ## Subscribing routers

  Often you will want one router to become a sink to another
  router and vice-versa. This can be done with the `subscribe/2`
  function, which tells the router to ask the given router for
  data, similar to the example shown above:

      {:ok, ref} = GenRouter.subscribe(sink, to: source)

  Subscription returns a pid and a reference. The reference can
  be given to unsubscribe from the sink:

      GenRouter.unsubscribe(sink, ref)

  Or to cancel directly in the source:

      GenRouter.cancel(source, ref)

  ## Callbacks

  Below we present a complete overview of `GenRouter` callbacks:

    * `init(args)` - invoked when the router is started.

      It must return:

      - `{:ok, state}`
      - `{:ok, state, timeout}`
      - `{:ok, state, :hibernate}`
      - `:ignore`
      - `{:stop, reason}`

    * `handle_up({pid, ref}, state)` - invoked when a sink asks
      for data (once per `ref`)

      It must return:

      - `{:ok, new_state}`
      - `{:ok, new_state, timeout}`
      - `{:ok, new_state, :hibernate}`
      - `{:error, reason, new_state}`
      - `{:error, reason, new_state, timeout}`
      - `{:error, reason, new_state, :hibernate}`
      - `{:stop, reason, new_state}`

    * `handle_down(reason, {pid, ref}, state)` - invoked when a sink
      cancels subscription or crashes

      It must return:

      - `{:ok, new_state}`
      - `{:ok, new_state, timeout}`
      - `{:ok, new_state, :hibernate}`
      - `{:stop, reason, new_state}`

    * `handle_dispatch(event, state)` - invoked to figure out to which
      event dispatch to

      It must return:

      - `{:ok, pids, new_state}`
      - `{:ok, pids, new_state, timeout}`
      - `{:ok, pids, new_state, :hibernate}`
      - `{:stop, reason, new_state}`

    * `handle_info(msg, state)` - invoked to handle all other messages
      which are received by the process.

      It must return:

      - `{:ok, state}`
      - `{:ok, state, timeout}`
      - `{:stop, reason, state}`

    * `terminate(reason, state)` - called when the server is about to
      terminate, useful for cleaning up. It must return `:ok`.
      If part of a supervision tree, terminate only gets called if the
      GenServer is set to trap exits using `Process.flag/2` *and*
      the shutdown strategy of the Supervisor is a timeout value,
      not `:brutal_kill`. The callback is also not invoked if links
      are broken unless trapping exits. For such reasons, we usually
      recommend important clean-up rules to happen in separated
      processes either by use of monitoring or by links themselves.

    * `code_change(old_vsn, state, extra)` - called when the application
      code is being upgraded live (hot code swapping)

      It must return:

      - `{:ok, new_state}`
      - `{:error, reason}`

  ## Name Registration

  GenRouter processes are bound to the same name registration rules
  as `GenServer`. Read more about it in the `GenServer` docs.

  ## Flow control

  The reason why the sink must ask for data is to provide
  flow control and alternate between push and pull.

  If the sink is faster than the source, it can ask for
  large chunks of data, which the source ends-up sending
  at will, working as if it was simply pushing data.

  However, if the sink is slower than the source, explicitly
  asking for data makes it a pull system, where the source
  needs to wait before sending more data to the sink. If
  the difference is large, the source should then buffer
  up to some point and then start loadshedding or drop the
  sink.

  The messages between source and sink are as follows:

    * `{:"$gen_ask", {pid, ref}, {count, options}}` -
      used to ask data from a source. Once this message is
      received, the source MUST monitor the sink and emit
      data up to the counter. Following messages will
      increase the counter kept by the source.

    * `{:"$gen_notify", {pid, ref}, [event]}` -
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

    * `{:"$gen_notify", {pid, ref}, {:eos, reason}}` -
      signals the end of the "event stream". Reason may
      be `:done`, `:halted`, `:cancelled` or even `{:error,
      reason}` (in case `handle_up/2` returns error).

  Note those messages are not tied to GenRouter at all.
  The GenRouter is just one of the many processes that
  implement the message format defined above.
  """

  use GenServer
end
