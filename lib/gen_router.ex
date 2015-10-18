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
      We will see why it happens from bottom to top later on.

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
  occasions. If the sink is faster than the source, the source will
  always send the data as soon as it arrives, ensuring the sink
  gets the new data as fast as possible, reducing idle time.

  However, if the sink is slower than the source, the source needs to
  wait before sending more data to the sink. If the difference is
  considerable, the sink won't overflow rather the lack of demand
  will be reflected upstream, forcing the definite source to either
  buffer messages (up to some limit) or to start discarding them.

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

  # TODO: Provide GenRouter.stop/1
  # TODO: Provide GenRouter.subscribe
  # TODO: Provide GenRouter.unsubscribe
  # TODO: Provide GenRouter.cancel

  # TODO: GenRouter.Supervisor
  # TODO: GenRouter.TCPAcceptor
  # TODO: GenRouter.Stream
  # TODO: ask with :via

  use GenServer

  defstruct [in_mod: nil, in_state: nil, out_mod: nil, out_state: nil,
    sinks: %{}, monitors: %{}]

  @typedoc "The router reference"
  @type router :: pid | atom | {:global, term} | {:via, module, term} | {atom, node}

  @doc """
  Start (and link) a `GenRouter` in a supervision tree.
  """
  @spec start_link(module, any, module, any, [GenServer.option]) ::
    GenServer.on_start
  def start_link(in_mod, in_args, out_mod, out_args, options \\ []) do
    args = {in_mod, in_args, out_mod, out_args}
    GenServer.start_link(__MODULE__, args, options)
  end

  @doc """
  Start (without a link) a `GenRouter` outside the supervision tree.
  """
  @spec start(module, any, module, any, [GenServer.option]) ::
    GenServer.on_start
  def start(in_mod, in_args, out_mod, out_args, options \\ []) do
    args = {in_mod, in_args, out_mod, out_args}
    GenServer.start(__MODULE__, args, options)
  end

  @spec call(router, any, timeout) :: any
  def call(router, request, timeout \\ 5_000)
  defdelegate call(router, request, timeout), to: GenServer

  @spec reply(GenServer.from, any) :: :ok
  defdelegate reply(from, response), to: GenServer

  @spec stop(router) :: :ok
  defdelegate stop(router), to: :gen_server

  @spec ask(router, pid, reference, pos_integer, Keyword.t) :: :ok
  def ask(router, pid, ref, demand, opts \\ []) do
    case GenServer.whereis(router) do
      nil ->
        exit({:noproc, {__MODULE__, :ask, [router, pid, ref, demand, opts]}})
      router ->
        _ = send(router, {:"$gen_ask", {pid, ref}, {demand, opts}})
        :ok
    end
  end

  @doc false
  def init({in_mod, in_args, out_mod, out_args}) do
    case init(out_mod, out_args) do
      {:ok, out_state} ->
        init_in(in_mod, in_args, out_mod, out_state)
      {:stop, _} = stop ->
        stop
    end
  end

  defp init(mod, args) do
    try do
      mod.init(args)
    catch
      kind, reason ->
        {:stop, exit_reason(kind, reason, System.stacktrace)}
    else
      {:ok, state} ->
        {:ok, state}
      {:stop, _} = stop ->
        stop
      other ->
        {:stop, {:bad_return_value, other}}
    end
  end

  defp init_in(in_mod, in_args, out_mod, out_state) do
    case init(in_mod, in_args) do
      {:ok, in_state} ->
        s = %GenRouter{in_mod: in_mod, in_state: in_state, out_mod: out_mod,
                       out_state: out_state}
        {:ok, s}
      {:stop, reason} = stop ->
        terminate(out_mod, reason, out_state)
        stop
    end
  end

  @doc false
  def handle_call(request, from, %GenRouter{in_mod: in_mod, in_state: in_state} = s) do
    try do
      in_mod.handle_call(request, from, in_state)
    catch
      :throw, value ->
        :erlang.raise(:error, {:nocatch, value}, System.stacktrace())
    else
      {:reply, reply, in_state} ->
        {:reply, reply, %GenRouter{s | in_state: in_state}}
      {:noreply, in_state} ->
        {:noreply, %GenRouter{s | in_state: in_state}}
      {:stop, reason, reply, in_state} ->
        {:stop, reason, reply, %GenRouter{s | in_state: in_state}}
      {:stop, reason, in_state} ->
        {:stop, reason, %GenRouter{s | in_state: in_state}}
      other ->
        {:stop, {:bad_return_value, other}, s}
    end
  end

  @doc false
  def handle_info({:"$gen_ask", {pid, ref} = sink, {demand, _}}, s)
  when is_pid(pid) and is_reference(ref) and is_integer(demand) and demand >= 0 do
    %GenRouter{out_mod: out_mod, out_state: out_state} = s
    try do
      out_mod.handle_demand(demand, sink, out_state)
    catch
      :throw, value ->
        :erlang.raise(:error, {:nocatch, value}, System.stacktrace())
    else
      {:ok, demand, out_state} when is_integer(demand) and demand >= 0 ->
        handle_demand(demand, out_mod, out_state, put_sink(s, sink))
      {:ok, demand, events, out_state} when is_integer(demand) and demand >= 0 ->
        ask_ok(sink, events, demand, out_mod, out_state, s)
      {:error, reason, out_state} ->
        ask_error(reason, sink, [], out_state, s)
      {:error, reason, events, out_state} ->
        ask_error(reason, sink, events, out_state, s)
      {:stop, reason, out_state} ->
        {:stop, reason, %GenRouter{s | out_state: out_state}}
      {:stop, reason, events, out_state} ->
        stop(reason, events, put_sink(%GenRouter{s | out_state: out_state}, sink))
      other ->
        {:stop, {:bad_return_value, other}, s}
    end
  end

  def handle_info({:"$gen_ask", {pid, ref} = sink, {:cancel, _}}, s)
  when is_pid(pid) and is_reference(ref) do
    %GenRouter{sinks: sinks, monitors: monitors} = s
    case Map.pop(sinks, ref) do
      {nil, _} ->
        send(pid, {:"$gen_route", {self(), ref}, {:eos, {:error, :not_found}}})
        {:noreply, s}
      {{monitor, pid}, sinks} ->
        Process.demonitor(monitor, [:flush])
        send(pid, {:"$gen_route", {self(), ref}, {:eos, :cancelled}})
        monitors = Map.delete(monitors, monitor)
        handle_down(:cancelled, sink, %GenRouter{s | sinks: sinks, monitors: monitors})
    end
  end

  def handle_info({:DOWN, monitor, :process, _, reason} = msg, s) do
    %GenRouter{sinks: sinks, monitors: monitors} = s
    case Map.pop(monitors, monitor) do
      {nil, _} ->
        do_handle_info(msg, s)
      {{_, ref} = sink, monitors} ->
        monitors = Map.delete(monitors, monitor)
        sinks = Map.delete(sinks, ref)
        handle_down(reason, sink, %GenRouter{s | sinks: sinks, monitors: monitors})
    end
  end

  def handle_info(msg, s) do
    do_handle_info(msg, s)
  end

  defp ask_ok(sink, events, demand, out_mod, out_state, s) do
    s = put_sink(s, sink)
    case handle_dispatch(events, out_mod, out_state, s) do
      {:ok, out_state} ->
        handle_demand(demand, out_mod, out_state, s)
      {:stop, reason, out_state} ->
        {:stop, reason, %GenRouter{s | out_state: out_state}}
      {kind, reason, stack, out_state} ->
        reraise_stop(kind, reason, stack, %GenRouter{s | out_state: out_state})
    end
  end

  defp put_sink(%GenRouter{sinks: sinks, monitors: monitors} = s, {pid, ref} = sink) do
    if Map.has_key?(sinks, ref) do
      s
    else
      monitor = Process.monitor(pid)
      %GenRouter{s | sinks: Map.put(sinks, ref, {monitor, pid}),
                     monitors: Map.put(monitors, monitor, sink)}
    end
  end

  defp ask_error(error, {pid, ref} = sink, events, out_state, s) do
    %GenRouter{out_mod: out_mod} = s
    send(pid, {:"$gen_route", {self(), ref}, {:eos, {:error, error}}})
    s = delete_sink(s, sink)
    case handle_dispatch(events, out_mod, out_state, s) do
      {:ok, out_state} ->
        {:noreply, %GenRouter{s | out_state: out_state}}
      {:stop, reason, out_state} ->
        {:stop, reason, %GenRouter{s | out_state: out_state}}
      {kind, reason, stack, out_state} ->
        reraise_stop(kind, reason, stack, %GenRouter{s | out_state: out_state})
    end
  end

  defp delete_sink(%GenRouter{sinks: sinks, monitors: monitors} = s, {_, ref}) do
    case Map.pop(sinks, ref) do
      {{monitor, _}, sinks} ->
        Process.demonitor(monitor, [:flush])
        %GenRouter{s | sinks: sinks, monitors: Map.delete(monitors, monitor)}
      _ ->
        s
    end
  end

  ## TODO: consider batching events per sink
  defp handle_dispatch([], _, out_state, _) do
    {:ok, out_state}
  end
  defp handle_dispatch([event | events], out_mod, out_state, s) do
    try do
      out_mod.handle_dispatch(event, out_state)
    catch
      kind, reason ->
        stack = System.stacktrace()
        {kind, reason, stack, out_state}
    else
      {:ok, refs, out_state} ->
        case do_dispatch(refs, [event], s) do
          :ok             -> handle_dispatch(events, out_mod, out_state, s)
          {:stop, reason} -> {:stop, reason, out_state}
        end
      {:stop, reason, refs, out_state} ->
        dispatch_stop(reason, refs, event, out_state, s)
      other ->
        {:stop, {:bad_return_value, other}, out_state}
    end
  end
  defp handle_dispatch(events, _, out_state, _) do
    {:stop, {:bad_events, events}, out_state}
  end

  defp do_dispatch([], _, _), do: :ok
  defp do_dispatch([ref | refs], events, %GenRouter{sinks: sinks} = s) do
    case Map.fetch(sinks, ref) do
      {:ok, {_, pid}} ->
        send(pid, {:"$gen_route", {self(), ref}, events})
        do_dispatch(refs, events, s)
      :error ->
        {:stop, {:bad_reference, ref}}
    end
  end
  defp do_dispatch(refs, _, _), do: {:stop, {:bad_references, refs}}

  defp dispatch_stop(reason, refs, event, out_state, s) do
    case do_dispatch(refs, event, s) do
      :ok             -> {:stop, reason, out_state}
      {:stop, reason} -> {:stop, reason, out_state}
    end
  end

  defp handle_demand(0, _, out_state, s) do
    {:noreply, %GenRouter{s | out_state: out_state}}
  end

  defp handle_demand(demand, out_mod, out_state, s) do
    %GenRouter{in_mod: in_mod, in_state: in_state} = s
    try do
      in_mod.handle_demand(demand, in_state)
    catch
      kind, reason ->
        stack = System.stacktrace()
        reraise_stop(kind, reason, stack, %GenRouter{s | out_state: out_state})
    else
      {:dispatch, events, in_state} ->
        in_dispatch(events, in_state, out_mod, out_state, s)
      {:noreply, in_state} ->
        {:noreply, %GenRouter{s | in_state: in_state, out_state: out_state}}
      {:stop, reason, in_state} ->
        {:stop, reason, %GenRouter{s | in_state: in_state, out_state: out_state}}
      other ->
        {:stop, {:bad_return_value, other}, %GenRouter{s | out_state: out_state}}
    end
  end

  defp in_dispatch(events, in_state, s) do
    %GenRouter{out_mod: out_mod, out_state: out_state} = s
    in_dispatch(events, in_state, out_mod, out_state, s)
  end

  defp in_dispatch(events, in_state, out_mod, out_state, s) do
    case handle_dispatch(events, out_mod, out_state, s) do
      {:ok, out_state} ->
        s =  %GenRouter{s | in_state: in_state, out_state: out_state}
        {:noreply, s}
      {:stop, reason, out_state} ->
        {:stop, reason, %GenRouter{s | in_state: in_state, out_state: out_state}}
      {kind, reason, stack, out_state} ->
        s = %GenRouter{s | in_state: in_state, out_state: out_state}
        reraise_stop(kind, reason, stack, s)
    end
  end

  defp handle_down(reason, sink, s) do
    %GenRouter{out_mod: out_mod, out_state: out_state} = s
    try do
      out_mod.handle_down(reason, sink, out_state)
    catch
      kind, reason ->
        stack = System.stacktrace()
        reraise_stop(kind, reason, stack, s)
    else
      {:ok, out_state} ->
        {:noreply, %GenRouter{s | out_state: out_state}}
      {:ok, events, out_state} ->
        down_dispatch(events, out_mod, out_state, s)
      {:stop, reason, out_state} ->
        {:stop, reason, %GenRouter{s | out_state: out_state}}
      {:stop, reason, events, out_state} ->
        stop(reason, events, %GenRouter{s | out_state: out_state})
      other ->
        {:stop, {:bad_return_value, other}, %GenRouter{s | out_state: out_state}}
    end
  end

  defp down_dispatch(events, out_mod, out_state, s) do
    case handle_dispatch(events, out_mod, out_state, s) do
      {:ok, out_state} ->
        {:noreply, %GenRouter{s | out_state: out_state}}
      {:stop, reason, out_state} ->
        {:stop, reason, %GenRouter{s | out_state: out_state}}
      {kind, reason, stack, out_state} ->
        reraise_stop(kind, reason, stack, %GenRouter{s | out_state: out_state})
    end
  end

  defp do_handle_info(msg, %GenRouter{in_mod: in_mod, in_state: in_state} = s) do
    try do
      in_mod.handle_info(msg, in_state)
    catch
      :throw, value ->
        :erlang.raise(:error, {:nocatch, value}, System.stacktrace())
    else
      {:dispatch, events, in_state} ->
        in_dispatch(events, in_state, s)
      {:noreply, in_state} ->
        {:noreply, %GenRouter{s | in_state: in_state}}
      {:stop, reason, in_state} ->
        {:stop, reason, %GenRouter{s | in_state: in_state}}
      other ->
        {:stop, {:bad_return_value, other}, s}
    end
  end

  @doc false
  def code_change(oldvsn, s, extra) do
    %GenRouter{in_mod: in_mod, in_state: in_state, out_mod: out_mod,
               out_state: out_state} = s
    case code_change(out_mod, oldvsn, out_state, extra) do
      {:ok, out_state} ->
        case code_change(in_mod, oldvsn, in_state, extra) do
          {:ok, in_state} ->
            {:ok, %GenRouter{s | in_state: in_state, out_state: out_state}}
          other ->
            other
        end
      other ->
        other
    end
  end

  defp code_change(mod, oldvsn, state, extra) do
    try do
      mod.code_change(oldvsn, state, extra)
    catch
      :throw, value ->
        :erlang.raise(:error, {:nocatch, value}, System.stacktrace())
    end
  end

  @doc false
  def format_status(:normal, [pdict, s]) do
    %GenRouter{in_mod: in_mod, in_state: in_state, out_mod: out_mod,
               out_state: out_state} = s
    normal_status('In', in_mod, pdict, in_state) ++
    normal_status('Out', out_mod, pdict, out_state)
  end
  def format_status(:terminate, [pdict, %GenRouter{} = s]) do
    %GenRouter{in_mod: in_mod, in_state: in_state, out_mod: out_mod,
               out_state: out_state} = s
    in_state = terminate_status(in_mod, pdict, in_state)
    out_state = terminate_status(out_mod, pdict, out_state)
    %GenRouter{s | in_state: in_state, out_state: out_state}
  end
  def format_status(:terminate, [pdict, {_kind, _reason, _stack, s}]) do
    format_status(:terminate, [pdict, s])
  end

  defp normal_status(prefix, mod, pdict, state) do
    if function_exported?(mod, :format_status, 2) do
      do_normal_status(prefix, mod, pdict, state)
    else
      normal_status_default(prefix, mod, state)
    end
  end

  defp do_normal_status(prefix, mod, pdict, state) do
    try do
      mod.format_status(:normal, [pdict, state])
    catch
      _, _ ->
        normal_status_default(prefix, mod, state)
    else
      status ->
        status
    end
  end

  defp normal_status_default(prefix, mod, state) do
    [{:data, [{prefix ++ ' Module', mod}, {prefix ++ ' State', state}]}]
  end

  defp terminate_status(mod, pdict, state) do
    if function_exported?(mod, :format_status, 2) do
      do_terminate_status(mod, pdict, state)
    else
      state
    end
  end

  defp do_terminate_status(mod, pdict, state) do
    try do
      mod.format_status(:terminate, [pdict, state])
    catch
      _, _ ->
        state
    else
      status ->
        status
    end
  end

  @doc false
  def terminate(reason, %GenRouter{} = s) do
    %GenRouter{in_mod: in_mod, in_state: in_state, out_mod: out_mod,
               out_state: out_state} = s
    try do
      terminate(out_mod, reason, out_state)
    after
      terminate(in_mod, reason, in_state)
    end
  end
  def terminate(exit_reason, {kind, reason, stack, %GenRouter{} = s}) do
    _ = terminate(exit_reason, s)
    case exit_reason do
      :normal        -> :ok
      :shutdown      -> :ok
      {:shutdown, _} -> :ok
      _              -> :erlang.raise(kind, reason, stack)
    end
  end

  defp terminate(mod, reason, state) do
    try do
      mod.terminate(reason, state)
    catch
      :throw, value ->
        exit(exit_reason(:throw, value, System.stacktrace()))
    end
  end

  ## Helpers

  ## This hack allows us to keep state when calling multiple callbacks.
  defp reraise_stop(kind, reason, stack, s) do
    {:stop, exit_reason(kind, reason, stack), {kind, reason, stack, s}}
  end

  defp stop(reason, events, s) do
    %GenRouter{out_mod: out_mod, out_state: out_state} = s
    case handle_dispatch(events, out_mod, out_state, s) do
      {:ok, out_state} ->
        {:stop, reason, %GenRouter{s | out_state: out_state}}
      {:stop, reason, out_state} ->
        {:stop, reason, %GenRouter{s | out_state: out_state}}
      {kind, reason, stack, out_state} ->
        s = %GenRouter{s | out_state: out_state}
        reraise_stop(kind, reason, stack, s)
    end
  end

  defp exit_reason(:exit, reason, _), do: reason
  defp exit_reason(:error, reason, stack), do: {reason, stack}
  defp exit_reason(:throw, value, stack), do: {{:nocatch, value}, stack}
end
