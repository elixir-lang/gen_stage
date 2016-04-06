defmodule GenRouter do
  defmodule Spec do
    def subscribe(producer, pid, ref, demand, opts \\ []) do
      producer = whereis(producer)
      _ = send(producer, {:"$gen_subscribe", {pid, ref}, {demand, opts}})
      :ok
    end

    def ask(producer, ref, demand) do
      producer = whereis(producer)
      _ = send(producer, {:"$gen_ask", {self(), ref}, demand})
      :ok
    end

    def route(consumer, ref, [_|_] = events) do
      consumer = whereis(consumer)
      _ = send(consumer, {:"$gen_route", {self(), ref}, events})
      :ok
    end
    def route(consumer, ref, {:eos, _} = eos) do
      consumer = whereis(consumer)
      _ = send(consumer, {:"$gen_route", {self(), ref}, eos})
      :ok
    end

    def unsubscribe(producer, ref, reason) do
      producer = whereis(producer)
      _ = send(producer, {:"$gen_unsubscribe", {self(), ref}, reason})
      :ok
    end

    ## Helpers

    defp whereis(router) do
      case GenServer.whereis(router) do
        nil   -> raise ArgumentError, "no process associated with #{inspect(router)}"
        proc -> proc
      end
    end
  end

  @moduledoc ~S"""
  A behaviour module for routing events from multiple producers
  to multiple consumers.

  `GenRouter` allows developers to receive events from multiple
  producers and/or send events to multiple consumers. The relationship
  between producers (the one sending messages) and consumers (the one
  receiving them) is established when the consumer subscribes to the
  producer.

  I tis quite common for the same process to send and receive data,
  acting as both producer and consumer. Producers that only send data
  and consumers that only receive data are called "sources" and "sinks",
  respectively.

  As an example, imagine the following processes A, B and C:

      [A] -> [B] -> [C]

  A is a producer to B and B is a producer to C. Similarly, C is a
  consumer to B and B is a consumer to A. A is also a source and C
  is a sink.

  ## Incoming and outgoing

  The router is split in two parts. Imagine `B` below is a router:

      [A] -> [B] -> [C]

  We can split it in two parts:

      [A] -> [consumer | producer] -> [C]

  The consumer part will receive messages from upstream while
  the producer will send them downstream. In other words, the consumer
  handles incoming messages and the producer outgoing ones.

  Those parts are defined by different callback modules, allowing
  developers to compose functionality at will. For example, you
  can have a router that receives messages dynamically from
  multiple processes using `GenRouter.DynamicIn` composed with an
  outgoing component that relays those messages to multiple
  consumers at the same time (`GenRouter.BroadcastOut`) or with a
  custom `RoundRobinOut` mechanism.

  Let's see an example where we define all three components above:
  "A", "B" and "C" and connect them so events are routed through
  the system.

  The first step is to define the sink (C) that will print events
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

  Now let's define a source (A), the router (B), and connect to the sink:

      # A is a source that receives events dynamically and broadcasts them
      iex> {:ok, producer} = GenRouter.start_link(GenRouter.DynamicIn, [],
                                                  GenRouter.BroadcastOut, [])

      # B is a router that receives events from one producer and broadcasts them
      iex> {:ok, router} = GenRouter.start_link(GenRouter.SingleIn, [],
                                                GenRouter.BroadcastOut, [])

      # C is the consumer (it doesn't care about outgoing events)
      iex> {:ok, consumer} = MySink.start_link()

      # Now let's subscribe the consumer to the router,
      # and the router to the producer
      iex> GenRouter.subscribe(router, to: producer)
      iex> GenRouter.subscribe(consumer, to: router)

      # Finally spawn a task that sends one event to the router
      iex> Task.start_link fn -> GenRouter.sync_notify(producer, :hello) end

      # You will eventually see the event printed by the consumer

  This short example shows a couple things:

    * Communication happens by subscribing consumers to producers.
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
      from producers

    * `GenRouter.SingleIn` - a router that has a single producer.
      The producer can be given on start or by calling
      `GenRouter.subscribe/3`

  The following are outgoing parts of a router:

    * `GenRouter.Out` - documents the callbacks required to implement
      the outgoing part of a router (not an implementation)

    * `GenRouter.BroadcastOut` - a router that has multiple consumers,
      synchronizing the demand between the consumers and broadcasting
      all incoming messages to all consumers

  The following are custom producers or consumers that are useful but
  cannot be broken into In and Out components (usually because
  the logic between the two parts is tightly coupled):

    * `GenRouter.TCPAcceptor` - a definite producer that accepts TCP
      connections and dispatches the socket to its consumer

    * `GenRouter.Sink` - a definite consumer. Useful for tidying up
      and when there is nowhere else to route the event to

    * `GenRouter.Supervisor` - a definite consumer that can receive
      routing messages and spawn children based on each message,
      with control of min and max amount of children (it is an
      improvement over simple_one_for_one supervisors)

  ## Subscribing

  In the example above, we have seen that we tell a consumer to
  subscribe to a producer. This is necessary because before
  the producer sends any data to a consumer, the consumer must ask for
  data first. In other words, the consumer initiates the whole process.

  All subscribe does is to tell a consumer to ask the producer for data.
  Let's go back to our previous diagram:

      [producer] - [router] - [consumer]

  Once the consumer subscribes to the router, it asks the router for data,
  which then asks the producer for data, so the demand flows upstream:

      [producer] <- [router] <- [consumer]

  Once events arrive to the producer (via `GenRouter.sync_notify/3`),
  it is sent downstream according to the demand:

      [producer] -> [router] -> [consumer]

  Note subscribing returns a pid and a reference. The reference
  can be given to ask a process to unsubscribe:

      # will unsubscribe from producer
      GenRouter.unsubscribe(router, router_producer_ref)

      # will unsubscribe from router
      GenRouter.unsubscribe(consumer, consumer_producer_ref)

  Or you can cancel directly in the producer using `GenRouter.Spec.unsubscribe/4`:

      GenRouter.Spec.unsubscribe(producer, router_producer_ref, reason \\ :cancel, opt \\ [])

  Finally, note it is not possible to ask a `GenRouter` with
  `GenRouter.DynamicIn` to subscribe to a producer. That's because
  `GenRouter.DynamicIn` expects by definition to receive events
  dynamically and not from fixed producers. Another way to put it
  is that a router with `GenRouter.DynamicIn` is always a definite
  producer.

  ## Flow control

  Now we know the consumer must subscribe to the producer so it asks
  the producer for data. The reason why the consumer must ask for data
  is to provide flow control and alternate between push and pull.

  In this section, we will document the messages used in the
  communication between producers and consumers. This communication is
  demand-driven. The producer won't send any data to the consumer unless
  the consumer first asks for it. Furthermore, the producer must never
  send more data to the consumer than the amount asked for.

  One workflow would look like:

    * The consumer asks for 10 items
    * The producer sends 3 items
    * The producer sends 2 items
    * The consumer asks for more 5 items (so it never has the buffer
      empty but always capping at some limit, in this case, 10)
    * The producer sends 4 items
    * ...
    * The producer sends EOS (end of stream) or
      the consumer cancels subscription

  This allows proper back-pressure and flow control in different
  occasions. If the consumer is faster than the producer, the producer will
  always send the data as soon as it arrives, ensuring the consumer
  gets the new data as fast as possible, reducing idle time.

  However, if the consumer is slower than the producer, the producer needs to
  wait before sending more data to the consumer. If the difference is
  considerable, the consumer won't overflow rather the lack of demand
  will be reflected upstream, forcing the definite producer to either
  buffer messages (up to some limit) or to start discarding them.

  The messages between producer and consumer are defined in
  `GenRouter.Spec`. Those messages are not tied to GenRouter at all.
  The GenRouter is just one of the many processes that implement
  the message format defined in `GenRouter.Spec`. Therefore, knowing
  the message format above is useful if you desire to write your
  own process.

  ## Name Registration

  `GenRouter` processes are bound to the same name registration rules
  as `GenServer`. Read more about it in the `GenServer` docs.
  """

  # TODO: Provide @callback in GenServer (documentation purposes)
  # TODO: Provide GenServer.stop/1

  # TODO: GenRouter.Supervisor
  # TODO: GenRouter.TCPAcceptor
  # TODO: GenRouter.Stream
  # TODO: ask with :via

  alias GenRouter.Spec
  @behaviour GenServer

  defstruct [in_mod: nil, in_state: nil, out_mod: nil, out_state: nil,
    consumers: %{}, monitors: %{}]

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
  defdelegate stop(router), to: GenServer

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
      # TODO: Remove this when it is fixed in Elixir's GenServer
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
  def handle_info({:"$gen_subscribe", {pid, ref} = consumer, {demand, _}}, s)
      when is_pid(pid) and is_reference(ref) and is_integer(demand) and demand >= 0 do
    %{consumers: consumers, monitors: monitors} = s

    case consumers do
      %{^ref => _} ->
        # TODO: Document error already_subscribed
        Spec.route(pid, ref, {:eos, {:error, :already_subscribed}})
        {:noreply, s}
      %{} ->
        monitor   = Process.monitor(pid)
        monitors  = Map.put(monitors, monitor, {pid, ref})
        consumers = Map.put_new(consumers, ref, {monitor, pid})
        handle_out_demand(demand, consumer, %GenRouter{s | consumers: consumers, monitors: monitors})
    end
  end

  @doc false
  def handle_info({:"$gen_ask", {pid, ref} = consumer, demand}, s)
      when is_pid(pid) and is_reference(ref) and is_integer(demand) and demand >= 0 do
    %{consumers: consumers} = s

    case consumers do
      %{^ref => _} ->
        handle_out_demand(demand, consumer, s)
      %{} ->
        # TODO: Document error not_found
        Spec.route(pid, ref, {:eos, {:error, :not_found}})
        {:noreply, s}
    end
  end

  def handle_info({:"$gen_unsubscribe", {pid, ref} = consumer, reason}, s)
      when is_pid(pid) and is_reference(ref) do
    %{consumers: consumers, monitors: monitors} = s

    case consumers do
      %{^ref => {monitor, pid}} ->
        Process.demonitor(monitor, [:flush])
        Spec.route(pid, ref, {:eos, :halted})
        monitors = Map.delete(monitors, monitor)
        consumers = Map.delete(consumers, ref)
        handle_down(reason, consumer, %GenRouter{s | consumers: consumers, monitors: monitors})
      %{} ->
        # TODO: Document error not_found
        Spec.route(pid, ref, {:eos, {:error, :not_found}})
        {:noreply, s}
    end
  end

  def handle_info({:DOWN, monitor, :process, _, reason} = msg, s) do
    %{consumers: consumers, monitors: monitors} = s

    case monitors do
      %{^monitor => {_, ref} = consumer} ->
        monitors = Map.delete(monitors, monitor)
        consumers = Map.delete(consumers, ref)
        handle_down(reason, consumer, %GenRouter{s | consumers: consumers, monitors: monitors})
      %{} ->
        do_handle_info(msg, s)
    end
  end

  def handle_info(msg, s) do
    do_handle_info(msg, s)
  end

  defp handle_out_demand(demand, consumer, s) do
    %{out_mod: out_mod, out_state: out_state} = s

    try do
      out_mod.handle_demand(demand, consumer, out_state)
    catch
      :throw, value ->
        reraise_stop(:throw, value, System.stacktrace, s)
    else
      {:ok, demand, out_state} when is_integer(demand) and demand >= 0 ->
        handle_in_demand(demand, out_mod, out_state, s)
      {:ok, demand, events, out_state} when is_integer(demand) and demand >= 0 ->
        ask_ok(events, demand, out_mod, out_state, s)
      {:error, reason, out_state} ->
        ask_error(reason, consumer, [], out_state, s)
      {:error, reason, events, out_state} ->
        ask_error(reason, consumer, events, out_state, s)
      {:stop, reason, out_state} ->
        {:stop, reason, %GenRouter{s | out_state: out_state}}
      {:stop, reason, events, out_state} ->
        stop(reason, events, %GenRouter{s | out_state: out_state})
      other ->
        {:stop, {:bad_return_value, other}, s}
    end
  end

  defp ask_ok(events, demand, out_mod, out_state, s) do
    case handle_dispatch(events, out_mod, out_state, s) do
      {:ok, out_state} ->
        handle_in_demand(demand, out_mod, out_state, s)
      {:stop, reason, out_state} ->
        {:stop, reason, %GenRouter{s | out_state: out_state}}
      {kind, reason, stack, out_state} ->
        reraise_stop(kind, reason, stack, %GenRouter{s | out_state: out_state})
    end
  end

  defp ask_error(error, {pid, ref} = consumer, events, out_state, s) do
    %{out_mod: out_mod} = s
    Spec.route(pid, ref, {:eos, {:error, error}})
    s = delete_consumer(s, consumer)
    case handle_dispatch(events, out_mod, out_state, s) do
      {:ok, out_state} ->
        {:noreply, %GenRouter{s | out_state: out_state}}
      {:stop, reason, out_state} ->
        {:stop, reason, %GenRouter{s | out_state: out_state}}
      {kind, reason, stack, out_state} ->
        reraise_stop(kind, reason, stack, %GenRouter{s | out_state: out_state})
    end
  end

  defp delete_consumer(%GenRouter{consumers: consumers, monitors: monitors} = s, {_, ref}) do
    case consumers do
      %{^ref => {monitor, _}} ->
        Process.demonitor(monitor, [:flush])
        monitors = Map.delete(monitors, monitor)
        consumers = Map.delete(consumers, ref)
        %{s | consumers: consumers, monitors: monitors}
      %{} ->
        s
    end
  end

  ## TODO: consider batching events per consumer
  ## TODO: Simplify dispatch logic by moving it to the OUT mechanism?
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
  defp do_dispatch([ref | refs], events, %{consumers: consumers} = s) do
    case consumers do
      %{^ref => {_, pid}} ->
        Spec.route(pid, ref, events)
        do_dispatch(refs, events, s)
      %{} ->
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

  defp handle_in_demand(0, _, out_state, s) do
    {:noreply, %GenRouter{s | out_state: out_state}}
  end

  defp handle_in_demand(demand, out_mod, out_state, s) do
    %{in_mod: in_mod, in_state: in_state} = s
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

  defp handle_down(reason, consumer, s) do
    %{out_mod: out_mod, out_state: out_state} = s
    try do
      out_mod.handle_down(reason, consumer, out_state)
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
        %{out_mod: out_mod, out_state: out_state} = s
        in_dispatch(events, in_state, out_mod, out_state, s)
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
    %{in_mod: in_mod, in_state: in_state, out_mod: out_mod, out_state: out_state} = s
    with {:ok, out_state} <- code_change(out_mod, oldvsn, out_state, extra),
         {:ok, in_state} <- code_change(in_mod, oldvsn, in_state, extra),
         do: {:ok, %GenRouter{s | in_state: in_state, out_state: out_state}}
  end

  defp code_change(mod, oldvsn, state, extra) do
    try do
      mod.code_change(oldvsn, state, extra)
    catch
      # TODO: Remove this when it is fixed in Elixir's GenServer
      :throw, value ->
        :erlang.raise(:error, {:nocatch, value}, System.stacktrace())
    end
  end

  @doc false
  def format_status(:normal, [pdict, s]) do
    %{in_mod: in_mod, in_state: in_state, out_mod: out_mod, out_state: out_state} = s
    normal_status('In', in_mod, pdict, in_state) ++
    normal_status('Out', out_mod, pdict, out_state)
  end
  def format_status(:terminate, [pdict, %GenRouter{} = s]) do
    %{in_mod: in_mod, in_state: in_state, out_mod: out_mod, out_state: out_state} = s
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
    %{in_mod: in_mod, in_state: in_state, out_mod: out_mod, out_state: out_state} = s
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
    %{out_mod: out_mod, out_state: out_state} = s
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
