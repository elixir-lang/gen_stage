defmodule ConsumerSupervisor do
  @moduledoc ~S"""
  A supervisor that starts children as events flow in.

  A `ConsumerSupervisor` can be used as the consumer in a `GenStage` pipeline.
  A new child process will be started per event, where the event is appended
  to the arguments in the child specification.

  A `ConsumerSupervisor` can be attached to a producer by returning
  `:subscribe_to` from `c:init/1` or explicitly with `GenStage.sync_subscribe/3`
  and `GenStage.async_subscribe/2`.

  Once subscribed, the supervisor will ask the producer for `:max_demand` events
  and start child processes as events arrive. As child processes terminate, the
  supervisor will accumulate demand and request more events once `:min_demand`
  is reached. This allows the `ConsumerSupervisor` to work similar to a pool,
  except a child process is started per event. The minimum amount of concurrent
  children per producer is specified by `:min_demand` and the maximum is given
  by `:max_demand`.

  ## Example

  Let's define a GenStage consumer as a `ConsumerSupervisor` that subscribes
  to a producer named `Producer` and starts a new process for each event
  received from the producer. Each new process will be started by calling
  `Printer.start_link/1`, which simply starts a task that will print the
  incoming event to the terminal.

      defmodule Consumer do
        use ConsumerSupervisor

        def start_link(arg) do
          ConsumerSupervisor.start_link(__MODULE__, arg)
        end

        def init(_arg) do
          # Note: By default the restart for a child is set to :permanent
          # which is not supported in ConsumerSupervisor. You need to explicitly
          # set the :restart option either to :temporary or :transient.
          children = [%{id: Printer, start: {Printer, :start_link, []}, restart: :transient}]
          opts = [strategy: :one_for_one, subscribe_to: [{Producer, max_demand: 50}]]
          ConsumerSupervisor.init(children, opts)
        end
      end

  Then in the `Printer` module:

      defmodule Printer do
        def start_link(event) do
          # Note: this function must return the format of `{:ok, pid}` and like
          # all children started by a Supervisor, the process must be linked
          # back to the supervisor (if you use `Task.start_link/1` then both
          # these requirements are met automatically)
          Task.start_link(fn ->
            IO.inspect({self(), event})
          end)
        end
      end

  Similar to `Supervisor`, `ConsumerSupervisor` also provides `start_link/3`,
  which allows developers to start a supervisor with the help of a callback
  module.

  ## Name Registration

  A supervisor is bound to the same name registration rules as a `GenServer`.
  Read more about it in the `GenServer` docs.
  """

  @behaviour GenStage

  @typedoc "Options used by the `start*` functions"
  @type option ::
          {:registry, atom}
          | {:name, Supervisor.name()}
          | {:strategy, Supervisor.Spec.strategy()}
          | {:max_restarts, non_neg_integer}
          | {:max_seconds, non_neg_integer}
          | {:subscribe_to, [GenStage.stage() | {GenStage.stage(), keyword()}]}

  @doc """
  Callback invoked to start the supervisor and during hot code upgrades.

  ## Options

    * `:strategy` - the restart strategy option. Only `:one_for_one`
      is supported by consumer supervisors.

    * `:max_restarts` - the maximum amount of restarts allowed in
      a time frame. Defaults to 3 times.

    * `:max_seconds` - the time frame in which `:max_restarts` applies
      in seconds. Defaults to 5 seconds.

    * `:subscribe_to` - a list of producers to subscribe to. Each element
      represents the producer or a tuple with the producer and the subscription
      options, for example, `[Producer]` or `[{Producer, max_demand: 20, min_demand: 10}]`.

  """
  @callback init(args :: term) ::
              {:ok, [:supervisor.child_spec()], options :: keyword()}
              | :ignore

  defstruct [
    :name,
    :mod,
    :args,
    :template,
    :max_restarts,
    :max_seconds,
    :strategy,
    children: %{},
    producers: %{},
    restarts: [],
    restarting: 0
  ]

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour ConsumerSupervisor
      import Supervisor.Spec

      @doc false
      def child_spec(arg) do
        default = %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [arg]},
          type: :supervisor
        }

        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end

      defoverridable child_spec: 1

      @doc false
      def init(arg)
    end
  end

  defmodule Default do
    @moduledoc false

    def init(args) do
      args
    end
  end

  @doc """
  Starts a supervisor with the given children.

  A strategy is required to be given as an option. Furthermore,
  the `:max_restarts`, `:max_seconds`, and `:subscribe_to`
  values can be configured as described in the documentation for the
  `c:init/1` callback.

  The options can also be used to register a supervisor name.
  The supported values are described under the "Name Registration"
  section in the `GenServer` module docs.

  The child processes specified in `children` will be started by appending
  the event to process to the existing function arguments in the child specification.

  Note that the consumer supervisor is linked to the parent process
  and will exit not only on crashes but also if the parent process
  exits with `:normal` reason.
  """
  @spec start_link([Supervisor.Spec.spec() | Supervisor.child_spec()], [option]) ::
          Supervisor.on_start()
  def start_link(children, options) when is_list(children) do
    {sup_options, start_options} =
      Keyword.split(options, [:strategy, :max_restarts, :max_seconds, :subscribe_to])

    start_link(Default, init(children, sup_options), start_options)
  end

  @doc """
  Starts a consumer supervisor module with the given `args`.

  To start the supervisor, the `c:init/1` callback will be invoked in the given
  module, with `args` passed to it. The `c:init/1` callback must return a
  supervision specification which can be created with the help of the
  `Supervisor` module.

  If the `c:init/1` callback returns `:ignore`, this function returns
  `:ignore` as well and the supervisor terminates with reason `:normal`.
  If it fails or returns an incorrect value, this function returns
  `{:error, term}` where `term` is a term with information about the
  error, and the supervisor terminates with reason `term`.

  The `:name` option can also be given in order to register a supervisor
  name. The supported values are described under the "Name Registration"
  section in the `GenServer` module docs.
  """
  @spec start_link(module, any) :: Supervisor.on_start()
  @spec start_link(module, any, [option]) :: Supervisor.on_start()
  def start_link(mod, args, opts \\ []) do
    GenStage.start_link(__MODULE__, {mod, args, opts[:name]}, opts)
  end

  @doc """
  Starts a child in the consumer supervisor.

  The child process will be started by appending the given list of
  `args` to the existing function arguments in the child specification.

  This child is started separately from any producer and does not
  count towards the demand of any of them.

  If the child process starts, function returns `{:ok, child}` or
  `{:ok, child, info}`, the pid is added to the supervisor, and the
  function returns the same value.

  If the child process start function returns `:ignore`, an error tuple,
  or an erroneous value, or if it fails, the child is discarded and
  `:ignore` or `{:error, error}` where `error` is a term containing
  information about the error is returned.
  """
  @spec start_child(Supervisor.supervisor(), [term]) :: Supervisor.on_start_child()
  def start_child(supervisor, args) when is_list(args) do
    call(supervisor, {:start_child, args})
  end

  @doc """
  Terminates the given child pid.

  If successful, the function returns `:ok`. If there is no
  such pid, the function returns `{:error, :not_found}`.
  """
  @spec terminate_child(Supervisor.supervisor(), pid) :: :ok | {:error, :not_found}
  def terminate_child(supervisor, pid) when is_pid(pid) do
    call(supervisor, {:terminate_child, pid})
  end

  @doc """
  Returns a list with information about all children.

  Note that calling this function when supervising a large number
  of children under low memory conditions can cause an out of memory
  exception.

  This function returns a list of tuples containing:

    * `id` - as defined in the child specification but is always
      set to `:undefined` for consumer supervisors

    * `child` - the pid of the corresponding child process or the
      atom `:restarting` if the process is about to be restarted

    * `type` - `:worker` or `:supervisor` as defined in the child
      specification

    * `modules` - as defined in the child specification

  """
  @spec which_children(Supervisor.supervisor()) :: [
          {:undefined, pid | :restarting, :worker | :supervisor, :dynamic | [module()]}
        ]
  def which_children(supervisor) do
    call(supervisor, :which_children)
  end

  @doc """
  Returns a map containing count values for the supervisor.

  The map contains the following keys:

    * `:specs` - always `1` as consumer supervisors have a single specification

    * `:active` - the count of all actively running child processes managed by
      this supervisor

    * `:supervisors` - the count of all supervisors whether or not the child
      process is still alive

    * `:workers` - the count of all workers, whether or not the child process
      is still alive

  """
  @spec count_children(Supervisor.supervisor()) :: %{
          specs: non_neg_integer,
          active: non_neg_integer,
          supervisors: non_neg_integer,
          workers: non_neg_integer
        }
  def count_children(supervisor) do
    call(supervisor, :count_children)
  end

  @doc """
  Receives a template to initialize and a set of options.

  This is typically invoked at the end of the `c:init/1` callback of module-based supervisors.

  This function returns a the child specification and the supervisor flags.

  ## Examples

  Using the child specification changes introduced in Elixir 1.5:

      defmodule MyConsumerSupervisor do
        use ConsumerSupervisor

        def start_link(arg) do
          ConsumerSupervisor.start_link(__MODULE__, arg)
        end

        def init(_arg) do
          ConsumerSupervisor.init([MyConsumer], strategy: :one_for_one, subscribe_to: MyProducer)
        end
      end

  """
  def init([{_, _, _, _, _, _} = template], opts) do
    {:ok, [template], opts}
  end

  def init([template], opts) when is_tuple(template) or is_map(template) or is_atom(template) do
    {:ok, {_, [template]}} = Supervisor.init([template], opts)
    {:ok, [template], opts}
  end

  @compile {:inline, call: 2}

  defp call(supervisor, req) do
    GenStage.call(supervisor, req, :infinity)
  end

  ## Callbacks

  @impl true
  def init({mod, args, name}) do
    Process.put(:"$initial_call", {:supervisor, mod, 1})
    Process.flag(:trap_exit, true)

    case mod.init(args) do
      {:ok, children, opts} ->
        case validate_specs(children) do
          :ok ->
            state = %ConsumerSupervisor{mod: mod, args: args, name: name || {self(), mod}}

            case init(state, children, opts) do
              {:ok, state, opts} -> {:consumer, state, opts}
              {:error, message} -> {:stop, {:bad_opts, message}}
            end

          {:error, message} ->
            {:stop, {:bad_specs, message}}
        end

      :ignore ->
        :ignore

      other ->
        {:stop, {:bad_return_value, other}}
    end
  end

  defp init(state, [child], opts) when is_list(opts) do
    {strategy, opts} = Keyword.pop(opts, :strategy)
    {max_restarts, opts} = Keyword.pop(opts, :max_restarts, 3)
    {max_seconds, opts} = Keyword.pop(opts, :max_seconds, 5)
    template = normalize_template(child)

    with :ok <- validate_strategy(strategy),
         :ok <- validate_restarts(max_restarts),
         :ok <- validate_seconds(max_seconds),
         :ok <- validate_template(template) do
      state = %{
        state
        | template: template,
          strategy: strategy,
          max_restarts: max_restarts,
          max_seconds: max_seconds
      }

      {:ok, state, opts}
    end
  end

  defp init(_state, [_], _opts) do
    {:error, "supervisor's init expects a keywords list as options"}
  end

  defp validate_specs([_] = children) do
    :supervisor.check_childspecs(children)
  end

  defp validate_specs(_children) do
    {:error, "consumer supervisor expects a list with a single item as a template"}
  end

  defp validate_strategy(strategy) when strategy in [:one_for_one], do: :ok
  defp validate_strategy(nil), do: {:error, "supervisor expects a strategy to be given"}
  defp validate_strategy(_), do: {:error, "unknown supervision strategy for consumer supervisor"}

  defp validate_restarts(restart) when is_integer(restart), do: :ok
  defp validate_restarts(_), do: {:error, "max_restarts must be an integer"}

  defp validate_seconds(seconds) when is_integer(seconds), do: :ok
  defp validate_seconds(_), do: {:error, "max_seconds must be an integer"}

  @impl true
  def handle_subscribe(:producer, opts, {_, ref} = from, state) do
    # GenStage checks these options before allowing susbcription
    max = Keyword.get(opts, :max_demand, 1000)
    min = Keyword.get(opts, :min_demand, div(max, 2))
    GenStage.ask(from, max)
    {:manual, put_in(state.producers[ref], {from, 0, 0, min, max})}
  end

  @impl true
  def handle_cancel(_, {_, ref}, state) do
    {:noreply, [], update_in(state.producers, &Map.delete(&1, ref))}
  end

  @impl true
  def handle_events(events, {pid, ref} = from, state) do
    %{template: child, children: children} = state
    {new, errors} = start_events(events, from, child, 0, [], state)
    new_children = Enum.into(new, children)
    started = map_size(new_children) - map_size(children)
    {:noreply, [], maybe_ask(ref, pid, started + errors, errors, new_children, state)}
  end

  defp start_events([extra | extras], from, child, errors, acc, state) do
    {_, ref} = from
    {_, {m, f, args}, restart, _, _, _} = child
    args = args ++ [extra]

    case start_child(m, f, args) do
      {:ok, pid, _} when restart == :temporary ->
        acc = [{pid, [ref | :undefined]} | acc]
        start_events(extras, from, child, errors, acc, state)

      {:ok, pid, _} ->
        acc = [{pid, [ref | args]} | acc]
        start_events(extras, from, child, errors, acc, state)

      {:ok, pid} when restart == :temporary ->
        acc = [{pid, [ref | :undefined]} | acc]
        start_events(extras, from, child, errors, acc, state)

      {:ok, pid} ->
        acc = [{pid, [ref | args]} | acc]
        start_events(extras, from, child, errors, acc, state)

      :ignore ->
        start_events(extras, from, child, errors + 1, acc, state)

      {:error, reason} ->
        :error_logger.error_msg(
          'ConsumerSupervisor failed to start child from: ~tp with reason: ~tp~n',
          [from, reason]
        )

        report_error(:start_error, reason, :undefined, args, child, state)
        start_events(extras, from, child, errors + 1, acc, state)
    end
  end

  defp start_events([], _, _, errors, acc, _) do
    {acc, errors}
  end

  defp maybe_ask(ref, pid, events, down, children, state) do
    %{producers: producers} = state

    case producers do
      %{^ref => {to, count, pending, min, max}} ->
        if count + events > max do
          :error_logger.error_msg(
            'ConsumerSupervisor has received ~tp events in excess from: ~tp~n',
            [count + events - max, {pid, ref}]
          )
        end

        pending =
          case pending + down do
            ask when ask >= min ->
              GenStage.ask(to, ask)
              0

            ask ->
              ask
          end

        count = count + events - down
        producers = Map.put(producers, ref, {to, count, pending, min, max})
        %{state | children: children, producers: producers}

      %{} ->
        %{state | children: children}
    end
  end

  @impl true
  def handle_call(:which_children, _from, state) do
    %{children: children, template: child} = state
    {_, _, _, _, type, mods} = child

    reply =
      for {pid, args} <- children do
        maybe_pid =
          case args do
            {:restarting, _} -> :restarting
            _ -> pid
          end

        {:undefined, maybe_pid, type, mods}
      end

    {:reply, reply, [], state}
  end

  def handle_call(:count_children, _from, state) do
    %{children: children, template: child, restarting: restarting} = state
    {_, _, _, _, type, _} = child

    specs = map_size(children)
    active = specs - restarting

    reply =
      case type do
        :supervisor ->
          %{specs: 1, active: active, workers: 0, supervisors: specs}

        :worker ->
          %{specs: 1, active: active, workers: specs, supervisors: 0}
      end

    {:reply, reply, [], state}
  end

  def handle_call({:terminate_child, pid}, _from, %{children: children} = state) do
    case children do
      %{^pid => [producer | _] = info} ->
        :ok = terminate_children(%{pid => info}, state)
        {:reply, :ok, [], delete_child_and_maybe_ask(producer, pid, state)}

      %{^pid => {:restarting, [producer | _]} = info} ->
        :ok = terminate_children(%{pid => info}, state)
        {:reply, :ok, [], delete_child_and_maybe_ask(producer, pid, state)}

      %{} ->
        {:reply, {:error, :not_found}, [], state}
    end
  end

  def handle_call({:start_child, extra}, _from, %{template: child} = state) do
    handle_start_child(child, extra, state)
  end

  defp handle_start_child({_, {m, f, args}, restart, _, _, _}, extra, state) do
    args = args ++ extra

    case reply = start_child(m, f, args) do
      {:ok, pid, _} ->
        {:reply, reply, [], save_child(restart, :dynamic, pid, args, state)}

      {:ok, pid} ->
        {:reply, reply, [], save_child(restart, :dynamic, pid, args, state)}

      _ ->
        {:reply, reply, [], state}
    end
  end

  defp start_child(m, f, a) do
    try do
      apply(m, f, a)
    catch
      kind, reason ->
        {:error, exit_reason(kind, reason, __STACKTRACE__)}
    else
      {:ok, pid, extra} when is_pid(pid) -> {:ok, pid, extra}
      {:ok, pid} when is_pid(pid) -> {:ok, pid}
      :ignore -> :ignore
      {:error, _} = error -> error
      other -> {:error, other}
    end
  end

  defp save_child(:temporary, producer, pid, _, state),
    do: put_in(state.children[pid], [producer | :undefined])

  defp save_child(_, producer, pid, args, state),
    do: put_in(state.children[pid], [producer | args])

  defp exit_reason(:exit, reason, _), do: reason
  defp exit_reason(:error, reason, stack), do: {reason, stack}
  defp exit_reason(:throw, value, stack), do: {{:nocatch, value}, stack}

  @impl true
  def handle_cast(_msg, state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    case maybe_restart_child(pid, reason, state) do
      {:ok, state} -> {:noreply, [], state}
      {:shutdown, state} -> {:stop, :shutdown, state}
    end
  end

  def handle_info({:"$gen_restart", pid}, state) do
    %{children: children, template: child, restarting: restarting} = state
    state = %{state | restarting: restarting - 1}

    case children do
      %{^pid => restarting_args} ->
        {:restarting, [producer | args]} = restarting_args

        case restart_child(producer, pid, args, child, state) do
          {:ok, state} ->
            {:noreply, [], state}

          {:shutdown, state} ->
            {:stop, :shutdown, state}
        end

      # We may hit clause if we send $gen_restart and then
      # someone calls terminate_child, removing the child.
      %{} ->
        {:noreply, [], state}
    end
  end

  def handle_info(msg, state) do
    :error_logger.error_msg('ConsumerSupervisor received unexpected message: ~tp~n', [msg])
    {:noreply, [], state}
  end

  @impl true
  def code_change(_, %{mod: mod, args: args} = state, _) do
    case mod.init(args) do
      {:ok, children, opts} ->
        case validate_specs(children) do
          :ok ->
            case init(state, children, opts) do
              {:ok, state, _} -> {:ok, state}
              {:error, message} -> {:error, {:bad_opts, message}}
            end

          {:error, message} ->
            {:error, {:bad_specs, message}}
        end

      :ignore ->
        {:ok, state}

      error ->
        error
    end
  end

  @impl true
  def terminate(_, %{children: children} = state) do
    :ok = terminate_children(children, state)
  end

  defp terminate_children(children, %{template: template} = state) do
    {_, _, restart, shutdown, _, _} = template

    {pids, stacks} = monitor_children(children, restart)
    size = map_size(pids)

    stacks =
      case shutdown do
        :brutal_kill ->
          for {pid, _} <- pids, do: Process.exit(pid, :kill)
          wait_children(restart, shutdown, pids, size, nil, stacks)

        :infinity ->
          for {pid, _} <- pids, do: Process.exit(pid, :shutdown)
          wait_children(restart, shutdown, pids, size, nil, stacks)

        time ->
          for {pid, _} <- pids, do: Process.exit(pid, :shutdown)
          timer = :erlang.start_timer(time, self(), :kill)
          wait_children(restart, shutdown, pids, size, timer, stacks)
      end

    for {pid, reason} <- stacks do
      report_error(:shutdown_error, reason, pid, :undefined, template, state)
    end

    :ok
  end

  defp monitor_children(children, restart) do
    Enum.reduce(children, {%{}, %{}}, fn
      {_, {:restarting, _}}, {pids, stacks} ->
        {pids, stacks}

      {pid, _}, {pids, stacks} ->
        case monitor_child(pid) do
          :ok ->
            {Map.put(pids, pid, true), stacks}

          {:error, :normal} when restart != :permanent ->
            {pids, stacks}

          {:error, reason} ->
            {pids, Map.put(stacks, pid, reason)}
        end
    end)
  end

  defp monitor_child(pid) do
    ref = Process.monitor(pid)
    Process.unlink(pid)

    receive do
      {:EXIT, ^pid, reason} ->
        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> {:error, reason}
        end
    after
      0 -> :ok
    end
  end

  defp wait_children(_restart, _shutdown, _pids, 0, nil, stacks) do
    stacks
  end

  defp wait_children(_restart, _shutdown, _pids, 0, timer, stacks) do
    _ = :erlang.cancel_timer(timer)

    receive do
      {:timeout, ^timer, :kill} -> :ok
    after
      0 -> :ok
    end

    stacks
  end

  defp wait_children(restart, :brutal_kill, pids, size, timer, stacks) do
    receive do
      {:DOWN, _ref, :process, pid, :killed} ->
        wait_children(restart, :brutal_kill, Map.delete(pids, pid), size - 1, timer, stacks)

      {:DOWN, _ref, :process, pid, reason} ->
        wait_children(
          restart,
          :brutal_kill,
          Map.delete(pids, pid),
          size - 1,
          timer,
          Map.put(stacks, pid, reason)
        )
    end
  end

  defp wait_children(restart, shutdown, pids, size, timer, stacks) do
    receive do
      {:DOWN, _ref, :process, pid, {:shutdown, _}} ->
        wait_children(restart, shutdown, Map.delete(pids, pid), size - 1, timer, stacks)

      {:DOWN, _ref, :process, pid, :shutdown} ->
        wait_children(restart, shutdown, Map.delete(pids, pid), size - 1, timer, stacks)

      {:DOWN, _ref, :process, pid, :normal} when restart != :permanent ->
        wait_children(restart, shutdown, Map.delete(pids, pid), size - 1, timer, stacks)

      {:DOWN, _ref, :process, pid, reason} ->
        stacks = Map.put(stacks, pid, reason)
        wait_children(restart, shutdown, Map.delete(pids, pid), size - 1, timer, stacks)

      {:timeout, ^timer, :kill} ->
        for {pid, _} <- pids, do: Process.exit(pid, :kill)
        wait_children(restart, shutdown, pids, size, nil, stacks)
    end
  end

  defp maybe_restart_child(pid, reason, state) do
    %{children: children, template: child} = state
    {_, _, restart, _, _, _} = child

    case children do
      %{^pid => [producer | args]} ->
        maybe_restart_child(restart, reason, producer, pid, args, child, state)

      %{} ->
        {:ok, state}
    end
  end

  defp maybe_restart_child(:permanent, reason, producer, pid, args, child, state) do
    report_error(:child_terminated, reason, pid, args, child, state)
    restart_child(producer, pid, args, child, state)
  end

  defp maybe_restart_child(_, :normal, producer, pid, _args, _child, state) do
    {:ok, delete_child_and_maybe_ask(producer, pid, state)}
  end

  defp maybe_restart_child(_, :shutdown, producer, pid, _args, _child, state) do
    {:ok, delete_child_and_maybe_ask(producer, pid, state)}
  end

  defp maybe_restart_child(_, {:shutdown, _}, producer, pid, _args, _child, state) do
    {:ok, delete_child_and_maybe_ask(producer, pid, state)}
  end

  defp maybe_restart_child(:transient, reason, producer, pid, args, child, state) do
    report_error(:child_terminated, reason, pid, args, child, state)
    restart_child(producer, pid, args, child, state)
  end

  defp maybe_restart_child(:temporary, reason, producer, pid, args, child, state) do
    report_error(:child_terminated, reason, pid, args, child, state)
    {:ok, delete_child_and_maybe_ask(producer, pid, state)}
  end

  defp delete_child_and_maybe_ask(:dynamic, pid, %{children: children} = state) do
    %{state | children: Map.delete(children, pid)}
  end

  defp delete_child_and_maybe_ask(ref, pid, %{children: children} = state) do
    children = Map.delete(children, pid)
    maybe_ask(ref, pid, 0, 1, children, state)
  end

  defp restart_child(producer, pid, args, child, state) do
    case add_restart(state) do
      {:ok, %{strategy: strategy} = state} ->
        case restart_child(strategy, producer, pid, args, child, state) do
          {:ok, state} ->
            {:ok, state}

          {:try_again, state} ->
            send(self(), {:"$gen_restart", pid})
            {:ok, state}
        end

      {:shutdown, state} ->
        report_error(:shutdown, :reached_max_restart_intensity, pid, args, child, state)
        {:shutdown, delete_child_and_maybe_ask(producer, pid, state)}
    end
  end

  defp add_restart(state) do
    %{max_seconds: max_seconds, max_restarts: max_restarts, restarts: restarts} = state
    now = :erlang.monotonic_time(1)
    restarts = add_restart([now | restarts], now, max_seconds)
    state = %{state | restarts: restarts}

    if length(restarts) <= max_restarts do
      {:ok, state}
    else
      {:shutdown, state}
    end
  end

  defp add_restart(restarts, now, period) do
    for then <- restarts, now <= then + period, do: then
  end

  defp restart_child(:one_for_one, producer, current_pid, args, child, state) do
    {_, {m, f, _}, restart, _, _, _} = child

    case start_child(m, f, args) do
      {:ok, pid, _} ->
        state = %{state | children: Map.delete(state.children, current_pid)}
        {:ok, save_child(restart, producer, pid, args, state)}

      {:ok, pid} ->
        state = %{state | children: Map.delete(state.children, current_pid)}
        {:ok, save_child(restart, producer, pid, args, state)}

      :ignore ->
        {:ok, delete_child_and_maybe_ask(producer, current_pid, state)}

      {:error, reason} ->
        report_error(:start_error, reason, {:restarting, current_pid}, args, child, state)
        state = restart_child(current_pid, state)
        {:try_again, update_in(state.restarting, &(&1 + 1))}
    end
  end

  defp restart_child(pid, %{children: children} = state) do
    case children do
      %{^pid => {:restarting, _}} ->
        state

      %{^pid => info} ->
        %{state | children: Map.put(children, pid, {:restarting, info})}
    end
  end

  defp report_error(error, reason, pid, args, child, %{name: name}) do
    :error_logger.error_report(
      :supervisor_report,
      supervisor: name,
      errorContext: error,
      reason: reason,
      offender: extract_child(pid, args, child)
    )
  end

  defp extract_child(pid, args, {id, {m, f, _}, restart, shutdown, type, _}) do
    [
      pid: pid,
      id: id,
      mfargs: {m, f, args},
      restart_type: restart,
      shutdown: shutdown,
      child_type: type
    ]
  end

  @impl true
  def format_status(:terminate, [_pdict, state]) do
    state
  end

  def format_status(_, [_pdict, %{mod: mod} = state]) do
    [
      data: [{~c"State", state}],
      supervisor: [{~c"Callback", mod}]
    ]
  end

  defp normalize_template(%{id: id, start: {mod, _, _} = start} = child),
    do: {
      id,
      start,
      Map.get(child, :restart, :permanent),
      Map.get(child, :shutdown, 5_000),
      Map.get(child, :type, :worker),
      Map.get(child, :modules, [mod])
    }

  defp normalize_template({_, _, _, _, _, _} = child), do: child

  defp validate_template({_, _, :permanent, _, _, _}) do
    error = """
    a child specification with :restart set to :permanent \
    is not supported in ConsumerSupervisor

    Set the :restart option either to :temporary, so children \
    spawned from events are never restarted, or :transient, \
    so they are restarted only on abnormal exits
    """

    {:error, error}
  end

  defp validate_template({_, _, _, _, _, _}) do
    :ok
  end
end
