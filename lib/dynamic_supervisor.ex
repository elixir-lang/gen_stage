defmodule DynamicSupervisor do
  @behaviour GenServer

  defstruct [:name, :mod, :args, :template, :max_restarts, :max_seconds, :strategy,
             children: %{}, restarts: [], restarting: 0]

  # TODO: Add start_link(specs, opts) as in Supervisor
  # TODO: Define behaviour
  # TODO: Add max_demand / min_demand

  @typedoc "The supervisor reference"
  @type supervisor :: pid | atom | {:global, term} | {:via, module, term} | {atom, node}

  @doc """
  Starts and links a `DynamicSupervisor`.
  """
  @spec start_link(module, any, [GenServer.option]) :: GenServer.on_start
  def start_link(mod, args, opts \\ []) do
    GenServer.start_link(__MODULE__, {mod, args, opts[:name]}, opts)
  end

  # TODO: Document me
  @spec start_child(supervisor, [any]) :: GenServer.on_start
  def start_child(supervisor, args) when is_list(args) do
    GenServer.call(supervisor, {:start_child, args})
  end

  @doc """
  Returns a map containing count values for the supervisor.

  The map contains the following keys:

    * `:specs` - always 1 as dynamic supervisors have a single specification

    * `:active` - the count of all actively running child processes managed by
      this supervisor

    * `:supervisors` - the count of all supervisors whether or not the child
      process is still alive

    * `:workers` - the count of all workers, whether or not the child process
      is still alive

  """
  @spec count_children(supervisor) ::
        %{specs: non_neg_integer, active: non_neg_integer,
          supervisors: non_neg_integer, workers: non_neg_integer}
  def count_children(supervisor) do
    GenServer.call(supervisor, :count_children)
  end

  ## Callbacks

  # TODO: Set initial call
  def init({mod, args, name}) do
    Process.flag(:trap_exit, true)
    case mod.init(args) do
      {:ok, children, opts} ->
        case validate_specs(children) do
          :ok ->
            case init(name, mod, args, children, opts) do
              {:ok, state} -> {:ok, state}
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

  defp init(name, mod, args, [child], opts) when is_list(opts) do
    strategy     = opts[:strategy]
    max_restarts = Keyword.get(opts, :max_restarts, 3)
    max_seconds  = Keyword.get(opts, :max_seconds, 5)

    with :ok <- validate_strategy(strategy),
         :ok <- validate_restarts(max_restarts),
         :ok <- validate_seconds(max_seconds) do
      {:ok, %DynamicSupervisor{mod: mod, args: args, template: child,
                               strategy: strategy, name: name || {self(), mod},
                               max_restarts: max_restarts, max_seconds: max_seconds}}
    end
  end
  defp init(_name, _mod, _args, [_], _opts) do
    {:error, "supervisor's init expects a keywords list as options"}
  end

  defp validate_specs([_]) do
    :ok # TODO: Do proper spec validation
  end
  defp validate_specs(_children) do
    {:error, "dynamic supervisor expects a list with a single item as a template"}
  end

  defp validate_strategy(strategy) when strategy in [:one_for_one], do: :ok
  defp validate_strategy(nil), do: {:error, "supervisor expects a strategy to be given"}
  defp validate_strategy(_), do: {:error, "unknown supervision strategy for dynamic supervisor"}

  defp validate_restarts(restart) when is_integer(restart), do: :ok
  defp validate_restarts(_), do: {:error, "max_restarts must be an integer"}

  defp validate_seconds(seconds) when is_integer(seconds), do: :ok
  defp validate_seconds(_), do: {:error, "max_seconds must be an integer"}

  @doc false
  def handle_call(:count_children, _from, state) do
    %{children: children, template: child, restarting: restarting} = state
    {_, _, _, _, type, _} = child

    specs  = map_size(children)
    active = specs - restarting
    reply  =
      case type do
        :supervisor ->
          %{specs: 1, active: active, workers: 0, supervisors: specs}
        :worker ->
          %{specs: 1, active: active, workers: specs, supervisors: 0}
      end

    {:reply, reply, state}
  end

  def handle_call({:start_child, extra}, _from, state) do
    %{template: child} = state
    {_, {m, f, args}, restart, _, _, _} = child
    args = args ++ extra

    case reply = start_child(m, f, args) do
      {:ok, pid, _} ->
        {:reply, reply, save_child(restart, pid, args, state)}
      {:ok, pid} ->
        {:reply, reply, save_child(restart, pid, args, state)}
      _ ->
        {:reply, reply, state}
    end
  end

  defp start_child(m, f, a) do
    try do
      apply(m, f, a)
    catch
      kind, reason ->
        {:error, exit_reason(kind, reason, System.stacktrace)}
    else
      {:ok, pid, extra} when is_pid(pid) -> {:ok, pid, extra}
      {:ok, pid} when is_pid(pid) -> {:ok, pid}
      :ignore -> :ignore
      {:error, _} = error -> error
      other -> {:error, other}
    end
  end

  defp save_child(:temporary, pid, _, state),
    do: put_in(state.children[pid], true)
  defp save_child(_, pid, args, state),
    do: put_in(state.children[pid], args)

  defp exit_reason(:exit, reason, _),      do: reason
  defp exit_reason(:error, reason, stack), do: {reason, stack}
  defp exit_reason(:throw, value, stack),  do: {{:nocatch, value}, stack}

  @doc false
  def handle_info({:EXIT, pid, reason}, state) do
    case maybe_restart_child(pid, reason, state) do
      {:ok, state} ->
        {:noreply, state}
      {:shutdown, state} ->
        {:stop, :shutdown, state}
    end
  end

  def handle_info({:"$gen_restart", pid}, state) do
    %{children: children, template: child, restarting: restarting} = state
    state = %{state | restarting: restarting - 1}

    case children do
      %{^pid => args} ->
        case restart_child(pid, args, child, state) do
          {:ok, state} ->
            {:noreply, state}
          {:shutdown, state} ->
            {:stop, :shutdown, state}
        end
      %{} ->
        {:noreply, state}
    end
  end

  def handle_info(msg, state) do
    :error_logger.error_msg('Supervisor received unexpected message: ~p~n', [msg])
    {:noreply, state}
  end

  @doc false
  def terminate(_, _) do
    # TODO: Implement me
    :ok
  end

  defp maybe_restart_child(pid, reason, state) do
    %{children: children, template: child} = state
    {_, _, restart, _, _, _} = child

    case children do
      %{^pid => args} -> maybe_restart_child(restart, reason, pid, args, child, state)
      %{} -> {:ok, state}
    end
  end

  defp maybe_restart_child(:permanent, reason, pid, args, child, state) do
    report_error(:child_terminated, reason, pid, args, child, state)
    restart_child(pid, args, child, state)
  end
  defp maybe_restart_child(_, :normal, pid, _args, _child, state) do
    {:ok, delete_child(pid, state)}
  end
  defp maybe_restart_child(_, :shutdown, pid, _args, _child, state) do
    {:ok, delete_child(pid, state)}
  end
  defp maybe_restart_child(_, {:shutdown, _}, pid, _args, _child, state) do
    {:ok, delete_child(pid, state)}
  end
  defp maybe_restart_child(:transient, reason, pid, args, child, state) do
    report_error(:child_terminated, reason, pid, args, child, state)
    restart_child(pid, args, child, state)
  end
  defp maybe_restart_child(:temporary, reason, pid, args, child, state) do
    report_error(:child_terminated, reason, pid, args, child, state)
    {:ok, delete_child(pid, state)}
  end

  defp delete_child(pid, %{children: children} = state) do
    %{state | children: Map.delete(children, pid)}
  end

  defp restart_child(pid, args, child, state) do
    case add_restart(state) do
      {:ok, %{strategy: strategy} = state} ->
        case restart_child(strategy, pid, args, child, state) do
          {:ok, state} ->
            {:ok, state}
          {:try_again, state} ->
            send(self(), {:"$gen_restart", pid})
            {:ok, state}
        end
      {:shutdown, state} ->
        report_error(:shutdown, :reached_max_restart_intensity, pid, args, child, state)
        {:shutdown, delete_child(pid, state)}
    end
  end

  defp add_restart(state) do
    %{max_seconds: max_seconds, max_restarts: max_restarts, restarts: restarts} = state
    now      = :erlang.monotonic_time(1)
    restarts = add_restart([now|restarts], now, max_seconds)
    state    = %{state | restarts: restarts}

    if length(restarts) <= max_restarts do
      {:ok, state}
    else
      {:shutdown, state}
    end
  end

  defp add_restart(restarts, now, period) do
    for then <- restarts, now <= then + period, do: then
  end

  defp restart_child(:one_for_one, current_pid, args, child, state) do
    {_, {m, f, _}, restart, _, _, _} = child

    case start_child(m, f, args) do
      {:ok, pid, _} ->
        {:ok, save_child(restart, pid, args, delete_child(current_pid, state))}
      {:ok, pid} ->
        {:ok, save_child(restart, pid, args, delete_child(current_pid, state))}
      :ignore ->
        {:ok, delete_child(current_pid, state)}
      {:error, reason} ->
        report_error(:start_error, reason, {:restarting, current_pid}, args, child, state)
        {:try_again, update_in(state.restarting, &(&1 + 1))}
    end
  end

  defp report_error(error, reason, pid, args, child, %{name: name}) do
    :error_logger.error_report(:supervision_report,
      supervisor: name,
      errorContext: error,
      reason: reason,
      offender: extract_child(pid, args, child)
    )
  end

  defp extract_child(pid, args, {id, {m, f, _}, restart, shutdown, type, _}) do
    [pid: pid,
     id: id,
     mfargs: {m, f, args},
     restart_type: restart,
     shutdown: shutdown,
     child_type: type]
  end
end
