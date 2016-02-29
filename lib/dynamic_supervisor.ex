defmodule DynamicSupervisor do
  @behaviour GenServer

  alias DynamicSupervisor, as: Sup
  defstruct [:mod, :args, :template, :max_restarts, :max_seconds, :strategy]

  # TODO: Add start_link/2
  # TODO: Define behaviour
  # TODO: Add minimum/maximum children
  # TODO: Add indexes start support

  @doc """
  Starts and links a `DynamicSupervisor`.
  """
  @spec start_link(module, any, [GenServer.option]) :: GenServer.on_start
  def start_link(mod, args, options \\ []) do
    GenServer.start_link(__MODULE__, {mod, args}, options)
  end

  ## Callbacks

  def init({mod, args}) do
    Process.flag(:trap_exit, true)
    case mod.init(args) do
      {:ok, children, opts} ->
        case validate_specs(children) do
          :ok ->
            case init(mod, args, children, opts) do
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

  defp init(mod, args, [child], opts) when is_list(opts) do
    strategy     = opts[:strategy]
    max_restarts = Keyword.get(opts, :max_restarts, 3)
    max_seconds  = Keyword.get(opts, :max_seconds, 5)

    with :ok <- validate_strategy(strategy),
         :ok <- validate_restarts(max_restarts),
         :ok <- validate_seconds(max_seconds) do
      {:ok, %Sup{mod: mod, args: args, template: child, strategy: strategy,
                 max_restarts: max_restarts, max_seconds: max_seconds}}
    end
  end
  defp init(_mod, _args, [_], _opts) do
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
end
