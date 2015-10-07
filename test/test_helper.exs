ExUnit.start()

defmodule RouterIn do

  def init(args), do: RouterCommon.init(__MODULE__, args)

  def terminate(reason, state), do: RouterCommon.terminate(__MODULE__, reason, state)

end

defmodule RouterOut do
  def init(args), do: RouterCommon.init(__MODULE__, args)
  def terminate(reason, state), do: RouterCommon.terminate(__MODULE__, reason, state)
end

defmodule RouterCommon do

  def init(mod, {:ok, parent}) do
    send(parent, {mod, self(), :init})
    {:ok, parent}
  end

  def init(_, {:stop, _} = stop), do: stop
  def init(_, :raise), do: raise "oops"
  def init(_, :throw), do: throw("oops")
  def init(_, :exit), do: exit("oops")

  def terminate(mod, reason, parent) when is_pid(parent) do
    send(parent, {mod, self(), {:terminate, reason}})
  end
end
