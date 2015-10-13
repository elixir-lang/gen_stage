ExUnit.start()

defmodule RouterIn do

  def init(args), do: RouterCommon.init(__MODULE__, args)

  def handle_call({:reply, reply}, _, parent) do
    {:reply, reply, parent}
  end
  def handle_call({:noreply, reply}, from, parent) do
    GenRouter.reply(from, reply)
    {:noreply, parent}
  end
  def handle_call({:stop, reason}, from, parent) do
    GenRouter.reply(from, :stopping)
    {:stop, reason, parent}
  end
  def handle_call({:stop, reason, reply}, _, parent) do
    {:stop, reason, reply, parent}
  end
  def handle_call({:bad_return, return}, _, _) do
    return
  end
  def handle_call(:throw, _, _) do
    throw "oops"
  end

  def handle_demand(demand, parent) do
    send(parent, {__MODULE__, self(), {:demand, demand}})
    receive do
      {:dispatch, refs} ->
        events = for ref <- refs, do: {__MODULE__, demand, ref}
        {:dispatch, events, parent}
    after
      0 ->
        {:noreply, parent}
    end
  end

  def handle_info({:bad_return, return}, _) do
    return
  end
  def handle_info(:throw, _) do
    throw "oops"
  end
  def handle_info(info, parent) do
    send(parent, {__MODULE__, self(), {:info, info}})
    {:noreply, parent}
  end

  def terminate(reason, state), do: RouterCommon.terminate(__MODULE__, reason, state)

end

defmodule RouterOut do
  def init(args), do: RouterCommon.init(__MODULE__, args)


  def handle_demand(demand, {_, ref} = sink, parent) do
    send(parent, {__MODULE__, self(), {:demand, demand, sink}})
    case demand do
      3 ->
        {:ok, 1, [{__MODULE__, 3, ref}], parent}
      4 ->
        send(self(), {:dispatch, [ref]})
        {:ok, 2, [], parent}
      5 ->
        {:ok, 2, [{__MODULE__, 5, ref}], parent}
      6 ->
        send(self(), {:dispatch, [ref]})
        {:ok, 3, [{__MODULE__, 6, ref}], parent}
      7 ->
        send(self(), {:dispatch, []})
        {:ok, 3, [], parent}
      8 ->
        {:error, "oops", parent}
      9 ->
        {:error, "oops", [], parent}
      10 ->
        {:stop, :normal, parent}
      11 ->
        {:stop, :normal, [{__MODULE__, 11, ref}], parent}
      12 ->
        :baddy
      13 ->
        throw "oops"
      demand ->
        {:ok, div(demand, 2), parent}
    end
  end

  def handle_dispatch({_, _, ref}, parent) do
    {:ok, [ref], parent}
  end

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
