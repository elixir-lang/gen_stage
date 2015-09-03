defmodule GenRouter.SingleIn do
  # TODO: Implement pseudo-code

  def init(_) do
    {:ok, 0}
  end

  # demand

  def handle_demand(demand, {pid, ref} = source) do
    GenRouter.ask(pid, self(), ref, demand)
    {:noreply, source}
  end

  def handle_demand(demand, current) when is_integer(current) do
    {:noreply, current+demand}
  end

  # call

  def handle_call({:subscribe, _to, _opts}, _from, {_, _} = source) do
    {:reply, {:error, :already_subscribed}, source}
  end

  def handle_call({:subscribe, to, _opts}, _from, demand) do
    pid = GenServer.whereis(to)
    ref = Process.monitor(pid)
    if demand != 0 do
      GenRouter.ask(pid, self(), ref, demand)
    end
    {:reply, {:ok, pid, ref}, {pid, ref}}
  end

  def handle_call({:unsubscribe, ref, _opts}, _from, {pid, ref}) do
    GenRouter.cancel(pid, ref)
    {:noreply, 0}
  end

  def handle_call({:unsubscribe, _ref, _opts}, _from, demand) do
    {:reply, {:error, :not_subscribed}, demand}
  end

  # info

  def handle_info({:"$gen_router", source, [_|_] = events}, source) do
    {:dispatch, events, source}
  end

  def handle_info({:"$gen_router", source, {:eos, _}}, source) do
    {:noreply, 0}
  end

  def handle_info({:DOWN, ref, _, pid, _}, {pid, ref}) do
    {:noreply, 0}
  end

  def handle_info(_, source_or_demand) do
    {:noreply, source_or_demand}
  end
end
