defmodule GenRouter.SingleIn do
  # TODO: Implement pseudo-code

  use GenRouter.In

  def init(_) do
    {:ok, {0, nil}}
  end

  # demand

  def handle_demand(demand, {current, nil}) when is_integer(current) do
    {:noreply, {current+demand, nil}}
  end

  def handle_demand(demand, {current, {pid, ref} = source}) do
    GenRouter.ask(pid, self(), ref, demand)
    {:noreply, {current+demand, source}}
  end

  # call

  def handle_call({:subscribe, to, _opts}, _from, {demand, nil}) do
    pid = GenServer.whereis(to)
    ref = Process.monitor(pid)
    if demand != 0 do
      GenRouter.ask(pid, self(), ref, demand)
    end
    {:reply, {:ok, pid, ref}, {demand, {pid, ref}}}
  end

  def handle_call({:subscribe, _to, _opts}, _from, state) do
    {:reply, {:error, :already_subscribed}, state}
  end

  def handle_call({:unsubscribe, ref, _opts}, _from, {demand, {pid, ref}}) do
    GenRouter.cancel(pid, ref)
    {:noreply, {demand, nil}}
  end

  def handle_call({:unsubscribe, _ref, _opts}, _from, state) do
    {:reply, {:error, :not_subscribed}, state}
  end

  # info

  def handle_info({:"$gen_router", source, [_|_] = events}, {demand, source}) do
    {:dispatch, events, {demand - length(events), source}}
  end

  def handle_info({:"$gen_router", source, {:eos, _}}, {demand, source}) do
    {:noreply, {demand, nil}}
  end

  def handle_info({:DOWN, ref, _, pid, _}, {demand, {pid, ref}}) do
    {:noreply, {demand, nil}}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end
end
