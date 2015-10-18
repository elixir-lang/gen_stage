ExUnit.start()

defmodule TestRouter do

  def start(in_agent, out_agent, opts \\ []) do
    GenRouter.start(TestRouter.In, in_agent, TestRouter.Out, out_agent, opts)
  end

  def start_link(in_agent, out_agent, opts \\ []) do
    GenRouter.start_link(TestRouter.In, in_agent, TestRouter.Out, out_agent, opts)
  end

  def init(mod, agent) do
    _ = Process.put(mod, agent)
    TestAgent.eval(agent, :init, [agent])
  end

  def eval(mod, fun, args) do
    agent = Process.get(mod)
    TestAgent.eval(agent, fun, args)
  end

  defmodule In do

    use GenRouter.In

    def init(args), do: TestRouter.init(__MODULE__, args)

    def handle_call(req, from, state) do
      TestRouter.eval(__MODULE__, :handle_call, [req, from, state])
    end

    def handle_demand(demand, state) do
      TestRouter.eval(__MODULE__, :handle_demand, [demand, state])
    end

    def handle_info(info, state) do
      TestRouter.eval(__MODULE__, :handle_info, [info, state])
    end

    def terminate(reason, state) do
      TestRouter.eval(__MODULE__, :terminate, [reason, state])
    end
  end

  defmodule Out do

    use GenRouter.Out

    def init(args), do: TestRouter.init(__MODULE__, args)

    def handle_demand(demand, sink, state) do
      TestRouter.eval(__MODULE__, :handle_demand, [demand, sink, state])
    end

    def handle_dispatch(event, state) do
      TestRouter.eval(__MODULE__, :handle_dispatch, [event, state])
    end

    def terminate(reason, state) do
      TestRouter.eval(__MODULE__, :terminate, [reason, state])
    end
  end
end

defmodule TestAgent do

  def start_link(stack), do: Agent.start_link(fn() -> {stack, []} end)

  def eval(agent, fun, args) do
    action = {fun, args}
    case Agent.get_and_update(agent, &get_and_update(&1, action)) do
      fun when is_function(fun) ->
        apply(fun, args)
      result ->
        result
    end
  end

  def record(agent) do
    Enum.reverse(Agent.get(agent, &elem(&1, 1)))
  end

  defp get_and_update({[next | stack], record}, action) do
    {next, {stack, [action | record]}}
  end
end
