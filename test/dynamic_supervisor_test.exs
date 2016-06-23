defmodule DynamicSupervisorTest do
  use ExUnit.Case, async: true

  import Supervisor.Spec

  defmodule Simple do
    def init(args), do: args
  end

  test "start_link/3 with non-ok init" do
    Process.flag(:trap_exit, true)
    worker = worker(Foo, [])

    assert DynamicSupervisor.start_link(Simple, {:ok, [], []}) ==
           {:error, {:bad_specs, "dynamic supervisor expects a list with a single item as a template"}}
    assert DynamicSupervisor.start_link(Simple, {:ok, [1, 2], []}) ==
           {:error, {:bad_specs, "dynamic supervisor expects a list with a single item as a template"}}
    assert DynamicSupervisor.start_link(Simple, {:ok, [worker], nil}) ==
           {:error, {:bad_opts, "supervisor's init expects a keywords list as options"}}
    assert DynamicSupervisor.start_link(Simple, {:ok, [worker], []}) ==
           {:error, {:bad_opts, "supervisor expects a strategy to be given"}}
    assert DynamicSupervisor.start_link(Simple, {:ok, [worker], [strategy: :unknown]}) ==
           {:error, {:bad_opts, "unknown supervision strategy for dynamic supervisor"}}
    assert DynamicSupervisor.start_link(Simple, :unknown) ==
           {:error, {:bad_return_value, :unknown}}
    assert DynamicSupervisor.start_link(Simple, :ignore) ==
           :ignore
  end

  test "sets initial call to the same as a regular supervisor" do
    {:ok, pid} = Supervisor.start_link([], strategy: :one_for_one)
    assert :proc_lib.initial_call(pid) ==
           {:supervisor, Supervisor.Default, [:Argument__1]}

    {:ok, pid} = DynamicSupervisor.start_link([worker(Foo, [])], strategy: :one_for_one)
    assert :proc_lib.initial_call(pid) ==
           {:supervisor, Supervisor.Default, [:Argument__1]}
  end

  # TODO: Verify this on Erlang 19
  if function_exported?(:supervisor, :get_callback_module, 1) do
    test "returns the callback module" do
      {:ok, pid} = Supervisor.start_link([], strategy: :one_for_one)
      assert :supervisor.get_callback_module(pid) == Supervisor.Default

      {:ok, pid} = DynamicSupervisor.start_link([worker(Foo, [])], strategy: :one_for_one)
      assert :supervisor.get_callback_module(pid) == Supervisor.Default
    end
  end

  test "start_link/3 with registered process" do
    spec = {:ok, [worker(Foo, [])], [strategy: :one_for_one]}
    {:ok, pid} = DynamicSupervisor.start_link(Simple, spec, name: __MODULE__)

    # Sets up a link
    {:links, links} = Process.info(self(), :links)
    assert pid in links

    # A name
    assert Process.whereis(__MODULE__) == pid

    # And the initial call
    assert {:supervisor, DynamicSupervisorTest.Simple, 1} =
           :proc_lib.translate_initial_call(pid)
  end

  ## Code change

  test "code_change/3 with non-ok init" do
    worker = worker(Task, [:timer, :sleep, [:infinity]])
    {:ok, pid} = DynamicSupervisor.start_link(Simple, {:ok, [worker], strategy: :one_for_one})

    assert fake_upgrade(pid, {:ok, [], []}) ==
           {:error, {:error, {:bad_specs, "dynamic supervisor expects a list with a single item as a template"}}}
    assert fake_upgrade(pid, {:ok, [1, 2], []}) ==
           {:error, {:error, {:bad_specs, "dynamic supervisor expects a list with a single item as a template"}}}
    assert fake_upgrade(pid, {:ok, [worker], nil}) ==
           {:error, {:error, {:bad_opts, "supervisor's init expects a keywords list as options"}}}
    assert fake_upgrade(pid, {:ok, [worker], []}) ==
           {:error, {:error, {:bad_opts, "supervisor expects a strategy to be given"}}}
    assert fake_upgrade(pid, {:ok, [worker], [strategy: :unknown]}) ==
           {:error, {:error, {:bad_opts, "unknown supervision strategy for dynamic supervisor"}}}
    assert fake_upgrade(pid, :unknown) ==
           {:error, :unknown}
    assert fake_upgrade(pid, :ignore) ==
           :ok
  end

  test "code_change/3 with ok init" do
    worker = worker(Task, [:timer, :sleep, [:infinity]])
    {:ok, pid} = DynamicSupervisor.start_link(Simple, {:ok, [worker], strategy: :one_for_one})

    {:ok, _} = DynamicSupervisor.start_child(pid, [])
    assert %{active: 1} = DynamicSupervisor.count_children(pid)

    worker = worker(Task, [Kernel, :send], restart: :temporary)
    assert fake_upgrade(pid, {:ok, [worker], [strategy: :one_for_one]}) == :ok
    assert %{active: 1} = DynamicSupervisor.count_children(pid)

    {:ok, _} = DynamicSupervisor.start_child(pid, [[self(), :sample]])
    assert_receive :sample
  end

  defp fake_upgrade(pid, args) do
    :ok = :sys.suspend(pid)
    :sys.replace_state(pid, fn state -> %{state | args: args} end)
    res = :sys.change_code(pid, :gen_server, 123, :extra)
    :ok = :sys.resume(pid)
    res
  end

  ## start_child/2

  def start_link(:ok3),     do: {:ok, spawn_link(fn -> :timer.sleep(:infinity) end), :extra}
  def start_link(:ok2),     do: {:ok, spawn_link(fn -> :timer.sleep(:infinity) end)}
  def start_link(:error),   do: {:error, :found}
  def start_link(:ignore),  do: :ignore
  def start_link(:unknown), do: :unknown

  def start_link(:try_again, notify) do
    if Process.get(:try_again) do
      Process.put(:try_again, false)
      send notify, {:try_again, false}
      {:error, :try_again}
    else
      Process.put(:try_again, true)
      send notify, {:try_again, true}
      start_link(:ok2)
    end
  end

  def start_link(:non_local, :throw), do: throw(:oops)
  def start_link(:non_local, :error), do: raise("oops")
  def start_link(:non_local, :exit),  do: exit(:oops)

  def start_link(:restart, value) do
    if Process.get({:restart, value}) do
      start_link(value)
    else
      Process.put({:restart, value}, true)
      start_link(:ok2)
    end
  end

  test "start_child/2" do
    children = [worker(__MODULE__, [])]
    {:ok, pid} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    assert {:ok, _, :extra} = DynamicSupervisor.start_child(pid, [:ok3])
    assert {:ok, _} = DynamicSupervisor.start_child(pid, [:ok2])
    assert {:error, :found} = DynamicSupervisor.start_child(pid, [:error])
    assert :ignore = DynamicSupervisor.start_child(pid, [:ignore])
    assert {:error, :unknown} = DynamicSupervisor.start_child(pid, [:unknown])
  end

  test "start_child/2 with throw/error/exit" do
    children = [worker(__MODULE__, [:non_local])]
    {:ok, pid} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    assert {:error, {{:nocatch, :oops}, [_|_]}} =
           DynamicSupervisor.start_child(pid, [:throw])
    assert {:error, {%RuntimeError{}, [_|_]}} =
           DynamicSupervisor.start_child(pid, [:error])
    assert {:error, :oops} =
           DynamicSupervisor.start_child(pid, [:exit])
  end

  test "start_child/2 with max_children" do
    children = [worker(__MODULE__, [])]
    opts = [strategy: :one_for_one, max_children: 0]
    {:ok, pid} = DynamicSupervisor.start_link(children, opts)

    assert {:error, :max_children} = DynamicSupervisor.start_child(pid, [:ok2])
  end

  test "temporary child is not restarted regardless of reason" do
    children = [worker(__MODULE__, [], restart: :temporary)]
    {:ok, pid} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :shutdown
    assert %{workers: 0, active: 0} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :whatever
    assert %{workers: 0, active: 0} = DynamicSupervisor.count_children(pid)
  end

  test "transient child is restarted unless normal/shutdown/{shutdown, _}" do
    children = [worker(__MODULE__, [], restart: :transient)]
    {:ok, pid} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :shutdown
    assert %{workers: 0, active: 0} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, {:shutdown, :signal}
    assert %{workers: 0, active: 0} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :whatever
    assert %{workers: 1, active: 1} = DynamicSupervisor.count_children(pid)
  end

  test "permanent child is restarted regardless of reason" do
    children = [worker(__MODULE__, [], restart: :permanent)]
    {:ok, pid} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :shutdown
    assert %{workers: 1, active: 1} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, {:shutdown, :signal}
    assert %{workers: 2, active: 2} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :whatever
    assert %{workers: 3, active: 3} = DynamicSupervisor.count_children(pid)
  end

  test "child is restarted with different values" do
    children = [worker(__MODULE__, [:restart], restart: :permanent)]
    {:ok, pid} = DynamicSupervisor.start_link(children, strategy: :one_for_one, max_restarts: 100_000)

    assert {:ok, child1} = DynamicSupervisor.start_child(pid, [:ok2])
    assert [{:undefined, ^child1, :worker, [DynamicSupervisorTest]}] =
           DynamicSupervisor.which_children(pid)
    assert_kill child1, :shutdown
    assert %{workers: 1, active: 1} = DynamicSupervisor.count_children(pid)

    assert {:ok, child2} = DynamicSupervisor.start_child(pid, [:ok3])
    assert [{:undefined, _, :worker, [DynamicSupervisorTest]},
            {:undefined, ^child2, :worker, [DynamicSupervisorTest]}] =
           DynamicSupervisor.which_children(pid)
    assert_kill child2, :shutdown
    assert %{workers: 2, active: 2} = DynamicSupervisor.count_children(pid)

    assert {:ok, child3} = DynamicSupervisor.start_child(pid, [:ignore])
    assert [{:undefined, _, :worker, [DynamicSupervisorTest]},
            {:undefined, _, :worker, [DynamicSupervisorTest]},
            {:undefined, _, :worker, [DynamicSupervisorTest]}] =
           DynamicSupervisor.which_children(pid)
    assert_kill child3, :shutdown
    assert %{workers: 2, active: 2} = DynamicSupervisor.count_children(pid)

    assert {:ok, child4} = DynamicSupervisor.start_child(pid, [:error])
    assert [{:undefined, _, :worker, [DynamicSupervisorTest]},
            {:undefined, _, :worker, [DynamicSupervisorTest]},
            {:undefined, _, :worker, [DynamicSupervisorTest]}] =
            DynamicSupervisor.which_children(pid)
    assert_kill child4, :shutdown
    assert %{workers: 3, active: 2} = DynamicSupervisor.count_children(pid)

    assert {:ok, child5} = DynamicSupervisor.start_child(pid, [:unknown])
    assert [{:undefined, _, :worker, [DynamicSupervisorTest]},
            {:undefined, _, :worker, [DynamicSupervisorTest]},
            {:undefined, :restarting, :worker, [DynamicSupervisorTest]},
            {:undefined, _, :worker, [DynamicSupervisorTest]}] =
            DynamicSupervisor.which_children(pid)
    assert_kill child5, :shutdown
    assert %{workers: 4, active: 2} = DynamicSupervisor.count_children(pid)
  end

  test "restarting children counted in max_children" do
    children = [worker(__MODULE__, [:restart], restart: :permanent)]
    opts = [strategy: :one_for_one, max_children: 1, max_restarts: 100_000]
    {:ok, pid} = DynamicSupervisor.start_link(children, opts)

    assert {:ok, child1} = DynamicSupervisor.start_child(pid, [:error])
    assert_kill child1, :shutdown
    assert %{workers: 1, active: 0} = DynamicSupervisor.count_children(pid)

    assert {:error, :max_children} = DynamicSupervisor.start_child(pid, [:ok2])
  end

  test "child is restarted when trying again" do
    children = [worker(__MODULE__, [], restart: :permanent)]
    {:ok, pid} = DynamicSupervisor.start_link(children, strategy: :one_for_one, max_restarts: 2)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:try_again, self()])
    assert_received {:try_again, true}
    assert_kill child, :shutdown
    assert_receive {:try_again, false}
    assert_receive {:try_again, true}
    assert %{workers: 1, active: 1} = DynamicSupervisor.count_children(pid)
  end

  test "child triggers maximum restarts" do
    Process.flag(:trap_exit, true)
    children = [worker(__MODULE__, [], restart: :permanent)]
    {:ok, pid} = DynamicSupervisor.start_link(children, strategy: :one_for_one, max_restarts: 1)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:restart, :error])
    assert_kill child, :shutdown
    assert_receive {:EXIT, ^pid, :shutdown}
  end

  test "child triggers maximum seconds" do
    Process.flag(:trap_exit, true)
    children = [worker(__MODULE__, [], restart: :permanent)]
    {:ok, pid} = DynamicSupervisor.start_link(children, strategy: :one_for_one, max_seconds: 0)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:restart, :error])
    assert_kill child, :shutdown
    assert_receive {:EXIT, ^pid, :shutdown}
  end

  test "child triggers maximum intensity when trying again" do
    Process.flag(:trap_exit, true)
    children = [worker(__MODULE__, [], restart: :permanent)]
    {:ok, pid} = DynamicSupervisor.start_link(children, strategy: :one_for_one, max_restarts: 10)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:restart, :error])
    assert_kill child, :shutdown
    assert_receive {:EXIT, ^pid, :shutdown}
  end

  ## terminate/2

  test "terminates children with brutal kill" do
    Process.flag(:trap_exit, true)
    children = [worker(Task, [], shutdown: :brutal_kill)]
    {:ok, sup} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    fun = fn -> :timer.sleep(:infinity) end
    assert {:ok, child1} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child2} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child3} = DynamicSupervisor.start_child(sup, [fun])

    Process.monitor(child1)
    Process.monitor(child2)
    Process.monitor(child3)
    assert_kill sup, :shutdown
    assert_receive {:DOWN, _, :process, ^child1, :killed}
    assert_receive {:DOWN, _, :process, ^child2, :killed}
    assert_receive {:DOWN, _, :process, ^child3, :killed}
  end

  test "terminates children with infinity shutdown" do
    Process.flag(:trap_exit, true)
    children = [worker(Task, [], shutdown: :infinity)]
    {:ok, sup} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    fun = fn -> :timer.sleep(:infinity) end
    assert {:ok, child1} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child2} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child3} = DynamicSupervisor.start_child(sup, [fun])

    Process.monitor(child1)
    Process.monitor(child2)
    Process.monitor(child3)
    assert_kill sup, :shutdown
    assert_receive {:DOWN, _, :process, ^child1, :shutdown}
    assert_receive {:DOWN, _, :process, ^child2, :shutdown}
    assert_receive {:DOWN, _, :process, ^child3, :shutdown}
  end

  test "terminates children with infinity shutdown and abnormal reason" do
    Process.flag(:trap_exit, true)
    children = [worker(Task, [], shutdown: :infinity)]
    {:ok, sup} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    fun = fn -> Process.flag(:trap_exit, true); receive(do: (_ -> exit({:shutdown, :oops}))) end
    assert {:ok, child1} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child2} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child3} = DynamicSupervisor.start_child(sup, [fun])

    Process.monitor(child1)
    Process.monitor(child2)
    Process.monitor(child3)
    assert_kill sup, :shutdown
    assert_receive {:DOWN, _, :process, ^child1, {:shutdown, :oops}}
    assert_receive {:DOWN, _, :process, ^child2, {:shutdown, :oops}}
    assert_receive {:DOWN, _, :process, ^child3, {:shutdown, :oops}}
  end

  test "terminates children with integer shutdown" do
    Process.flag(:trap_exit, true)
    children = [worker(Task, [], shutdown: 1000)]
    {:ok, sup} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    fun = fn -> :timer.sleep(:infinity) end
    assert {:ok, child1} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child2} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child3} = DynamicSupervisor.start_child(sup, [fun])

    Process.monitor(child1)
    Process.monitor(child2)
    Process.monitor(child3)
    assert_kill sup, :shutdown
    assert_receive {:DOWN, _, :process, ^child1, :shutdown}
    assert_receive {:DOWN, _, :process, ^child2, :shutdown}
    assert_receive {:DOWN, _, :process, ^child3, :shutdown}
  end

  test "terminates children with integer shutdown and abnormal reason" do
    Process.flag(:trap_exit, true)
    children = [worker(Task, [], shutdown: 1000)]
    {:ok, sup} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    fun = fn -> Process.flag(:trap_exit, true); receive(do: (_ -> exit({:shutdown, :oops}))) end
    assert {:ok, child1} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child2} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child3} = DynamicSupervisor.start_child(sup, [fun])

    Process.monitor(child1)
    Process.monitor(child2)
    Process.monitor(child3)
    assert_kill sup, :shutdown
    assert_receive {:DOWN, _, :process, ^child1, {:shutdown, :oops}}
    assert_receive {:DOWN, _, :process, ^child2, {:shutdown, :oops}}
    assert_receive {:DOWN, _, :process, ^child3, {:shutdown, :oops}}
  end

  test "terminates children with expired integer shutdown" do
    Process.flag(:trap_exit, true)
    children = [worker(Task, [], shutdown: 0)]
    {:ok, sup} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    fun = fn -> :timer.sleep(:infinity) end
    tmt = fn -> Process.flag(:trap_exit, true); :timer.sleep(:infinity) end
    assert {:ok, child1} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child2} = DynamicSupervisor.start_child(sup, [tmt])
    assert {:ok, child3} = DynamicSupervisor.start_child(sup, [fun])

    Process.monitor(child1)
    Process.monitor(child2)
    Process.monitor(child3)
    assert_kill sup, :shutdown
    assert_receive {:DOWN, _, :process, ^child1, :shutdown}
    assert_receive {:DOWN, _, :process, ^child2, :killed}
    assert_receive {:DOWN, _, :process, ^child3, :shutdown}
  end

  test "terminates children with permanent restart and normal reason" do
    Process.flag(:trap_exit, true)
    children = [worker(Task, [], shutdown: :infinity, restart: :permanent)]
    {:ok, sup} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    fun = fn -> Process.flag(:trap_exit, true); receive(do: (_ -> exit(:normal))) end
    assert {:ok, child1} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child2} = DynamicSupervisor.start_child(sup, [fun])
    assert {:ok, child3} = DynamicSupervisor.start_child(sup, [fun])

    Process.monitor(child1)
    Process.monitor(child2)
    Process.monitor(child3)
    assert_kill sup, :shutdown
    assert_receive {:DOWN, _, :process, ^child1, :normal}
    assert_receive {:DOWN, _, :process, ^child2, :normal}
    assert_receive {:DOWN, _, :process, ^child3, :normal}
  end

  ## terminate_child/2

  test "terminates child with brutal kill" do
    children = [worker(Task, [], shutdown: :brutal_kill)]
    {:ok, sup} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    fun = fn -> :timer.sleep(:infinity) end
    assert {:ok, child} = DynamicSupervisor.start_child(sup, [fun])

    Process.monitor(child)
    assert :ok = DynamicSupervisor.terminate_child(sup, child)
    assert_receive {:DOWN, _, :process, ^child, :killed}

    assert {:error, :not_found} = DynamicSupervisor.terminate_child(sup, child)
    assert %{workers: 0, active: 0} = DynamicSupervisor.count_children(sup)
  end

  test "terminates child with integer shutdown" do
    children = [worker(Task, [], shutdown: 1000)]
    {:ok, sup} = DynamicSupervisor.start_link(children, strategy: :one_for_one)

    fun = fn -> :timer.sleep(:infinity) end
    assert {:ok, child} = DynamicSupervisor.start_child(sup, [fun])

    Process.monitor(child)
    assert :ok = DynamicSupervisor.terminate_child(sup, child)
    assert_receive {:DOWN, _, :process, ^child, :shutdown}

    assert {:error, :not_found} = DynamicSupervisor.terminate_child(sup, child)
    assert %{workers: 0, active: 0} = DynamicSupervisor.count_children(sup)
  end

  test "terminates restarting child" do
    children = [worker(__MODULE__, [:restart], restart: :permanent)]
    {:ok, sup} = DynamicSupervisor.start_link(children, strategy: :one_for_one, max_restarts: 100_000)

    assert {:ok, child} = DynamicSupervisor.start_child(sup, [:error])
    assert_kill child, :shutdown
    assert :ok = DynamicSupervisor.terminate_child(sup, child)

    assert {:error, :not_found} = DynamicSupervisor.terminate_child(sup, child)
    assert %{workers: 0, active: 0} = DynamicSupervisor.count_children(sup)
  end

  defp assert_kill(pid, reason) do
    ref = Process.monitor(pid)
    Process.exit(pid, reason)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
