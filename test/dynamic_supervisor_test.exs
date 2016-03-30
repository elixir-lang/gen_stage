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
    init = {:ok, [worker(__MODULE__, [])], strategy: :one_for_one}
    {:ok, pid} = DynamicSupervisor.start_link(Simple, init)

    assert {:ok, _, :extra} = DynamicSupervisor.start_child(pid, [:ok3])
    assert {:ok, _} = DynamicSupervisor.start_child(pid, [:ok2])
    assert {:error, :found} = DynamicSupervisor.start_child(pid, [:error])
    assert :ignore = DynamicSupervisor.start_child(pid, [:ignore])
    assert {:error, :unknown} = DynamicSupervisor.start_child(pid, [:unknown])
  end

  test "start_child/2 with throw/error/exit" do
    init = {:ok, [worker(__MODULE__, [:non_local])], strategy: :one_for_one}
    {:ok, pid} = DynamicSupervisor.start_link(Simple, init)

    assert {:error, {{:nocatch, :oops}, [_|_]}} =
           DynamicSupervisor.start_child(pid, [:throw])
    assert {:error, {%RuntimeError{}, [_|_]}} =
           DynamicSupervisor.start_child(pid, [:error])
    assert {:error, :oops} =
           DynamicSupervisor.start_child(pid, [:exit])
  end

  test "temporary child is not restarted regardless of reason" do
    init = {:ok, [worker(__MODULE__, [], restart: :temporary)], strategy: :one_for_one}
    {:ok, pid} = DynamicSupervisor.start_link(Simple, init)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :shutdown
    assert %{specs: 0, active: 0} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :whatever
    assert %{specs: 0, active: 0} = DynamicSupervisor.count_children(pid)
  end

  test "transient child is restarted unless normal/shutdown/{shutdown, _}" do
    init = {:ok, [worker(__MODULE__, [], restart: :transient)], strategy: :one_for_one}
    {:ok, pid} = DynamicSupervisor.start_link(Simple, init)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :shutdown
    assert %{specs: 0, active: 0} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, {:shutdown, :signal}
    assert %{specs: 0, active: 0} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :whatever
    assert %{specs: 1, active: 1} = DynamicSupervisor.count_children(pid)
  end

  test "permanent child is restarted regardless of reason" do
    init = {:ok, [worker(__MODULE__, [], restart: :permanent)], strategy: :one_for_one}
    {:ok, pid} = DynamicSupervisor.start_link(Simple, init)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :shutdown
    assert %{specs: 1, active: 1} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, {:shutdown, :signal}
    assert %{specs: 2, active: 2} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :whatever
    assert %{specs: 3, active: 3} = DynamicSupervisor.count_children(pid)
  end

  test "child is restarted with different values" do
    init = {:ok, [worker(__MODULE__, [:restart], restart: :permanent)],
                 strategy: :one_for_one, max_restarts: 100_000}
    {:ok, pid} = DynamicSupervisor.start_link(Simple, init)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok2])
    assert_kill child, :shutdown
    assert %{specs: 1, active: 1} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ok3])
    assert_kill child, :shutdown
    assert %{specs: 2, active: 2} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:ignore])
    assert_kill child, :shutdown
    assert %{specs: 2, active: 2} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:error])
    assert_kill child, :shutdown
    assert %{specs: 3, active: 2} = DynamicSupervisor.count_children(pid)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:unknown])
    assert_kill child, :shutdown
    assert %{specs: 4, active: 2} = DynamicSupervisor.count_children(pid)
  end

  test "child is restarted when trying again" do
    init = {:ok, [worker(__MODULE__, [], restart: :permanent)],
                 strategy: :one_for_one, max_restarts: 2}
    {:ok, pid} = DynamicSupervisor.start_link(Simple, init)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:try_again, self()])
    assert_received {:try_again, true}
    assert_kill child, :shutdown
    assert_receive {:try_again, false}
    assert_receive {:try_again, true}
    assert %{specs: 1, active: 1} = DynamicSupervisor.count_children(pid)
  end

  test "child triggers maximum restarts" do
    Process.flag(:trap_exit, true)
    init = {:ok, [worker(__MODULE__, [], restart: :permanent)],
                 strategy: :one_for_one, max_restarts: 1}
    {:ok, pid} = DynamicSupervisor.start_link(Simple, init)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:restart, :error])
    assert_kill child, :shutdown
    assert_receive {:EXIT, ^pid, :shutdown}
  end

  test "child triggers maximum seconds" do
    Process.flag(:trap_exit, true)
    init = {:ok, [worker(__MODULE__, [], restart: :permanent)],
                 strategy: :one_for_one, max_seconds: 0}
    {:ok, pid} = DynamicSupervisor.start_link(Simple, init)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:restart, :error])
    assert_kill child, :shutdown
    assert_receive {:EXIT, ^pid, :shutdown}
  end

  test "child triggers maximum intensity when trying again" do
    Process.flag(:trap_exit, true)
    init = {:ok, [worker(__MODULE__, [], restart: :permanent)],
                 strategy: :one_for_one, max_restarts: 10}
    {:ok, pid} = DynamicSupervisor.start_link(Simple, init)

    assert {:ok, child} = DynamicSupervisor.start_child(pid, [:restart, :error])
    assert_kill child, :shutdown
    assert_receive {:EXIT, ^pid, :shutdown}
  end

  defp assert_kill(pid, reason) do
    ref = Process.monitor(pid)
    Process.exit(pid, reason)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
