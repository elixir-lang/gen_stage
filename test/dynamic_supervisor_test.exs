defmodule DynamicSupervisorTest do
  use ExUnit.Case, async: true

  import Supervisor.Spec

  defmodule Simple do
    def init(args), do: args
  end

  test "supervisor with non-ok init" do
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
    assert DynamicSupervisor.start_link(Simple, :ignore) ==
           :ignore
    assert DynamicSupervisor.start_link(Simple, :unknown) ==
           {:error, {:bad_return_value, :unknown}}
  end
end
