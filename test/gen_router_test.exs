defmodule GenRouterTest do
  use ExUnit.Case, async: true

  test "start_link/2 workflow with unregistered name" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()},
                                      RouterOut, {:ok, self()})

    {:links, links} = Process.info(self, :links)
    assert pid in links

    assert_received {RouterIn, ^pid, :init}
    assert_received {RouterOut, ^pid, :init}

    assert {GenRouter, _, _} = :proc_lib.translate_initial_call(pid)
  end

  test "start/2 worflow with registered name" do
    {:ok, pid} = GenRouter.start(RouterIn, {:ok, self()},
                                 RouterOut, {:ok, self()}, [name: :router])

    {:links, links} = Process.info(self, :links)
    Process.link(pid)
    refute pid in links

    assert_received {RouterIn, ^pid, :init}
    assert_received {RouterOut, ^pid, :init}

    assert Process.info(pid, :registered_name) == {:registered_name, :router}

    assert {GenRouter, _, _} = :proc_lib.translate_initial_call(pid)
  end

  test "start/2 returns error on In :stop or error" do
    assert GenRouter.start(RouterIn, {:stop, "oops"}, RouterOut, {:ok, self()}) == {:error, "oops"}
    assert_received {RouterOut, pid, :init}
    assert_received {RouterOut, ^pid, {:terminate, "oops"}}

    assert {:error, reason = {%RuntimeError{}, _}} = GenRouter.start(RouterIn, :raise, RouterOut, {:ok, self()})
    assert_received {RouterOut, pid, :init}
    assert_received {RouterOut, ^pid, {:terminate, ^reason}}

    assert {:error, reason = {{:nocatch, "oops"}, _}} = GenRouter.start(RouterIn, :throw, RouterOut, {:ok, self()})
    assert_received {RouterOut, pid, :init}
    assert_received {RouterOut, ^pid, {:terminate, ^reason}}

    assert GenRouter.start(RouterIn, :exit, RouterOut, {:ok, self()}) == {:error, "oops"}
    assert_received {RouterOut, pid, :init}
    assert_received {RouterOut, ^pid, {:terminate, "oops"}}

    # In never succeeds
    refute_received {RouterIn, _, _}
  end

  test "start/2 returns error on Out :stop or error" do
    assert GenRouter.start(RouterIn, {:ok, self()}, RouterOut, {:stop, "oops"}) == {:error, "oops"}

    assert {:error, {%RuntimeError{}, _}} = GenRouter.start(RouterIn, {:ok, self()}, RouterOut, :raise)

    assert {:error, {{:nocatch, "oops"}, _}} = GenRouter.start(RouterIn, {:ok, self()}, RouterOut, :throw)

    assert GenRouter.start(RouterIn, {:ok, self()}, RouterOut, :exit) == {:error, "oops"}

    # Neither ever succeeds
    refute_received {RouterIn, _, _}
    refute_received {RouterOut, _, _}
  end
end
