defmodule GenRouterTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

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

  test "In handle_call/3 sends reply with :reply return" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    assert GenRouter.call(pid, {:reply, :my_reply}) == :my_reply
  end

  test "In handle_call/3 sends reply explicitly" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    assert GenRouter.call(pid, {:noreply, :my_noreply}) == :my_noreply
  end

  test "In handle_call/3 stops" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    Process.flag(:trap_exit, true)
    assert GenRouter.call(pid, {:stop, :normal}) == :stopping
    assert_receive {:EXIT, ^pid, :normal}
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}
    assert_received {RouterOut, ^pid, {:terminate, :normal}}
    assert_received {RouterIn, ^pid, {:terminate, :normal}}
    refute_received {_, ^pid, _}
  end

  test "In handle_call/3 stops and replies" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    Process.flag(:trap_exit, true)
    assert GenRouter.call(pid, {:stop, :normal, :stopping}) == :stopping
    assert_receive {:EXIT, ^pid, :normal}
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}
    assert_received {RouterOut, ^pid, {:terminate, :normal}}
    assert_received {RouterIn, ^pid, {:terminate, :normal}}
    refute_received {_, ^pid, _}
  end

  test "In handle_call/3 stops with bad_return_value" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      assert {{:bad_return_value, :baddy}, _} =
        catch_exit(GenRouter.call(pid, {:bad_return, :baddy}))
      assert_receive {:EXIT, ^pid, {:bad_return_value, :baddy}}
    end) =~ "bad return value: :baddy"
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}
    assert_received {RouterOut, ^pid, {:terminate, {:bad_return_value, :baddy}}}
    assert_received {RouterIn, ^pid, {:terminate, {:bad_return_value, :baddy}}}
    refute_received {_, ^pid, _}
  end

  test "In handle_call/3 throws causing :nocatch error" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      assert {{{:nocatch, "oops"}, [_|_]}, _} =
        catch_exit(GenRouter.call(pid, :throw))
      assert_receive {:EXIT, ^pid, {{:nocatch, "oops"}, [_|_]}}
    end) =~ "{:nocatch, \"oops\"}"
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}
    assert_received {RouterOut, ^pid, {:terminate, {{:nocatch, "oops"}, _}}}
    assert_received {RouterIn, ^pid, {:terminate, {{:nocatch, "oops"}, _}}}
    refute_received {_, ^pid, _}
  end

  test "In handle_info/2 returns :noreply" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    send(pid, :hello)
    assert_receive {RouterIn, ^pid, {:info, :hello}}
    down = {:DOWN, make_ref(), :process, self(), "oops"}
    send(pid, down)
    assert_receive {RouterIn, ^pid, {:info, ^down}}
  end

  test "In handle_info/2 stops with bad_return_value" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      send(pid, {:bad_return, :baddy})
      assert_receive {:EXIT, ^pid, {:bad_return_value, :baddy}}
    end) =~ "bad return value: :baddy"
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}
    assert_received {RouterOut, ^pid, {:terminate, {:bad_return_value, :baddy}}}
    assert_received {RouterIn, ^pid, {:terminate, {:bad_return_value, :baddy}}}
    refute_received {_, ^pid, _}
  end

  test "In handle_info/2 throws causing :nocatch error" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      send(pid, :throw)
      assert_receive {:EXIT, ^pid, {{:nocatch, "oops"}, [_|_]}}
    end) =~ "{:nocatch, \"oops\"}"
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}
    assert_received {RouterOut, ^pid, {:terminate, {{:nocatch, "oops"}, _}}}
    assert_received {RouterIn, ^pid, {:terminate, {{:nocatch, "oops"}, _}}}
    refute_received {_, ^pid, _}
  end

  test "Out/In handle_demand/3 with :ok (Out) and :noreply (In)" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}

    ref = make_ref()
    parent = self()
    GenRouter.ask(pid, parent, ref, 0)
    # 0 demand always passed to Out
    assert_receive {RouterOut, ^pid, {:demand, 0, {^parent, ^ref}}}

    GenRouter.ask(pid, parent, ref, 1)
    assert_receive {RouterOut, ^pid, {:demand, 1, {^parent, ^ref}}}

    # div(demand, 2) == 0 so far so does not trigger callback for In
    refute_received {RouterIn, ^pid, {:demand, _}}

    GenRouter.ask(pid, parent, ref, 2)
    assert_receive {RouterOut, ^pid, {:demand, 2, {^parent, ^ref}}}
    # demand halved by Out
    assert_receive {RouterIn, ^pid, {:demand, 1}}

    refute_received {_, ^pid, _}
  end

  test "Out/In handle_demand/3 with dispatch" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}

    ref = make_ref()
    parent = self()
    GenRouter.ask(pid, parent, ref, 3)
    assert_receive {RouterOut, ^pid, {:demand, 3, {^parent, ^ref}}}
    # demand halved from Out to In
    assert_receive {RouterIn, ^pid, {:demand, 1}}
    # demand of 3 causes dispatch by Out of a single event {RouterOut, ref}
    assert_receive {:"$gen_route", {^pid, ^ref}, [{RouterOut, 3, ^ref}]}

    GenRouter.ask(pid, parent, ref, 4)
    assert_receive {RouterOut, ^pid, {:demand, 4, {^parent, ^ref}}}
    # demand halved from Out to In
    assert_receive {RouterIn, ^pid, {:demand, 2}}
    # demand of 4 causes dispatch by Out of empty list and triggers dispatch
    # by In of a single event {RouterIn, ref}
    assert_receive {:"$gen_route", {^pid, ^ref}, [{RouterIn, 2, ^ref}]}
    refute_received {:"$gen_route", _, _}

    GenRouter.ask(pid, parent, ref, 5)
    assert_receive {RouterOut, ^pid, {:demand, 5, {^parent, ^ref}}}
    # demand halved from Out to In
    assert_receive {RouterIn, ^pid, {:demand, 2}}
    # demand of 5 causes dispatch by Out of single event {RouterOut, ref} and by
    # In of empty list
    assert_receive {:"$gen_route", {^pid, ^ref}, [{RouterOut, 5, ^ref}]}

    GenRouter.ask(pid, parent, ref, 6)
    assert_receive {RouterOut, ^pid, {:demand, 6, {^parent, ^ref}}}
    # demand halved from Out to In
    assert_receive {RouterIn, ^pid, {:demand, 3}}
    # demand of 6 causes dispatch by Out of single event {RouterOut, ref} and by
    # In of single event {RouterIn, ref}
    assert_receive {:"$gen_route", {^pid, ^ref}, [{RouterOut, 6, ^ref}]}
    assert_receive {:"$gen_route", {^pid, ^ref}, [{RouterIn, 3, ^ref}]}

    GenRouter.ask(pid, parent, ref, 7)
    assert_receive {RouterOut, ^pid, {:demand, 7, {^parent, ^ref}}}
    # demand halved from Out to In
    assert_receive {RouterIn, ^pid, {:demand, 3}}
    # demand of 7 causes dispatch by Out of empty list and In of empty list
    send(pid, :hello)
    assert_receive {RouterIn, ^pid, {:info, :hello}}
    refute_received {:"$gen_route", _, _}

    refute_received {_, ^pid, _}
  end

  test "Out/In handle_demand/3 with Out demand error" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}

    ref = make_ref()
    parent = self()
    GenRouter.ask(pid, parent, ref, 8)
    assert_receive {RouterOut, ^pid, {:demand, 8, {^parent, ^ref}}}
    assert_receive {:"$gen_route", {^pid, ^ref}, {:eos, {:error, "oops"}}}

    assert (:sys.get_state(pid)).sinks == %{}

    GenRouter.ask(pid, parent, ref, 1)
    assert_receive {RouterOut, ^pid, {:demand, 1, {^parent, ^ref}}}

    GenRouter.ask(pid, parent, ref, 8)
    assert_receive {RouterOut, ^pid, {:demand, 8, {^parent, ^ref}}}
    assert_receive {:"$gen_route", {^pid, ^ref}, {:eos, {:error, "oops"}}}

    assert (:sys.get_state(pid)).sinks == %{}

    GenRouter.ask(pid, parent, ref, 9)
    assert_receive {RouterOut, ^pid, {:demand, 9, {^parent, ^ref}}}
    assert_receive {:"$gen_route", {^pid, ^ref}, {:eos, {:error, "oops"}}}

    assert (:sys.get_state(pid)).sinks == %{}

    GenRouter.ask(pid, parent, ref, 1)
    assert_receive {RouterOut, ^pid, {:demand, 1, {^parent, ^ref}}}

    GenRouter.ask(pid, parent, ref, 9)
    assert_receive {RouterOut, ^pid, {:demand, 9, {^parent, ^ref}}}
    assert_receive {:"$gen_route", {^pid, ^ref}, {:eos, {:error, "oops"}}}

    assert (:sys.get_state(pid)).sinks == %{}

    refute_received {:"$gen_route", _, _}
    refute_received {_, ^pid, _}
  end

  test "Out/In handle_demand/3 with Out demand stop" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}

    ref = make_ref()
    parent = self()
    Process.flag(:trap_exit, true)
    GenRouter.ask(pid, parent, ref, 10)
    assert_receive {RouterOut, ^pid, {:demand, 10, {^parent, ^ref}}}

    assert_receive {:EXIT, ^pid, :normal}
    assert_received {RouterOut, ^pid, {:terminate, :normal}}
    assert_received {RouterIn, ^pid, {:terminate, :normal}}

    refute_received {:"$gen_route", _, _}
    refute_received {_, ^pid, _}
  end

  test "Out/In handle_demand/3 with Out stop and dispatch" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}

    ref = make_ref()
    parent = self()
    Process.flag(:trap_exit, true)
    GenRouter.ask(pid, parent, ref, 11)
    assert_receive {RouterOut, ^pid, {:demand, 11, {^parent, ^ref}}}
    assert_receive {:"$gen_route", {^pid, ^ref}, [{RouterOut, 11, ^ref}]}

    assert_receive {:EXIT, ^pid, :normal}
    assert_received {RouterOut, ^pid, {:terminate, :normal}}
    assert_received {RouterIn, ^pid, {:terminate, :normal}}

    refute_received {:"$gen_route", _, _}
    refute_received {_, ^pid, _}
  end

  test "Out/In handle_demand/3 Out demand stops with bad_return_value" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}

    assert capture_log(fn() ->
      ref = make_ref()
      parent = self()
      Process.flag(:trap_exit, true)
      GenRouter.ask(pid, parent, ref, 12)
      assert_receive {RouterOut, ^pid, {:demand, 12, {^parent, ^ref}}}
      assert_receive {:EXIT, ^pid, {:bad_return_value, :baddy}}
    end) =~ "bad return value: :baddy"
    assert_received {RouterOut, ^pid, {:terminate, {:bad_return_value, :baddy}}}
    assert_received {RouterIn, ^pid, {:terminate, {:bad_return_value, :baddy}}}

    refute_received {:"$gen_route", _, _}
    refute_received {_, ^pid, _}
  end

  test "Out/In handle_demand/3 Out demand throws causing :nocatch error" do
    {:ok, pid} = GenRouter.start_link(RouterIn, {:ok, self()}, RouterOut, {:ok, self()})
    assert_received {RouterOut, ^pid, :init}
    assert_received {RouterIn, ^pid, :init}

    assert capture_log(fn() ->
      ref = make_ref()
      parent = self()
      Process.flag(:trap_exit, true)
      GenRouter.ask(pid, parent, ref, 13)
      assert_receive {RouterOut, ^pid, {:demand, 13, {^parent, ^ref}}}
      assert_receive {:EXIT, ^pid, {{:nocatch, "oops"}, [_|_]}}
    end) =~ "{:nocatch, \"oops\"}"
    assert_received {RouterOut, ^pid, {:terminate, {{:nocatch, "oops"}, _}}}
    assert_received {RouterIn, ^pid, {:terminate, {{:nocatch, "oops"}, _}}}

    refute_received {:"$gen_route", _, _}
    refute_received {_, ^pid, _}
  end

end
