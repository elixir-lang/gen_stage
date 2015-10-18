defmodule GenRouterTest do
  use ExUnit.Case, async: true

  alias TestRouter, as: TR
  alias TestAgent, as: TA

  import ExUnit.CaptureLog

  test "start_link/2 workflow with unregistered name" do
    stack = [{:ok, :state}]
    {:ok, in_agent} = TA.start_link(stack)
    {:ok, out_agent} = TA.start_link(stack)
    {:ok, router} = TR.start_link(in_agent, out_agent)

    {:links, links} = Process.info(self, :links)
    assert router in links

    assert TA.record(in_agent) == [{:init, [in_agent]}]
    assert TA.record(out_agent) == [{:init, [out_agent]}]

    assert {GenRouter, _, _} = :proc_lib.translate_initial_call(router)
  end

  test "start/2 worflow with registered name" do
    stack = [{:ok, :state}]
    {:ok, in_agent} = TA.start_link(stack)
    {:ok, out_agent} = TA.start_link(stack)
    {:ok, router} = TR.start(in_agent, out_agent, [name: :router])

    {:links, links} = Process.info(self, :links)
    Process.link(router)
    refute router in links

    assert TA.record(in_agent) == [{:init, [in_agent]}]
    assert TA.record(out_agent) == [{:init, [out_agent]}]

    assert Process.info(router, :registered_name) == {:registered_name, :router}

    assert {GenRouter, _, _} = :proc_lib.translate_initial_call(router)
  end

  test "start/2 returns error on In :stop" do
    {:ok, in_agent} = TA.start_link([{:stop, "oops"}])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])
    assert TR.start(in_agent, out_agent) == {:error, "oops"}

    assert TA.record(in_agent) == [{:init, [in_agent]}]
    assert TA.record(out_agent) ==
      [{:init, [out_agent]},
       {:terminate, ["oops", 1]}]
  end

  test "start/2 returns error on In raise" do
    {:ok, in_agent} = TA.start_link([fn(_) -> raise "oops" end])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])
    assert {:error, reason = {%RuntimeError{}, _}} = TR.start(in_agent, out_agent)

    assert TA.record(in_agent) == [{:init, [in_agent]}]
    assert TA.record(out_agent) ==
      [{:init, [out_agent]},
       {:terminate, [reason, 1]}]
  end

  test "start/2 returns error on In throw" do
    {:ok, in_agent} = TA.start_link([fn(_) -> throw "oops" end])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])
    assert {:error, reason = {{:nocatch, "oops"}, _}} = TR.start(in_agent, out_agent)

    assert TA.record(in_agent) == [{:init, [in_agent]}]
    assert TA.record(out_agent) ==
      [{:init, [out_agent]},
       {:terminate, [reason, 1]}]
  end

  test "start/2 returns error on In exit" do
    {:ok, in_agent} = TA.start_link([fn(_) -> exit "oops" end])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])
    assert {:error, reason = "oops"} = TR.start(in_agent, out_agent)

    assert TA.record(in_agent) == [{:init, [in_agent]}]
    assert TA.record(out_agent) ==
      [{:init, [out_agent]},
       {:terminate, [reason, 1]}]
  end

  test "start/2 returns error on Out :stop" do
    {:ok, in_agent} = TA.start_link([])
    {:ok, out_agent} = TA.start_link([{:stop, "oops"}])
    assert TR.start(in_agent, out_agent) == {:error, "oops"}

    assert TA.record(out_agent) == [{:init, [out_agent]}]
  end

  test "start/2 returns error on Out raise" do
    {:ok, in_agent} = TA.start_link([])
    {:ok, out_agent} = TA.start_link([fn(_) -> raise "oops" end])
    assert {:error, {%RuntimeError{}, _}} = TR.start(in_agent, out_agent)

    assert TA.record(in_agent) == []
    assert TA.record(out_agent) == [{:init, [out_agent]}]
  end

  test "start/2 returns error on Out throw" do
    {:ok, in_agent} = TA.start_link([])
    {:ok, out_agent} = TA.start_link([fn(_) -> throw "oops" end])
    assert {:error, {{:nocatch, "oops"}, _}} = TR.start(in_agent, out_agent)

    assert TA.record(in_agent) == []
    assert TA.record(out_agent) == [{:init, [out_agent]}]
  end

  test "start/2 returns error on Out exit" do
    {:ok, in_agent} = TA.start_link([])
    {:ok, out_agent} = TA.start_link([fn(_) -> exit "oops" end])
    assert {:error, "oops"} = TR.start(in_agent, out_agent)

    assert TA.record(in_agent) == []
    assert TA.record(out_agent) == [{:init, [out_agent]}]
  end

  test "In handle_call/3 sends reply with :reply return" do
    {:ok, in_agent} = TA.start_link([{:ok, 1}, {:reply, :my_reply, 2}, :ok])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])
    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    assert GenRouter.call(router, :hi) == :my_reply
    assert GenRouter.stop(router) == :ok

    caller = self
    assert [init: [^in_agent],
      handle_call: [:hi, {^caller, _}, 1],
      terminate: [:normal, 2]] = TA.record(in_agent)

    assert TA.record(out_agent) ==
      [init: [out_agent],
       terminate: [:normal, 1]]
  end

  test "In handle_call/3 sends reply explicitly" do
    noreply = fn(_, from, state) ->
      GenRouter.reply(from, :my_reply)
      {:noreply, state + 1}
    end
    {:ok, in_agent} = TA.start_link([{:ok, 1}, noreply, :ok])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])
    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    assert GenRouter.call(router, :hi) == :my_reply
    assert GenRouter.stop(router) == :ok

    caller = self
    assert [init: [^in_agent],
       handle_call: [:hi, {^caller, _}, 1],
       terminate: [:normal, 2]] = TA.record(in_agent)

    assert TA.record(out_agent) ==
      [init: [out_agent],
       terminate: [:normal, 1]]
  end

  test "In handle_call/3 stops" do
    {:ok, in_agent} = TA.start_link([{:ok, 1}, {:stop, :normal, 2}, :ok])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])
    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    assert catch_exit(GenRouter.call(router, :hi)) ==
      {:normal, {GenServer, :call, [router, :hi, 5_000]}}

    caller = self
    assert [init: [^in_agent],
       handle_call: [:hi, {^caller, _}, 1],
       terminate: [:normal, 2]] = TA.record(in_agent)

    assert TA.record(out_agent) ==
      [init: [out_agent],
       terminate: [:normal, 1]]
  end

  test "In handle_call/3 stops and replies" do
    {:ok, in_agent} = TA.start_link([{:ok, 1}, {:stop, :normal, :my_reply, 2}, :ok])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])
    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    ref = Process.monitor(router)
    assert GenRouter.call(router, :hi) == :my_reply
    assert_receive {:DOWN, ^ref, :process, ^router, :normal}

    caller = self
    assert [init: [^in_agent],
       handle_call: [:hi, {^caller, _}, 1],
       terminate: [:normal, 2]] = TA.record(in_agent)

    assert TA.record(out_agent) ==
      [init: [out_agent],
       terminate: [:normal, 1]]
  end

  test "In handle_call/3 stops with bad_return_value" do
    {:ok, in_agent} = TA.start_link([{:ok, 1}, "oops", :ok])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])

    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      assert {{:bad_return_value, "oops"}, _} =
        catch_exit(GenRouter.call(router, :hi))
      assert_receive {:EXIT, ^router, {:bad_return_value, "oops"}}
    end) =~ "bad return value: \"oops\""

    caller = self
    assert [init: [^in_agent],
       handle_call: [:hi, {^caller, _}, 1],
       # state doesn't change on bad return
       terminate: [{:bad_return_value, "oops"} = reason, 1]] = TA.record(in_agent)

    assert TA.record(out_agent) ==
      [init: [out_agent],
       terminate: [reason, 1]]
  end

  test "In handle_call/3 throws causing :nocatch error" do
    in_stack = [{:ok, 1}, fn(_, _, _) -> throw "oops" end, :ok]
    {:ok, in_agent} = TA.start_link(in_stack)
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])

    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      assert {{{:nocatch, "oops"}, stack = [_|_]}, _} =
        catch_exit(GenRouter.call(router, :hi))
      assert_receive {:EXIT, ^router, {{:nocatch, "oops"}, ^stack}}
    end) =~ "{:nocatch, \"oops\"}"

    caller = self
    assert [init: [^in_agent],
       handle_call: [:hi, {^caller, _}, 1],
       # state doesn't change on throw
       terminate: [{{:nocatch, "oops"}, [_|_]} = reason, 1]] = TA.record(in_agent)

    assert TA.record(out_agent) ==
      [init: [out_agent],
       terminate: [reason, 1]]
  end

  test "In handle_info/2 returns :noreply" do
    {:ok, in_agent} = TA.start_link([{:ok, 1}, {:noreply, 2}, :ok])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])

    assert {:ok, router} = TR.start_link(in_agent, out_agent)
    send(router, :hello)
    assert GenRouter.stop(router) == :ok

    assert TA.record(in_agent) ==
      [init: [in_agent],
       handle_info: [:hello, 1],
       terminate: [:normal, 2]]

    assert TA.record(out_agent) ==
      [init: [out_agent],
       terminate: [:normal, 1]]
  end

  test "In handle_info/2 stops with bad_return_value" do
    {:ok, in_agent} = TA.start_link([{:ok, 1}, "oops", :ok])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])

    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      send(router, :hello)
      assert_receive {:EXIT, ^router, {:bad_return_value, "oops"}}
    end) =~ "bad return value: \"oops\""

    assert TA.record(in_agent) ==
      [init: [in_agent],
       handle_info: [:hello, 1],
       # state doesn't change on bad return
       terminate: [{:bad_return_value, "oops"}, 1]]

    assert TA.record(out_agent) ==
      [init: [out_agent],
       terminate: [{:bad_return_value, "oops"}, 1]]
  end

  test "In handle_info/2 throws causing :nocatch error" do
    in_stack = [{:ok, 1}, fn(_, _) -> throw "oops" end, :ok]
    {:ok, in_agent} = TA.start_link(in_stack)
    {:ok, out_agent} = TA.start_link([{:ok, 1}, :ok])

    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      send(router, :hello)
      assert_receive {:EXIT, ^router, {{:nocatch, "oops"}, [_|_]}}
    end) =~ "{:nocatch, \"oops\"}"

    assert [init: [^in_agent],
       handle_info: [:hello, 1],
       # state doesn't change on throw
       terminate: [{{:nocatch, "oops"}, [_|_]} = reason, 1]] = TA.record(in_agent)

    assert TA.record(out_agent) ==
      [init: [out_agent],
       terminate: [reason, 1]]
  end

  test "Out handle_demand/3 with 0 demand returned (Out) " do
    ref = make_ref()

    {:ok, in_agent} = TA.start_link([{:ok, 1}, :ok])
    {:ok, out_agent} =
      TA.start_link([{:ok, 1},
        {:ok, 0, 2}, # handle_demand
        {:ok, 0, [], 3}, # handle_demand
        {:ok, 0, [:out_event], 4}, # handle_demand
        {:ok, [ref], 5}, # handle_dispatch
        :ok])
    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    parent = self()
    GenRouter.ask(router, parent, ref, 1)
    GenRouter.ask(router, parent, ref, 2)
    GenRouter.ask(router, parent, ref, 3)
    assert GenRouter.stop(router) == :ok

    assert_received {:"$gen_route", {^router, ^ref}, [:out_event]}
    refute_received {:"$gen_route", _, _}

    caller = self
    assert TA.record(in_agent) ==
      [init: [in_agent],
       terminate: [:normal, 1]]

    assert TA.record(out_agent) ==
      [init: [out_agent],
       handle_demand: [1, {caller, ref}, 1],
       handle_demand: [2, {caller, ref}, 2],
       handle_demand: [3, {caller, ref}, 3],
       handle_dispatch: [:out_event, 4],
       terminate: [:normal, 5]]
  end

  test "Out/In handle_demand/3 with positive demand returned (Out) and dispatch (In) " do
    ref = make_ref()

    {:ok, in_agent} =
      TA.start_link([{:ok, 1}, {:dispatch, [:in_event], 2},
        {:dispatch, [:in_event], 3}, {:dispatch, [:in_event], 4}, :ok])
    {:ok, out_agent} =
      TA.start_link([{:ok, 1},
        {:ok, 1, 2}, # handle_demand
        {:ok, [ref], 3}, # handle_dispatch (:in_event)
        {:ok, 2, [], 4}, # handle_demand
        {:ok, [ref], 5}, # handle_dispatch (:in_event)
        {:ok, 3, [:out_event], 6}, # handle_demand
        {:ok, [ref], 7}, # handle_dispatch (:out_event)
        {:ok, [], 8}, # handle_dispatch (:in_event)
        :ok])

    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    parent = self()
    GenRouter.ask(router, parent, ref, 1)
    assert_receive {:"$gen_route", {^router, ^ref}, [:in_event]}
    GenRouter.ask(router, parent, ref, 2)
    assert_receive {:"$gen_route", {^router, ^ref}, [:in_event]}
    GenRouter.ask(router, parent, ref, 3)
    assert_receive {:"$gen_route", {^router, ^ref}, [:out_event]}

    assert GenRouter.stop(router) == :ok

    refute_received {:"$gen_route", _, _}

    caller = self
    assert TA.record(in_agent) ==
      [init: [in_agent],
       handle_demand: [1, 1],
       handle_demand: [2, 2],
       handle_demand: [3, 3],
       terminate: [:normal, 4]]

    assert TA.record(out_agent) ==
      [init: [out_agent],
       handle_demand: [1, {caller, ref}, 1],
       handle_dispatch: [:in_event, 2],
       handle_demand: [2, {caller, ref}, 3],
       handle_dispatch: [:in_event, 4],
       handle_demand: [3, {caller, ref}, 5],
       handle_dispatch: [:out_event, 6],
       handle_dispatch: [:in_event, 7],
       terminate: [:normal, 8]]
  end

  test "Out handle_demand/3 with demand error (Out)" do
    ref1 = make_ref()
    ref2 = make_ref()

    {:ok, in_agent} = TA.start_link([{:ok, 1}, :ok])
    {:ok, out_agent} =
      TA.start_link([{:ok, 1},
        {:ok, 0, 2}, # handle_demand (ref1)
        {:error, "oops", 3}, # handle_demand (ref2)
        {:error, "oops", [], 4}, # handle_demand (ref2)
        {:error, "oops", [:out_event], 5}, # handle_demand (ref2)
        {:ok, [ref1], 6}, # handle_dispatch
        :ok])
    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    parent = self()
    GenRouter.ask(router, parent, ref1, 1)
    GenRouter.ask(router, parent, ref2, 2)
    GenRouter.ask(router, parent, ref2, 3)
    GenRouter.ask(router, parent, ref2, 4)
    assert GenRouter.stop(router) == :ok

    assert_received {:"$gen_route", {^router, ^ref2}, {:eos, {:error, "oops"}}}
    assert_received {:"$gen_route", {^router, ^ref2}, {:eos, {:error, "oops"}}}
    assert_received {:"$gen_route", {^router, ^ref2}, {:eos, {:error, "oops"}}}
    assert_received {:"$gen_route", {^router, ^ref1}, [:out_event]}
    refute_received {:"$gen_route", _, _}

    caller = self
    assert TA.record(in_agent) ==
      [init: [in_agent],
       terminate: [:normal, 1]]

    assert TA.record(out_agent) ==
      [init: [out_agent],
       handle_demand: [1, {caller, ref1}, 1],
       handle_demand: [2, {caller, ref2}, 2],
       handle_demand: [3, {caller, ref2}, 3],
       handle_demand: [4, {caller, ref2}, 4],
       handle_dispatch: [:out_event, 5],
       terminate: [:normal, 6]]
  end

  test "Out handle_demand/3 with demand error (Out) and fail to dispatch to error stream" do
    ref = make_ref()

    {:ok, in_agent} = TA.start_link([{:ok, 1}, :ok])
    {:ok, out_agent} =
      TA.start_link([{:ok, 1},
        {:ok, 0, 2}, # handle_demand
        {:error, "oops", [:out_event], 3}, # handle_demand
        {:ok, [ref], 4}, # handle_dispatch
        :ok])
    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      parent = self()
      GenRouter.ask(router, parent, ref, 1)
      GenRouter.ask(router, parent, ref, 2)
      assert_receive {:EXIT, ^router, {:bad_reference, ^ref}}
      assert_received {:"$gen_route", {^router, ^ref}, {:eos, {:error, "oops"}}}
    end) =~ "{:bad_reference, #Reference"

    refute_received {:"$gen_route", _, _}

    caller = self
    assert TA.record(in_agent) ==
      [init: [in_agent],
       terminate: [{:bad_reference, ref}, 1]]

    assert TA.record(out_agent) ==
      [init: [out_agent],
       handle_demand: [1, {caller, ref}, 1],
       handle_demand: [2, {caller, ref}, 2],
       handle_dispatch: [:out_event, 3],
       terminate: [{:bad_reference, ref}, 4]]
  end

  test "Out handle_demand/3 with Out demand stop" do
    ref = make_ref()

    {:ok, in_agent} = TA.start_link([{:ok, 1}, :ok])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, {:stop, "oops", 2}, :ok])
    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      parent = self()
      GenRouter.ask(router, parent, ref, 1)
      assert_receive {:EXIT, ^router, "oops"}
    end) =~ "\"oops\""

    refute_received {:"$gen_route", _, _}

    caller = self
    assert TA.record(in_agent) ==
      [init: [in_agent],
       terminate: ["oops", 1]]

    assert TA.record(out_agent) ==
      [init: [out_agent],
       handle_demand: [1, {caller, ref}, 1],
       terminate: ["oops", 2]]
  end

  test "Out/In handle_demand/3 with Out stop and dispatch" do
    ref = make_ref()

    {:ok, in_agent} = TA.start_link([{:ok, 1}, :ok])
    {:ok, out_agent} =
      TA.start_link([{:ok, 1},
        {:stop, "oops", [:out_event], 2}, # handle_demand
        {:ok, [ref], 3}, # handle_dispatch
        :ok])
    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      parent = self()
      GenRouter.ask(router, parent, ref, 1)
      assert_receive {:EXIT, ^router, "oops"}
      assert_received {:"$gen_route", {^router, ^ref}, [:out_event]}
    end) =~ "\"oops\""

    refute_received {:"$gen_route", _, _}

    caller = self
    assert TA.record(in_agent) ==
      [init: [in_agent],
       terminate: ["oops", 1]]

    assert TA.record(out_agent) ==
      [init: [out_agent],
       handle_demand: [1, {caller, ref}, 1],
       handle_dispatch: [:out_event, 2],
       terminate: ["oops", 3]]
  end

  test "Out handle_demand/3 demand stops with bad_return_value" do
    ref = make_ref()

    {:ok, in_agent} = TA.start_link([{:ok, 1}, :ok])
    {:ok, out_agent} = TA.start_link([{:ok, 1}, "oops", :ok])
    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      parent = self()
      GenRouter.ask(router, parent, ref, 1)
      assert_receive {:EXIT, ^router, {:bad_return_value, "oops"}}
    end) =~ "\"oops\""

    refute_received {:"$gen_route", _, _}

    caller = self
    assert TA.record(in_agent) ==
      [init: [in_agent],
       terminate: [{:bad_return_value, "oops"}, 1]]

    assert TA.record(out_agent) ==
      [init: [out_agent],
       handle_demand: [1, {caller, ref}, 1],
       # state doesnt change on bad return
       terminate: [{:bad_return_value, "oops"}, 1]]
  end

  test "Out/In handle_demand/3 Out demand throws causing :nocatch error" do
    ref = make_ref()

    {:ok, in_agent} = TA.start_link([{:ok, 1}, :ok])
    {:ok, out_agent} =
      TA.start_link([{:ok, 1}, fn(_, _, _) -> throw "oops" end, :ok])
    assert {:ok, router} = TR.start_link(in_agent, out_agent)

    Process.flag(:trap_exit, true)
    assert capture_log(fn() ->
      parent = self()
      GenRouter.ask(router, parent, ref, 1)
      assert_receive {:EXIT, ^router, {{:nocatch, "oops"}, [_|_]}}
    end) =~ "{:nocatch, \"oops\"}"

    refute_received {:"$gen_route", _, _}

    caller = self
    assert [init: [^in_agent],
       terminate: [{{:nocatch, "oops"}, [_|_]}, 1]] = TA.record(in_agent)

    assert [init: [^out_agent],
       handle_demand: [1, {^caller, ^ref}, 1],
       # state doesnt change on throw
       terminate: [{{:nocatch, "oops"}, [_|_]}, 1]] = TA.record(out_agent)
  end
end
