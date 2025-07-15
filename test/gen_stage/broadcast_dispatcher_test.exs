defmodule GenStage.BroadcastDispatcherTest do
  use ExUnit.Case, async: true

  alias GenStage.BroadcastDispatcher, as: D

  defp dispatcher(opts) do
    {:ok, {[], 0, 0, _subscribers} = state} = D.init(opts)
    state
  end

  test "subscribes and cancels" do
    pid = self()
    ref = make_ref()
    disp = dispatcher([])
    expected_subscribers = MapSet.new([pid])

    {:ok, 0, disp} = D.subscribe([], {pid, ref}, disp)
    assert disp == {[{0, pid, ref, nil}], 0, 0, expected_subscribers}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert disp == {[], 0, 0, MapSet.new()}
  end

  test "subscribes, asks, and cancels" do
    pid = self()
    ref = make_ref()
    disp = dispatcher([])
    expected_subscribers = MapSet.new([pid])

    {:ok, 0, disp} = D.subscribe([], {pid, ref}, disp)
    assert disp == {[{0, pid, ref, nil}], 0, 0, expected_subscribers}

    {:ok, 10, disp} = D.ask(10, {pid, ref}, disp)
    assert disp == {[{0, pid, ref, nil}], 10, 10, expected_subscribers}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert disp == {[], 0, 10, MapSet.new()}

    # Now attempt to dispatch with no consumers
    {:ok, [1, 2, 3], disp} = D.dispatch([1, 2, 3], 3, disp)
    assert disp == {[], 0, 10, MapSet.new()}
  end

  test "multiple subscriptions with early demand" do
    pid1 = self()
    pid2 = spawn(fn -> :ok end)
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])

    expected_subscribers = MapSet.new([pid1])

    {:ok, 0, disp} = D.subscribe([], {pid1, ref1}, disp)
    assert disp == {[{0, pid1, ref1, nil}], 0, 0, expected_subscribers}

    {:ok, 10, disp} = D.ask(10, {pid1, ref1}, disp)
    assert disp == {[{0, pid1, ref1, nil}], 10, 10, expected_subscribers}

    expected_subscribers = MapSet.put(expected_subscribers, pid2)

    {:ok, 0, disp} = D.subscribe([], {pid2, ref2}, disp)
    assert disp == {[{0, pid2, ref2, nil}, {10, pid1, ref1, nil}], 0, 10, expected_subscribers}

    expected_subscribers = MapSet.delete(expected_subscribers, pid1)

    {:ok, 0, disp} = D.cancel({pid1, ref1}, disp)
    assert disp == {[{0, pid2, ref2, nil}], 0, 10, expected_subscribers}

    {:ok, 0, disp} = D.ask(10, {pid2, ref2}, disp)
    assert disp == {[{0, pid2, ref2, nil}], 10, 10, expected_subscribers}
  end

  test "multiple subscriptions with late demand" do
    pid1 = self()
    pid2 = spawn_forwarder()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])

    expected_subscribers = MapSet.new([pid1])

    {:ok, 0, disp} = D.subscribe([], {pid1, ref1}, disp)
    assert disp == {[{0, pid1, ref1, nil}], 0, 0, expected_subscribers}

    expected_subscribers = MapSet.put(expected_subscribers, pid2)

    {:ok, 0, disp} = D.subscribe([], {pid2, ref2}, disp)
    assert disp == {[{0, pid2, ref2, nil}, {0, pid1, ref1, nil}], 0, 0, expected_subscribers}

    {:ok, 0, disp} = D.ask(10, {pid1, ref1}, disp)
    assert disp == {[{10, pid1, ref1, nil}, {0, pid2, ref2, nil}], 0, 0, expected_subscribers}

    expected_subscribers = MapSet.delete(expected_subscribers, pid2)

    {:ok, 10, disp} = D.cancel({pid2, ref2}, disp)
    assert disp == {[{0, pid1, ref1, nil}], 10, 10, expected_subscribers}

    {:ok, 10, disp} = D.ask(10, {pid1, ref1}, disp)
    assert disp == {[{0, pid1, ref1, nil}], 20, 20, expected_subscribers}
  end

  test "subscribes, asks and dispatches to multiple consumers" do
    pid1 = spawn_forwarder()
    pid2 = spawn_forwarder()
    pid3 = spawn_forwarder()
    ref1 = make_ref()
    ref2 = make_ref()
    ref3 = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe([], {pid1, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([], {pid2, ref2}, disp)

    {:ok, 0, disp} = D.ask(3, {pid1, ref1}, disp)
    {:ok, 2, disp} = D.ask(2, {pid2, ref2}, disp)

    expected_subscribers = MapSet.new([pid1, pid2])

    assert disp == {[{0, pid2, ref2, nil}, {1, pid1, ref1, nil}], 2, 2, expected_subscribers}

    # One batch fits all
    {:ok, [], disp} = D.dispatch([:a, :b], 2, disp)
    assert disp == {[{0, pid2, ref2, nil}, {1, pid1, ref1, nil}], 0, 0, expected_subscribers}

    assert_receive {:"$gen_consumer", {_, ^ref1}, [:a, :b]}
    assert_receive {:"$gen_consumer", {_, ^ref2}, [:a, :b]}

    # A batch with left-over
    {:ok, 1, disp} = D.ask(2, {pid2, ref2}, disp)

    {:ok, [:d], disp} = D.dispatch([:c, :d], 2, disp)
    assert disp == {[{1, pid2, ref2, nil}, {0, pid1, ref1, nil}], 0, 0, expected_subscribers}
    assert_receive {:"$gen_consumer", {_, ^ref1}, [:c]}
    assert_receive {:"$gen_consumer", {_, ^ref2}, [:c]}

    # A batch with no demand
    {:ok, [:d], disp} = D.dispatch([:d], 1, disp)
    assert disp == {[{1, pid2, ref2, nil}, {0, pid1, ref1, nil}], 0, 0, expected_subscribers}
    refute_received {:"$gen_consumer", {_, _}, _}

    # Add a late subscriber
    {:ok, 1, disp} = D.ask(1, {pid1, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([], {pid3, ref3}, disp)
    {:ok, [:d, :e], disp} = D.dispatch([:d, :e], 2, disp)

    expected_subscribers = MapSet.put(expected_subscribers, pid3)

    assert disp ==
             {[{0, pid3, ref3, nil}, {1, pid1, ref1, nil}, {1, pid2, ref2, nil}], 0, 1,
              expected_subscribers}

    # Even out
    {:ok, 0, disp} = D.ask(2, {pid1, ref1}, disp)
    {:ok, 0, disp} = D.ask(2, {pid2, ref2}, disp)
    {:ok, 2, disp} = D.ask(3, {pid3, ref3}, disp)
    {:ok, [], disp} = D.dispatch([:d, :e, :f], 3, disp)

    assert disp ==
             {[{0, pid3, ref3, nil}, {0, pid2, ref2, nil}, {0, pid1, ref1, nil}], 0, 0,
              expected_subscribers}

    assert_receive {:"$gen_consumer", {_, ^ref1}, [:d, :e, :f]}
    assert_receive {:"$gen_consumer", {_, ^ref2}, [:d, :e, :f]}
    assert_receive {:"$gen_consumer", {_, ^ref3}, [:d, :e, :f]}
  end

  test "subscribes, asks, dispatches, and repeats" do
    pid1 = spawn_forwarder()
    pid2 = spawn_forwarder()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe([], {pid1, ref1}, disp)
    expected_subscribers = MapSet.new([pid1])
    assert disp == {[{0, pid1, ref1, nil}], 0, 0, expected_subscribers}

    {:ok, 10, disp} = D.ask(10, {pid1, ref1}, disp)
    assert disp == {[{0, pid1, ref1, nil}], 10, 10, expected_subscribers}

    {:ok, [], disp} = D.dispatch([:a, :b, :c], 3, disp)
    assert disp == {[{0, pid1, ref1, nil}], 7, 7, expected_subscribers}

    assert_receive {:"$gen_consumer", {_, ^ref1}, [:a, :b, :c]}

    {:ok, 0, disp} = D.subscribe([], {pid2, ref2}, disp)
    expected_subscribers = MapSet.put(expected_subscribers, pid2)
    assert disp == {[{0, pid2, ref2, nil}, {7, pid1, ref1, nil}], 0, 7, expected_subscribers}

    {:ok, 0, disp} = D.ask(20, {pid2, ref2}, disp)
    assert disp == {[{13, pid2, ref2, nil}, {0, pid1, ref1, nil}], 7, 7, expected_subscribers}

    {:ok, [], disp} = D.dispatch([:d, :e, :f, :g, :h, :i, :j], 7, disp)
    assert disp == {[{13, pid2, ref2, nil}, {0, pid1, ref1, nil}], 0, 0, expected_subscribers}

    assert_receive {:"$gen_consumer", {_, ^ref1}, [:d, :e, :f, :g, :h, :i, :j]}
    assert_receive {:"$gen_consumer", {_, ^ref2}, [:d, :e, :f, :g, :h, :i, :j]}
  end

  test "subscribes, asks, cancels, and dispatcher reuses events for another subscriber" do
    pid1 = spawn_forwarder()
    pid2 = spawn_forwarder()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe([], {pid1, ref1}, disp)
    expected_subscribers = MapSet.new([pid1])
    assert disp == {[{0, pid1, ref1, nil}], 0, 0, expected_subscribers}

    {:ok, 10, disp} = D.ask(10, {pid1, ref1}, disp)
    assert disp == {[{0, pid1, ref1, nil}], 10, 10, expected_subscribers}

    {:ok, 0, disp} = D.cancel({pid1, ref1}, disp)
    expected_subscribers = MapSet.delete(expected_subscribers, pid1)
    assert disp == {[], 0, 10, expected_subscribers}

    {:ok, 0, disp} = D.subscribe([], {pid2, ref2}, disp)
    expected_subscribers = MapSet.put(expected_subscribers, pid2)
    assert disp == {[{0, pid2, ref2, nil}], 0, 10, expected_subscribers}

    {:ok, 0, disp} = D.ask(5, {pid2, ref2}, disp)
    assert disp == {[{0, pid2, ref2, nil}], 5, 10, expected_subscribers}

    {:ok, [], disp} = D.dispatch([:a, :b, :c], 3, disp)
    assert disp == {[{0, pid2, ref2, nil}], 2, 7, expected_subscribers}

    assert_receive {:"$gen_consumer", {_, ^ref2}, [:a, :b, :c]}
  end

  test "cancels blocking subscriber while there is already a requested demand" do
    pid1 = spawn_forwarder()
    pid2 = spawn_forwarder()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe([], {pid1, ref1}, disp)
    expected_subscribers = MapSet.new([pid1])
    assert disp == {[{0, pid1, ref1, nil}], 0, 0, expected_subscribers}

    {:ok, 10, disp} = D.ask(10, {pid1, ref1}, disp)
    assert disp == {[{0, pid1, ref1, nil}], 10, 10, expected_subscribers}

    {:ok, 0, disp} = D.subscribe([], {pid2, ref2}, disp)
    expected_subscribers = MapSet.put(expected_subscribers, pid2)
    assert disp == {[{0, pid2, ref2, nil}, {10, pid1, ref1, nil}], 0, 10, expected_subscribers}

    {:ok, 0, disp} = D.ask(5, {pid1, ref1}, disp)
    assert disp == {[{15, pid1, ref1, nil}, {0, pid2, ref2, nil}], 0, 10, expected_subscribers}

    {:ok, 5, disp} = D.cancel({pid2, ref2}, disp)
    expected_subscribers = MapSet.delete(expected_subscribers, pid2)
    assert disp == {[{0, pid1, ref1, nil}], 15, 15, expected_subscribers}
  end

  test "subscribing with a selector function" do
    pid1 = spawn_forwarder()
    pid2 = spawn_forwarder()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])
    selector1 = fn %{key: key} -> String.starts_with?(key, "pre") end
    selector2 = fn %{key: key} -> String.starts_with?(key, "pref") end

    {:ok, 0, disp} = D.subscribe([selector: selector1], {pid1, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([selector: selector2], {pid2, ref2}, disp)
    assert {[{0, ^pid2, ^ref2, _selector2}, {0, ^pid1, ^ref1, _selector1}], 0, 0, _} = disp

    {:ok, 0, disp} = D.ask(4, {pid2, ref2}, disp)
    {:ok, 4, disp} = D.ask(4, {pid1, ref1}, disp)

    events = [%{key: "pref-1234"}, %{key: "pref-5678"}, %{key: "pre0000"}, %{key: "foo0000"}]
    {:ok, [], _disp} = D.dispatch(events, 4, disp)

    assert_receive {:"$gen_producer", {_, ^ref1}, {:ask, 1}}
    assert_receive {:"$gen_producer", {_, ^ref2}, {:ask, 2}}

    assert_receive {:"$gen_consumer", {_, ^ref1},
                    [%{key: "pref-1234"}, %{key: "pref-5678"}, %{key: "pre0000"}]}

    assert_receive {:"$gen_consumer", {_, ^ref2}, [%{key: "pref-1234"}, %{key: "pref-5678"}]}
  end

  test "delivers info to current process" do
    pid1 = spawn_forwarder()
    pid2 = spawn_forwarder()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe([], {pid1, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([], {pid2, ref2}, disp)
    {:ok, 0, disp} = D.ask(3, {pid1, ref1}, disp)

    {:ok, notify_disp} = D.info(:hello, disp)
    assert disp == notify_disp
    assert_receive :hello
  end

  test "subscribing is idempotent" do
    pid = self()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])
    expected_subscribers = MapSet.new([pid])

    {:ok, 0, disp} = D.subscribe([], {pid, ref1}, disp)

    assert ExUnit.CaptureLog.capture_log(fn ->
             assert {:error, _} = D.subscribe([], {pid, ref2}, disp)
             assert disp == {[{0, pid, ref1, nil}], 0, 0, expected_subscribers}
           end) =~ "already registered"
  end

  defp spawn_forwarder do
    parent = self()

    spawn_link(fn -> forwarder_loop(parent) end)
  end

  defp forwarder_loop(parent) do
    receive do
      msg ->
        send(parent, msg)
        forwarder_loop(parent)
    end
  end
end
