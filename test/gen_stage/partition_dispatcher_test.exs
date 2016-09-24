alias Experimental.GenStage

defmodule GenStage.PartitionDispatcherTest do
  use ExUnit.Case, async: true

  alias GenStage.PartitionDispatcher, as: D

  defp dispatcher(opts) do
    {:ok, state} = D.init(opts)
    state
  end

  test "subscribes, asks and cancels" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher(partitions: 0..1)

    # Subscribe, ask and cancel and leave some demand
    {:ok, 0, disp}  = D.subscribe([partition: 0], {pid, ref}, disp)
    {:ok, 10, disp} = D.ask(10, {pid, ref}, disp)
    assert {_, _, 10, 0, _, _} = disp
    {:ok, 0, disp}  = D.cancel({pid, ref}, disp)
    assert {_, _, 10, 10, _, _} = disp

    # Subscribe again and the same demand is back
    {:ok, 0, disp} = D.subscribe([partition: 1], {pid, ref}, disp)
    {:ok, 0, disp} = D.ask(5, {pid, ref}, disp)
    assert {_, _, 10, 5, _, _} = disp
    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert {_, _, 10, 10, _, _} = disp
  end

  test "subscribes, asks and dispatches" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher(partitions: 0..1)
    {:ok, 0, disp} = D.subscribe([partition: 0], {pid, ref}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    {:ok, [], disp} = D.dispatch([1], disp)
    assert {_, _, 2, 0, _, _} = disp
    assert_received {:"$gen_consumer", {_, ^ref}, [1]}

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    assert {_, _, 5, 0, _, _} = disp

    {:ok, [9, 11], disp} = D.dispatch([2, 5, 6, 7, 8, 9, 11], disp)
    assert {_, _, 0, 0, _, _} = disp
    assert_received {:"$gen_consumer", {_, ^ref}, [2, 5, 6, 7, 8]}
  end

  test "buffers events before subscription" do
    disp = dispatcher(partitions: 0..1)

    # Use one subscription to queue
    pid = self()
    ref = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 1], {pid, ref}, disp)

    {:ok, 5, disp} = D.ask(5, {pid, ref}, disp)
    {:ok, [], disp} = D.dispatch([1, 2, 5, 6, 7], disp)
    assert {_, _, 0, 0, _, _} = disp
    refute_received {:"$gen_consumer", {_, ^ref}, _}

    {:ok, [8, 9], disp} = D.dispatch([8, 9], disp)
    assert {_, _, 0, 0, _, _} = disp

    # Use another subscription to get events back
    pid = self()
    ref = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 0], {pid, ref}, disp)
    {:ok, 5, disp} = D.ask(5, {pid, ref}, disp)
    assert {_, _, 5, 0, _, _} = disp
    assert_received {:"$gen_consumer", {_, ^ref}, [1, 2, 5, 6, 7]}

    {:ok, [], disp} = D.dispatch([1, 2], disp)
    assert {_, _, 3, 0, _, _} = disp
  end

  test "buffers events after subscription" do
    disp = dispatcher(partitions: 0..1)

    pid0 = self()
    ref0 = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 0], {pid0, ref0}, disp)
    {:ok, 5, disp} = D.ask(5, {pid0, ref0}, disp)
    assert {_, _, 5, 0, _, _} = disp

    pid1 = self()
    ref1 = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 1], {pid1, ref1}, disp)
    {:ok, 5, disp} = D.ask(5, {pid1, ref1}, disp)
    assert {_, _, 10, 0, _, _} = disp

    # Send all events to the same partition, half of them will be buffered
    {:ok, [], disp} = D.dispatch([1, 2], disp)
    {:ok, [], disp} = D.dispatch([5, 6, 7, 1, 2, 5, 6, 7], disp)
    assert {_, _, 0, 0, _, _} = disp
    assert_received {:"$gen_consumer", {_, ^ref0}, [1, 2]}
    assert_received {:"$gen_consumer", {_, ^ref0}, [5, 6, 7]}

    {:ok, 5, disp} = D.ask(5, {pid0, ref0}, disp)
    assert {_, _, 5, 0, _, _} = disp
    assert_received {:"$gen_consumer", {_, ^ref0}, [1, 2, 5, 6, 7]}
  end

  test "subscribes, asks and cancels with buffer" do
    disp = dispatcher(partitions: 0..1)

    pid1 = self()
    ref1 = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 1], {pid1, ref1}, disp)
    {:ok, 5, disp} = D.ask(5, {pid1, ref1}, disp)
    assert {_, _, 5, 0, _, _} = disp

    pid0 = self()
    ref0 = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 0], {pid0, ref0}, disp)
    {:ok, [], disp} = D.dispatch([1, 2, 5, 6, 7], disp)
    assert {_, _, 0, 0, _, _} = disp
    refute_received {:"$gen_consumer", {_, ^ref0}, _}

    # The notification should not count as an event
    {:ok, disp} = D.notify(:hello, disp)
    {:ok, 5, disp} = D.cancel({pid0, ref0}, disp)
    assert {_, _, 5, 0, _, _} = disp
  end

  test "delivers notifications to all consumers" do
    pid0  = self()
    ref0 = make_ref()
    pid1  = self()
    ref1 = make_ref()
    disp = dispatcher(partitions: 0..1)

    {:ok, 0, disp} = D.subscribe([partition: 0], {pid0, ref0}, disp)
    {:ok, 0, disp} = D.subscribe([partition: 1], {pid1, ref1}, disp)
    {:ok, 3, disp} = D.ask(3, {pid1, ref1}, disp)

    {:ok, notify_disp} = D.notify(:hello, disp)
    assert disp == notify_disp

    assert_received {:"$gen_consumer", {_, ^ref0}, {:notification, :hello}}
    assert_received {:"$gen_consumer", {_, ^ref1}, {:notification, :hello}}
  end

  test "queues notifications for non-existing consumers" do
    pid0 = self()
    ref0 = make_ref()
    pid1 = self()
    ref1 = make_ref()
    disp = dispatcher(partitions: 0..1)

    {:ok, disp} = D.notify(:hello, disp)
    refute_received {:"$gen_consumer", {_, ^ref0}, {:notification, :hello}}
    refute_received {:"$gen_consumer", {_, ^ref1}, {:notification, :hello}}

    {:ok, 0, disp}  = D.subscribe([partition: 0], {pid0, ref0}, disp)
    {:ok, 0, disp}  = D.subscribe([partition: 1], {pid0, ref1}, disp)
    {:ok, 3, disp}  = D.ask(3, {pid0, ref0}, disp)
    {:ok, 3, disp}  = D.ask(3, {pid1, ref1}, disp)
    _ = disp

    assert Process.info(self(), :messages) == {:messages, [
      {:"$gen_consumer", {pid0, ref0}, {:notification, :hello}},
      {:"$gen_consumer", {pid1, ref1}, {:notification, :hello}}
    ]}
  end

  test "queues notifications to backed up consumers" do
    pid0 = self()
    ref0 = make_ref()
    pid1 = self()
    ref1 = make_ref()
    disp = dispatcher(partitions: 0..1)

    {:ok, 0, disp}  = D.subscribe([partition: 0], {pid0, ref0}, disp)
    {:ok, 0, disp}  = D.subscribe([partition: 1], {pid0, ref1}, disp)
    {:ok, 3, disp}  = D.ask(3, {pid1, ref1}, disp)
    {:ok, [], disp} = D.dispatch([1, 2, 5], disp)

    {:ok, disp} = D.notify(:hello, disp)
    refute_received {:"$gen_consumer", {_, ^ref0}, {:notification, :hello}}
    assert_received {:"$gen_consumer", {_, ^ref1}, {:notification, :hello}}

    {:ok, 5, _}  = D.ask(5, {pid0, ref0}, disp)

    assert Process.info(self(), :messages) == {:messages, [
      {:"$gen_consumer", {self(), ref0}, [1, 2, 5]},
      {:"$gen_consumer", {self(), ref0}, {:notification, :hello}}
    ]}
  end

  test "errors on init" do
    assert_raise ArgumentError, ~r/the enumerable of :partitions is required/, fn ->
      dispatcher([])
    end
  end

  test "errors on subscribe" do
    pid = self()
    ref = make_ref()
    disp = dispatcher([partitions: 0..1])

    assert_raise ArgumentError, ~r/the :partition option is required when subscribing/, fn ->
      D.subscribe([], {pid, ref}, disp)
    end

    assert_raise ArgumentError, ~r/the partition 0 is already taken by/, fn ->
      {:ok, _, disp} = D.subscribe([partition: 0], {pid, ref}, disp)
      D.subscribe([partition: 0], {pid, ref}, disp)
    end

    assert_raise ArgumentError, ~r/:partition must be one of 0, 1/, fn ->
      D.subscribe([partition: -1], {pid, ref}, disp)
    end

    assert_raise ArgumentError, ~r/:partition must be one of 0, 1/, fn ->
      D.subscribe([partition: 2], {pid, ref}, disp)
    end

    assert_raise ArgumentError, ~r/:partition must be one of 0, 1/, fn ->
      D.subscribe([partition: :oops], {pid, ref}, disp)
    end
  end
end
