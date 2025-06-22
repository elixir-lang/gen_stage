defmodule GenStage.PartitionDispatcherTest do
  use ExUnit.Case, async: true

  alias GenStage.PartitionDispatcher, as: D

  defp dispatcher(opts) do
    {:ok, state} = D.init(opts)
    state
  end

  defp waiting_and_pending({_, _, waiting, pending, _, _, _}) do
    {waiting, pending}
  end

  test "subscribes, asks and cancels" do
    pid = self()
    ref = make_ref()
    disp = dispatcher(partitions: 2)

    # Subscribe, ask and cancel and leave some demand
    {:ok, 0, disp} = D.subscribe([partition: 0], {pid, ref}, disp)
    {:ok, 10, disp} = D.ask(10, {pid, ref}, disp)
    assert {10, 0} = waiting_and_pending(disp)
    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert {10, 10} = waiting_and_pending(disp)

    # Subscribe again and the same demand is back
    {:ok, 0, disp} = D.subscribe([partition: 1], {pid, ref}, disp)
    {:ok, 0, disp} = D.ask(5, {pid, ref}, disp)
    assert {10, 5} = waiting_and_pending(disp)
    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert {10, 10} = waiting_and_pending(disp)

    # Now attempt to dispatch with no consumers
    {:ok, [], disp} = D.dispatch([0, 1, 2, 3], 4, disp)
    assert {6, 10} = waiting_and_pending(disp)
  end

  test "subscribes, asks and dispatches" do
    pid = self()
    ref = make_ref()
    disp = dispatcher(partitions: 1)
    {:ok, 0, disp} = D.subscribe([partition: 0], {pid, ref}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    {:ok, [], disp} = D.dispatch([1], 1, disp)
    assert {2, 0} = waiting_and_pending(disp)
    assert_received {:"$gen_consumer", {_, ^ref}, [1]}

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    assert {5, 0} = waiting_and_pending(disp)

    {:ok, [9, 11], disp} = D.dispatch([2, 5, 6, 7, 8, 9, 11], 7, disp)
    assert {0, 0} = waiting_and_pending(disp)
    assert_received {:"$gen_consumer", {_, ^ref}, [2, 5, 6, 7, 8]}
  end

  test "subscribes, asks and dispatches to custom partitions" do
    pid = self()
    ref = make_ref()

    hash_fun = fn event ->
      {event, if(rem(event, 2) == 0, do: :even, else: :odd)}
    end

    disp = dispatcher(partitions: [:odd, :even], hash: hash_fun)

    {:ok, 0, disp} = D.subscribe([partition: :odd], {pid, ref}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    {:ok, [], disp} = D.dispatch([1], 1, disp)
    assert {2, 0} = waiting_and_pending(disp)
    assert_received {:"$gen_consumer", {_, ^ref}, [1]}

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    assert {5, 0} = waiting_and_pending(disp)

    {:ok, [15, 17], disp} = D.dispatch([5, 7, 9, 11, 13, 15, 17], 7, disp)
    assert {0, 0} = waiting_and_pending(disp)
    assert_received {:"$gen_consumer", {_, ^ref}, [5, 7, 9, 11, 13]}
  end

  test "subscribes, asks and dispatches to partitions or none" do
    pid = self()
    even_ref = make_ref()
    odd_ref = make_ref()

    hash_fun = fn event ->
      cond do
        rem(event, 3) == 0 -> :none
        rem(event, 2) == 0 -> {event, :even}
        true -> {event, :odd}
      end
    end

    disp = dispatcher(partitions: [:odd, :even], hash: hash_fun)

    {:ok, 0, disp} = D.subscribe([partition: :even], {pid, even_ref}, disp)
    {:ok, 0, disp} = D.subscribe([partition: :odd], {pid, odd_ref}, disp)

    {:ok, 4, disp} = D.ask(4, {pid, even_ref}, disp)
    {:ok, 4, disp} = D.ask(4, {pid, odd_ref}, disp)
    {:ok, [12], disp} = D.dispatch([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 12, disp)

    assert_received {:"$gen_consumer", {_, ^even_ref}, [2, 4, 8, 10]}
    assert_received {:"$gen_consumer", {_, ^odd_ref}, [1, 5, 7, 11]}
    assert {0, 0} = waiting_and_pending(disp)
  end

  test "buffers events before subscription" do
    disp = dispatcher(partitions: 2)

    # Use one subscription to queue
    pid = self()
    ref = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 1], {pid, ref}, disp)

    {:ok, 5, disp} = D.ask(5, {pid, ref}, disp)
    {:ok, [], disp} = D.dispatch([1, 2, 5, 6, 7], 5, disp)
    assert {0, 0} = waiting_and_pending(disp)
    refute_received {:"$gen_consumer", {_, ^ref}, _}

    {:ok, [8, 9], disp} = D.dispatch([8, 9], 2, disp)
    assert {0, 0} = waiting_and_pending(disp)

    # Use another subscription to get events back
    pid = self()
    ref = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 0], {pid, ref}, disp)
    {:ok, 5, disp} = D.ask(5, {pid, ref}, disp)
    assert {5, 0} = waiting_and_pending(disp)
    assert_received {:"$gen_consumer", {_, ^ref}, [1, 2, 5, 6, 7]}

    {:ok, [], disp} = D.dispatch([1, 2], 2, disp)
    assert {3, 0} = waiting_and_pending(disp)
  end

  test "buffers events after subscription" do
    disp = dispatcher(partitions: 2)

    pid0 = self()
    ref0 = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 0], {pid0, ref0}, disp)
    {:ok, 5, disp} = D.ask(5, {pid0, ref0}, disp)
    assert {5, 0} = waiting_and_pending(disp)

    pid1 = self()
    ref1 = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 1], {pid1, ref1}, disp)
    {:ok, 5, disp} = D.ask(5, {pid1, ref1}, disp)
    assert {10, 0} = waiting_and_pending(disp)

    # Send all events to the same partition, half of them will be buffered
    {:ok, [], disp} = D.dispatch([1, 2], 2, disp)
    {:ok, [], disp} = D.dispatch([5, 6, 7, 1, 2, 5, 6, 7], 8, disp)
    assert {0, 0} = waiting_and_pending(disp)
    assert_received {:"$gen_consumer", {_, ^ref0}, [1, 2]}
    assert_received {:"$gen_consumer", {_, ^ref0}, [5, 6, 7]}

    {:ok, 5, disp} = D.ask(5, {pid0, ref0}, disp)
    assert {5, 0} = waiting_and_pending(disp)
    assert_received {:"$gen_consumer", {_, ^ref0}, [1, 2, 5, 6, 7]}
  end

  test "subscribes, asks and cancels with buffer" do
    disp = dispatcher(partitions: 2)

    pid1 = self()
    ref1 = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 1], {pid1, ref1}, disp)
    {:ok, 5, disp} = D.ask(5, {pid1, ref1}, disp)
    assert {5, 0} = waiting_and_pending(disp)

    pid0 = self()
    ref0 = make_ref()
    {:ok, 0, disp} = D.subscribe([partition: 0], {pid0, ref0}, disp)
    {:ok, [], disp} = D.dispatch([1, 2, 5, 6, 7], 5, disp)
    assert {0, 0} = waiting_and_pending(disp)
    refute_received {:"$gen_consumer", {_, ^ref0}, _}

    # The notification should not count as an event
    {:ok, disp} = D.info(:hello, disp)
    {:ok, 5, disp} = D.cancel({pid0, ref0}, disp)
    assert {5, 0} = waiting_and_pending(disp)
    assert_received :hello
  end

  test "delivers info to current process" do
    pid0 = self()
    ref0 = make_ref()
    pid1 = self()
    ref1 = make_ref()
    disp = dispatcher(partitions: 2)

    {:ok, 0, disp} = D.subscribe([partition: 0], {pid0, ref0}, disp)
    {:ok, 0, disp} = D.subscribe([partition: 1], {pid1, ref1}, disp)
    {:ok, 3, disp} = D.ask(3, {pid1, ref1}, disp)

    {:ok, notify_disp} = D.info(:hello, disp)
    assert disp == notify_disp
    assert_received :hello
  end

  test "does not queue info for non-existing consumers" do
    disp = dispatcher(partitions: 2)
    D.info(:hello, disp)
    assert_received :hello
  end

  test "queues info to backed up consumers" do
    pid0 = self()
    ref0 = make_ref()
    pid1 = self()
    ref1 = make_ref()
    disp = dispatcher(partitions: 2)

    {:ok, 0, disp} = D.subscribe([partition: 0], {pid0, ref0}, disp)
    {:ok, 0, disp} = D.subscribe([partition: 1], {pid0, ref1}, disp)
    {:ok, 3, disp} = D.ask(3, {pid1, ref1}, disp)
    {:ok, [], disp} = D.dispatch([1, 2, 5], 3, disp)

    {:ok, disp} = D.info(:hello, disp)
    refute_received :hello

    {:ok, 5, _} = D.ask(5, {pid0, ref0}, disp)
    assert_received :hello
  end

  test "raises on unknown partition" do
    pid = self()
    ref = make_ref()
    disp = dispatcher(partitions: [:foo, :bar], hash: &{&1, :oops})

    {:ok, 0, disp} = D.subscribe([partition: :foo], {pid, ref}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)

    assert_raise RuntimeError, ~r/unknown partition :oops/, fn ->
      D.dispatch([1, 2, 5], 3, disp)
    end
  end

  test "errors if the :hash function returns a bad value" do
    pid = self()
    ref = make_ref()
    disp = dispatcher(partitions: [:foo, :bar], hash: fn _ -> :not_a_tuple end)
    {:ok, 0, disp} = D.subscribe([partition: :foo], {pid, ref}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)

    assert_raise RuntimeError, ~r/the :hash function should return/, fn ->
      D.dispatch([1, 2, 5], 3, disp)
    end
  end

  test "errors on init" do
    assert_raise ArgumentError, ~r/the enumerable of :partitions is required/, fn ->
      dispatcher([])
    end

    assert_raise ArgumentError, ~r/when :partitions contains partitions/, fn ->
      dispatcher(partitions: [:even, :odd])
    end
  end

  test "errors on subscribe" do
    pid = self()
    ref = make_ref()
    disp = dispatcher(partitions: 2)

    assert_raise ArgumentError, ~r/the :partition option is required when subscribing/, fn ->
      D.subscribe([], {pid, ref}, disp)
    end

    assert_raise ArgumentError, ~r/the partition 0 is already taken by/, fn ->
      {:ok, _, disp} = D.subscribe([partition: 0], {pid, ref}, disp)
      D.subscribe([partition: 0], {pid, ref}, disp)
    end

    assert_raise ArgumentError, ~r/:partition must be one of \[0, 1]/, fn ->
      D.subscribe([partition: -1], {pid, ref}, disp)
    end

    assert_raise ArgumentError, ~r/:partition must be one of \[0, 1]/, fn ->
      D.subscribe([partition: 2], {pid, ref}, disp)
    end

    assert_raise ArgumentError, ~r/:partition must be one of \[0, 1]/, fn ->
      D.subscribe([partition: :oops], {pid, ref}, disp)
    end
  end
end
