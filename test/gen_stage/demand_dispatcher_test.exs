defmodule GenStage.DemandDispatcherTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  alias GenStage.DemandDispatcher, as: D

  defp dispatcher(opts) do
    {:ok, {[], nil} = state} = D.init(opts)
    state
  end

  test "subscribes and cancels" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe({pid, ref}, disp)
    assert disp == {[], nil}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert disp == {[], nil}
  end

  test "subscribes, asks and cancels" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe({pid, ref}, disp)
    assert disp == {[], nil}

    {:ok, 10, disp} = D.ask(10, {pid, ref}, disp)
    assert disp == {[{10, pid, ref}], 10}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert disp == {[], 10}
  end

  test "subscribes, asks and dispatches" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher([])
    {:ok, 0, disp} = D.subscribe({pid, ref}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    assert disp == {[{3, pid, ref}], 3}

    {:ok, [], disp} = D.dispatch([:a], disp)
    assert disp == {[{2, pid, ref}], 3}
    assert_received {:"$gen_consumer", {_, ^ref}, [:a]}

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    assert disp == {[{5, pid, ref}], 3}

    {:ok, [:g, :h], disp} = D.dispatch([:b, :c, :d, :e, :f, :g, :h], disp)
    assert disp == {[{0, pid, ref}], 3}
    assert_received {:"$gen_consumer", {_, ^ref}, [:b, :c, :d, :e, :f]}

    {:ok, [:i, :j], disp} = D.dispatch([:i, :j], disp)
    assert disp == {[{0, pid, ref}], 3}
    refute_received {:"$gen_consumer", {_, ^ref}, _}
  end

  test "subscribes, asks multiple consumers" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    ref3 = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe({pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe({pid, ref2}, disp)
    {:ok, 0, disp} = D.subscribe({pid, ref3}, disp)

    {:ok, 4, disp} = D.ask(4, {pid, ref1}, disp)
    {:ok, 2, disp} = D.ask(2, {pid, ref2}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref3}, disp)
    assert disp == {[{4, pid, ref1}, {3, pid, ref3}, {2, pid, ref2}], 4}

    {:ok, 2, disp} = D.ask(2, {pid, ref3}, disp)
    assert disp == {[{5, pid, ref3}, {4, pid, ref1}, {2, pid, ref2}], 4}

    {:ok, 4, disp} = D.ask(4, {pid, ref2}, disp)
    assert disp == {[{6, pid, ref2}, {5, pid, ref3}, {4, pid, ref1}], 4}
  end

  test "subscribes, asks and dispatches to multiple consumers" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe({pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe({pid, ref2}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
    {:ok, 2, disp} = D.ask(2, {pid, ref2}, disp)
    assert disp == {[{3, pid, ref1}, {2, pid, ref2}], 3}

    # One batch fits all
    {:ok, [], disp} = D.dispatch([:a, :b, :c, :d, :e], disp)
    assert disp == {[{0, pid, ref1}, {0, pid, ref2}], 3}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:a, :b, :c]}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:d, :e]}

    {:ok, [:a, :b, :c], disp} = D.dispatch([:a, :b, :c], disp)
    assert disp == {[{0, pid, ref1}, {0, pid, ref2}], 3}
    refute_received {:"$gen_consumer", {_, _}, _}

    # Two batches with left over
    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref2}, disp)
    assert disp == {[{3, pid, ref1}, {3, pid, ref2}], 3}

    {:ok, [], disp} = D.dispatch([:a, :b], disp)
    assert disp == {[{3, pid, ref2}, {1, pid, ref1}], 3}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:a, :b]}

    {:ok, [], disp} = D.dispatch([:c, :d], disp)
    assert disp == {[{1, pid, ref1}, {1, pid, ref2}], 3}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:c, :d]}

    # Eliminate the left-over
    {:ok, [:g], disp} = D.dispatch([:e, :f, :g], disp)
    assert disp == {[{0, pid, ref1}, {0, pid, ref2}], 3}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:e]}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:f]}
  end

  test "warns with demand mismatch" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe({pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe({pid, ref2}, disp)

    assert capture_log(fn ->
      {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
      {:ok, 4, disp} = D.ask(4, {pid, ref2}, disp)
      disp
    end) =~ "GenStage producer DemandDispatcher expects a maximum demand of 3"
  end
end
