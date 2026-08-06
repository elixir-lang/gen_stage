alias Experimental.GenStage

defmodule GenStage.FairDispatcherTest do
  use ExUnit.Case, async: true

  alias GenStage.FairDispatcher, as: D

  defp dispatcher(opts) do
    {:ok, {_, %{}, 0, 0, _} = state} = D.init(opts)
    state
  end

  test "subscribes and cancels" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe([], {pid, ref}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref]
    assert rest == {%{ref => {pid, 0}}, 0, 0, 100}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    {queue, rest} = split(disp)
    assert queue == []
    assert rest == {%{}, 0, 0, 100}
  end

  test "subscribes, asks and cancels" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher([])

    # Subscribe, ask and cancel and leave some demand
    {:ok, 0, disp} = D.subscribe([], {pid, ref}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref]
    assert rest == {%{ref => {pid, 0}}, 0, 0, 100}

    {:ok, 10, disp} = D.ask(10, {pid, ref}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref]
    assert rest == {%{ref => {pid, 10}}, 10, 0, 100}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    {queue, rest} = split(disp)
    assert queue == []
    assert rest == {%{}, 0, 10, 100}

    # Subscribe, ask and cancel and leave the same demand
    {:ok, 0, disp} = D.subscribe([], {pid, ref}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref]
    assert rest == {%{ref => {pid, 0}}, 0, 10, 100}

    {:ok, 0, disp} = D.ask(5, {pid, ref}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref]
    assert rest == {%{ref => {pid, 5}}, 5, 5, 100}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    {queue, rest} = split(disp)
    assert queue == []
    assert rest == {%{}, 0, 10, 100}
  end

  test "subscribes, asks and dispatches" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher([])
    {:ok, 0, disp} = D.subscribe([], {pid, ref}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref]
    assert rest == {%{ref => {pid, 3}}, 3, 0, 100}

    {:ok, [], disp} = D.dispatch([:a], disp)
    {queue, rest} = split(disp)
    assert queue == [ref]
    assert rest == {%{ref => {pid, 2}}, 2, 0, 100}
    assert_received {:"$gen_consumer", {_, ^ref}, [:a]}

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref]
    assert rest == {%{ref => {pid, 5}}, 5, 0, 100}

    {:ok, [:g, :h], disp} = D.dispatch([:b, :c, :d, :e, :f, :g, :h], disp)
    {queue, rest} = split(disp)
    assert queue == [ref]
    assert rest == {%{ref => {pid, 0}}, 0, 0, 100}
    assert_received {:"$gen_consumer", {_, ^ref}, [:b, :c, :d, :e, :f]}

    {:ok, [:i, :j], disp} = D.dispatch([:i, :j], disp)
    {queue, rest} = split(disp)
    assert queue == [ref]
    assert rest == {%{ref => {pid, 0}}, 0, 0, 100}
    refute_received {:"$gen_consumer", {_, ^ref}, _}
  end

  test "subscribes, asks multiple consumers" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    ref3 = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe([], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([], {pid, ref2}, disp)
    {:ok, 0, disp} = D.subscribe([], {pid, ref3}, disp)

    {:ok, 4, disp} = D.ask(4, {pid, ref1}, disp)
    {:ok, 2, disp} = D.ask(2, {pid, ref2}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref3}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref1, ref2, ref3]
    assert rest == {%{ref1 => {pid, 4}, ref2 => {pid, 2}, ref3 => {pid, 3}},
                    9, 0, 100}

    {:ok, 2, disp} = D.ask(2, {pid, ref3}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref1, ref2, ref3]
    assert rest == {%{ref1 => {pid, 4}, ref2 => {pid, 2}, ref3 => {pid, 5}},
                    11, 0, 100}

    {:ok, 4, disp} = D.ask(4, {pid, ref2}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref1, ref2, ref3]
    assert rest == {%{ref1 => {pid, 4}, ref2 => {pid, 6}, ref3 => {pid, 5}},
                    15, 0, 100}
  end

  test "subscribes, asks and dispatches to multiple consumers" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe([], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([], {pid, ref2}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
    {:ok, 2, disp} = D.ask(2, {pid, ref2}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref1, ref2]
    assert rest == {%{ref1 => {pid, 3}, ref2 => {pid, 2}}, 5, 0, 100}

    # One batch fits all
    {:ok, [], disp} = D.dispatch([:a, :b, :c, :d, :e], disp)
    {queue, rest} = split(disp)
    assert queue == [ref1, ref2]
    assert rest == {%{ref1 => {pid, 0}, ref2 => {pid, 0}}, 0, 0, 100}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:a, :b, :c]}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:d, :e]}

    {:ok, [:a, :b, :c], disp} = D.dispatch([:a, :b, :c], disp)
    assert {^queue, ^rest} = split(disp)
    refute_received {:"$gen_consumer", {_, _}, _}

    # Two batches with left over
    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref2}, disp)
    {queue, rest} = split(disp)
    assert queue == [ref1, ref2]
    assert rest == {%{ref1 => {pid, 3}, ref2 => {pid, 3}}, 6, 0, 100}

    {:ok, [], disp} = D.dispatch([:a, :b], disp)
    {queue, rest} = split(disp)
    assert queue == [ref2, ref1]
    assert rest == {%{ref1 => {pid, 1}, ref2 => {pid, 3}}, 4, 0, 100}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:a, :b]}

    {:ok, [], disp} = D.dispatch([:c, :d], disp)
    {queue, rest} = split(disp)
    assert queue == [ref1, ref2]
    assert rest == {%{ref1 => {pid, 1}, ref2 => {pid, 1}}, 2, 0, 100}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:c, :d]}

    # Eliminate the left-over
    {:ok, [:g], disp} = D.dispatch([:e, :f, :g], disp)
    {queue, rest} = split(disp)
    assert queue == [ref1, ref2]
    assert rest == {%{ref1 => {pid, 0}, ref2 => {pid, 0}}, 0, 0, 100}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:e]}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:f]}
  end

  test "delivers notifications to all consumers" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])

    {:ok, 0, disp} = D.subscribe([], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([], {pid, ref2}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)

    {:ok, notify_disp} = D.notify(:hello, disp)
    assert disp == notify_disp

    assert_received {:"$gen_consumer", {_, ^ref1}, {:notification, :hello}}
    assert_received {:"$gen_consumer", {_, ^ref2}, {:notification, :hello}}
  end

  test "batch limits max events per consumer" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([batch: 2])
    {_, rest} = split(disp)
    assert rest == {%{}, 0, 0, 2}

    {:ok, 0, disp} = D.subscribe([], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([], {pid, ref2}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
    {:ok, 4, disp} = D.ask(4, {pid, ref2}, disp)

    {:ok, [], disp} = D.dispatch([:a, :b, :c], disp)
    {queue, rest} = split(disp)
    assert queue == [ref1, ref2]
    assert rest == {%{ref1 => {pid, 1}, ref2 => {pid, 3}}, 4, 0, 2}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:a, :b]}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:c]}
  end

  defp split(disp) do
    {:queue.to_list(elem(disp, 0)), Tuple.delete_at(disp, 0)}
  end
end
