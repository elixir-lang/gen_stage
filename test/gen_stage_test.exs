defmodule GenStageTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  defmodule Counter do
    @moduledoc """
    A producer that works as a counter in batches.
    It also supports events to be queued via sync
    and async calls. A negative counter disables
    the counting behaviour.
    """

    use GenStage

    def start_link(init, opts \\ []) do
      GenStage.start_link(__MODULE__, init, opts)
    end

    def sync_queue(stage, events) do
      GenStage.call(stage, {:queue, events})
    end

    def async_queue(stage, events) do
      GenStage.cast(stage, {:queue, events})
    end

    def stop(stage) do
      GenStage.call(stage, :stop)
    end

    ## Callbacks

    def init(init) do
      init
    end

    def handle_call(:stop, _from, state) do
      {:stop, :shutdown, :ok, state}
    end

    def handle_call({:early_reply_queue, events}, from, state) do
      GenStage.reply(from, state)
      {:noreply, events, state}
    end

    def handle_call({:queue, events}, _from, state) do
      {:reply, state, events, state}
    end

    def handle_cast({:queue, events}, state) do
      {:noreply, events, state}
    end

    def handle_info({:queue, events}, state) do
      {:noreply, events, state}
    end

    def handle_demand(demand, -1) when demand > 0 do
      {:noreply, [], -1}
    end

    def handle_demand(demand, counter) when demand > 0 do
      # If the counter is 3 and we ask for 2 items, we will
      # emit the items 3 and 4, and set the state to 5.
      events = Enum.to_list(counter..counter+demand-1)
      {:noreply, events, counter + demand}
    end
  end

  defmodule Forwarder do
    @moduledoc """
    A consumer that forwards messages to the given process.
    """

    use GenStage

    def start(init, opts \\ []) do
      GenStage.start(__MODULE__, init, opts)
    end

    def start_link(init, opts \\ []) do
      GenStage.start_link(__MODULE__, init, opts)
    end

    def ask(forwarder, to, n) do
      GenStage.call(forwarder, {:ask, to, n})
    end

    def init(init) do
      init
    end

    def handle_call({:ask, to, n}, _, state) do
      GenStage.ask(to, n)
      {:reply, :ok, [], state}
    end

    def handle_subscribe(opts, from, recipient) do
      send recipient, {:subscribe, from}
      {Keyword.get(opts, :demand, :automatic), recipient}
    end

    def handle_events(events, _from, recipient) do
      send recipient, {:consumed, events}
      {:noreply, [], recipient}
    end

    def handle_cancel(reason, _from, recipient) do
      send recipient, {:cancel, reason}
      {:noreply, recipient}
    end

    def terminate(reason, state) do
      send state, {:terminate, reason}
    end
  end

  describe "producer-to-consumer demand" do
    test "with default max and min demand" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, _} = Forwarder.start_link({:consumer, self(), subscribe_to: [producer]})

      batch = Enum.to_list(0..99)
      assert_receive {:consumed, ^batch}
      batch = Enum.to_list(100..199)
      assert_receive {:consumed, ^batch}
    end

    test "with 1 max and 0 min demand" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      :ok = GenStage.async_subscribe(consumer, to: producer, max_demand: 1, min_demand: 0)

      assert_receive {:consumed, [0]}
      assert_receive {:consumed, [1]}
      assert_receive {:consumed, [2]}
    end

    test "with shared (broadcast) demand" do
      {:ok, producer} = Counter.start_link({:producer, 0, dispatcher: GenStage.BroadcastDispatcher})
      {:ok, consumer1} = Forwarder.start_link({:consumer, self()})
      {:ok, consumer2} = Forwarder.start_link({:consumer, self()})

      :ok = GenStage.async_subscribe(consumer1, to: producer, max_demand: 10, min_demand: 0)
      :ok = GenStage.async_subscribe(consumer2, to: producer, max_demand: 20, min_demand: 0)

      # Because there is a race condition between subscriptions
      # we will assert for events just later on.
      assert_receive {:consumed, [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009]}
      assert_receive {:consumed, [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009]}
    end
  end

  describe "buffer" do
    test "stores events when there is no demand" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      send producer, {:queue, [:a, :b, :c]}
      Counter.async_queue(producer, [:d, :e])

      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      :ok = GenStage.async_subscribe(consumer, to: producer, max_demand: 4, min_demand: 0)

      assert_receive {:consumed, [:a, :b, :c, :d]}
      assert_receive {:consumed, [:e]}
      assert_receive {:consumed, [0, 1, 2]}
      assert_receive {:consumed, [3, 4, 5, 6]}
    end

    test "stores events when there is manual demand" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      :ok = GenStage.async_subscribe(consumer, to: producer, demand: :manual)

      assert_receive {:subscribe, sub}
      Forwarder.ask(consumer, sub, 50)
      batch = Enum.to_list(0..49)
      assert_receive {:consumed, ^batch}
      Forwarder.ask(consumer, sub, 50)
      batch = Enum.to_list(50..99)
      assert_receive {:consumed, ^batch}
    end

  test "stores events when there is manual demand on init" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self(), subscribe_to: [{producer, demand: :manual}]})

      assert_receive {:subscribe, sub}
      Forwarder.ask(consumer, sub, 50)
      batch = Enum.to_list(0..49)
      assert_receive {:consumed, ^batch}
      Forwarder.ask(consumer, sub, 50)
      batch = Enum.to_list(50..99)
      assert_receive {:consumed, ^batch}
    end

    test "emits warning if trying to emit events from a consumer" do
      {:ok, consumer} = Counter.start_link({:consumer, 0})

      assert capture_log(fn ->
        0 = Counter.sync_queue(consumer, [:f, :g, :h])
      end) =~ "GenStage consumer cannot dispatch events"
    end

    test "emits warning and keeps first when it exceeds configured size" do
      {:ok, producer} = Counter.start_link({:producer, 0, buffer_size: 5, buffer_keep: :first})
      0 = Counter.sync_queue(producer, [:a, :b, :c, :d, :e])

      assert capture_log(fn ->
        0 = Counter.sync_queue(producer, [:f, :g, :h])
      end) =~ "GenStage producer has discarded 3 events from buffer"

      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      :ok = GenStage.async_subscribe(consumer, to: producer, max_demand: 4, min_demand: 0)
      assert_receive {:consumed, [:a, :b, :c, :d]}
      assert_receive {:consumed, [:e]}
    end

    test "emits warning and keeps last when it exceeds configured size" do
      {:ok, producer} = Counter.start_link({:producer, 0, buffer_size: 5})
      0 = Counter.sync_queue(producer, [:a, :b, :c, :d, :e])

      assert capture_log(fn ->
        0 = Counter.sync_queue(producer, [:f, :g, :h])
      end) =~ "GenStage producer has discarded 3 events from buffer"

      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      :ok = GenStage.async_subscribe(consumer, to: producer, max_demand: 4, min_demand: 0)
      assert_receive {:consumed, [:d, :e, :f, :g]}
      assert_receive {:consumed, [:h]}
    end
  end

  describe "sync_subscribe" do
    test "returns ok with reference" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      assert {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer)
      assert is_reference(ref)
    end

    @tag :capture_log
    test "returns errors on bad options" do
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      assert {:error, {:bad_opts, message}} =
             GenStage.sync_subscribe(consumer, to: :whatever, max_demand: 0)
      assert message == "expected :max_demand to be equal to or greater than 1, got: 0"

      assert {:error, {:bad_opts, message}} =
             GenStage.sync_subscribe(consumer, to: :whatever, min_demand: 200)
      assert message == "expected :min_demand to be equal to or less than 99, got: 200"
    end

    @tag :capture_log
    test "consumer exits when there is no named producer and subscription is permanent" do
      Process.flag(:trap_exit, true)
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      assert {:ok, _} = GenStage.sync_subscribe(consumer, to: :unknown)
      assert_receive {:EXIT, ^consumer, :noproc}
    end

    @tag :capture_log
    test "consumer exits when producer is dead and subscription is permanent" do
      Process.flag(:trap_exit, true)
      {:ok, producer} = Counter.start_link({:producer, 0})
      GenStage.stop(producer)
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      assert {:ok, _} = GenStage.sync_subscribe(consumer, to: producer)
      assert_receive {:EXIT, ^consumer, :noproc}
    end

    @tag :capture_log
    test "consumer does not exit when there is no named producer and subscription is temporary" do
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      assert {:ok, _} = GenStage.sync_subscribe(consumer, to: :unknown, cancel: :temporary)
    end

    @tag :capture_log
    test "consumer does not exit when producer is dead and subscription is persistent is temporary" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      GenStage.stop(producer)
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      assert {:ok, _} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
    end

    test "caller exits when the consumer is dead" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      GenStage.stop(consumer)
      assert {:noproc, _} = catch_exit(GenStage.sync_subscribe(consumer, to: producer))
    end
  end

  describe "producer callbacks" do
    test "init/1", context do
      Process.flag(:trap_exit, true)
      assert Counter.start_link(:ignore) == :ignore

      assert Counter.start_link({:stop, :oops}) == {:error, :oops}
      assert_receive {:EXIT, _, :oops}
      assert Counter.start_link(:unknown) == {:error, {:bad_return_value, :unknown}}
      assert_receive {:EXIT, _, {:bad_return_value, :unknown}}

      assert Counter.start_link({:producer, 0, buffer_size: -1}) ==
             {:error, {:bad_opts, "expected :buffer_size to be equal to or greater than 0, got: -1"}}

      assert Counter.start_link({:producer, 0, unknown: :value}) ==
             {:error, {:bad_opts, "unknown options [unknown: :value]"}}

      assert {:ok, pid} =
             Counter.start_link({:producer, 0}, name: context.test)
      assert {:error, {:already_started, ^pid}} =
             Counter.start_link({:producer, 0}, name: context.test)
    end

    test "handle_call/3 sends events before reply" do
      {:ok, producer} = Counter.start_link({:producer, -1})

      # Subscribe
      stage_ref = make_ref()
      send producer, {:"$gen_producer", {self(), stage_ref}, {:subscribe, []}}
      send producer, {:"$gen_producer", {self(), stage_ref}, {:ask, 3}}

      # Emulate a call
      call_ref = make_ref()
      send producer, {:"$gen_call", {self(), call_ref}, {:queue, [1, 2, 3]}}

      # Do a blocking call
      GenStage.stop(producer)

      {:messages, messages} = Process.info(self(), :messages)
      assert messages == [
        {:"$gen_consumer", {producer, stage_ref}, [1, 2, 3]},
        {call_ref, -1},
      ]
    end

    test "handle_call/3 allows replies before sending events" do
      {:ok, producer} = Counter.start_link({:producer, -1})

      # Subscribe
      stage_ref = make_ref()
      send producer, {:"$gen_producer", {self(), stage_ref}, {:subscribe, []}}
      send producer, {:"$gen_producer", {self(), stage_ref}, {:ask, 3}}

      # Emulate a call
      call_ref = make_ref()
      send producer, {:"$gen_call", {self(), call_ref}, {:early_reply_queue, [1, 2, 3]}}

      # Do a blocking call
      GenStage.stop(producer)

      {:messages, messages} = Process.info(self(), :messages)
      assert messages == [
        {call_ref, -1},
        {:"$gen_consumer", {producer, stage_ref}, [1, 2, 3]},
      ]
    end

    test "handle_call/3 may shut stage down" do
      Process.flag(:trap_exit, true)
      {:ok, producer} = Counter.start_link({:producer, -1})
      assert Counter.stop(producer) == :ok
      assert_receive {:EXIT, ^producer, :shutdown}
    end
  end

  # TODO: Test third-party cancellation

  describe "consumer callbacks" do
    test "init/1", context do
      Process.flag(:trap_exit, true)
      assert Forwarder.start_link(:ignore) == :ignore

      assert Forwarder.start_link({:stop, :oops}) == {:error, :oops}
      assert_receive {:EXIT, _, :oops}
      assert Forwarder.start_link(:unknown) == {:error, {:bad_return_value, :unknown}}
      assert_receive {:EXIT, _, {:bad_return_value, :unknown}}

      assert Forwarder.start_link({:consumer, self(), unknown: :value}) ==
             {:error, {:bad_opts, "unknown options [unknown: :value]"}}

      assert {:ok, pid} =
             Forwarder.start_link({:consumer, self()}, name: context.test)
      assert {:error, {:already_started, ^pid}} =
             Forwarder.start_link({:consumer, self()}, name: context.test)
    end

    test "terminate/1" do
      {:ok, pid} = Forwarder.start_link({:consumer, self()})
      :ok = GenStage.stop(pid)
      assert_receive {:terminate, :normal}
    end
  end
end
