alias Experimental.GenStage

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

    def handle_info(other, state) do
      is_pid(state) && send(state, other)
      {:noreply, [], state}
    end

    def handle_subscribe(:consumer, opts, from, state) do
      is_pid(state) && send(state, {:producer_subscribed, from})
      {Keyword.get(opts, :producer_demand, :automatic), state}
    end

    def handle_cancel(reason, from, state) do
      is_pid(state) && send(state, {:producer_cancelled, from, reason})
      {:noreply, [], state}
    end

    def handle_demand(demand, pid) when is_pid(pid) and demand > 0 do
      {:noreply, [], pid}
    end

    def handle_demand(demand, counter) when demand > 0 do
      # If the counter is 3 and we ask for 2 items, we will
      # emit the items 3 and 4, and set the state to 5.
      events = Enum.to_list(counter..counter+demand-1)
      {:noreply, events, counter + demand}
    end
  end

  defmodule Doubler do
    @moduledoc """
    Multiples every event by two.
    """

    use GenStage

    def start_link(init, opts \\ []) do
      GenStage.start_link(__MODULE__, init, opts)
    end

    def init(init) do
      init
    end

    def handle_subscribe(kind, opts, from, recipient) do
      send recipient, {:producer_consumer_subscribed, kind, from}
      {Keyword.get(opts, :producer_consumer_demand, :automatic), recipient}
    end

    def handle_cancel(reason, from, recipient) do
      send recipient, {:producer_consumer_cancelled, from, reason}
      {:noreply, [], recipient}
    end

    def handle_events(events, _from, recipient) do
      send recipient, {:producer_consumed, events}
      {:noreply, Enum.flat_map(events, &[&1, &1]), recipient}
    end
  end

  defmodule Discarder do
    @moduledoc """
    Multiples every event by two.
    """

    use GenStage

    def start_link(init, opts \\ []) do
      GenStage.start_link(__MODULE__, init, opts)
    end

    def init(init) do
      init
    end

    def handle_events(events, _from, recipient) do
      send recipient, {:producer_consumed, events}
      {:noreply, [], recipient}
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

    def handle_subscribe(:producer, opts, from, recipient) do
      send recipient, {:consumer_subscribed, from}
      {Keyword.get(opts, :consumer_demand, :automatic), recipient}
    end

    def handle_info(other, recipient) do
      send(recipient, other)
      {:noreply, [], recipient}
    end

    def handle_events(events, _from, recipient) do
      send recipient, {:consumed, events}
      {:noreply, [], recipient}
    end

    def handle_cancel(reason, from, recipient) do
      send recipient, {:consumer_cancelled, from, reason}
      {:noreply, [], recipient}
    end

    def terminate(reason, state) do
      send state, {:terminated, reason}
    end
  end

  describe "producer-to-consumer demand" do
    test "with default max and min demand" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, _} = Forwarder.start_link({:consumer, self(), subscribe_to: [producer]})

      batch = Enum.to_list(0..499)
      assert_receive {:consumed, ^batch}
      batch = Enum.to_list(500..999)
      assert_receive {:consumed, ^batch}
    end

    test "with 80% min demand" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, _} = Forwarder.start_link({:consumer, self(),
                                       subscribe_to: [{producer, min_demand: 80, max_demand: 100}]})

      batch = Enum.to_list(0..19)
      assert_receive {:consumed, ^batch}
      batch = Enum.to_list(20..39)
      assert_receive {:consumed, ^batch}
      batch = Enum.to_list(1000..1019)
      assert_receive {:consumed, ^batch}
    end

    test "with 20% min demand" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, _} = Forwarder.start_link({:consumer, self(),
                                       subscribe_to: [{producer, min_demand: 20, max_demand: 100}]})

      batch = Enum.to_list(0..79)
      assert_receive {:consumed, ^batch}
      batch = Enum.to_list(80..99)
      assert_receive {:consumed, ^batch}
      batch = Enum.to_list(100..179)
      assert_receive {:consumed, ^batch}
      batch = Enum.to_list(180..259)
      assert_receive {:consumed, ^batch}
      batch = Enum.to_list(260..279)
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

    test "with shared (broadcast) demand and synchronizer subscriber" do
      {:ok, producer} = Counter.start_link({:producer, 0, dispatcher: GenStage.BroadcastDispatcher})
      {:ok, consumer1} = Forwarder.start_link({:consumer, self()})
      {:ok, consumer2} = Forwarder.start_link({:consumer, self()})

      # Subscribe but not demand
      send producer, {:"$gen_producer", {self(), stage_ref = make_ref()}, {:subscribe, []}}

      # Further subscriptions will block
      GenStage.sync_subscribe(consumer1, to: producer, max_demand: 10, min_demand: 0)
      GenStage.sync_subscribe(consumer2, to: producer, max_demand: 20, min_demand: 0)
      refute_received {:consumed, _}

      # Cancel the stale one
      send producer, {:"$gen_producer", {self(), stage_ref}, {:cancel, :killed}}

      # Because there is a race condition between subscriptions
      # we will assert for events just later on.
      assert_receive {:consumed, [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009]}
      assert_receive {:consumed, [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009]}
    end
  end

  describe "producer-producer_consumer-consumer demand" do
    test "with 80% min demand with init subscription" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, doubler} = Doubler.start_link({:producer_consumer, self(),
                                           subscribe_to: [{producer, max_demand: 100, min_demand: 80}]})
      {:ok, _} = Forwarder.start_link({:consumer, self(),
                                       subscribe_to: [{doubler, max_demand: 100, min_demand: 50}]})

      batch = Enum.to_list(0..19)
      assert_receive {:producer_consumed, ^batch}
      batch = Enum.flat_map(0..19, &[&1, &1])
      assert_receive {:consumed, ^batch}
      batch = Enum.flat_map(20..39, &[&1, &1])
      assert_receive {:consumed, ^batch}

      batch = Enum.to_list(100..119)
      assert_receive {:producer_consumed, ^batch}
      batch = Enum.flat_map(120..124, &[&1, &1])
      assert_receive {:consumed, ^batch}
      batch = Enum.flat_map(125..139, &[&1, &1])
      assert_receive {:consumed, ^batch}
    end

    test "with 20% min demand with init subscription" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, doubler} = Doubler.start_link({:producer_consumer, self(),
                                           subscribe_to: [{producer, max_demand: 100, min_demand: 20}]})
      {:ok, _} = Forwarder.start_link({:consumer, self(),
                                       subscribe_to: [{doubler, max_demand: 100, min_demand: 50}]})

      batch = Enum.to_list(0..79)
      assert_receive {:producer_consumed, ^batch}
      batch = Enum.flat_map(0..24, &[&1, &1])
      assert_receive {:consumed, ^batch}
      batch = Enum.flat_map(25..49, &[&1, &1])
      assert_receive {:consumed, ^batch}
      batch = Enum.flat_map(50..74, &[&1, &1])
      assert_receive {:consumed, ^batch}
      batch = Enum.to_list(100..179)
      assert_receive {:producer_consumed, ^batch}
    end

    test "with 80% min demand with late subscription" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, doubler}  = Doubler.start_link({:producer_consumer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})

      # Now let's try consumer first
      GenStage.sync_subscribe(consumer, to: doubler, min_demand: 50, max_demand: 100)
      GenStage.sync_subscribe(doubler, to: producer, min_demand: 80, max_demand: 100)

      batch = Enum.to_list(0..19)
      assert_receive {:producer_consumed, ^batch}
      batch = Enum.flat_map(0..19, &[&1, &1])
      assert_receive {:consumed, ^batch}
      batch = Enum.flat_map(20..39, &[&1, &1])
      assert_receive {:consumed, ^batch}

      batch = Enum.to_list(100..119)
      assert_receive {:producer_consumed, ^batch}
      batch = Enum.flat_map(120..124, &[&1, &1])
      assert_receive {:consumed, ^batch}
      batch = Enum.flat_map(125..139, &[&1, &1])
      assert_receive {:consumed, ^batch}
    end

    test "with 20% min demand with later subscription" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, doubler}  = Doubler.start_link({:producer_consumer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})

      # Now let's try consumer first
      GenStage.sync_subscribe(consumer, to: doubler, min_demand: 50, max_demand: 100)
      GenStage.sync_subscribe(doubler, to: producer, min_demand: 20, max_demand: 100)

      batch = Enum.to_list(0..79)
      assert_receive {:producer_consumed, ^batch}
      batch = Enum.flat_map(0..24, &[&1, &1])
      assert_receive {:consumed, ^batch}
      batch = Enum.flat_map(25..49, &[&1, &1])
      assert_receive {:consumed, ^batch}
      batch = Enum.flat_map(50..74, &[&1, &1])
      assert_receive {:consumed, ^batch}
      batch = Enum.to_list(100..179)
      assert_receive {:producer_consumed, ^batch}
    end

    test "keeps emitting events even when discarded" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, doubler} = Discarder.start_link({:producer_consumer, self(),
                                            subscribe_to: [{producer, max_demand: 100, min_demand: 80}]})
      {:ok, _} = Forwarder.start_link({:consumer, self(),
                                       subscribe_to: [{doubler, max_demand: 100, min_demand: 50}]})

      batch = Enum.to_list(0..19)
      assert_receive {:producer_consumed, ^batch}
      batch = Enum.to_list(100..119)
      assert_receive {:producer_consumed, ^batch}
      batch = Enum.to_list(1000..1019)
      assert_receive {:producer_consumed, ^batch}
    end

    test "with shared (broadcast) demand" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, doubler}  = Doubler.start_link({:producer_consumer, self(),
                                            dispatcher: GenStage.BroadcastDispatcher,
                                            subscribe_to: [producer]})
      {:ok, consumer1} = Forwarder.start_link({:consumer, self()})
      {:ok, consumer2} = Forwarder.start_link({:consumer, self()})

      :ok = GenStage.async_subscribe(consumer1, to: doubler, max_demand: 10, min_demand: 0)
      :ok = GenStage.async_subscribe(consumer2, to: doubler, max_demand: 20, min_demand: 0)

      # Because there is a race condition between subscriptions
      # we will assert for events just later on.
      assert_receive {:consumed, [200, 200, 201, 201, 202, 202, 203, 203, 204, 204]}
      assert_receive {:consumed, [200, 200, 201, 201, 202, 202, 203, 203, 204, 204]}
    end

    test "with shared (broadcast) demand and synchronizer subscriber" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, doubler}  = Doubler.start_link({:producer_consumer, self(),
                                            dispatcher: GenStage.BroadcastDispatcher,
                                            subscribe_to: [producer]})

      {:ok, consumer1} = Forwarder.start_link({:consumer, self()})
      {:ok, consumer2} = Forwarder.start_link({:consumer, self()})

      # Subscribe but not demand
      send doubler, {:"$gen_producer", {self(), stage_ref = make_ref()}, {:subscribe, []}}

      # Further subscriptions will block
      GenStage.sync_subscribe(consumer1, to: doubler, max_demand: 10, min_demand: 0)
      GenStage.sync_subscribe(consumer2, to: doubler, max_demand: 20, min_demand: 0)
      refute_received {:consumed, _}

      # Cancel the stale one
      send doubler, {:"$gen_producer", {self(), stage_ref}, {:cancel, :killed}}

      # Because there is a race condition between subscriptions
      # we will assert for events just later on.
      assert_receive {:consumed, [200, 200, 201, 201, 202, 202, 203, 203, 204, 204]}
      assert_receive {:consumed, [200, 200, 201, 201, 202, 202, 203, 203, 204, 204]}
    end

    test "queued events with lost producer" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, doubler}  = Doubler.start_link({:producer_consumer, self()})

      {:ok, ref} = GenStage.sync_subscribe(doubler, to: producer, cancel: :temporary,
                                           min_demand: 50, max_demand: 100)
      assert_receive {:producer_consumer_subscribed, :producer, {^producer, ^ref}}

      GenStage.cancel({producer, ref}, :done)
      assert_receive {:producer_consumer_cancelled, {^producer, ^ref}, {:cancel, :done}}
      refute_received {:producer_consumed, _}

      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      GenStage.sync_subscribe(consumer, to: doubler, min_demand: 50, max_demand: 100)

      batch = Enum.to_list(0..99)
      assert_receive {:producer_consumed, ^batch}
      batch = Enum.flat_map(0..24, &[&1, &1])
      assert_receive {:consumed, ^batch}
      batch = Enum.flat_map(25..49, &[&1, &1])
      assert_receive {:consumed, ^batch}
      refute_received {:producer_consumed, _}
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

    test "emits warning and keeps first when it exceeds configured size" do
      {:ok, producer} = Counter.start_link({:producer, 0, buffer_size: 5, buffer_keep: :first})
      0 = Counter.sync_queue(producer, [:a, :b, :c, :d, :e])

      assert capture_log(fn ->
        0 = Counter.sync_queue(producer, [:f, :g, :h])
      end) =~ "GenStage producer #{inspect producer} has discarded 3 events from buffer"

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
      end) =~ "GenStage producer #{inspect producer} has discarded 3 events from buffer"

      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      :ok = GenStage.async_subscribe(consumer, to: producer, max_demand: 4, min_demand: 0)
      assert_receive {:consumed, [:d, :e, :f, :g]}
      assert_receive {:consumed, [:h]}
    end

    test "may have limit set to infinity" do
      {:ok, producer} = Counter.start_link({:producer, 0, buffer_size: :infinity})
      0 = Counter.sync_queue(producer, [:a, :b, :c, :d, :e])
      0 = Counter.sync_queue(producer, [:f, :g, :h])

      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      :ok = GenStage.async_subscribe(consumer, to: producer, max_demand: 4, min_demand: 0)
      assert_receive {:consumed, [:a, :b, :c, :d]}
      assert_receive {:consumed, [:e, :f, :g, :h]}
    end
  end

  describe "notifications" do
    test "delivers notifications immediately when there is no buffer" do
      {:ok, producer} = Counter.start_link({:producer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer)

      :ok = GenStage.sync_notify(producer, :sync)
      assert_receive {{_, ^ref}, :sync}

      :ok = GenStage.async_notify(producer, :async)
      assert_receive {{_, ^ref}, :async}
    end

    test "delivers notifications eventually with infinity buffer size" do
      {:ok, producer} = Counter.start_link({:producer, self(), buffer_size: :infinity})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      Counter.sync_queue(producer, [:a, :b, :c])
      GenStage.sync_notify(producer, :done)

      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer)
      assert_receive {:consumed, [:a, :b, :c]}
      assert_receive {{_, ^ref}, :done}
    end

    test "delivers notifications eventually with one-item filled buffer" do
      {:ok, producer} = Counter.start_link({:producer, self(), buffer_size: 3})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      Counter.sync_queue(producer, [:a])
      GenStage.sync_notify(producer, :done)

      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer)
      assert_receive {:consumed, [:a]}
      assert_receive {{_, ^ref}, :done}
    end

    test "delivers notifications eventually with mid filled buffer" do
      {:ok, producer} = Counter.start_link({:producer, self(), buffer_size: 3})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      Counter.sync_queue(producer, [:a, :b])
      GenStage.sync_notify(producer, :done)

      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer)
      assert_receive {:consumed, [:a, :b]}
      assert_receive {{_, ^ref}, :done}
    end

    test "delivers notifications eventually with filled buffer" do
      {:ok, producer} = Counter.start_link({:producer, self(), buffer_size: 3})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      Counter.sync_queue(producer, [:a, :b, :c])
      GenStage.sync_notify(producer, :done)

      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer)
      assert_receive {:consumed, [:a, :b, :c]}
      assert_receive {{_, ^ref}, :done}
    end

    test "delivers notifications eventually with moving buffer" do
      {:ok, producer} = Counter.start_link({:producer, self(), buffer_size: 3})

      # Subscribe
      ref = make_ref()
      send producer, {:"$gen_producer", {self(), ref}, {:subscribe, []}}

      # Queue events and notification
      Counter.sync_queue(producer, [:a, :b, :c])
      GenStage.sync_notify(producer, :done1)

      # Ask for event and notification
      send producer, {:"$gen_producer", {self(), ref}, {:ask, 2}}
      assert_receive {:"$gen_consumer", {_, ^ref}, [:a, :b]}
      refute_received {{_, ^ref}, :done1}

      # Queue more events and another notification
      Counter.sync_queue(producer, [:d, :e])
      GenStage.sync_notify(producer, :done2)

      # Ask the remaining events and notifications
      send producer, {:"$gen_producer", {self(), ref}, {:ask, 1}}
      send producer, {:"$gen_producer", {self(), ref}, {:ask, 2}}
      assert_receive {:"$gen_consumer", {_, ^ref}, [:c]}
      assert_receive {:"$gen_consumer", {_, ^ref}, {:notification, :done1}}
      assert_receive {:"$gen_consumer", {_, ^ref}, [:d, :e]}
      assert_receive {:"$gen_consumer", {_, ^ref}, {:notification, :done2}}
    end

    @tag :capture_log
    test "delivers notifications eventually when dropping buffer" do
      {:ok, producer} = Counter.start_link({:producer, self(), buffer_keep: :last, buffer_size: 3})
      Counter.sync_queue(producer, [:a, :b])
      GenStage.sync_notify(producer, :done)

      ref = make_ref()
      send producer, {:"$gen_producer", {self(), ref}, {:subscribe, []}}
      Counter.sync_queue(producer, [:c, :d, :e])
      assert_receive {:"$gen_consumer", {_, ^ref}, {:notification, :done}}
    end
  end

  describe "sync_subscribe/2" do
    test "returns ok with reference" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      assert {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer)
      assert is_reference(ref)
    end

    test "caller exits when the consumer is dead" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      GenStage.stop(consumer)
      assert {:noproc, _} = catch_exit(GenStage.sync_subscribe(consumer, to: producer))
    end

    @tag :capture_log
    test "returns errors on bad options" do
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      assert {:error, {:bad_opts, message}} =
             GenStage.sync_subscribe(consumer, to: :whatever, max_demand: 0)
      assert message == "expected :max_demand to be equal to or greater than 1, got: 0"

      assert {:error, {:bad_opts, message}} =
             GenStage.sync_subscribe(consumer, to: :whatever, min_demand: 2000)
      assert message == "expected :min_demand to be equal to or less than 999, got: 2000"
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

    test "consumer does not exit when there is no named producer and subscription is temporary" do
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      assert {:ok, _} = GenStage.sync_subscribe(consumer, to: :unknown, cancel: :temporary)
    end

    test "consumer does not exit when producer is dead and subscription is temporary" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      GenStage.stop(producer)
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      assert {:ok, _} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
    end
  end

  describe "manual demand" do
    test "can be set on subscribe" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      :ok = GenStage.async_subscribe(consumer, to: producer, consumer_demand: :manual)

      assert_receive {:consumer_subscribed, sub}
      Forwarder.ask(consumer, sub, 50)
      batch = Enum.to_list(0..49)
      assert_receive {:consumed, ^batch}
      Forwarder.ask(consumer, sub, 50)
      batch = Enum.to_list(50..99)
      assert_receive {:consumed, ^batch}
    end

    test "can be set on subscribe on init" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      subscribe_to = [{producer, consumer_demand: :manual}]
      {:ok, consumer} = Forwarder.start_link({:consumer, self(), subscribe_to: subscribe_to})

      assert_receive {:consumer_subscribed, sub}
      Forwarder.ask(consumer, sub, 50)
      batch = Enum.to_list(0..49)
      assert_receive {:consumed, ^batch}
      Forwarder.ask(consumer, sub, 50)
      batch = Enum.to_list(50..99)
      assert_receive {:consumed, ^batch}
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

      assert Counter.start_link({:producer, 0, dispatcher: 0}) ==
             {:error, {:bad_opts, "expected :dispatcher to be an atom or a {atom, list}, got: 0"}}

      assert Counter.start_link({:producer, 0, unknown: :value}) ==
             {:error, {:bad_opts, "unknown options [unknown: :value]"}}

      assert {:ok, pid} =
             Counter.start_link({:producer, 0}, name: context.test)
      assert {:error, {:already_started, ^pid}} =
             Counter.start_link({:producer, 0}, name: context.test)
    end

    test "handle_subscribe/4" do
      {:ok, producer} = Counter.start_link({:producer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer)
      assert_receive {:producer_subscribed, {^consumer, ^ref}}
    end

    @tag :capture_log
    test "handle_subscribe/4 does not accept manual demand" do
      Process.flag(:trap_exit, true)
      {:ok, producer} = Counter.start_link({:producer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, producer_demand: :manual)
      assert_receive {:producer_subscribed, {^consumer, ^ref}}
      assert_receive {:EXIT, ^producer, {:bad_return_value, {:manual, pid}}} when pid == self()
    end

    test "handle_cancel/3" do
      {:ok, producer} = Counter.start_link({:producer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
      assert_receive {:producer_subscribed, {^consumer, ^ref}}
      GenStage.cancel({producer, ref}, :oops)
      assert_receive {:producer_cancelled, {^consumer, ^ref}, {:cancel, :oops}}
    end

    test "handle_cancel/3 on consumer down" do
      {:ok, producer} = Counter.start_link({:producer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
      assert_receive {:producer_subscribed, {^consumer, ^ref}}
      Process.unlink(consumer)
      Process.exit(consumer, :kill)
      assert_receive {:producer_cancelled, {^consumer, ^ref}, {:down, :killed}}
    end

    test "handle_call/3 sends events before reply" do
      {:ok, producer} = Counter.start_link({:producer, self()})

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
        {:producer_subscribed, {self(), stage_ref}},
        {:"$gen_consumer", {producer, stage_ref}, [1, 2, 3]},
        {call_ref, self()},
      ]
    end

    test "handle_call/3 allows replies before sending events" do
      {:ok, producer} = Counter.start_link({:producer, self()})

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
        {:producer_subscribed, {self(), stage_ref}},
        {call_ref, self()},
        {:"$gen_consumer", {producer, stage_ref}, [1, 2, 3]},
      ]
    end

    test "handle_call/3 may shut stage down" do
      Process.flag(:trap_exit, true)
      {:ok, producer} = Counter.start_link({:producer, -1})
      assert Counter.stop(producer) == :ok
      assert_receive {:EXIT, ^producer, :shutdown}
    end

    test "handle_info/2 is called for unmatched down messages" do
      {:ok, consumer} = Counter.start_link({:producer, self()})
      ref = make_ref()
      send consumer, {:DOWN, ref, :process, self(), :oops}
      assert_receive {:DOWN, ^ref, :process, pid, :oops} when pid == self()
    end

    test "terminate/2" do
      {:ok, pid} = Forwarder.start_link({:producer, self()})
      :ok = GenStage.stop(pid)
      assert_receive {:terminated, :normal}
    end

    test "format_status/2" do
      {:ok, producer} = Counter.start_link({:producer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self(), subscribe_to: [producer]})

      assert {:status, _, _, [_, _, _, _, [header: _, data: _, data: data]]} = :sys.get_status(producer)
      assert data == [{'State', self()}, {'Stage', :producer}, {'Dispatcher', GenStage.DemandDispatcher},
                      {'Consumers', [consumer]}, {'Buffer size', 0}]
    end
  end

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

    test "handle_subscribe/4" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer)
      assert_receive {:consumer_subscribed, {^producer, ^ref}}
    end

    @tag :capture_log
    test "handle_subscribe/4 with invalid demand" do
      Process.flag(:trap_exit, true)
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      GenStage.async_subscribe(consumer, to: producer, consumer_demand: :unknown)
      assert_receive {:EXIT, ^consumer, {:bad_return_value, {:unknown, pid}}} when pid == self()
    end

    test "handle_cancel/3 with temporary subscription" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
      GenStage.cancel({producer, ref}, :oops)
      assert_receive {:consumer_cancelled, {^producer, ^ref}, {:cancel, :oops}}
    end

    @tag :capture_log
    test "handle_cancel/3 with permanent subscription" do
      Process.flag(:trap_exit, true)
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :permanent)
      GenStage.cancel({producer, ref}, self())
      assert_receive {:consumer_cancelled, {^producer, ^ref}, {:cancel, pid}} when pid == self()
      assert_receive {:EXIT, ^consumer, pid} when pid == self()
    end

    test "handle_cancel/3 on producer down with temporary subscription" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
      Process.unlink(producer)
      Process.exit(producer, :kill)
      assert_receive {:consumer_cancelled, {^producer, ^ref}, {:down, :killed}}
    end

    @tag :capture_log
    test "handle_cancel/3 on producer down with permanent subscription" do
      Process.flag(:trap_exit, true)
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :permanent)
      Process.unlink(producer)
      Process.exit(producer, :kill)
      assert_receive {:consumer_cancelled, {^producer, ^ref}, {:down, :killed}}
      assert_receive {:EXIT, ^consumer, :killed}
    end

    test "handle_info/2 is called for unmatched down messages" do
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      ref = make_ref()
      send consumer, {:DOWN, ref, :process, self(), :oops}
      assert_receive {:DOWN, ^ref, :process, pid, :oops} when pid == self()
    end

    test "terminate/1" do
      {:ok, pid} = Forwarder.start_link({:consumer, self()})
      :ok = GenStage.stop(pid)
      assert_receive {:terminated, :normal}
    end

    test "emit warning if trying to dispatch events from a consumer" do
      {:ok, consumer} = Counter.start_link({:consumer, 0}, name: :gen_stage_error)

      assert capture_log(fn ->
        0 = Counter.sync_queue(consumer, [:f, :g, :h])
      end) =~ "GenStage consumer :gen_stage_error cannot dispatch events"
    end

    test "format_status/2" do
      {:ok, producer} = Counter.start_link({:producer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self(), subscribe_to: [producer]})

      assert {:status, _, _, [_, _, _, _, [header: _, data: _, data: data]]} = :sys.get_status(consumer)
      assert data == [{'State', self()}, {'Stage', :consumer}, {'Producers', [producer]}]
    end
  end

  describe "producer_consumer callbacks" do
    test "init/1", context do
      Process.flag(:trap_exit, true)
      assert Doubler.start_link(:ignore) == :ignore

      assert Doubler.start_link({:stop, :oops}) == {:error, :oops}
      assert_receive {:EXIT, _, :oops}
      assert Doubler.start_link(:unknown) == {:error, {:bad_return_value, :unknown}}
      assert_receive {:EXIT, _, {:bad_return_value, :unknown}}

      assert Doubler.start_link({:producer_consumer, self(), unknown: :value}) ==
             {:error, {:bad_opts, "unknown options [unknown: :value]"}}

      assert Doubler.start_link({:producer_consumer, 0, dispatcher: 0}) ==
             {:error, {:bad_opts, "expected :dispatcher to be an atom or a {atom, list}, got: 0"}}

      assert {:ok, pid} =
             Doubler.start_link({:producer_consumer, self()}, name: context.test)
      assert {:error, {:already_started, ^pid}} =
             Doubler.start_link({:producer_consumer, self()}, name: context.test)
    end

    test "producer handle_subscribe/4" do
      {:ok, producer} = Doubler.start_link({:producer_consumer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer)
      assert_receive {:producer_consumer_subscribed, :consumer, {^consumer, ^ref}}
    end

    @tag :capture_log
    test "producer handle_subscribe/4 does not accept manual demand" do
      Process.flag(:trap_exit, true)
      {:ok, producer} = Doubler.start_link({:producer_consumer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, producer_consumer_demand: :manual)
      assert_receive {:producer_consumer_subscribed, :consumer, {^consumer, ^ref}}
      assert_receive {:EXIT, ^producer, {:bad_return_value, {:manual, pid}}} when pid == self()
    end

    test "producer handle_cancel/3" do
      {:ok, producer} = Doubler.start_link({:producer_consumer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
      assert_receive {:producer_consumer_subscribed, :consumer, {^consumer, ^ref}}
      GenStage.cancel({producer, ref}, :oops)
      assert_receive {:producer_consumer_cancelled, {^consumer, ^ref}, {:cancel, :oops}}
    end

    test "producer handle_cancel/3 on consumer down" do
      {:ok, producer} = Doubler.start_link({:producer_consumer, self()})
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
      assert_receive {:producer_consumer_subscribed, :consumer, {^consumer, ^ref}}
      Process.unlink(consumer)
      Process.exit(consumer, :kill)
      assert_receive {:producer_consumer_cancelled, {^consumer, ^ref}, {:down, :killed}}
    end

    test "consumer handle_subscribe/4" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Doubler.start_link({:consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer)
      assert_receive {:producer_consumer_subscribed, :producer, {^producer, ^ref}}
    end

    @tag :capture_log
    test "consumer handle_subscribe/4 with invalid demand" do
      Process.flag(:trap_exit, true)
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Doubler.start_link({:producer_consumer, self()})
      GenStage.async_subscribe(consumer, to: producer, producer_consumer_demand: :unknown)
      assert_receive {:EXIT, ^consumer, {:bad_return_value, {:unknown, pid}}} when pid == self()
    end

    test "consumer handle_cancel/3 with temporary subscription" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Doubler.start_link({:producer_consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
      GenStage.cancel({producer, ref}, :oops)
      assert_receive {:producer_consumer_cancelled, {^producer, ^ref}, {:cancel, :oops}}
    end

    @tag :capture_log
    test "consumer handle_cancel/3 with permanent subscription" do
      Process.flag(:trap_exit, true)
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Doubler.start_link({:producer_consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :permanent)
      GenStage.cancel({producer, ref}, self())
      assert_receive {:producer_consumer_cancelled, {^producer, ^ref}, {:cancel, pid}} when pid == self()
      assert_receive {:EXIT, ^consumer, pid} when pid == self()
    end

    test "consumer handle_cancel/3 on producer down with temporary subscription" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Doubler.start_link({:producer_consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
      Process.unlink(producer)
      Process.exit(producer, :kill)
      assert_receive {:producer_consumer_cancelled, {^producer, ^ref}, {:down, :killed}}
    end

    @tag :capture_log
    test "consumer handle_cancel/3 on producer down with permanent subscription" do
      Process.flag(:trap_exit, true)
      {:ok, producer} = Counter.start_link({:producer, 0})
      {:ok, consumer} = Doubler.start_link({:producer_consumer, self()})
      {:ok, ref} = GenStage.sync_subscribe(consumer, to: producer, cancel: :permanent)
      Process.unlink(producer)
      Process.exit(producer, :kill)
      assert_receive {:producer_consumer_cancelled, {^producer, ^ref}, {:down, :killed}}
      assert_receive {:EXIT, ^consumer, :killed}
    end

    test "format_status/2" do
      {:ok, producer} = Counter.start_link({:producer, self()})
      {:ok, producer_consumer} = Doubler.start_link({:producer_consumer, self(), subscribe_to: [producer]})
      {:ok, consumer} = Forwarder.start_link({:consumer, self(), subscribe_to: [producer_consumer]})

      assert {:status, _, _, [_, _, _, _, [header: _, data: _, data: data]]} = :sys.get_status(producer_consumer)
      assert data == [{'State', self()}, {'Stage', :producer_consumer},
                      {'Dispatcher', GenStage.DemandDispatcher}, {'Producers', [producer]},
                      {'Consumers', [consumer]}, {'Buffer size', 0}]
    end
  end

  describe "$gen_producer message errors" do
    @describetag :capture_log

    test "duplicated subscriptions" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      ref = make_ref()
      send producer, {:"$gen_producer", {self(), ref}, {:subscribe, []}}
      send producer, {:"$gen_producer", {self(), ref}, {:subscribe, []}}
      assert_receive {:"$gen_consumer", {^producer, ^ref}, {:cancel, :duplicated_subscription}}
    end

    test "unknown demand" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      ref = make_ref()
      send producer, {:"$gen_producer", {self(), ref}, {:ask, 10}}
      assert_receive {:"$gen_consumer", {^producer, ^ref}, {:cancel, :unknown_subscription}}
    end

    test "not a producer" do
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      send consumer, {:"$gen_producer", {self(), make_ref()}, {:subscribe, []}}
    end
  end

  describe "$gen_consumer message errors" do
    @describetag :capture_log

    test "unknown events" do
      {:ok, consumer} = Forwarder.start_link({:consumer, self()})
      ref = make_ref()
      send consumer, {:"$gen_consumer", {self(), ref}, [1, 2, 3]}
      assert_receive {:"$gen_producer", {^consumer, ^ref}, {:cancel, :unknown_subscription}}
    end

    test "not a consumer" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      send producer, {:"$gen_consumer", {self(), make_ref()}, {:events, []}}
    end
  end

  describe "stream" do
    test "may consume 10 events out of demand of 1000" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      stream = GenStage.stream([producer])
      assert Enum.take(stream, 10) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
      refute_received {:"$gen_consumer", _, _}
    end

    test "may consume 300 events out of demand of 100" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      stream = GenStage.stream([{producer, max_demand: 100}])
      assert length(Enum.take(stream, 300)) == 300
      refute_received {:"$gen_consumer", _, _}
    end

    test "does not remove unknown $gen_consumer and DOWN messages" do
      pid = self()
      ref = make_ref()
      send self(), {:"$gen_consumer", {pid, ref}, [1, 2, 3]}
      send self(), {:DOWN, ref, :process, pid, :oops}

      {:ok, producer} = Counter.start_link({:producer, 0})
      stream = GenStage.stream([producer])
      assert Enum.take(stream, 10) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

      assert_received {:"$gen_consumer", {^pid, ^ref}, [1, 2, 3]}
      assert_received {:DOWN, ^ref, :process, ^pid, :oops}
      refute_received {:"$gen_consumer", _, _}
      refute_received {:DOWN, _, _, _, _}
    end

    test "exits when there is no named producer and subscription is permanent" do
      assert {:noproc, {GenStage, :init_stream, [_]}} =
             catch_exit(GenStage.stream([:unknown]) |> Enum.take(10))
    end

    test "exits when producer is dead and subscription is permanent" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      GenStage.stop(producer)
      assert {:noproc, {GenStage, :close_stream, [_]}} =
             catch_exit(GenStage.stream([producer]) |> Enum.take(10))
    end

    test "exits when producer does not ack and subscription is permanent" do
      {:ok, producer} = Task.start_link(fn ->
        receive do
          {:"$gen_producer", {pid, ref}, {:subscribe, _}} ->
            send(pid, {:"$gen_consumer", {pid, ref}, {:cancel, :no_thanks}})
        end
      end)
      assert catch_exit(GenStage.stream([producer]) |> Enum.take(10)) ==
             {{:cancel, :no_thanks}, {GenStage, :close_stream, [%{}]}}
    end

    test "exits when producer does not ack and lives and subscription is permanent" do
      {:ok, producer} = Task.start_link(fn ->
        receive do
          {:"$gen_producer", {pid, ref}, {:subscribe, _}} ->
            send(pid, {:"$gen_consumer", {pid, ref}, {:cancel, :no_thanks}})
            Process.sleep(:infinity)
        end
      end)
      assert catch_exit(GenStage.stream([producer]) |> Enum.take(10)) ==
             {{:cancel, :no_thanks}, {GenStage, :close_stream, [%{}]}}
    end

    test "exits when there is no named producer and subscription is temporary" do
      assert GenStage.stream([{:unknown, cancel: :temporary}]) |> Enum.take(10) == []
    end

    test "exits when producer is dead and subscription is temporary" do
      {:ok, producer} = Counter.start_link({:producer, 0})
      GenStage.stop(producer)
      assert GenStage.stream([{producer,cancel: :temporary}]) |> Enum.take(10) == []
    end

    test "exits when producer does not ack and subscription is temporary" do
      {:ok, producer} = Task.start_link(fn ->
        receive do
          {:"$gen_producer", {pid, ref}, {:subscribe, _}} ->
            send(pid, {:"$gen_consumer", {pid, ref}, {:cancel, :no_thanks}})
        end
      end)
      assert GenStage.stream([{producer, cancel: :temporary}]) |> Enum.take(10) == []
    end

    test "exits when producer does not ack and lives and subscription is temporary" do
      {:ok, producer} = Task.start_link(fn ->
        receive do
          {:"$gen_producer", {pid, ref}, {:subscribe, _}} ->
            send(pid, {:"$gen_consumer", {pid, ref}, {:cancel, :no_thanks}})
            Process.sleep(:infinity)
        end
      end)
      assert GenStage.stream([{producer, cancel: :temporary}]) |> Enum.take(10) == []
    end

    test "sends termination message on done to permanent producer" do
      stream = Stream.iterate(0, & &1 + 1)
      {:ok, producer} = GenStage.from_enumerable(stream, consumers: :permanent)
      assert GenStage.stream([producer]) |> Enum.take(10) ==
             [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
      ref = Process.monitor(producer)
      assert_receive {:DOWN, ^ref, _, _, _}
    end

    test "sends termination message on halt to permanent producer" do
      stream = Stream.iterate(0, & &1 + 1) |> Stream.take(10)
      {:ok, producer} = GenStage.from_enumerable(stream, consumers: :permanent)
      assert GenStage.stream([producer]) |> Enum.to_list ==
             [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
      ref = Process.monitor(producer)
      assert_receive {:DOWN, ^ref, _, _, _}
    end

    test "sends termination message on done to temporary producer" do
      stream = Stream.iterate(0, & &1 + 1)
      {:ok, producer} = GenStage.from_enumerable(stream, consumers: :temporary)
      assert GenStage.stream([producer]) |> Enum.take(10) ==
             [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
      ref = Process.monitor(producer)
      refute_received {:DOWN, ^ref, _, _, _}
    end

    test "sends termination message on halt to temporary producer" do
      stream = Stream.iterate(0, & &1 + 1) |> Stream.take(10)
      {:ok, producer} = GenStage.from_enumerable(stream, consumers: :temporary)
      assert GenStage.stream([producer]) |> Enum.to_list ==
             [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
      ref = Process.monitor(producer)
      refute_received {:DOWN, ^ref, _, _, _}
    end

    test "raises on bad options" do
      msg = "invalid options for :unknown producer " <>
            "(expected :max_demand to be equal to or greater than 1, got: 0)"

      assert_raise ArgumentError, msg, fn ->
        GenStage.stream([{:unknown, max_demand: 0}])
      end
    end
  end

  describe "from_enumerable/2" do
    test "accepts a :link option" do
      {:ok, producer} = GenStage.from_enumerable([])
      {:links, links} = Process.info(self(), :links)
      assert producer in links

      {:ok, producer} = GenStage.from_enumerable([], link: false)
      {:links, links} = Process.info(self(), :links)
      refute producer in links
    end

    test "accepts a :name option" do
      {:ok, producer} = GenStage.from_enumerable([], name: :gen_stage_from_enumerable)
      assert Process.info(producer, :registered_name) ==
             {:registered_name, :gen_stage_from_enumerable}
    end
  end
end
