defmodule GenStageTest do
  use ExUnit.Case, async: true

  defmodule Producer do
    use GenStage

    def start_link(init, opts \\ []) do
      GenStage.start_link(__MODULE__, init, opts)
    end

    def init(init) do
      init
    end

    def handle_demand(demand, counter) when demand > 0 do
      # If the counter is 3 and we ask for 2 items, we will
      # emit the items 3 and 4, and set the state to 5.
      events = Enum.to_list(counter..counter+demand-1)
      {:noreply, events, counter + demand}
    end
  end

  defmodule Consumer do
    use GenStage

    def start_link(init, opts \\ []) do
      GenStage.start_link(__MODULE__, init, opts)
    end

    def init(init) do
      init
    end

    def handle_events(events, _from, recipient) do
      send recipient, {:consumed, events}
      {:noreply, [], recipient}
    end
  end

  describe "one-to-one demand handling between producer and consumer" do
    test "with default max and min demand" do
      {:ok, producer} = Producer.start_link({:producer, 0})
      {:ok, consumer} = Consumer.start_link({:consumer, self()})
      :ok = GenStage.sync_subscribe(consumer, to: producer)

      batch = Enum.to_list(0..99)
      assert_receive {:consumed, ^batch}
      batch = Enum.to_list(100..199)
      assert_receive {:consumed, ^batch}
    end
  end

  # describe "producer" do
  #   test "init/1" do
  #     Sample.start_link({:producer, []})
  #   end
  # end

  # describe "consumer" do
  #   test "init/1" do
  #     Sample.start_link({:consumer, []})
  #   end
  # end
end
