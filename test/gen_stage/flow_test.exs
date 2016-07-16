alias Experimental.GenStage

defmodule GenStage.FlowTest do
  use ExUnit.Case, async: true

  alias GenStage.Flow
  doctest Flow

  defmodule Counter do
    use GenStage

    def init(counter) do
      {:producer, counter}
    end

    def handle_demand(demand, counter) when demand > 0 do
      # If the counter is 3 and we ask for 2 items, we will
      # emit the items 3 and 4, and set the state to 5.
      events = Enum.to_list(counter..counter+demand-1)
      {:noreply, events, counter + demand}
    end
  end

  describe "enumerable-stream" do
    @flow Flow.new(mappers: [stages: 2])
          |> Flow.from_enumerables([[1, 2, 3], [4, 5, 6]])

    test "each" do
      parent = self()
      assert @flow |> Flow.each(&send(parent, &1)) |> Enum.sort() ==
             [1, 2, 3, 4, 5, 6]
      assert_received 1
      assert_received 2
      assert_received 3
    end

    test "filter" do
      assert @flow |> Flow.filter(&rem(&1, 2) == 0) |> Enum.sort() ==
             [2, 4, 6]
    end

    test "filter_map" do
      assert @flow |> Flow.filter_map(&rem(&1, 2) == 0, & &1 * 2) |> Enum.sort() ==
             [4, 8, 12]
    end

    test "flat_map" do
      assert @flow |> Flow.flat_map(&[&1, &1]) |> Enum.sort() ==
             [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6]
    end

    test "map" do
      assert @flow |> Flow.map(& &1 * 2) |> Enum.sort() ==
             [2, 4, 6, 8, 10, 12]
    end

    test "reject" do
      assert @flow |> Flow.reject(&rem(&1, 2) == 0) |> Enum.sort() ==
             [1, 3, 5]
    end

    test "keeps ordering" do
      flow =
        @flow
        |> Flow.filter(&rem(&1, 2) == 0)
        |> Flow.map(fn(x) -> x + 1 end)
        |> Flow.map(fn(x) -> x * 2 end)
      assert Enum.sort(flow) == [6, 10, 14]
    end
  end

  describe "enumerable-mappers-stream" do
    @flow Flow.new(mappers: [stages: 4])
          |> Flow.from_enumerables([[1, 2, 3], [4, 5, 6]])

    test "each" do
      parent = self()
      assert @flow |> Flow.each(&send(parent, &1)) |> Enum.sort() ==
             [1, 2, 3, 4, 5, 6]
      assert_received 1
      assert_received 2
      assert_received 3
    end

    test "filter" do
      assert @flow |> Flow.filter(&rem(&1, 2) == 0) |> Enum.sort() ==
             [2, 4, 6]
    end

    test "filter_map" do
      assert @flow |> Flow.filter_map(&rem(&1, 2) == 0, & &1 * 2) |> Enum.sort() ==
             [4, 8, 12]
    end

    test "flat_map" do
      assert @flow |> Flow.flat_map(&[&1, &1]) |> Enum.sort() ==
             [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6]
    end

    test "map" do
      assert @flow |> Flow.map(& &1 * 2) |> Enum.sort() ==
             [2, 4, 6, 8, 10, 12]
    end

    test "reject" do
      assert @flow |> Flow.reject(&rem(&1, 2) == 0) |> Enum.sort() ==
             [1, 3, 5]
    end

    test "keeps ordering" do
      flow =
        @flow
        |> Flow.filter(&rem(&1, 2) == 0)
        |> Flow.map(fn(x) -> x + 1 end)
        |> Flow.map(fn(x) -> x * 2 end)
      assert Enum.sort(flow) == [6, 10, 14]
    end
  end

  describe "stages-mappers-stream" do
    @flow Flow.new(mappers: [stages: 1])
          |> Flow.from_stage(Counter)

    setup do
      GenStage.start_link(Counter, 0, name: Counter)
      :ok
    end

    test "each" do
      parent = self()
      assert @flow |> Flow.each(&send(parent, &1)) |> Enum.take(5) |> Enum.sort() ==
             [0, 1, 2, 3, 4]
      assert_received 1
      assert_received 2
      assert_received 3
    end

    test "filter" do
      assert @flow |> Flow.filter(&rem(&1, 2) == 0) |> Enum.take(5) |> Enum.sort() ==
             [0, 2, 4, 6, 8]
    end

    test "filter_map" do
      assert @flow |> Flow.filter_map(&rem(&1, 2) == 0, & &1 * 2) |> Enum.take(5) |> Enum.sort() ==
             [0, 4, 8, 12, 16]
    end

    test "flat_map" do
      assert @flow |> Flow.flat_map(&[&1, &1]) |> Enum.take(5) |> Enum.sort() ==
             [0, 0, 1, 1, 2]
    end

    test "map" do
      assert @flow |> Flow.map(& &1 * 2) |> Enum.take(5) |> Enum.sort() ==
             [0, 2, 4, 6, 8]
    end

    test "reject" do
      assert @flow |> Flow.reject(&rem(&1, 2) == 0) |> Enum.take(5) |> Enum.sort() ==
             [1, 3, 5, 7, 9]
    end

    test "keeps ordering" do
      flow =
        @flow
        |> Flow.filter(&rem(&1, 2) == 0)
        |> Flow.map(fn(x) -> x + 1 end)
        |> Flow.map(fn(x) -> x * 2 end)
      assert flow |> Enum.take(5) |> Enum.sort() == [2, 6, 10, 14, 18]
    end
  end
end
