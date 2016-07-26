alias Experimental.GenStage

defmodule GenStage.Flow.MaterializeTest do
  use ExUnit.Case, async: true

  @schedulers System.schedulers_online
  alias GenStage.Flow

  def split(%{operations: operations}) do
    GenStage.Flow.Materialize.split_operations(operations, [])
  end

  describe "split_operations/2" do
    test "splits in multiple partitions" do
      assert [{:mapper, [], [stages: @schedulers]},
              {:mapper, [], [stages: 10]},
              {:mapper, [], [stages: @schedulers]}] =
             Flow.new
             |> Flow.partition([stages: 10])
             |> Flow.partition()
             |> split()
    end

    test "accumulates mapper stages" do
      assert [{:mapper, [{:mapper, :map, [_]}], [stages: @schedulers]}] =
             Flow.new
             |> Flow.map(& &1 + 2)
             |> split()

      assert [{:mapper, [{:mapper, :map, [_]}, {:mapper, :filter, [_]}], [stages: @schedulers]}] =
             Flow.new
             |> Flow.map(& &1 + 2)
             |> Flow.filter(& &1 < 2)
             |> split()
    end

    test "accumulates mapper and reducer operations" do
      assert [{:mapper, [{:mapper, :map, [_]}], [stages: @schedulers]},
              {:reducer, [{:reduce, _, _}], [stages: 10]}] =
             Flow.new
             |> Flow.map(& &1)
             |> Flow.partition(stages: 10)
             |> Flow.reduce(fn -> 0 end, & &1 * &2)
             |> split()

      assert [{:reducer, [{:mapper, :map, [_]}, {:reduce, _, _}], [stages: @schedulers]},
              {:reducer, [{:reduce, _, _}], [stages: 10]}] =
             Flow.new
             |> Flow.map(& &1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.partition(stages: 10)
             |> Flow.reduce(fn -> 0 end, & &1 * &2)
             |> split()
    end
  end

  test "errors on flow without producer" do
    assert_raise ArgumentError, ~r"cannot enumerate a flow without producers", fn ->
      Flow.new
      |> Enum.to_list
    end
  end

  test "errors on multiple reduce calls" do
    assert_raise ArgumentError, ~r"cannot call reduce/group_by on a flow after a reduce/group_by operation", fn ->
      Flow.from_enumerable([1, 2, 3])
      |> Flow.reduce(fn -> 0 end, & &1 + &2)
      |> Flow.reduce(fn -> 0 end, & &1 + &2)
      |> Enum.to_list
    end
  end

  test "errors on map_state without reduce" do
    assert_raise ArgumentError, ~r"map_state/each_state/emit must be called after a reduce/group_by operation", fn ->
      Flow.from_enumerable([1, 2, 3])
      |> Flow.map_state(fn x -> x end)
      |> Enum.to_list
    end
  end
end
