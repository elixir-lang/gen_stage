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
              {:mapper, [], [hash: _, stages: 10]},
              {:mapper, [], [stages: @schedulers, hash: _]}] =
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

    test "accumulates reducer stages" do
      assert [{:reducer, [{:reduce, _, _}, {:reduce, _, _}], [stages: @schedulers]}] =
             Flow.new
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.reduce(fn -> 0 end, & &1 * &2)
             |> split()
    end

    test "accumulates mapper and reducer operations" do
      assert [{:mapper, [{:mapper, :map, [_]}], [stages: @schedulers]},
              {:reducer, [{:reduce, _, _}], [hash: _, stages: 10]}] =
             Flow.new
             |> Flow.map(& &1)
             |> Flow.partition(stages: 10)
             |> Flow.reduce(fn -> 0 end, & &1 * &2)
             |> split()

      assert [{:reducer, [{:mapper, :map, [_]}, {:reduce, _, _}], [stages: @schedulers]},
              {:reducer, [{:reduce, _, _}], [hash: _, stages: 10]}] =
             Flow.new
             |> Flow.map(& &1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.partition(stages: 10)
             |> Flow.reduce(fn -> 0 end, & &1 * &2)
             |> split()
    end
  end
end
