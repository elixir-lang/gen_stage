alias Experimental.GenStage

defmodule GenStage.Flow.MaterializeTest do
  use ExUnit.Case, async: true

  alias GenStage.Flow

  def split(%{operations: operations}) do
    GenStage.Flow.Materialize.split_operations(operations, 2)
  end

  describe "split_operations/2" do
    test "splits mapper stages" do
      assert {[{:mapper, :map, [_]}],
              []} =
             Flow.new
             |> Flow.map(& &1 + 2)
             |> split()

      assert {[{:mapper, :map, [_]},
               {:mapper, :filter, [_]}],
              []} =
             Flow.new
             |> Flow.map(& &1 + 2)
             |> Flow.filter(& &1 + 2)
             |> split()
    end

    test "splits in multiple partitions" do
      assert {[],
              [{[], [stages: 10]},
               {[], [stages: 2]}]} =
             Flow.new
             |> Flow.partition_with([stages: 10])
             |> Flow.partition_with([])
             |> split()
    end

    test "accumulates reducer operations" do
      assert {[],
              [{[{:reducer, :reduce, [_, _]},
                 {:reducer, :reduce, [_, _]}], [stages: 2]}]} =
             Flow.new
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.reduce(fn -> 0 end, & &1 * &2)
             |> split()
    end

    test "accumulates reducer operations unless mapper is between" do
      assert {[],
              [{[{:reducer, :reduce, [_, _]},
                 {:mapper, :map, [_]}], [stages: 2]},
               {[{:reducer, :reduce, [_, _]}], [stages: 2]}]} =
             Flow.new
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map(& &1)
             |> Flow.reduce(fn -> 0 end, & &1 * &2)
             |> split()
    end

    test "accumulates reducer operations unless partition is between" do
      assert {[],
              [{[{:reducer, :reduce, [_, _]}], [stages: 2]},
               {[{:reducer, :reduce, [_, _]}], [stages: 10]}]} =
             Flow.new
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.partition_with(stages: 10)
             |> Flow.reduce(fn -> 0 end, & &1 * &2)
             |> split()
    end
  end
end
