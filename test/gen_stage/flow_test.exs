defmodule GenStage.FlowTest do
  use ExUnit.Case, async: true

  alias Experimental.GenStage.Flow
  doctest Flow

  test "mappers are performed in correct order when unfused" do
    flow =
      Flow.new(mappers: 2)
      |> Flow.from_enumerable(["a"])
      |> Flow.filter(& &1 == "a")
      |> Flow.map(fn(x) -> x <> "b" end)
      |> Flow.map(fn(x) -> x <> "c" end)

    assert Enum.to_list(flow) == ["abc"]
  end

  test "mappers are performed in correct order when fused" do
    flow =
      Flow.new(mappers: 2)
      |> Flow.from_enumerables([["a", 1], [2, "a"]])
      |> Flow.filter(& &1 == "a")
      |> Flow.map(fn(x) -> x <> "b" end)
      |> Flow.map(fn(x) -> x <> "c" end)

    assert Enum.to_list(flow) == ["abc", "abc"]
  end
end
