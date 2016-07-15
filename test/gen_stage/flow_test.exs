defmodule GenStage.FlowTest do
  use ExUnit.Case, async: true

  alias Experimental.GenStage.Flow
  doctest Flow

  test "mappers are performed in correct order" do
    flow = Flow.from_enumerable(["a"])
    |> Flow.map(fn(x) -> x <> "b" end)
    |> Flow.map(fn(x) -> x <> "c" end)

    assert Enum.to_list(flow) == ["abc"]
  end
end
