defmodule GenStage.FlowTest do
  use ExUnit.Case, async: true

  alias Experimental.GenStage.Flow
  doctest Flow

  describe "enumerable-stream" do
    @flow Flow.new(mappers: [stages: 2])
          |> Flow.from_enumerables([[1, 2, 3], [4, 5, 6]])

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

    test "keeps ordering" do
      flow =
        @flow
        |> Flow.filter(&rem(&1, 2) == 0)
        |> Flow.map(fn(x) -> x + 1 end)
        |> Flow.map(fn(x) -> x * 2 end)
      assert Enum.sort(flow) == [6, 10, 14]
    end
  end
end
