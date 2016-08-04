alias Experimental.Flow

defmodule Flow.Window.Test do
  use ExUnit.Case, async: true
  doctest Flow.Window

  test "periodic triggers" do
    assert Flow.Window.global
           |> Flow.Window.trigger_periodically(10, :seconds, :keep)
           |> Map.fetch!(:periodically) ==
           [{10000, :keep, {:periodically, 10, :seconds}}]

    assert Flow.Window.global
           |> Flow.Window.trigger_periodically(10, :minutes, :keep)
           |> Map.fetch!(:periodically) ==
           [{600000, :keep, {:periodically, 10, :minutes}}]

    assert Flow.Window.global
           |> Flow.Window.trigger_periodically(10, :hours, :keep)
           |> Map.fetch!(:periodically) ==
           [{36000000, :keep, {:periodically, 10, :hours}}]
  end
end
