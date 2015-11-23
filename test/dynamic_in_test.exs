defmodule DynamicInTest do
  use ExUnit.Case, async: true

  alias TestRouter, as: TR
  alias TestAgent, as: TA

  import ExUnit.CaptureLog

  test "starts with empty queue and no demand" do
      assert {:ok, {0, :queue.new}} == GenRouter.DynamicIn.init nil
  end

  test "when notified of events with 0 demand, returns no reply" do
      initial_state = {0, :queue.new}
      assert {:noreply, _} = GenRouter.DynamicIn.handle_info({:"$gen_notify", self(), :some_event}, initial_state)
  end
end
