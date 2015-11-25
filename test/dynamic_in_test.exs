defmodule DynamicInTest do
  use ExUnit.Case, async: true

  test "starts with empty queue and no demand" do
      assert {:ok, {0, :queue.new}} == GenRouter.DynamicIn.init nil
  end

  test "events arriving with no existing demand are added to queue" do
      sender = {self(),make_ref}
      initial_state = {0, :queue.new}
      assert {:noreply, {_demand, queue}} = GenRouter.DynamicIn.handle_info({:"$gen_notify", sender, :some_event}, initial_state)
      assert :queue.peek(queue) == {:value, {sender, :some_event}}
  end

  test "event arriving with existing demand should dispatch event and reduce outstanding demand" do
      sender = {self(),make_ref}
      empty_queue = :queue.new
      initial_state = {2, empty_queue}
      assert {:dispatch, [:some_event], {1, empty_queue}} == GenRouter.DynamicIn.handle_info({:"$gen_notify", sender, :some_event}, initial_state)
  end

  test "demand arriving with sufficient events already in queue should dispatch events to fulfill demand and remove them from queue" do
      sender = {self(),make_ref}
      initial_queue =
        Stream.cycle([sender])
        |> Enum.zip([:a, :b, :c])
        |> :queue.from_list

      initial_state = {0, initial_queue}
      assert {:dispatch, [:a, :b], {0, final_queue}} = GenRouter.DynamicIn.handle_demand(2,initial_state)
      assert :queue.to_list(final_queue) == [{sender, :c}]
  end

  test "demand arriving with insufficient events already in queue should dispatch all events in queue and maintain unfulfilled portion of demand" do
      sender = {self(),make_ref}
      empty_queue = :queue.new

      initial_queue =
        Stream.cycle([sender])
        |> Enum.zip([:a, :b, :c])
        |> :queue.from_list

      initial_state = {0, initial_queue}
      assert {:dispatch, [:a, :b, :c], {2, empty_queue}} == GenRouter.DynamicIn.handle_demand(5,initial_state)
  end
end
