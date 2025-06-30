defmodule GenStage.BufferTest do
  use ExUnit.Case, async: true

  alias GenStage.Buffer

  describe "estimate_size/1" do
    test "does not count permanent events in size estimate" do
      buffer = Buffer.new(10)
      {buffer, _excess, _perms} = Buffer.store_temporary(buffer, [:temp1, :temp2], :first)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm3)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm4)

      # Size should still be 2 (only temporary events)
      assert Buffer.estimate_size(buffer) == 2
    end
  end

  describe "store_temporary/3 with :first keep strategy" do
    test "discards excess events when buffer is full" do
      buffer = Buffer.new(3)
      assert {buffer, 0, _perms} = Buffer.store_temporary(buffer, [:a, :b, :c], :first)
      assert {buffer, 3, _perms} = Buffer.store_temporary(buffer, [:d, :e, :f], :first)

      assert Buffer.estimate_size(buffer) == 3

      assert {:ok, _buffer, _counter, [:a, :b, :c], []} =
               Buffer.take_count_or_until_permanent(buffer, 3)
    end

    test "handles infinity buffer size" do
      buffer = Buffer.new(:infinity)
      events = Enum.to_list(1..1000)
      assert {buffer, 0, []} = Buffer.store_temporary(buffer, events, :first)

      assert Buffer.estimate_size(buffer) == 1000
    end
  end

  describe "store_temporary/3 with :last keep strategy" do
    test "keeps last events when buffer overflows" do
      buffer = Buffer.new(3)
      assert {buffer, 0, _perms} = Buffer.store_temporary(buffer, [:a, :b, :c], :last)
      assert {buffer, 3, _perms} = Buffer.store_temporary(buffer, [:d, :e, :f], :last)

      assert {:ok, _buffer, _counter, [:d, :e, :f], []} =
               Buffer.take_count_or_until_permanent(buffer, 3)
    end

    test "emits permanent events when they are displaced by new temporaries" do
      buffer = Buffer.new(3)
      {buffer, _excess, _perms} = Buffer.store_temporary(buffer, [:temp1, :temp2], :last)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm3)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm4)

      # Now overflow with :last strategy, which should displace permanents
      {_buffer, excess, perms} = Buffer.store_temporary(buffer, [:temp5, :temp6, :temp7], :last)

      assert excess == 2
      assert length(perms) > 0
    end
  end

  describe "store_permanent_unless_empty/2" do
    test "returns :empty when buffer is empty" do
      buffer = Buffer.new(10)
      result = Buffer.store_permanent_unless_empty(buffer, :perm1)
      assert result == :empty
    end
  end

  describe "take_count_or_until_permanent/2" do
    test "returns :empty when buffer is empty" do
      buffer = Buffer.new(10)
      result = Buffer.take_count_or_until_permanent(buffer, 5)
      assert result == :empty
    end

    test "takes temporary events in FIFO order, stopping at permanent events and returning them" do
      buffer = Buffer.new(10)
      {buffer, _excess, _perms} = Buffer.store_temporary(buffer, [:temp1, :temp2], :first)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm3)
      {buffer, _excess, _perms} = Buffer.store_temporary(buffer, [:temp4], :first)

      {:ok, _buffer, remaining_count, temps, perms} =
        Buffer.take_count_or_until_permanent(buffer, 5)

      assert temps == [:temp1, :temp2]
      assert perms == [:perm3]
      # We wanted 5, got 2 temps, stopped at perm
      assert remaining_count == 3
    end

    test "maintains FIFO order for permanent events stored at same position" do
      buffer = Buffer.new(10)
      {buffer, _excess, _perms} = Buffer.store_temporary(buffer, [:temp1, :temp2], :first)

      # Store multiple permanent events at the same wheel position
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm3)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm4)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm5)

      {:ok, _buffer, _remaining_count, temps, perms} =
        Buffer.take_count_or_until_permanent(buffer, 5)

      assert temps == [:temp1, :temp2]
      assert perms == [:perm3, :perm4, :perm5]
    end

    test "interleaves temporary and permanent events correctly across multiple wheel positions" do
      buffer = Buffer.new(10)

      {buffer, _excess, _perms} = Buffer.store_temporary(buffer, [:temp1, :temp2], :first)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm3)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm4)

      {buffer, _excess, _perms} = Buffer.store_temporary(buffer, [:temp5, :temp6, :temp7], :first)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm8)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm9)

      {buffer, _excess, _perms} = Buffer.store_temporary(buffer, [:temp10], :first)
      {:ok, buffer} = Buffer.store_permanent_unless_empty(buffer, :perm11)

      {:ok, buffer, _remaining, temps1, perms1} = Buffer.take_count_or_until_permanent(buffer, 3)
      assert temps1 == [:temp1, :temp2]
      assert perms1 == [:perm3, :perm4]

      {:ok, buffer, _remaining, temps2, perms2} = Buffer.take_count_or_until_permanent(buffer, 4)
      assert temps2 == [:temp5, :temp6, :temp7]
      assert perms2 == [:perm8, :perm9]

      {:ok, _buffer, _remaining, temps3, perms3} = Buffer.take_count_or_until_permanent(buffer, 2)
      assert temps3 == [:temp10]
      assert perms3 == [:perm11]
    end
  end
end
