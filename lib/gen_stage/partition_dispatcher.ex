alias Experimental.GenStage

defmodule GenStage.PartitionDispatcher do
  @moduledoc """
  A dispatcher that sends events according to partitions.

  Keep in mind that, if partitions are not evenly distributed,
  a backed-up partition will slow all other ones.
  """

  @behaviour GenStage.Dispatcher
  @init {nil, nil, 0}

  @doc false
  def init(opts) do
    hash = Keyword.get(opts, :hash, &:erlang.phash2/1)
    max  = Keyword.get(opts, :partitions) ||
             raise ArgumentError, "the number of :partitions is required when using the partition dispatcher"

    partitions = for i <- 0..max-1, do: {i, @init}, into: %{}
    {:ok, {hash, 0, 0, partitions, %{}}}
  end

  @doc false
  def subscribe(opts, {pid, ref}, {hash, waiting, pending, partitions, references}) do
    partition = Keyword.get(opts, :partition)
    case partitions do
      %{^partition => @init} ->
        partitions = Map.put(partitions, partition, {pid, ref, 0})
        references = Map.put(references, ref, partition)
        {:ok, 0, {hash, waiting, pending, partitions, references}}
      %{^partition => {pid, _demand_or_queue}} ->
        raise ArgumentError, "the partition #{partition} is already taken by #{pid}"
      _ when is_nil(partition) ->
        raise ArgumentError, "a :partition is required when subscribing to a producer with partition dispatcher"
      _ ->
        raise ArgumentError, ":partition must be an integer between 0..#{map_size(partitions)-1}, got: #{partition}"
    end
  end

  @doc false
  def cancel({_, ref}, {hash, waiting, pending, partitions, references}) do
    {partition, references} = Map.pop(references, ref)
    {_pid, _ref, demand_or_queue} = Map.get(partitions, partition)
    partitions = Map.put(partitions, partition, @init)
    pending = pending + to_demand(demand_or_queue)
    {:ok, 0, {hash, waiting, pending, partitions, references}}
  end

  defp to_demand(demand) when is_integer(demand), do: demand
  defp to_demand(_), do: 0

  @doc false
  def ask(counter, {_, ref}, {hash, waiting, pending, partitions, references}) do
    partition = Map.fetch!(references, ref)
    {pid, ref, demand_or_queue} = Map.fetch!(partitions, partition)

    {events, demand_or_queue} =
      case demand_or_queue do
        demand when is_integer(demand) ->
          {[], demand + counter}
        queue ->
          take_from_queue([], queue, counter)
      end

    maybe_send(events, pid, ref)
    partitions = Map.put(partitions, partition, {pid, ref, demand_or_queue})

    already_sent = min(pending, counter)
    demand = counter - already_sent
    {:ok, demand, {hash, waiting + demand, pending - already_sent, partitions, references}}
  end

  defp take_from_queue(acc, queue, 0) do
    {Enum.reverse(acc), queue}
  end
  defp take_from_queue(acc, queue, counter) do
    case :queue.out(queue) do
      {{:value, event}, queue} ->
        take_from_queue([event | acc], queue, counter - 1)
      {:empty, _queue} ->
        {Enum.reverse(acc), counter}
    end
  end

  defp maybe_send([], _pid, _ref),
    do: :ok
  defp maybe_send(events, pid, ref),
    do: send(pid, {:"$gen_consumer", {self(), ref}, events})

  @doc false
  def dispatch(events, {hash, waiting, pending, partitions, references}) do
    {deliver_now, deliver_later, waiting} =
      split_events(events, waiting, [])

    partitioned = Enum.reduce(deliver_now, %{}, fn event, acc ->
      partition = rem(hash.(event), map_size(partitions))
      Map.update(acc, partition, [event], &[event | &1])
    end)

    partitions = Enum.reduce partitioned, partitions, &dispatch_per_partition/2
    {:ok, deliver_later, {hash, waiting, pending, partitions, references}}
  end

  defp split_events(events, 0, acc),
    do: {Enum.reverse(acc), events, 0}
  defp split_events([], counter, acc),
    do: {Enum.reverse(acc), [], counter}
  defp split_events([event | events], counter, acc),
    do: split_events(events, counter - 1, [event | acc])

  defp dispatch_per_partition({partition, events}, partitions) do
    events = Enum.reverse(events)
    {pid, ref, demand_or_queue} = Map.fetch!(partitions, partition)

    {events, demand_or_queue} =
      case demand_or_queue do
        demand when is_integer(demand) ->
          split_into_queue(events, demand, [])
        queue ->
          {[], put_into_queue(events, queue)}
      end

    maybe_send(events, pid, ref)
    Map.put(partitions, partition, {pid, ref, demand_or_queue})
  end

  defp split_into_queue(events, 0, acc),
    do: {Enum.reverse(acc), put_into_queue(events, :queue.new)}
  defp split_into_queue([], counter, acc),
    do: {Enum.reverse(acc), counter}
  defp split_into_queue([event | events], counter, acc),
    do: split_into_queue(events, counter - 1, [event | acc])

  defp put_into_queue(events, queue) do
    Enum.reduce(events, queue, &:queue.in/2)
  end
end
