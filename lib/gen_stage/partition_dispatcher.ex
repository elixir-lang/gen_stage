alias Experimental.GenStage

defmodule GenStage.PartitionDispatcher do
  @moduledoc """
  A dispatcher that sends events according to partitions.

  Keep in mind that, if partitions are not evenly distributed,
  a backed-up partition will slow all other ones.

  ## Options

  The partition dispatcher accepts the following options
  on initialization:

    * `:partitions` - an eumerable that sets the names of the partitions
      we will dispatch to.

    * `:hash` - the hashing algorithm, which receives the event and returns
      the event and partition in a 2 element tuple. The default uses
      `&:erlang.phash2(&1, Enum.count(partitions))` on the event to select
      the partition.

  ## Subscribe options

  When subscribing to a `GenStage` with a partition dispatcher the following
  option is required:

    * `:partition` - the name of the partition, must be included in the
      `:partitions` enumerable set on initialization of the dispatcher.
  """

  @behaviour GenStage.Dispatcher
  @init {nil, nil, 0}

  @doc false
  def init(opts) do
    partitions = Keyword.get(opts, :partitions) ||
             raise ArgumentError, "the enumerable of :partitions is required when using the partition dispatcher"

    partitions = for i <- partitions, do: {i, @init}, into: %{}
    range = map_size(partitions)
    hash = Keyword.get(opts, :hash, &hash(&1, range))
    {:ok, {make_ref(), hash, 0, 0, partitions, %{}}}
  end

  defp hash(event, range) do
    {event, :erlang.phash2(event, range)}
  end

  @doc false
  def notify(msg, {tag, hash, waiting, pending, partitions, references}) do
    partitions =
      Enum.reduce(partitions, partitions, fn
        {partition, @init}, acc ->
          Map.put(acc, partition, {nil, nil, :queue.in({tag, msg}, :queue.new)})
        {partition, {pid, ref, queue}}, acc when not is_integer(queue) ->
          Map.put(acc, partition, {pid, ref, :queue.in({tag, msg}, queue)})
        {_, {pid, ref, _}}, acc ->
          Process.send(pid, {:"$gen_consumer", {self(), ref}, {:notification, msg}}, [:noconnect])
          acc
      end)

    {:ok, {tag, hash, waiting, pending, partitions, references}}
  end

  @doc false
  def subscribe(opts, {pid, ref}, {tag, hash, waiting, pending, partitions, references}) do
    partition = Keyword.get(opts, :partition)
    case partitions do
      %{^partition => {nil, nil, demand_or_queue}} ->
        partitions = Map.put(partitions, partition, {pid, ref, demand_or_queue})
        references = Map.put(references, ref, partition)
        {:ok, 0, {tag, hash, waiting, pending, partitions, references}}
      %{^partition => {pid, _, _}} ->
        raise ArgumentError, "the partition #{partition} is already taken by #{inspect pid}"
      _ when is_nil(partition) ->
        raise ArgumentError, "the :partition option is required when subscribing to a producer with partition dispatcher"
      _ ->
        keys =
          partitions
          |> Map.keys()
          |> Enum.join(", ")
        raise ArgumentError, ":partition must be one of #{keys} but got: #{partition}"
    end
  end

  @doc false
  def cancel({_, ref}, {tag, hash, waiting, pending, partitions, references}) do
    {partition, references} = Map.pop(references, ref)
    {_pid, _ref, demand_or_queue} = Map.get(partitions, partition)
    partitions = Map.put(partitions, partition, @init)
    case demand_or_queue do
      demand when is_integer(demand) ->
        {:ok, 0, {tag, hash, waiting, pending + demand, partitions, references}}
      queue ->
        length = count_from_queue(queue, tag, 0)
        {:ok, length, {tag, hash, waiting + length, pending, partitions, references}}
    end
  end

  @doc false
  def ask(counter, {_, ref}, {tag, hash, waiting, pending, partitions, references}) do
    partition = Map.fetch!(references, ref)
    {pid, ref, demand_or_queue} = Map.fetch!(partitions, partition)

    demand_or_queue =
      case demand_or_queue do
        demand when is_integer(demand) ->
          demand + counter
        queue ->
          send_from_queue(queue, tag, pid, ref, counter, [])
      end

    partitions = Map.put(partitions, partition, {pid, ref, demand_or_queue})
    already_sent = min(pending, counter)
    demand = counter - already_sent
    {:ok, demand, {tag, hash, waiting + demand, pending - already_sent, partitions, references}}
  end

  defp send_from_queue(queue, _tag, pid, ref, 0, acc) do
    maybe_send(acc, pid, ref)
    queue
  end
  defp send_from_queue(queue, tag, pid, ref, counter, acc) do
    case :queue.out(queue) do
      {{:value, {^tag, msg}}, queue} ->
        maybe_send(acc, pid, ref)
        Process.send(pid, {:"$gen_consumer", {self(), ref}, {:notification, msg}}, [:noconnect])
        send_from_queue(queue, tag, pid, ref, counter, [])
      {{:value, event}, queue} ->
        send_from_queue(queue, tag, pid, ref, counter - 1, [event | acc])
      {:empty, _queue} ->
        maybe_send(acc, pid, ref)
        counter
    end
  end

  defp count_from_queue(queue, tag, counter) do
    case :queue.out(queue) do
      {{:value, {^tag, _}}, queue} ->
        count_from_queue(queue, tag, counter)
      {{:value, _}, queue} ->
        count_from_queue(queue, tag, counter + 1)
      {:empty, _queue} ->
        counter
    end
  end

  # Important: events must be in reverse order
  defp maybe_send([], _pid, _ref),
    do: :ok
  defp maybe_send(events, pid, ref),
    do: Process.send(pid, {:"$gen_consumer", {self(), ref}, :lists.reverse(events)}, [:noconnect])

  @doc false
  def dispatch(events, {tag, hash, waiting, pending, partitions, references}) do
    {deliver_now, deliver_later, waiting} =
      split_events(events, waiting, [])

    size = map_size(partitions)
    countdown size, nil, fn i, nil -> Process.put(i, []) end

    for event <- deliver_now do
      {event, partition} = hash.(event)
      Process.put(partition, [event | Process.get(partition)])
    end

    partitions = countdown size, partitions, &dispatch_per_partition/2
    {:ok, deliver_later, {tag, hash, waiting, pending, partitions, references}}
  end

  defp countdown(0, acc, _), do: acc
  defp countdown(count, acc, fun), do: countdown(count - 1, fun.(count - 1, acc), fun)

  defp split_events(events, 0, acc),
    do: {:lists.reverse(acc), events, 0}
  defp split_events([], counter, acc),
    do: {:lists.reverse(acc), [], counter}
  defp split_events([event | events], counter, acc),
    do: split_events(events, counter - 1, [event | acc])

  defp dispatch_per_partition(partition, partitions) do
    case Process.delete(partition) do
      [] ->
        partitions
      events ->
        events = :lists.reverse(events)
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
  end

  defp split_into_queue(events, 0, acc),
    do: {acc, put_into_queue(events, :queue.new)}
  defp split_into_queue([], counter, acc),
    do: {acc, counter}
  defp split_into_queue([event | events], counter, acc),
    do: split_into_queue(events, counter - 1, [event | acc])

  defp put_into_queue(events, queue) do
    Enum.reduce(events, queue, &:queue.in/2)
  end
end
