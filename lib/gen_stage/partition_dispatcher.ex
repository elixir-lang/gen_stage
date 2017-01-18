defmodule GenStage.PartitionDispatcher do
  @moduledoc """
  A dispatcher that sends events according to partitions.

  Keep in mind that, if partitions are not evenly distributed,
  a backed-up partition will slow all other ones.

  ## Options

  The partition dispatcher accepts the following options
  on initialization:

    * `:partitions` - the number of partitions to dispatch to. It may be
      an integer with a total number of partitions, where each partition
      is named from 0 up to `integer - 1`. For example, `partitions: 4`
      will contain 4 partitions named 0, 1, 2 and 3.

      It may also be an enumerable that specifies the name of every partition.
      For instance, `partitions: [:odd, :even]` will build two partitions,
      named `:odd` and `:even`.

    * `:hash` - the hashing algorithm, which receives the event and returns
      a tuple with two elements, containing the event and the partition.
      The partition must be one of the partitions specified in `:partitions`
      above. The default uses `&:erlang.phash2(&1, Enum.count(partitions))`
      on the event to select the partition.

  ### Examples

  To start a producer with four partitions named 0, 1, 2 and 3:

      {:producer, state, dispatcher: {GenStage.PartitionDispatcher, partitions: 0..3}}

  To start a producer with two partitions named `:odd` and `:even`:

      {:producer, state, dispatcher: {GenStage.PartitionDispatcher, partitions: [:odd, :even]}}

  ## Subscribe options

  When subscribing to a `GenStage` with a partition dispatcher the following
  option is required:

    * `:partition` - the name of the partition. The partition must be one of
      the partitions specified in `:partitions` above.

  ### Examples

  The partition function can be given either on `init`'s subscribe_to:

      {:consumer, :ok, subscribe_to: [{producer, partition: 0}]}

  Or when calling `sync_subscribe`:

      GenStage.sync_subscribe(consumer, to: producer, partition: 0)

  """

  @behaviour GenStage.Dispatcher
  @init {nil, nil, 0}

  @doc false
  def init(opts) do
    partitions =
      case Keyword.get(opts, :partitions) do
        nil ->
          raise ArgumentError, "the enumerable of :partitions is required when using the partition dispatcher"
        partitions when is_integer(partitions) ->
          0..partitions-1
        partitions ->
          partitions
      end

    partitions =
      for i <- partitions, into: %{} do
        Process.put(i, [])
        {i, @init}
      end

    size = map_size(partitions)
    hash = Keyword.get(opts, :hash, &hash(&1, size))
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
        keys = Map.keys(partitions)
        raise ArgumentError, ":partition must be one of #{inspect keys}, got: #{partition}"
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
  def dispatch(events, _length, {tag, hash, waiting, pending, partitions, references}) do
    {deliver_now, deliver_later, waiting} =
      split_events(events, waiting, [])

    for event <- deliver_now do
      {event, partition} = hash.(event)
      Process.put(partition, [event | Process.get(partition)])
    end

    partitions =
      partitions
      |> :maps.to_list
      |> dispatch_per_partition
      |> :maps.from_list

    {:ok, deliver_later, {tag, hash, waiting, pending, partitions, references}}
  end

  defp split_events(events, 0, acc),
    do: {:lists.reverse(acc), events, 0}
  defp split_events([], counter, acc),
    do: {:lists.reverse(acc), [], counter}
  defp split_events([event | events], counter, acc),
    do: split_events(events, counter - 1, [event | acc])

  defp dispatch_per_partition([{partition, {pid, ref, demand_or_queue} = value} | rest]) do
    case Process.put(partition, []) do
      [] ->
        [{partition, value} | dispatch_per_partition(rest)]
      events ->
        events = :lists.reverse(events)

        {events, demand_or_queue} =
          case demand_or_queue do
            demand when is_integer(demand) ->
              split_into_queue(events, demand, [])
            queue ->
              {[], put_into_queue(events, queue)}
          end

        maybe_send(events, pid, ref)
        [{partition, {pid, ref, demand_or_queue}} | dispatch_per_partition(rest)]
    end
  end
  defp dispatch_per_partition([]) do
    []
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
