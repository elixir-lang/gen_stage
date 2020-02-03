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
      a tuple with two elements, the event to be dispatched as first argument
      and the partition as second. The partition must be one of the partitions
      specified in `:partitions` above. The default uses
      `fn event -> {event, :erlang.phash2(event, Enum.count(partitions))} end`
      on the event to select the partition. If it returns `:none`, the event
      is discarded.

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

  require Logger

  @doc false
  def init(opts) do
    partitions =
      case Keyword.get(opts, :partitions) do
        nil ->
          raise ArgumentError,
                "the enumerable of :partitions is required when using the partition dispatcher"

        partitions when is_integer(partitions) ->
          0..(partitions - 1)

        partitions ->
          partitions
      end

    hash_present? = Keyword.has_key?(opts, :hash)

    partitions =
      for partition <- partitions, into: %{} do
        if not hash_present? and not is_integer(partition) do
          raise ArgumentError,
                "when :partitions contains partitions that are not integers, you have to pass " <>
                  "in the :hash option as well"
        end

        Process.put(partition, [])
        {partition, @init}
      end

    size = map_size(partitions)
    hash = Keyword.get(opts, :hash, &hash(&1, size))
    {:ok, {make_ref(), hash, 0, 0, partitions, %{}, %{}}}
  end

  defp hash(event, range) do
    {event, :erlang.phash2(event, range)}
  end

  @doc false
  def info(msg, {tag, hash, waiting, pending, partitions, references, infos}) do
    info = make_ref()

    {partitions, queued} =
      Enum.reduce(partitions, {partitions, []}, fn
        {partition, {pid, ref, queue}}, {partitions, queued} when not is_integer(queue) ->
          {Map.put(partitions, partition, {pid, ref, :queue.in({tag, info}, queue)}),
           [partition | queued]}

        _, {partitions, queued} ->
          {partitions, queued}
      end)

    infos =
      case queued do
        [] ->
          send(self(), msg)
          infos

        _ ->
          Map.put(infos, info, {msg, queued})
      end

    {:ok, {tag, hash, waiting, pending, partitions, references, infos}}
  end

  @doc false
  def subscribe(opts, {pid, ref}, {tag, hash, waiting, pending, partitions, references, infos}) do
    partition = Keyword.get(opts, :partition)

    case partitions do
      %{^partition => {nil, nil, demand_or_queue}} ->
        partitions = Map.put(partitions, partition, {pid, ref, demand_or_queue})
        references = Map.put(references, ref, partition)
        {:ok, 0, {tag, hash, waiting, pending, partitions, references, infos}}

      %{^partition => {pid, _, _}} ->
        raise ArgumentError, "the partition #{partition} is already taken by #{inspect(pid)}"

      _ when is_nil(partition) ->
        raise ArgumentError,
              "the :partition option is required when subscribing to a producer with partition dispatcher"

      _ ->
        keys = Map.keys(partitions)
        raise ArgumentError, ":partition must be one of #{inspect(keys)}, got: #{partition}"
    end
  end

  @doc false
  def cancel({_, ref}, {tag, hash, waiting, pending, partitions, references, infos}) do
    {partition, references} = Map.pop(references, ref)
    {_pid, _ref, demand_or_queue} = Map.get(partitions, partition)
    partitions = Map.put(partitions, partition, @init)

    case demand_or_queue do
      demand when is_integer(demand) ->
        {:ok, 0, {tag, hash, waiting, pending + demand, partitions, references, infos}}

      queue ->
        {length, infos} = clear_queue(queue, tag, partition, 0, infos)
        {:ok, length, {tag, hash, waiting + length, pending, partitions, references, infos}}
    end
  end

  @doc false
  def ask(counter, {_, ref}, {tag, hash, waiting, pending, partitions, references, infos}) do
    partition = Map.fetch!(references, ref)
    {pid, ref, demand_or_queue} = Map.fetch!(partitions, partition)

    {demand_or_queue, infos} =
      case demand_or_queue do
        demand when is_integer(demand) ->
          {demand + counter, infos}

        queue ->
          send_from_queue(queue, tag, pid, ref, partition, counter, [], infos)
      end

    partitions = Map.put(partitions, partition, {pid, ref, demand_or_queue})
    already_sent = min(pending, counter)
    demand = counter - already_sent
    pending = pending - already_sent
    {:ok, demand, {tag, hash, waiting + demand, pending, partitions, references, infos}}
  end

  defp send_from_queue(queue, _tag, pid, ref, _partition, 0, acc, infos) do
    maybe_send(acc, pid, ref)
    {queue, infos}
  end

  defp send_from_queue(queue, tag, pid, ref, partition, counter, acc, infos) do
    case :queue.out(queue) do
      {{:value, {^tag, info}}, queue} ->
        maybe_send(acc, pid, ref)
        infos = maybe_info(infos, info, partition)
        send_from_queue(queue, tag, pid, ref, partition, counter, [], infos)

      {{:value, event}, queue} ->
        send_from_queue(queue, tag, pid, ref, partition, counter - 1, [event | acc], infos)

      {:empty, _queue} ->
        maybe_send(acc, pid, ref)
        {counter, infos}
    end
  end

  defp clear_queue(queue, tag, partition, counter, infos) do
    case :queue.out(queue) do
      {{:value, {^tag, info}}, queue} ->
        clear_queue(queue, tag, partition, counter, maybe_info(infos, info, partition))

      {{:value, _}, queue} ->
        clear_queue(queue, tag, partition, counter + 1, infos)

      {:empty, _queue} ->
        {counter, infos}
    end
  end

  # Important: events must be in reverse order
  defp maybe_send([], _pid, _ref), do: :ok

  defp maybe_send(events, pid, ref),
    do: Process.send(pid, {:"$gen_consumer", {self(), ref}, :lists.reverse(events)}, [:noconnect])

  defp maybe_info(infos, info, partition) do
    case infos do
      %{^info => {msg, [^partition]}} ->
        send(self(), msg)
        Map.delete(infos, info)

      %{^info => {msg, partitions}} ->
        Map.put(infos, info, {msg, List.delete(partitions, partition)})
    end
  end

  @doc false
  def dispatch(events, _length, {tag, hash, waiting, pending, partitions, references, infos}) do
    {deliver_later, waiting} = split_events(events, waiting, hash, partitions)

    partitions =
      partitions
      |> :maps.to_list()
      |> dispatch_per_partition()
      |> :maps.from_list()

    {:ok, deliver_later, {tag, hash, waiting, pending, partitions, references, infos}}
  end

  defp split_events(events, 0, _hash, _partitions), do: {events, 0}
  defp split_events([], counter, _hash, _partitions), do: {[], counter}

  defp split_events([event | events], counter, hash, partitions) do
    case hash.(event) do
      {event, partition} ->
        case :erlang.get(partition) do
          :undefined ->
            raise "unknown partition #{inspect(partition)} computed for GenStage event " <>
                    "#{inspect(event)}. The known partitions are #{inspect(Map.keys(partitions))}. " <>
                    "See the :partitions option to set your own. This event has been discarded."

          current ->
            Process.put(partition, [event | current])
            split_events(events, counter - 1, hash, partitions)
        end

      :none ->
        split_events(events, counter, hash, partitions)

      other ->
        raise "the :hash function should return {event, partition}, got: #{inspect(other)}"
    end
  end

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

  defp split_into_queue(events, 0, acc), do: {acc, put_into_queue(events, :queue.new())}
  defp split_into_queue([], counter, acc), do: {acc, counter}

  defp split_into_queue([event | events], counter, acc),
    do: split_into_queue(events, counter - 1, [event | acc])

  defp put_into_queue(events, queue) do
    Enum.reduce(events, queue, &:queue.in/2)
  end
end
