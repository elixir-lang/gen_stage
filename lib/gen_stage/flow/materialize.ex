alias Experimental.GenStage

defmodule GenStage.Flow.Materialize do
  @moduledoc false

  @map_reducer_opts [:buffer_keep, :buffer_size, :dispatcher]

  @doc """
  Materializes a flow for stream consumption.
  """
  def to_stream(%{producers: nil}) do
    raise ArgumentError, "cannot enumerable a flow without producers, " <>
                         "please call `from_enumerable` or `from_stage` accordingly"
  end

  def to_stream(%{operations: operations, mappers: mapper_opts,
                  producers: producers, stages: stages}) do
    {mapper_ops, reducer_ops} = split_operations(operations, stages)
    mapper_opts = mapper_opts |> Keyword.put_new(:stages, stages) |> dispatcher(reducer_ops)
    mappers = start_mappers(producers, mapper_ops, mapper_opts)
    consumers = start_reducers(reducer_ops, mappers)
    GenStage.stream(consumers)
  end

  ## Helpers

  @doc """
  Splits the flow operations into layers of stages.
  """
  def split_operations(operations, stages) do
    {mappers, reducers} = Enum.split_while(Enum.reverse(operations), &elem(&1, 0) == :mapper)
    {mappers, split_reducers(reducers, stages)}
  end

  defp split_reducers([{:partition, opts} | operations], stages),
    do: split_reducers(operations, true, [], opts, stages)
  defp split_reducers([_ | _] = operations, stages),
    do: split_reducers(operations, true, [], [], stages)
  defp split_reducers([], _stages),
    do: []

  defp split_reducers([], _reducer?, acc_ops, acc_opts, stages) do
    [reducer(acc_ops, acc_opts, stages)]
  end
  defp split_reducers([{:partition, opts} | operations], _reducer?, acc_ops, acc_opts, stages) do
    [reducer(acc_ops, acc_opts, stages) | split_reducers(operations, true, [], opts, stages)]
  end
  defp split_reducers([{:reducer, _, _} = op | operations], false, acc_ops, acc_opts, stages) do
    [reducer(acc_ops, acc_opts, stages) | split_reducers(operations, true, [op], [], stages)]
  end
  defp split_reducers([{:reducer, _, _} = op | operations], true, acc_ops, acc_opts, stages) do
    split_reducers(operations, true, [op | acc_ops], acc_opts, stages)
  end
  defp split_reducers([op | operations], _reducer?, acc_ops, acc_opts, stages) do
    split_reducers(operations, false, [op | acc_ops], acc_opts, stages)
  end

  defp reducer(ops, opts, stages) do
    {Enum.reverse(ops), Keyword.put_new(opts, :stages, stages)}
  end

  defp dispatcher(opts, []), do: opts
  defp dispatcher(opts, [{_reducer_ops, reducer_opts} | _]) do
    partitions = Keyword.fetch!(reducer_opts, :stages)
    hash = Keyword.get(reducer_opts, :hash, &:erlang.phash2/2)
    put_in opts[:dispatcher], {GenStage.PartitionDispatcher, partitions: partitions, hash: hash}
  end

  defp start_map_reducers(type, producers, opts, change, acc, reducer) do
    {stages, opts} = Keyword.pop(opts, :stages)
    {init_opts, subscribe_opts} = Keyword.split(opts, @map_reducer_opts)

    for i <- 0..stages-1 do
      subscriptions =
        for producer <- producers do
          {producer, [partition: i] ++ subscribe_opts}
        end

      arg = {type, [subscribe_to: subscriptions] ++ init_opts, i, change, acc, reducer}
      {:ok, pid} = GenStage.start_link(GenStage.Flow.MapReducer, arg)
      pid
    end
  end

  ## Mappers

  defp start_mappers({:stages, producers}, ops, opts) do
    start_producer_consumer_mappers(producers, ops, opts)
  end

  defp start_mappers({:enumerables, enumerables}, ops, opts) do
    if Keyword.fetch!(opts, :stages) > length(enumerables) do
      producers =
        for enumerable <- enumerables do
          {:ok, pid} =
            GenStage.from_enumerable(enumerable, consumers: :permanent)
          pid
        end
      start_producer_consumer_mappers(producers, ops, opts)
    else
      start_enumerable_mappers(enumerables, ops, opts)
    end
  end

  defp start_producer_consumer_mappers(producers, ops, opts) do
    reducer = Enum.reduce(Enum.reverse(ops), &[&1 | &2], &mapper/2)
    change = fn current, acc, _index ->
      GenStage.async_notify(self(), current)
      {[], acc}
    end
    acc = fn -> [] end
    start_map_reducers(:producer_consumer, producers, opts, change, acc, fn events, [] ->
      {Enum.reverse(Enum.reduce(events, [], reducer)), []}
    end)
  end

  defp start_enumerable_mappers(enumerables, ops, opts) do
    init_opts = [consumers: :permanent] ++ Keyword.take(opts, @map_reducer_opts)

    for enumerable <- enumerables do
      enumerable =
        Enum.reduce(ops, enumerable, fn {:mapper, fun, args}, acc ->
          apply(Stream, fun, [acc | args])
        end)
      {:ok, pid} = GenStage.from_enumerable(enumerable, init_opts)
      pid
    end
  end

  # Merge mapper computations for mapper stage.
  defp mapper({:mapper, :each, [each]}, fun) do
    fn x, acc -> each.(x); fun.(x, acc) end
  end
  defp mapper({:mapper, :filter, [filter]}, fun) do
    fn x, acc ->
      if filter.(x) do
        fun.(x, acc)
      else
        acc
      end
    end
  end
  defp mapper({:mapper, :filter_map, [filter, mapper]}, fun) do
    fn x, acc ->
      if filter.(x) do
        fun.(mapper.(x), acc)
      else
        acc
      end
    end
  end
  defp mapper({:mapper, :flat_map, [flat_mapper]}, fun) do
    fn x, acc ->
      Enum.reduce(flat_mapper.(x), acc, fun)
    end
  end
  defp mapper({:mapper, :map, [mapper]}, fun) do
    fn x, acc -> fun.(mapper.(x), acc) end
  end
  defp mapper({:mapper, :reject, [filter]}, fun) do
    fn x, acc ->
      if filter.(x) do
        acc
      else
        fun.(x, acc)
      end
    end
  end

  ## Reducers

  defp start_reducers([], producers) do
    producers
  end
  defp start_reducers([{ops, opts} | rest], producers) do
    start_reducers(rest, start_reducers(ops, dispatcher(opts, rest), producers))
  end

  defp start_reducers(ops, opts, producers) do
    [{:reducer, :reduce, [acc, reducer]}] = ops
    change = fn current, acc, _index ->
      GenStage.async_notify(self(), {:stream, acc})
      GenStage.async_notify(self(), current)
      {[], acc}
    end
    start_map_reducers(:producer_consumer, producers, opts, change, acc, fn events, acc ->
      {[], Enum.reduce(events, acc, reducer)}
    end)
  end
end
