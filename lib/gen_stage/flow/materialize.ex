alias Experimental.GenStage

defmodule GenStage.Flow.Materialize do
  @moduledoc false

  @mapper_opts [:buffer_keep, :buffer_size, :dispatch]

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
    mappers = start_mappers(producers, mapper_ops, dispatcher(mapper_opts, reducer_ops), stages)
    GenStage.stream(mappers)
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
    put_in opts[:dispatcher], {GenStage.PartitionDispatcher, partitions: partitions}
  end

  ## Mappers

  defp start_mappers({:stages, stages}, ops, opts, count) do
    start_producer_consumer_mappers(stages, ops, opts, count)
  end

  defp start_mappers({:enumerables, enumerables}, ops, opts, count) do
    count = Keyword.get(opts, :stages, count)

    if count > length(enumerables) do
      stages =
        for enumerable <- enumerables do
          {:ok, pid} =
            GenStage.from_enumerable(enumerable, consumers: :permanent)
          pid
        end
      start_producer_consumer_mappers(stages, ops, opts, count)
    else
      start_enumerable_mappers(enumerables, ops, opts, count)
    end
  end

  defp start_producer_consumer_mappers(stages, ops, opts, count) do
    {count, opts} = Keyword.pop(opts, :stages, count)
    {init_opts, subscribe_opts} = Keyword.split(opts, @mapper_opts)

    producers =
      for stage <- stages do
        {stage, subscribe_opts}
      end

    init = {Enum.reduce(Enum.reverse(ops), &[&1 | &2], &mapper/2),
            [subscribe_to: producers] ++ init_opts}

    for _ <- 1..count do
      {:ok, pid} = GenStage.start_link(GenStage.Flow.Mapper, init)
      pid
    end
  end

  defp start_enumerable_mappers(enumerables, ops, opts, _count) do
    init_opts = [consumers: :permanent] ++ Keyword.take(opts, @mapper_opts)

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
end
