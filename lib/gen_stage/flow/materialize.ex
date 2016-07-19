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

  def to_stream(%{operations: operations, options: options, producers: producers}) do
    ops = split_operations(operations, options)
    {producers, ops} = start_producers(producers, ops)
    consumers = start_stages(ops, :stream, producers)
    GenStage.stream(consumers)
  end

  ## Helpers

  @doc """
  Splits the flow operations into layers of stages.
  """
  def split_operations([], _) do
    []
  end
  def split_operations(operations, opts) do
    split_operations(Enum.reverse(operations), :mapper, [], opts)
  end

  defp split_operations([{:partition, opts} | ops], type, acc_ops, acc_opts) do
    [stage(type, acc_ops, acc_opts) | split_operations(ops, :mapper, [], opts)]
  end
  defp split_operations([{:mapper, _, _} = op | ops], :mapper, acc_ops, acc_opts) do
    split_operations(ops, :mapper, [op | acc_ops], acc_opts)
  end
  defp split_operations([op | ops], _type, acc_ops, acc_opts) do
    split_operations(ops, :reducer, [op | acc_ops], acc_opts)
  end
  defp split_operations([], type, acc_ops, acc_opts) do
    [stage(type, acc_ops, acc_opts)]
  end

  defp stage(type, ops, opts) do
    {type, Enum.reverse(ops), Keyword.put_new(opts, :stages, System.schedulers_online)}
  end

  defp dispatcher(opts, []), do: opts
  defp dispatcher(opts, [{_, _stage_ops, stage_opts} | _]) do
    partitions = Keyword.fetch!(stage_opts, :stages)
    hash = Keyword.get(stage_opts, :hash, &:erlang.phash2/2)
    put_in opts[:dispatcher], {GenStage.PartitionDispatcher, partitions: partitions, hash: hash}
  end

  ## Stages

  defp start_stages([], _last, producers) do
    producers
  end
  defp start_stages([{type, ops, opts} | rest], last, producers) do
    next = if rest == [], do: last, else: :stage
    start_stages(rest, last, start_stages(type, next, ops, dispatcher(opts, rest), producers))
  end

  defp start_stages(:reducer, next, ops, opts, producers) do
    type = if next == :nothing, do: :consumer, else: :producer_consumer
    {reducer_acc, reducer_fun, map_stages} = split_reducers(ops)

    change =
      fn current, acc, index ->
        acc =
          Enum.reduce(map_stages, acc, & &1.(&2, index))

        events =
          case next do
            :stage ->
              GenStage.async_notify(self(), current)
              acc
            :stream ->
              GenStage.async_notify(self(), {:enumerable, acc})
              GenStage.async_notify(self(), current)
              []
            :nothing ->
              []
          end

        {events, reducer_acc.()}
      end

    start_stages(type, producers, opts, change, reducer_acc, fn events, reducer_acc ->
      {[], Enum.reduce(events, reducer_acc, reducer_fun)}
    end)
  end
  defp start_stages(:mapper, _next, ops, opts, producers) do
    reducer = Enum.reduce(Enum.reverse(ops), &[&1 | &2], &mapper/2)
    change = fn current, acc, _index ->
      GenStage.async_notify(self(), current)
      {[], acc}
    end
    acc = fn -> [] end
    start_stages(:producer_consumer, producers, opts, change, acc, fn events, [] ->
      {Enum.reverse(Enum.reduce(events, [], reducer)), []}
    end)
  end

  defp start_stages(type, producers, opts, change, acc, reducer) do
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

  ## Reducers

  defp split_reducers(ops) do
    {acc, fun, ops} = merge_reducer([], merge_mappers(ops))
    {acc, fun, Enum.map(ops, &build_map_stage/1)}
  end

  defp build_map_stage({:reduce, acc, fun}) do
    fn old_acc, _ -> Enum.reduce(old_acc, acc.(), fun) end
  end
  defp build_map_stage({:map_stage, fun}) do
    fun
  end

  defp merge_mappers(ops) do
    case take_mappers(ops, []) do
      {[], [op | ops]} ->
        [op | merge_mappers(ops)]
      {[], []} ->
        []
      {mappers, ops} ->
        {acc, fun, ops} = merge_reducer(mappers, ops)
        [{:reduce, acc, fun} | merge_mappers(ops)]
    end
  end

  defp merge_reducer(mappers, [{:reduce, acc, fun} | ops]) do
    {acc, Enum.reduce(mappers, fun, &mapper/2), ops}
  end
  defp merge_reducer(mappers, ops) do
    {fn -> [] end, Enum.reduce(mappers, &[&1 | &2], &mapper/2), ops}
  end

  defp take_mappers([{:mapper, _, _} = mapper | ops], acc),
    do: take_mappers(ops, [mapper | acc])
  defp take_mappers(ops, acc),
    do: {acc, ops}

  ## Mappers

  defp start_producers({:stages, producers}, ops) do
    {producers, ops}
  end
  defp start_producers({:enumerables, enumerables}, ops) do
    more_enumerables_than_stages? = more_enumerables_than_stages?(ops, enumerables)

    # Fuse mappers into enumerables if we have more enumerables than stages.
    case ops do
      [{:mapper, mapper_ops, mapper_opts} | ops] when more_enumerables_than_stages? ->
        {start_enumerables(enumerables, mapper_ops, dispatcher(mapper_opts, ops)), ops}
      _ ->
        {start_enumerables(enumerables, [], []), ops}
    end
  end

  defp more_enumerables_than_stages?([{_, _, opts} | _], enumerables) do
    Keyword.fetch!(opts, :stages) < length(enumerables)
  end
  defp more_enumerables_than_stages?([], _enumerables) do
    false
  end

  defp start_enumerables(enumerables, ops, opts) do
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
end
