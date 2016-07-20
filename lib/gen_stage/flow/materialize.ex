alias Experimental.GenStage

defmodule GenStage.Flow.Materialize do
  @moduledoc false

  @map_reducer_opts [:buffer_keep, :buffer_size, :dispatcher]

  @doc """
  Materializes a flow for stream consumption.
  """
  def to_stream(%{producers: nil}) do
    raise ArgumentError, "cannot enumerate a flow without producers, " <>
                         "please call `from_enumerable` or `from_stage` accordingly"
  end

  def to_stream(%{operations: operations, options: options, producers: producers}) do
    ops = split_operations(operations, options)
    {producers, ops} = start_producers(producers, ops)
    consumers = start_stages(ops, producers)
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

  @reduce "reduce/group_by"
  @map_state "map_state/each_state"

  defp split_operations([{:partition, opts} | ops], type, acc_ops, acc_opts) do
    [stage(type, acc_ops, acc_opts) | split_operations(ops, :mapper, [], opts)]
  end
  defp split_operations([{:mapper, _, _} = op | ops], :mapper, acc_ops, acc_opts) do
    split_operations(ops, :mapper, [op | acc_ops], acc_opts)
  end
  defp split_operations([{:reduce, _, _} | _], :reducer, _, _) do
    raise ArgumentError, "cannot call #{@reduce} on flow after a #{@reduce} operation without repartitioning the data"
  end
  defp split_operations([{:map_state, _} | _], :mapper, _, _) do
    raise ArgumentError, "#{@reduce} must be called before calling #{@map_state}"
  end
  defp split_operations([op | ops], _type, acc_ops, acc_opts) do
    split_operations(ops, :reducer, [op | acc_ops], acc_opts)
  end
  defp split_operations([], type, acc_ops, acc_opts) do
    [stage(type, acc_ops, acc_opts)]
  end

  defp stage(type, ops, opts) do
    opts =
      opts
      |> Keyword.put_new(:stages, System.schedulers_online)
      |> Keyword.put_new(:emit, :events)
    {stage_type(type, Keyword.fetch!(opts, :emit)), Enum.reverse(ops), opts}
  end

  defp stage_type(type, :events), do: type
  defp stage_type(_, :state), do: :reducer
  defp stage_type(_, other), do: raise ArgumentError, "unknown value #{inspect other} for :emit"

  defp dispatcher(opts, []), do: opts
  defp dispatcher(opts, [{_, _stage_ops, stage_opts} | _]) do
    partitions = Keyword.fetch!(stage_opts, :stages)
    hash = Keyword.get(stage_opts, :hash, &:erlang.phash2/2)
    put_in opts[:dispatcher], {GenStage.PartitionDispatcher, partitions: partitions, hash: hash}
  end

  ## Stages

  defp start_stages([], producers) do
    producers
  end
  defp start_stages([{type, ops, opts} | rest], producers) do
    start_stages(rest, start_stages(type, ops, dispatcher(opts, rest), producers))
  end

  @protocol_undefined "if you would like to emit a modified state from flow, like " <>
                      "a counter or a custom data-structure, please set the :emit " <>
                      "option on Flow.new/1 or Flow.partition/2 accordingly"

  defp start_stages(:reducer, ops, opts, producers) do
    {emit, opts} = Keyword.pop(opts, :emit)
    {reducer_acc, reducer_fun, map_states} = split_reducers(ops)

    trigger =
      fn acc, index ->
        acc =
          Enum.reduce(map_states, acc, & &1.(&2, index))

        events =
          case emit do
            :events ->
              try do
                Enum.to_list(acc)
              rescue
                e in Protocol.UndefinedError ->
                  msg = @protocol_undefined

                  e = update_in e.description, fn
                    "" -> msg
                    ot -> ot <> " (#{msg})"
                  end

                  reraise e, System.stacktrace
              end
            :state ->
              [acc]
          end

        {events, reducer_acc.()}
      end

    start_stages(:producer_consumer, producers, opts, trigger, reducer_acc, fn events, reducer_acc ->
      {[], Enum.reduce(events, reducer_acc, reducer_fun)}
    end)
  end
  defp start_stages(:mapper, ops, opts, producers) do
    reducer = Enum.reduce(Enum.reverse(ops), &[&1 | &2], &mapper/2)
    trigger = fn acc, _index -> {[], acc} end
    acc = fn -> [] end
    start_stages(:producer_consumer, producers, opts, trigger, acc, fn events, [] ->
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
      arg = {type, [subscribe_to: subscriptions] ++ init_opts, {i, stages}, change, acc, reducer}
      {:ok, pid} = GenStage.start_link(GenStage.Flow.MapReducer, arg)
      pid
    end
  end

  ## Reducers

  defp split_reducers(ops) do
    {acc, fun, ops} = merge_reducer([], merge_mappers(ops))
    {acc, fun, Enum.map(ops, &build_map_state/1)}
  end

  defp build_map_state({:reduce, acc, fun}) do
    fn old_acc, _ -> Enum.reduce(old_acc, acc.(), fun) end
  end
  defp build_map_state({:map_state, fun}) do
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
