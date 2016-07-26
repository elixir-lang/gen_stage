alias Experimental.GenStage

defmodule GenStage.Flow.Materialize do
  @moduledoc false

  @compile :inline_list_funcs
  @map_reducer_opts [:buffer_keep, :buffer_size, :dispatcher, :trigger]
  @dispatcher_opts [:hash]

  def materialize(%{producers: nil}, _) do
    raise ArgumentError, "cannot execute a flow without producers, " <>
                         "please call \"from_enumerable\" or \"from_stage\" accordingly"
  end

  def materialize(%{operations: operations, options: options, producers: producers}, last) do
    ops = split_operations(operations, options)
    {producers, ops} = start_producers(producers, ops, last)
    {producers, start_stages(ops, producers, last)}
  end

  ## Helpers

  @doc """
  Splits the flow operations into layers of stages.
  """
  def split_operations([], _) do
    []
  end
  def split_operations(operations, opts) do
    split_operations(:lists.reverse(operations), :mapper, :none, [], opts)
  end

  @reduce "reduce/group_by"
  @map_state "map_state/each_state/emit"
  @trigger "trigger/trigger_every/window"

  defp split_operations([{:partition, opts} | ops], type, trigger, acc_ops, acc_opts) do
    [stage(type, trigger, acc_ops, acc_opts) | split_operations(ops, :mapper, :none, [], opts)]
  end

  # reducing? is false
  defp split_operations([{:mapper, _, _} = op | ops], :mapper, trigger, acc_ops, acc_opts) do
    split_operations(ops, :mapper, trigger, [op | acc_ops], acc_opts)
  end
  defp split_operations([{:map_state, _} | _], :mapper, _, _, _) do
    raise ArgumentError, "#{@map_state} must be called after a #{@reduce} operation"
  end
  defp split_operations([{:punctuation, _, _} = op| ops], :mapper, :none, acc_ops, acc_opts) do
    split_operations(ops, :mapper, op, [op | acc_ops], acc_opts)
  end
  defp split_operations([{:punctuation, _, _}| _], :mapper, _, _, _) do
    raise ArgumentError, "cannot call #{@trigger} on a flow after a #{@trigger} operation " <>
                         "(it must be called only once per partition)"
  end
  defp split_operations([{:trigger, _, _, _} = op| ops], :mapper, :none, acc_ops, acc_opts) do
    split_operations(ops, :mapper, op, acc_ops, acc_opts)
  end
  defp split_operations([{:trigger, _, _, _}| _], :mapper, _, _, _) do
    raise ArgumentError, "cannot call #{@trigger} on a flow after a #{@trigger} operation " <>
                         "(it must be called only once per partition)"
  end

  # reducing? is true
  defp split_operations([{:reduce, _, _} | _], :reducer, _, _, _) do
    raise ArgumentError, "cannot call #{@reduce} on a flow after a #{@reduce} operation " <>
                         "(it must be called only once per partition, consider using map_state/2 instead)"
  end
  defp split_operations([{:punctuation, _, _} | _], :reducer, _, _, _) do
    raise ArgumentError, "cannot call #{@trigger} on a flow after a #{@reduce} operation " <>
                         "(it must be called before reduce and only once per partition)"
  end
  defp split_operations([{:trigger, _, _, _} | _], :reducer, _, _, _) do
    raise ArgumentError, "cannot call #{@trigger} on a flow after a #{@reduce} operation " <>
                         "(it must be called before reduce and only once per partition)"
  end

  # Remaining
  defp split_operations([op | ops], _type, trigger, acc_ops, acc_opts) do
    split_operations(ops, :reducer, trigger, [op | acc_ops], acc_opts)
  end
  defp split_operations([], type, trigger, acc_ops, acc_opts) do
    [stage(type, trigger, acc_ops, acc_opts)]
  end

  defp stage(type, trigger, ops, opts) do
    opts = Keyword.put_new(opts, :stages, System.schedulers_online)
    {stage_type(type, trigger), Enum.reverse(ops), stage_opts(opts, trigger)}
  end

  defp stage_type(:mapper, trigger) when trigger != :none,
    do: raise ArgumentError, "cannot invoke #{@trigger} without a #{@reduce} operation"
  defp stage_type(type, _),
    do: type

  defp stage_opts(opts, {:trigger, _, _, _} = trigger),
    do: Keyword.put(opts, :trigger, trigger)
  defp stage_opts(opts, _),
    do: Keyword.delete(opts, :trigger)

  defp dispatcher(opts, [], {kind, kind_opts}) do
    {kind, Keyword.merge(opts, kind_opts)}
  end
  defp dispatcher(opts, [{_, _stage_ops, stage_opts} | _], _last) do
    partitions = Keyword.fetch!(stage_opts, :stages)
    dispatcher_opts = [partitions: partitions] ++ Keyword.take(stage_opts, @dispatcher_opts)
    dispatcher = {GenStage.PartitionDispatcher, [partitions: partitions] ++ dispatcher_opts}
    {:producer_consumer, put_in(opts[:dispatcher], dispatcher)}
  end

  ## Producers

  defp start_producers({:stages, producers}, ops, {_, opts}) do
    # If there are no more stages and there is a need for a custom
    # dispatcher, we need to wrap the sources in a custom stage.
    if ops == [] and Keyword.has_key?(opts, :dispatcher) do
      {producers, [{:mapper, [], []}]}
    else
      {producers, ops}
    end
  end
  defp start_producers({:enumerables, enumerables}, ops, last) do
    more_enumerables_than_stages? = more_enumerables_than_stages?(ops, enumerables)

    case ops do
      [{:mapper, mapper_ops, mapper_opts} | ops] when more_enumerables_than_stages? ->
        # Fuse mappers into enumerables if we have more enumerables than stages.
        {start_enumerables(enumerables, mapper_ops, dispatcher(mapper_opts, ops, last)), ops}
      [] ->
        # If there are no ops, we need to pass the last dispatcher info.
        {start_enumerables(enumerables, [], last), ops}
      _ ->
        # Otherwise it is a regular producer consumer with demand dispatcher.
        {start_enumerables(enumerables, [], {:producer_consumer, []}), ops}
    end
  end

  defp more_enumerables_than_stages?([{_, _, opts} | _], enumerables) do
    Keyword.fetch!(opts, :stages) < length(enumerables)
  end
  defp more_enumerables_than_stages?([], _enumerables) do
    false
  end

  defp start_enumerables(enumerables, ops, {_, opts}) do
    init_opts = [consumers: :permanent] ++ Keyword.take(opts, @map_reducer_opts)

    for enumerable <- enumerables do
      enumerable =
        :lists.foldl(fn {:mapper, fun, args}, acc ->
          apply(Stream, fun, [acc | args])
        end, enumerable, ops)
      {:ok, pid} = GenStage.from_enumerable(enumerable, init_opts)
      pid
    end
  end

  ## Stages

  defp start_stages([], producers, _last) do
    producers
  end
  defp start_stages([{type, ops, opts} | rest], producers, last) do
    start_stages(rest, start_stages(type, ops, dispatcher(opts, rest, last), producers), last)
  end

  defp start_stages(:reducer, ops, {kind, opts}, producers) do
    {reducer_acc, reducer_fun, trigger} = split_reducers(ops)
    start_stages(kind, producers, opts, trigger, reducer_acc, reducer_fun)
  end
  defp start_stages(:mapper, ops, {kind, opts}, producers) do
    reducer = :lists.foldl(&mapper/2, &[&1 | &2], :lists.reverse(ops))
    trigger = fn _acc, _index, _trigger -> [] end
    acc = fn -> [] end
    start_stages(kind, producers, opts, trigger, acc, fn events, [], _index ->
      {:lists.reverse(:lists.foldl(reducer, [], events)), []}
    end)
  end

  defp start_stages(type, producers, opts, trigger, acc, reducer) do
    {stages, opts} = Keyword.pop(opts, :stages)
    {init_opts, subscribe_opts} = Keyword.split(opts, @map_reducer_opts)

    for i <- 0..stages-1 do
      subscriptions =
        for producer <- producers do
          {producer, [partition: i] ++ subscribe_opts}
        end
      arg = {type, [subscribe_to: subscriptions] ++ init_opts, {i, stages}, trigger, acc, reducer}
      {:ok, pid} = GenStage.start_link(GenStage.Flow.MapReducer, arg)
      pid
    end
  end

  ## Reducers

  defp split_reducers(ops) do
    case take_mappers(ops, []) do
      {mappers, [{:reduce, reducer_acc, reducer_fun} | ops]} ->
        {reducer_acc, build_reducer(mappers, reducer_fun), build_trigger(ops)}
      {punctuation_mappers, [{:punctuation, punctuation_acc, punctuation_fun} | ops]} ->
        {reducer_mappers, [{:reduce, reducer_acc, reducer_fun} | ops]} = take_mappers(ops, [])
        trigger = build_trigger(ops)
        acc = fn -> {punctuation_acc.(), reducer_acc.()} end
        fun = build_punctuated_reducer(punctuation_mappers, punctuation_fun,
                                       reducer_mappers, reducer_acc, reducer_fun, trigger)
        {acc, fun, unpunctuate_trigger(trigger)}
      {mappers, ops} ->
        {fn -> [] end, build_reducer(mappers, &[&1 | &2]), build_trigger(ops)}
    end
  end

  defp build_punctuated_reducer(punctuation_mappers, punctuation_fun,
                                reducer_mappers, reducer_acc, reducer_fun, trigger) do
    pre_reducer = :lists.foldl(&mapper/2, &[&1 | &2], punctuation_mappers)
    pos_reducer = :lists.foldl(&mapper/2, reducer_fun, reducer_mappers)

    fn events, {pun_acc, red_acc}, index ->
      :lists.foldl(pre_reducer, [], events)
      |> :lists.reverse()
      |> maybe_punctuate(punctuation_fun, reducer_acc, pun_acc,
                         red_acc, pos_reducer, index, trigger, [])
    end
  end

  defp maybe_punctuate(events, punctuation_fun, reducer_acc, pun_acc,
                       red_acc, red_fun, index, trigger, acc) do
    case punctuation_fun.(events, pun_acc) do
      {:trigger, name, pre, op, pos, pun_acc} ->
        red_acc = :lists.foldl(red_fun, red_acc, pre)
        emit    = trigger.(red_acc, index, name)
        red_acc =
          case op do
            :keep  -> red_acc
            :reset -> reducer_acc.()
          end
        maybe_punctuate(pos, punctuation_fun, reducer_acc, pun_acc,
                        red_acc, red_fun, index, trigger, acc ++ emit)
      {:cont, pun_acc} ->
        {acc, {pun_acc, :lists.foldl(red_fun, red_acc, events)}}
    end
  end

  defp build_reducer(mappers, fun) do
    reducer = :lists.foldl(&mapper/2, fun, mappers)
    fn events, acc, _index ->
      {[], :lists.foldl(reducer, acc, events)}
    end
  end

  @protocol_undefined "if you would like to emit a modified state from flow, like " <>
                      "a counter or a custom data-structure, please call Flow.emit/2 accordingly"

  defp build_trigger(ops) do
    map_states = merge_mappers(ops)

    fn acc, index, name ->
      acc = :lists.foldl(& &1.(&2, index, name), acc, map_states)

      try do
        Enum.to_list(acc)
      rescue
        e in Protocol.UndefinedError ->
          msg = @protocol_undefined

          e = update_in e.description, fn
            "" -> msg
            dc -> dc <> " (#{msg})"
          end

          reraise e, System.stacktrace
      end
    end
  end

  defp unpunctuate_trigger(trigger) do
    fn {_, acc}, index, name -> trigger.(acc, index, name) end
  end

  defp merge_mappers(ops) do
    case take_mappers(ops, []) do
      {[], [{:map_state, fun} | ops]} ->
        [fun | merge_mappers(ops)]
      {[], []} ->
        []
      {mappers, ops} ->
        reducer = :lists.foldl(&mapper/2, &[&1 | &2], mappers)
        [fn old_acc, _, _ -> Enum.reduce(old_acc, [], reducer) end | merge_mappers(ops)]
    end
  end

  defp take_mappers([{:mapper, _, _} = mapper | ops], acc),
    do: take_mappers(ops, [mapper | acc])
  defp take_mappers(ops, acc),
    do: {acc, ops}

  ## Mappers

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
