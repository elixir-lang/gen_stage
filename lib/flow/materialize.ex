alias Experimental.{GenStage, Flow}

defmodule Flow.Materialize do
  @moduledoc false

  @compile :inline_list_funcs
  @map_reducer_opts [:buffer_keep, :buffer_size, :dispatcher]
  @dispatcher_opts [:hash]

  def materialize(%{producers: nil}, _) do
    raise ArgumentError, "cannot execute a flow without producers, " <>
                         "please call \"from_enumerable\" or \"from_stage\" accordingly"
  end

  def materialize(%{operations: operations, options: options, producers: producers}, last) do
    options = Keyword.put_new(options, :stages, System.schedulers_online)
    ops = split_operations(operations, options)
    {producers, next, ops} = start_producers(producers, ops, options, last)
    {producers, start_stages(ops, next, last)}
  end

  ## Helpers

  @doc """
  Splits the flow operations into layers of stages.
  """
  def split_operations([], _) do
    []
  end
  def split_operations(operations, opts) do
    split_operations(:lists.reverse(operations), :mapper, false, [], opts)
  end

  @reduce "reduce/group_by"
  @map_state "map_state/each_state/emit"
  @window "window"

  defp split_operations([{:partition, opts} | ops], type, window?, acc_ops, acc_opts) do
    [stage(type, window?, acc_ops, acc_opts) | split_operations(ops, :mapper, false, [], opts)]
  end

  # reducing? is false
  defp split_operations([{:mapper, _, _} = op | ops], :mapper, window?, acc_ops, acc_opts) do
    split_operations(ops, :mapper, window?, [op | acc_ops], acc_opts)
  end
  defp split_operations([{:map_state, _} | _], :mapper, _, _, _) do
    raise ArgumentError, "#{@map_state} must be called after a #{@reduce} operation"
  end
  defp split_operations([{:window, _} = op| ops], :mapper, false, acc_ops, acc_opts) do
    split_operations(ops, :mapper, true, [op | acc_ops], acc_opts)
  end
  defp split_operations([{:window, _}| _], :mapper, true, _, _) do
    raise ArgumentError, "cannot call #{@window} on a flow after a #{@window} operation " <>
                         "(it must be called only once per partition)"
  end

  # reducing? is true
  defp split_operations([{:reduce, _, _} | _], :reducer, _, _, _) do
    raise ArgumentError, "cannot call #{@reduce} on a flow after a #{@reduce} operation " <>
                         "(it must be called only once per partition, consider using map_state/2 instead)"
  end
  defp split_operations([{:window, _} | _], :reducer, _, _, _) do
    raise ArgumentError, "cannot call #{@window} on a flow after a #{@reduce} operation " <>
                         "(it must be called before reduce and only once per partition)"
  end

  # Remaining
  defp split_operations([{:reduce, _, _} = op | ops], :mapper, false, acc_ops, acc_opts) do
    split_operations(ops, :reducer, true, [op, {:window, Flow.Window.global} | acc_ops], acc_opts)
  end
  defp split_operations([op | ops], _type, window?, acc_ops, acc_opts) do
    split_operations(ops, :reducer, window?, [op | acc_ops], acc_opts)
  end
  defp split_operations([], type, window?, acc_ops, acc_opts) do
    [stage(type, window?, acc_ops, acc_opts)]
  end

  defp stage(:mapper, true, _ops, _opts) do
    raise ArgumentError, "cannot invoke #{@window} without a #{@reduce} operation"
  end
  defp stage(type, _window?, ops, opts) do
    ops = :lists.reverse(ops)
    opts = Keyword.put_new(opts, :stages, System.schedulers_online)
    {type, stage_ops(type, ops), ops, opts}
  end

  defp stage_ops(:mapper, ops),
    do: mapper_ops(ops)
  defp stage_ops(:reducer, ops),
    do: reducer_ops(ops)

  defp dispatcher(opts, [], {kind, kind_opts}) do
    {kind, Keyword.merge(opts, kind_opts)}
  end
  defp dispatcher(opts, [{_, _compiled_ops, _stage_ops, stage_opts} | _], _last) do
    partitions = Keyword.fetch!(stage_opts, :stages)
    dispatcher_opts = [partitions: partitions] ++ Keyword.take(stage_opts, @dispatcher_opts)
    dispatcher = {GenStage.PartitionDispatcher, [partitions: partitions] ++ dispatcher_opts}
    {:producer_consumer, put_in(opts[:dispatcher], dispatcher)}
  end

  defp start_stages([], producers, _last) do
    producers
  end
  defp start_stages([{_type, compiled_ops, _ops, opts} | rest], producers, last) do
    start_stages(rest, start_stage(compiled_ops, dispatcher(opts, rest, last), producers), last)
  end

  defp start_stage({acc, reducer, trigger}, {type, opts}, producers) do
    {stages, opts} = Keyword.pop(opts, :stages)
    {init_opts, subscribe_opts} = Keyword.split(opts, @map_reducer_opts)

    for i <- 0..stages-1 do
      subscriptions =
        for {producer, producer_opts} <- producers do
          {producer, [partition: i] ++ Keyword.merge(subscribe_opts, producer_opts)}
        end
      arg = {type, [subscribe_to: subscriptions] ++ init_opts, {i, stages}, trigger, acc, reducer}
      {:ok, pid} = GenStage.start_link(Flow.MapReducer, arg)
      {pid, []}
    end
  end

  ## Producers

  defp start_producers({:join, bounded, kind, left, right, left_key, right_key, join}, ops, options, _) do
    partitions = Keyword.fetch!(options, :stages)
    {left_producers, left_consumers} = start_join(:left, left, left_key, partitions)
    {right_producers, right_consumers} = start_join(:right, right, right_key, partitions)
    [{type, {acc, fun, trigger}, ops, options} | rest] = at_least_one_ops(ops, options)
    {left_producers ++ right_producers,
     left_consumers ++ right_consumers,
     [{type, join_ops(bounded, kind, join, acc, fun, trigger), ops, options} | rest]}
  end
  defp start_producers({:stages, producers}, ops, options, {_, last_opts}) do
    producers = for producer <- producers, do: {producer, []}

    # If there are no more stages and there is a need for a custom
    # dispatcher, we need to wrap the sources in a custom stage.
    if Keyword.has_key?(last_opts, :dispatcher) do
      {producers, producers, at_least_one_ops(ops, options)}
    else
      {producers, producers, ops}
    end
  end
  defp start_producers({:enumerables, enumerables}, ops, options, last) do
    # options configures all stages before partition, so it effectively
    # controls the number of stages consuming the enumerables.
    stages = Keyword.fetch!(options, :stages)

    case ops do
      [{:mapper, _compiled_ops, mapper_ops, mapper_opts} | ops] when stages < length(enumerables) ->
        # Fuse mappers into enumerables if we have more enumerables than stages.
        # In this case, mapper_opts contains the given options.
        producers = start_enumerables(enumerables, mapper_ops, dispatcher(mapper_opts, ops, last))
        {producers, producers, ops}
      [] ->
        # If there are no ops, we need to pass the last dispatcher info.
        # In this case, options are discarded because there is no producer_consumer.
        producers = start_enumerables(enumerables, [], last)
        {producers, producers, ops}
      _ ->
        # Otherwise it is a regular producer consumer with demand dispatcher.
        # In this case, options is used by subsequent mapper/reducer stages.
        producers = start_enumerables(enumerables, [], {:producer_consumer, []})
        {producers, producers, ops}
    end
  end

  defp start_enumerables(enumerables, ops, {_, opts}) do
    init_opts = [consumers: :permanent, demand: :accumulate] ++ Keyword.take(opts, @map_reducer_opts)

    for enumerable <- enumerables do
      enumerable =
        :lists.foldl(fn {:mapper, fun, args}, acc ->
          apply(Stream, fun, [acc | args])
        end, enumerable, ops)
      {:ok, pid} = GenStage.from_enumerable(enumerable, init_opts)
      {pid, []}
    end
  end

  defp at_least_one_ops([], options),
    do: [stage(:mapper, :none, [], options)]
  defp at_least_one_ops(ops, _opts),
    do: ops

  ## Joins

  defp start_join(side, flow, key_fun, partitions) do
    hash = fn event, count ->
      key = key_fun.(event)
      {{key, event}, :erlang.phash2(key, count)}
    end

    opts = [dispatcher: {GenStage.PartitionDispatcher, partitions: partitions, hash: hash}]
    {producers, consumers} = materialize(flow, {:producer_consumer, opts})

    {producers,
      for {consumer, consumer_opts} <- consumers do
        {consumer, [tag: side] ++ consumer_opts}
      end}
  end

  defp join_ops(:bounded, kind, join, acc, fun, trigger) do
    acc = fn -> {%{}, %{}, acc.()} end
    events = fn tag, events, {left, right, acc}, index ->
      {events, left, right} = dispatch_join(events, tag, left, right, join, [])
      {events, acc} = fun.(events, acc, index)
      {events, {left, right, acc}}
    end
    trigger = fn {left, right, acc}, index, op, name ->
      {kind_events, acc} =
        case kind do
          :inner ->
            {[], acc}
          :left_outer ->
            fun.(left_events(Map.keys(left), Map.keys(right), left, join), acc, index)
          :right_outer ->
            fun.(right_events(Map.keys(right), Map.keys(left), right, join), acc, index)
          :full_outer ->
            left_keys = Map.keys(left)
            right_keys = Map.keys(right)
            {left_events, acc} = fun.(left_events(left_keys, right_keys, left, join), acc, index)
            {right_events, acc} = fun.(right_events(right_keys, left_keys, right, join), acc, index)
            {left_events ++ right_events, acc}
        end
      {trigger_events, acc} = trigger.(acc, index, op, name)
      {kind_events ++ trigger_events, {left, right, acc}}
    end
    {acc, events, trigger}
  end

  defp left_events(left, right, source, join) do
    for key <- left -- right, entry <- Map.fetch!(source, key), do: join.(entry, nil)
  end

  defp right_events(right, left, source, join) do
    for key <- right -- left, entry <- Map.fetch!(source, key), do: join.(nil, entry)
  end

  defp dispatch_join([{key, left} | rest], :left, left_acc, right_acc, join, acc) do
    acc =
      case right_acc do
        %{^key => rights} ->
          :lists.foldl(fn right, acc -> [join.(left, right) | acc] end, acc, rights)
        %{} -> acc
      end
    left_acc = Map.update(left_acc, key, [left], &[&1 | left])
    dispatch_join(rest, :left, left_acc, right_acc, join, acc)
  end
  defp dispatch_join([{key, right} | rest], :right, left_acc, right_acc, join, acc) do
    acc =
      case left_acc do
        %{^key => lefties} ->
          :lists.foldl(fn left, acc -> [join.(left, right) | acc] end, acc, lefties)
        %{} -> acc
      end
    right_acc = Map.update(right_acc, key, [right], &[&1 | right])
    dispatch_join(rest, :right, left_acc, right_acc, join, acc)
  end
  defp dispatch_join([], _, left_acc, right_acc, _join, acc) do
    {:lists.reverse(acc), left_acc, right_acc}
  end

  ## Reducers

  defp reducer_ops(ops) do
    case take_mappers(ops, []) do
      {mappers, [{:reduce, reducer_acc, reducer_fun} | ops]} ->
        {reducer_acc, build_reducer(mappers, reducer_fun), build_trigger(ops, reducer_acc)}
      {mappers, [{:window, window} | ops]} ->
        {reducer_acc, reducer_fun, trigger} = reducer_ops(ops)
        window_ops(window, mappers, reducer_acc, reducer_fun, trigger)
      {mappers, ops} ->
        {fn -> [] end, build_reducer(mappers, &[&1 | &2]), build_trigger(ops, fn -> [] end)}
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

  defp build_trigger(ops, acc_fun) do
    map_states = merge_map_state(ops)

    fn acc, index, op, name ->
      events = :lists.foldl(& &1.(&2, index, name), acc, map_states)

      try do
        Enum.to_list(events)
      rescue
        e in Protocol.UndefinedError ->
          msg = @protocol_undefined

          e = update_in e.description, fn
            "" -> msg
            dc -> dc <> " (#{msg})"
          end

          reraise e, System.stacktrace
      else
        events ->
          case op do
            :keep -> {events, acc}
            :reset -> {events, acc_fun.()}
          end
      end
    end
  end

  defp merge_map_state(ops) do
    case take_mappers(ops, []) do
      {[], [{:map_state, fun} | ops]} ->
        [fun | merge_map_state(ops)]
      {[], []} ->
        []
      {mappers, ops} ->
        reducer = :lists.foldl(&mapper/2, &[&1 | &2], mappers)
        [fn old_acc, _, _ -> Enum.reduce(old_acc, [], reducer) end | merge_map_state(ops)]
    end
  end

  ## Windows

  defp window_ops(%Flow.Window{trigger: trigger, periodically: periodically},
                  mappers, reducer_acc, reducer_fun, reducer_trigger) do
    {window_acc, window_fun, window_trigger} =
      window_trigger(trigger, mappers, reducer_acc, reducer_fun, reducer_trigger)

    {window_periodically(window_acc, periodically), window_fun, window_trigger}
  end

  defp window_periodically(window_acc, []) do
    window_acc
  end
  defp window_periodically(window_acc, periodically) do
    fn ->
      for {time, keep_or_reset, name} <- periodically do
        {:ok, _} = :timer.send_interval(time, self(), {:trigger, keep_or_reset, name})
      end
      window_acc.()
    end
  end

  defp window_trigger(nil, [], reducer_acc, reducer_fun, reducer_trigger) do
    {reducer_acc,
     reducer_fun,
     build_unpunctuated_trigger(reducer_trigger)}
  end
  defp window_trigger(nil, mappers, reducer_acc, reducer_fun, reducer_trigger) do
    {reducer_acc,
     build_unpunctuated_reducer(mappers, reducer_fun),
     build_unpunctuated_trigger(reducer_trigger)}
  end
  defp window_trigger({punctuation_acc, punctuation_fun}, mappers,
                      reducer_acc, reducer_fun, reducer_trigger) do
    {fn -> {punctuation_acc.(), reducer_acc.()} end,
     build_punctuated_reducer(mappers, punctuation_fun, reducer_fun, reducer_trigger),
     build_punctuated_trigger(reducer_trigger)}
  end

  defp build_unpunctuated_reducer(mappers, reducer_fun) do
    reducer = :lists.foldl(&mapper/2, &[&1 | &2], mappers)
    fn events, acc, index ->
      :lists.foldl(reducer, [], events)
      |> :lists.reverse()
      |> reducer_fun.(acc, index)
    end
  end

  defp build_unpunctuated_trigger(trigger) do
    fn acc, index, op, name ->
      trigger.(acc, index, op, {:global, :global, name})
    end
  end

  defp build_punctuated_reducer(punctuation_mappers, punctuation_fun, red_fun, trigger) do
    pre_reducer = :lists.foldl(&mapper/2, &[&1 | &2], punctuation_mappers)

    fn events, {pun_acc, red_acc}, index ->
      :lists.foldl(pre_reducer, [], events)
      |> :lists.reverse()
      |> maybe_punctuate(punctuation_fun, pun_acc, red_acc, red_fun, index, trigger, [])
    end
  end

  defp build_punctuated_trigger(trigger) do
    fn {trigger_acc, red_acc}, index, op, name ->
      {events, red_acc} = trigger.(red_acc, index, op, {:global, :global, name})
      {events, {trigger_acc, red_acc}}
    end
  end

  defp maybe_punctuate(events, punctuation_fun, pun_acc, red_acc,
                       red_fun, index, trigger, collected) do
    case punctuation_fun.(events, pun_acc) do
      {:trigger, name, pre, op, pos, pun_acc} ->
        {red_events, red_acc} = red_fun.(pre, red_acc, index)
        {trigger_events, red_acc} = trigger.(red_acc, index, op, {:global, :global, name})
        maybe_punctuate(pos, punctuation_fun, pun_acc, red_acc,
                        red_fun, index, trigger, collected ++ trigger_events ++ red_events)
      {:cont, pun_acc} ->
        {red_events, red_acc} = red_fun.(events, red_acc, index)
        {collected ++ red_events, {pun_acc, red_acc}}
    end
  end

  ## Mappers

  defp mapper_ops(ops) do
    reducer = :lists.foldl(&mapper/2, &[&1 | &2], :lists.reverse(ops))
    {fn -> [] end,
     fn events, [], _index -> {:lists.reverse(:lists.foldl(reducer, [], events)), []} end,
     fn _acc, _index, _op, _trigger -> {[], []} end}
  end

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

  defp take_mappers([{:mapper, _, _} = mapper | ops], acc),
    do: take_mappers(ops, [mapper | acc])
  defp take_mappers(ops, acc),
    do: {acc, ops}
end
