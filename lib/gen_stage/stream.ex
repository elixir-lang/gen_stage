defmodule GenStage.Stream do
  @moduledoc false
  require GenStage.Utils, as: Utils

  def build(subscriptions, options) do
    subscriptions = :lists.map(&stream_validate_opts/1, subscriptions)

    Stream.resource(
      fn -> init_stream(subscriptions, options) end,
      &consume_stream/1,
      &close_stream/1
    )
  end

  defp stream_validate_opts({to, opts}) when is_list(opts) do
    with {:ok, max, _} <- Utils.validate_integer(opts, :max_demand, 1000, 1, :infinity, false),
         {:ok, min, _} <-
           Utils.validate_integer(opts, :min_demand, div(max, 2), 0, max - 1, false),
         {:ok, cancel, _} <-
           Utils.validate_in(opts, :cancel, :permanent, [:temporary, :transient, :permanent]) do
      {to, cancel, min, max, opts}
    else
      {:error, message} ->
        raise ArgumentError, "invalid options for #{inspect(to)} producer (#{message})"
    end
  end

  defp stream_validate_opts(to) do
    stream_validate_opts({to, []})
  end

  defp init_stream(subscriptions, options) do
    parent = self()
    demand = options[:demand] || :forward

    {monitor_pid, monitor_ref} = spawn_monitor(fn -> init_monitor(parent, demand) end)
    send(monitor_pid, {parent, monitor_ref})
    send(monitor_pid, {monitor_ref, {:subscribe, subscriptions}})

    receive do
      {:DOWN, ^monitor_ref, _, _, reason} ->
        exit(reason)

      {^monitor_ref, {:subscriptions, demand, subscriptions}} ->
        if producers = options[:producers] do
          for pid <- producers, do: GenStage.demand(pid, demand)
        else
          demand_stream_subscriptions(demand, subscriptions)
        end

        {:receive, monitor_pid, monitor_ref, subscriptions}
    end
  end

  defp demand_stream_subscriptions(demand, subscriptions) do
    Enum.each(subscriptions, fn {_, {:subscribed, pid, _, _, _, _}} ->
      GenStage.demand(pid, demand)
    end)
  end

  defp init_monitor(parent, demand) do
    parent_ref = Process.monitor(parent)

    receive do
      {:DOWN, ^parent_ref, _, _, reason} ->
        exit(reason)

      {^parent, monitor_ref} ->
        loop_monitor(parent, parent_ref, monitor_ref, demand, [])
    end
  end

  defp subscriptions_monitor(parent, monitor_ref, subscriptions) do
    fold_fun = fn {to, cancel, min, max, opts}, acc ->
      producer_pid = GenServer.whereis(to)

      cond do
        producer_pid != nil ->
          inner_ref = Process.monitor(producer_pid)
          from = {parent, {monitor_ref, inner_ref}}
          send_noconnect(producer_pid, {:"$gen_producer", from, {:subscribe, nil, opts}})
          send_noconnect(producer_pid, {:"$gen_producer", from, {:ask, max}})
          Map.put(acc, inner_ref, {:subscribed, producer_pid, cancel, min, max, max})

        cancel == :permanent or cancel == :transient ->
          exit({:noproc, {GenStage, :init_stream, [subscriptions]}})

        cancel == :temporary ->
          acc
      end
    end

    :lists.foldl(fold_fun, %{}, subscriptions)
  end

  defp loop_monitor(parent, parent_ref, monitor_ref, demand, keys) do
    receive do
      {^monitor_ref, {:subscribe, pairs}} ->
        subscriptions = subscriptions_monitor(parent, monitor_ref, pairs)
        send(parent, {monitor_ref, {:subscriptions, demand, subscriptions}})
        loop_monitor(parent, parent_ref, monitor_ref, demand, Map.keys(subscriptions) ++ keys)

      {:DOWN, ^parent_ref, _, _, reason} ->
        exit(reason)

      {:DOWN, ref, _, _, reason} ->
        if ref in keys do
          send(parent, {monitor_ref, {:DOWN, ref, reason}})
        end

        loop_monitor(parent, parent_ref, monitor_ref, demand, keys -- [ref])
    end
  end

  defp cancel_monitor(monitor_pid, monitor_ref) do
    # Cancel the old ref and get a fresh one since
    # the monitor_ref may already have been received.
    Process.demonitor(monitor_ref, [:flush])

    ref = Process.monitor(monitor_pid)
    Process.exit(monitor_pid, :kill)

    receive do
      {:DOWN, ^ref, _, _, _} ->
        flush_monitor(monitor_ref)
    end
  end

  defp flush_monitor(monitor_ref) do
    receive do
      {^monitor_ref, _} ->
        flush_monitor(monitor_ref)
    after
      0 -> :ok
    end
  end

  defp consume_stream({:receive, monitor_pid, monitor_ref, subscriptions}) do
    receive_stream(monitor_pid, monitor_ref, subscriptions)
  end

  defp consume_stream({:ask, from, ask, batches, monitor_pid, monitor_ref, subscriptions}) do
    GenStage.ask(from, ask, [:noconnect])
    deliver_stream(batches, from, monitor_pid, monitor_ref, subscriptions)
  end

  defp close_stream({:receive, monitor_pid, monitor_ref, subscriptions}) do
    request_to_cancel_stream(monitor_pid, monitor_ref, subscriptions)
    cancel_monitor(monitor_pid, monitor_ref)
  end

  defp close_stream({:ask, _, _, _, monitor_pid, monitor_ref, subscriptions}) do
    request_to_cancel_stream(monitor_pid, monitor_ref, subscriptions)
    cancel_monitor(monitor_pid, monitor_ref)
  end

  defp close_stream({:exit, reason, monitor_pid, monitor_ref, subscriptions}) do
    request_to_cancel_stream(monitor_pid, monitor_ref, subscriptions)
    cancel_monitor(monitor_pid, monitor_ref)
    exit({reason, {GenStage, :close_stream, [subscriptions]}})
  end

  defp receive_stream(monitor_pid, monitor_ref, subscriptions)
       when map_size(subscriptions) == 0 do
    {:halt, {:receive, monitor_pid, monitor_ref, subscriptions}}
  end

  defp receive_stream(monitor_pid, monitor_ref, subscriptions) do
    receive do
      {:"$gen_consumer", {producer_pid, {^monitor_ref, inner_ref} = ref}, events}
      when is_list(events) ->
        case subscriptions do
          %{^inner_ref => {:subscribed, producer_pid, cancel, min, max, demand}} ->
            from = {producer_pid, ref}
            {demand, batches} = Utils.split_batches(events, from, min, max, demand)
            subscribed = {:subscribed, producer_pid, cancel, min, max, demand}

            deliver_stream(
              batches,
              from,
              monitor_pid,
              monitor_ref,
              Map.put(subscriptions, inner_ref, subscribed)
            )

          %{^inner_ref => {:cancel, _}} ->
            # We received this message before the cancellation was processed
            receive_stream(monitor_pid, monitor_ref, subscriptions)

          _ ->
            # Cancel if messages are out of order or unknown
            msg = {:"$gen_producer", {self(), ref}, {:cancel, :unknown_subscription}}
            send_noconnect(producer_pid, msg)
            receive_stream(monitor_pid, monitor_ref, Map.delete(subscriptions, inner_ref))
        end

      {:"$gen_consumer", {_, {^monitor_ref, inner_ref}}, {:cancel, reason}} ->
        cancel_stream(inner_ref, reason, monitor_pid, monitor_ref, subscriptions)

      {:"$gen_cast", {:"$subscribe", nil, to, opts}} ->
        send(monitor_pid, {monitor_ref, {:subscribe, [stream_validate_opts({to, opts})]}})

        receive do
          {^monitor_ref, {:subscriptions, demand, new_subscriptions}} ->
            demand_stream_subscriptions(demand, new_subscriptions)
            receive_stream(monitor_pid, monitor_ref, Map.merge(subscriptions, new_subscriptions))

          {^monitor_ref, {:DOWN, inner_ref, reason}} ->
            cancel_stream(inner_ref, reason, monitor_pid, monitor_ref, subscriptions)
        end

      {:DOWN, ^monitor_ref, _, _, reason} ->
        {:halt, {:exit, reason, monitor_pid, monitor_ref, subscriptions}}

      {^monitor_ref, {:DOWN, inner_ref, reason}} ->
        cancel_stream(inner_ref, reason, monitor_pid, monitor_ref, subscriptions)
    end
  end

  defp deliver_stream([], _from, monitor_pid, monitor_ref, subscriptions) do
    receive_stream(monitor_pid, monitor_ref, subscriptions)
  end

  defp deliver_stream([{events, ask} | batches], from, monitor_pid, monitor_ref, subscriptions) do
    {events, {:ask, from, ask, batches, monitor_pid, monitor_ref, subscriptions}}
  end

  defp request_to_cancel_stream(monitor_pid, monitor_ref, subscriptions) do
    fold_fun = fn inner_ref, tuple, acc ->
      request_to_cancel_stream(inner_ref, tuple, monitor_ref, acc)
    end

    subscriptions = :maps.fold(fold_fun, subscriptions, subscriptions)
    receive_stream(monitor_pid, monitor_ref, subscriptions)
  end

  defp request_to_cancel_stream(_ref, {:cancel, _}, _monitor_ref, subscriptions) do
    subscriptions
  end

  defp request_to_cancel_stream(inner_ref, tuple, monitor_ref, subscriptions) do
    process_pid = elem(tuple, 1)
    GenStage.cancel({process_pid, {monitor_ref, inner_ref}}, :normal, [:noconnect])
    Map.put(subscriptions, inner_ref, {:cancel, process_pid})
  end

  defp cancel_stream(inner_ref, reason, monitor_pid, monitor_ref, subscriptions) do
    case subscriptions do
      %{^inner_ref => {_, _, cancel, _, _, _}}
      when cancel == :permanent
      when cancel == :transient and not Utils.is_transient_shutdown(reason) ->
        Process.demonitor(inner_ref, [:flush])
        {:halt, {:exit, reason, monitor_pid, monitor_ref, Map.delete(subscriptions, inner_ref)}}

      %{^inner_ref => _} ->
        Process.demonitor(inner_ref, [:flush])
        receive_stream(monitor_pid, monitor_ref, Map.delete(subscriptions, inner_ref))

      %{} ->
        receive_stream(monitor_pid, monitor_ref, subscriptions)
    end
  end

  defp send_noconnect(pid, msg) do
    Process.send(pid, msg, [:noconnect])
  end
end
