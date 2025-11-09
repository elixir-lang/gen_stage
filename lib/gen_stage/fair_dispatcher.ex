alias Experimental.GenStage

defmodule GenStage.FairDispatcher do
  @moduledoc """
  A dispatcher that sends batches to consumers using round robin.

  ## Options

  The partition dispatcher accepts the following options
  on initialization:

    * `:batch` - the maximum batch size before moving on to the next consumer
      when dispatching. For example `batch: 5` will send at most 5 events to a
      consumer at a time when there are other consumers.
  """

  @behaviour GenStage.Dispatcher

  @doc false
  def init(opts) do
    {:ok, {:queue.new(), %{}, 0, 0, batch_size(opts)}}
  end

  @doc false
  def notify(msg, {_, demands, _, _, _} = state) do
    Enum.each(demands, fn {ref, {pid, _}} ->
      Process.send(pid, {:"$gen_consumer", {self(), ref}, {:notification, msg}}, [:noconnect])
    end)
    {:ok, state}
  end

  @doc false
  def subscribe(_, {pid, ref}, {robin, demands, held, pending, batch}) do
    robin = :queue.in(ref, robin)
    demands = Map.put(demands, ref, {pid, 0})
    {:ok, 0, {robin, demands, held, pending, batch}}
  end

  @doc false
  def cancel({_, ref}, {robin, demands, held, pending, batch}) do
    robin = :queue.filter(&(&1 != ref), robin)
    {{_, current}, demands} = Map.pop(demands, ref)
    {:ok, 0, {robin, demands, held-current, current+pending, batch}}
  end

  @doc false
  def ask(counter, {_, ref}, {robin, demands, held, pending, batch}) do
    update = fn({pid, current}) -> {pid, current+counter} end
    demands = Map.update!(demands, ref, update)

    already_sent = min(pending, counter)

    new = counter-already_sent
    held = held+counter
    pending = pending-already_sent

    {:ok, new, {robin, demands, held, pending, batch}}
  end

  @doc false
  def dispatch(events, {robin, demands, held, pending, batch}) do
    {events, robin, demands, held} =
      dispatch(events, robin, demands, held, batch)
    {:ok, events, {robin, demands, held, pending, batch}}
  end

  defp batch_size(opts) do
    case Keyword.get(opts, :batch, 100) do
      batch when is_integer(batch) and batch > 0 ->
        batch
      batch ->
        msg = "expected :batch to be a positive integer, got #{inspect batch}"
        raise ArgumentError, msg
    end
  end

  defp dispatch(events, robin, demands, 0, _batch) do
    {events, robin, demands, 0}
  end

  defp dispatch([], robin, demands, held, _batch) do
    {[], robin, demands, held}
  end

  defp dispatch(events, robin, demands, held, batch) do
    {{:value, ref}, robin} = :queue.out(robin)
    dispatch_to(ref, events, :queue.in(ref, robin), demands, held, batch)
  end

  defp dispatch_to(ref, events, robin, demands, held, batch) do
    case demands do
      %{^ref => {_pid, 0}} ->
        dispatch(events, robin, demands, held, batch)
      %{^ref => {pid, counter}} ->
        {deliver_now, deliver_later} = split_events(events, counter, held, batch)
        Process.send(pid, {:"$gen_consumer", {self(), ref}, deliver_now}, [:noconnect])
        len = length(deliver_now)
        counter = counter - len
        held = held - len
        demands = %{demands | ref => {pid, counter}}
        dispatch(deliver_later, robin, demands, held, batch)
    end
  end

  defp split_events(events, held, held, _batch) do
    Enum.split(events, held)
  end
  defp split_events(events, counter, _held, batch) do
    Enum.split(events, min(counter, batch))
  end
end
