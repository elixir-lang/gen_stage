defmodule GenStage.DemandDispatcher do
  @moduledoc """
  A dispatcher that sends batches to the highest demand.

  This is the default dispatcher used by `GenStage`. In order
  to avoid greedy consumers, it is recommended that all consumers
  have exactly the same maximum demand.

  ## Options

  The demand dispatcher accepts the following options
  on initialization:

    * `:shuffle_demands_on_first_dispatch` - when `true`, shuffle the initial demands list
      which is constructed on subscription before first dispatch. It prevents overloading
      the first consumer on first dispatch. Defaults to `false`.

    * `:max_demand` - the maximum demand expected on `GenStage.ask/3`.
      Defaults to the first demand asked.

  ### Examples

  To start a producer with demands shuffled on first dispatch:

      {:producer, state, dispatcher: {GenStage.DemandDispatcher, shuffle_demands_on_first_dispatch: true}}
  """

  @behaviour GenStage.Dispatcher

  @doc false
  def init(opts) do
    shuffle_demand = Keyword.get(opts, :shuffle_demands_on_first_dispatch, false)
    max_demand = Keyword.get(opts, :max_demand)

    {:ok, {[], 0, max_demand, shuffle_demand}}
  end

  @doc false
  def info(msg, state) do
    send(self(), msg)
    {:ok, state}
  end

  @doc false
  def subscribe(_opts, {pid, ref}, {demands, pending, max, shuffle_demand}) do
    {:ok, 0, {demands ++ [{0, pid, ref}], pending, max, shuffle_demand}}
  end

  @doc false
  def cancel({_, ref}, {demands, pending, max, shuffle_demand}) do
    {current, demands} = pop_demand(ref, demands)
    {:ok, 0, {demands, current + pending, max, shuffle_demand}}
  end

  @doc false
  def ask(counter, {pid, ref}, {demands, pending, max, shuffle_demand}) do
    max = max || counter

    if counter > max do
      warning =
        ~c"GenStage producer DemandDispatcher expects a maximum demand of ~tp. " ++
          ~c"Using different maximum demands will overload greedy consumers. " ++
          ~c"Got demand for ~tp events from ~tp~n"

      :error_logger.warning_msg(warning, [max, counter, pid])
    end

    {current, demands} = pop_demand(ref, demands)
    demands = add_demand(current + counter, pid, ref, demands)

    already_sent = min(pending, counter)
    {:ok, counter - already_sent, {demands, pending - already_sent, max, shuffle_demand}}
  end

  @doc false
  def dispatch(events, length, {demands, pending, max, true}) do
    dispatch(events, length, {Enum.shuffle(demands), pending, max, false})
  end

  def dispatch(events, length, {demands, pending, max, false}) do
    {events, to_buffer, demands} = dispatch_demand(events, length, demands)
    {:ok, events, {demands, max(pending - to_buffer, 0), max, false}}
  end

  defp dispatch_demand([], length, demands) do
    {[], length, demands}
  end

  defp dispatch_demand(events, length, []) do
    {events, length, []}
  end

  defp dispatch_demand(events, length, [{0, _, _} | _] = demands) do
    {events, length, demands}
  end

  defp dispatch_demand(events, length, [{counter, pid, ref} | demands]) do
    {deliver_now, deliver_later, length, counter} = split_events(events, length, counter)
    Process.send(pid, {:"$gen_consumer", {self(), ref}, deliver_now}, [:noconnect])
    demands = add_demand(counter, pid, ref, demands)
    dispatch_demand(deliver_later, length, demands)
  end

  defp split_events(events, length, counter) when length <= counter do
    {events, [], 0, counter - length}
  end

  defp split_events(events, length, counter) do
    {now, later} = Enum.split(events, counter)
    {now, later, length - counter, 0}
  end

  defp add_demand(counter, pid, ref, [{current, _, _} | _] = demands) when counter > current do
    [{counter, pid, ref} | demands]
  end

  defp add_demand(counter, pid, ref, [demand | demands]) do
    [demand | add_demand(counter, pid, ref, demands)]
  end

  defp add_demand(counter, pid, ref, []) when is_integer(counter) do
    [{counter, pid, ref}]
  end

  defp pop_demand(ref, demands) do
    {{current, _pid, ^ref}, rest} = List.keytake(demands, ref, 2)
    {current, rest}
  end
end
