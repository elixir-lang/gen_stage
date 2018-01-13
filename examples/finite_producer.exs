# Usage: mix run examples/finite_producer.exs

defmodule FiniteProducer do
  @moduledoc """
  A producer emitting a finite number of events.
  """

  use GenStage

  # Start a producer which will emit up to `total` events
  def start_link(total) do
    GenStage.start_link(__MODULE__, total, name: __MODULE__)
  end

  ## Callbacks

  def init(total) do
    {:producer, {0, total}}
  end

  def handle_demand(_demand, {produced, total}) when produced >= total  do
    IO.puts "producer: stop"

    # We have reached the end of the stream, stop the producer
    # Leftover events in the dispatcher buffer will be flushed
    {:stop, :normal, {produced, total}}
  end
  def handle_demand(demand, {produced, total}) when demand > 0 do
    # Produce up to `demand` events (less if the end of the stream is reached)
    # In this case, the events will be kept in the Dispatcher buffer
    # until `min_demand` is satisfied or the producer stops.
    left_to_produce = total - produced
    to_produce      = min(demand, left_to_produce)

    IO.puts "producer: demand=#{demand}, to_produce=#{to_produce} progress=#{produced}/#{total}"

    events = Enum.to_list(produced..(produced + to_produce - 1))
    IO.puts "producer: events=#{inspect(events)}"

    Process.sleep(500)

    {:noreply, events, {produced + to_produce, total}}
  end

end

defmodule Consumer do
  @moduledoc """
  A simple consumer, used to demonstrate that all events
  emitted by the FiniteProducer are actually consumed.
  """

  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, 0, name: __MODULE__)
  end

  ## Callbacks

  def init(consumed_so_far) do
    {:consumer, consumed_so_far}
  end

  def handle_events(events, _from, consumed_so_far) do
    total_consumed = consumed_so_far + length(events)

    IO.puts "consumer: events=#{inspect(events)}, total_consumed=#{total_consumed}"

    {:noreply, [], total_consumed}
  end
end

# In this example, the producer will stop after 23 events with a max_demand of 10
# which will cause leftover events as:
#   rem(23, 10) < min_demand, with default min_demand = div(max_demand, 2)

{:ok, producer} = FiniteProducer.start_link(23)
{:ok, consumer} = Consumer.start_link()

monitor = Process.monitor(producer)

GenStage.sync_subscribe(consumer, to: producer, max_demand: 10)

# Block until the producer terminates
receive do
  {:DOWN, ^monitor, :process, ^producer, :normal} ->
    IO.puts "producer terminated gracefully"
end
