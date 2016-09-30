# Usage: mix run examples/producer_consumer.exs
#
# Hit Ctrl+C twice to stop it.
#
# This is a base example where a producer A emits items,
# which are amplified by a producer consumer B and printed
# by consumer C.
alias Experimental.{GenStage}

defmodule A do
  use GenStage

  def init(counter) do
    {:producer, counter}
  end

  def handle_demand(demand, counter) when demand > 0 do
    # If the counter is 3 and we ask for 2 items, we will
    # emit the items 3 and 4, and set the state to 5.
    events = Enum.to_list(counter..counter+demand-1)
    {:noreply, events, counter + demand}
  end
end

defmodule B do
  use GenStage

  def init(number) do
    {:producer_consumer, number}
  end

  def handle_events(events, _from, number) do
    # If we receive [0, 1, 2], this will transform
    # it into [0, 1, 2, 1, 2, 3, 2, 3, 4].
    events =
      for event <- events,
          entry <- event..event+number,
          do: entry
    {:noreply, events, number}
  end
end

defmodule C do
  use GenStage

  def init(:ok) do
    {:consumer, :the_state_does_not_matter}
  end

  def handle_events(events, _from, state) do
    # Wait for a second.
    :timer.sleep(1000)

    # Inspect the events.
    IO.inspect(events)

    # We are a consumer, so we would never emit items.
    {:noreply, [], state}
  end
end

{:ok, a} = GenStage.start_link(A, 0)   # starting from zero
{:ok, b} = GenStage.start_link(B, 2)   # expand by 2
{:ok, c} = GenStage.start_link(C, :ok) # state does not matter

GenStage.sync_subscribe(b, to: a)
GenStage.sync_subscribe(c, to: b)
Process.sleep(:infinity)