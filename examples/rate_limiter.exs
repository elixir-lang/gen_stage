# Usage: mix run examples/rate_limiter.exs
#
# Hit Ctrl+C twice to stop it.
#
# This is an example of using manual demand for
# doing rate limiting work on a consumer.
alias Experimental.{GenStage}

defmodule Producer do
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

defmodule RateLimiter do
  use GenStage

  def init(_) do
    {:consumer, %{}}
  end

  def handle_subscribe(:producer, opts, from, producers) do
    # We will only allow max_demand events every 5000 miliseconds
    pending = opts[:max_demand] || 1000
    interval = opts[:interval] || 5000

    # Register the producer in the state
    producers = Map.put(producers, from, {pending, interval})
    # Ask for the pending events and schedule the next time around
    producers = ask_and_schedule(producers, from)

    # Returns manual as we want control over the demand
    {:manual, producers}
  end

  def handle_cancel(_, from, producers) do
    # Remove the producers from the map on unsubscribe
    {:noreply, [], Map.delete(producers, from)}
  end

  def handle_events(events, from, producers) do
    # Bump the amount of pending events for the given producer
    producers = Map.update!(producers, from, fn {pending, interval} ->
      {pending + length(events), interval}
    end)

    # Consume the events by printing them.
    IO.inspect(events)

    # A producer_consumer would return the processed events here.
    {:noreply, [], producers}
  end

  def handle_info({:ask, from}, producers) do
    # This callback is invoked by the Process.send_after/3 message below.
    {:noreply, [], ask_and_schedule(producers, from)}
  end

  defp ask_and_schedule(producers, from) do
    case producers do
      %{^from => {pending, interval}} ->
        GenStage.ask(from, pending)
        Process.send_after(self(), {:ask, from}, interval)
        Map.put(producers, from, {0, interval})
      %{} ->
        producers
    end
  end
end

{:ok, a} = GenStage.start_link(Producer, 0)      # starting from zero
{:ok, b} = GenStage.start_link(RateLimiter, :ok) # expand by 2

# Ask for 10 items every 2 seconds.
GenStage.sync_subscribe(b, to: a, max_demand: 10, interval: 2000)
Process.sleep(:infinity)
