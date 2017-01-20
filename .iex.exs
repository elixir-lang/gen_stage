defmodule GenStageConsoleWatcher do
  defmacro __using__(_) do
    quote location: :keep do
      def handle_metrics(:handle_events, [data|t], stageinfo) do
        IO.puts "#{stageinfo.mod} got events! #{length(data)} of them"
        {:noreply}
      end

      def handle_metrics(:handle_subscribe, [:consumer], stageinfo) do
        count = stageinfo.consumers |> Map.keys |> length
        IO.puts "#{stageinfo.mod} got a new subscriber! We got #{count} so far!"
        {:noreply}
      end

      def handle_metrics(:handle_subscribe, [:producer], stageinfo) do
        IO.puts "#{stageinfo.mod} subscribed successfully!"
        {:noreply}
      end

      def handle_metrics(:handle_demand, [demanded, owed], stageinfo) do
        IO.puts "#{stageinfo.mod} got demand! Was asked #{demanded}, but #{owed} so far"
        {:noreply}
      end
    end
  end
end

defmodule A do
  use GenStage
  use GenStageConsoleWatcher

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
  use GenStageConsoleWatcher

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
  use GenStageConsoleWatcher

  def init(:ok) do
    {:consumer, :the_state_does_not_matter}
  end

  def handle_events(events, _from, state) do
    # Wait for a second.
    :timer.sleep(1000)

    # Inspect the events.
    #IO.inspect(events)

    # We are a consumer, so we would never emit items.
    {:noreply, [], state}
  end
end
# :debugger.start()
# :int.ni(GenStage)
# :int.break(GenStage, 2163)

{:ok, a} = GenStage.start_link(A, 0)   # starting from zero
{:ok, b} = GenStage.start_link(B, 2)   # expand by 2
{:ok, c} = GenStage.start_link(C, :ok) # state does not matter

GenStage.sync_subscribe(b, to: a)
GenStage.sync_subscribe(c, to: a)
#Process.sleep(:infinity)
