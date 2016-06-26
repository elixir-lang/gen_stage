# Usage: mix run examples/gen_event.exs

defmodule Broadcaster do
  @moduledoc """
  Using a GenStage for implementing a GenEvent replacement
  where each handler runs as a separate process.

  This is the GenEvent manager implementation. It is around
  30 LOC without docs and comments.
  """

  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  This will reply after events are dispatched or after
  events are buffered (in case there is no demand).
  """
  def sync_notify(events, timeout \\ 5000) when is_list(events) do
    GenStage.call(__MODULE__, {:notify, events}, timeout)
  end

  @doc """
  Similar to sync but there is no delivery guarantee (cast).
  """
  def async_notify(events) when is_list(events) do
    GenStage.cast(__MODULE__, {:notify, events})
  end

  ## Callbacks

  def init(:ok) do
    {:producer, :ok,
      dispatcher: GenStage.BroadcastDispatcher, # We want to broadcast events
      buffer_size: 1000} # The size of the buffer for when there are no consumers
  end

  def handle_call({:notify, events}, _from, state) do
    {:reply, :ok, events, state}
  end

  def handle_cast({:notify, events}, state) do
    {:noreply, events, state}
  end

  def handle_demand(_demand, state) do
    # All the events come from cast/call.
    # So if there is demand, there is nothing we can do.
    {:noreply, [], state}
  end
end

defmodule Consumer do
  @moduledoc """
  The GenEvent handler implementation is a simple consumer.
  """

  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, :ok)
  end

  # Callbacks

  def init(:ok) do
    # Starts a permanent subscription to the broadcaster,
    # demand will automatically flow upstream.
    {:consumer, :ok, subscribe_to: [Broadcaster]}
  end

  def handle_events(events, _from, state) do
    for event <- events do
      IO.inspect {self(), event}
    end
    {:noreply, [], state}
  end
end

defmodule App do
  @moduledoc """
  Your application entry-point.

  For actual applications, start/0 should be start/2.
  """

  def start do
    import Supervisor.Spec

    children = [
      worker(Broadcaster, []),
      worker(Consumer, [], id: 1),
      worker(Consumer, [], id: 2),
      worker(Consumer, [], id: 3),
      worker(Consumer, [], id: 4)
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

# Start the app
App.start

# Broadcast events
Broadcaster.sync_notify([1, 2, 3, 4, 5])
Broadcaster.async_notify([:a, :b, :c, :d, :e])

# Wait for them to be printed
Process.sleep(2000)