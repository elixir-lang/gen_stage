# Usage: mix run examples/dynamic_supervisor.exs
#
# Hit Ctrl+C twice to stop it.

defmodule Counter do
  @moduledoc """
  This is a simple producer that counts from the given
  number whenever there is a demand.
  """

  use GenStage

  def start_link(initial) when is_integer(initial) do
    GenStage.start_link(__MODULE__, initial, name: __MODULE__)
  end

  ## Callbacks

  def init(initial) do
    {:producer, initial}
  end

  def handle_demand(demand, counter) when demand > 0 do
    # If the counter is 3 and we ask for 2 items, we will
    # emit the items 3 and 4, and set the state to 5.
    events = Enum.to_list(counter..counter+demand-1)
    {:noreply, events, counter + demand}
  end
end

defmodule Consumer do
  @moduledoc """
  A consumer will be a consumer supervisor that will
  spawn printer tasks for each event.
  """

  use ConsumerSupervisor

  def start_link() do
    ConsumerSupervisor.start_link(__MODULE__, :ok)
  end

  # Callbacks

  def init(:ok) do
    children = [
      worker(Printer, [], restart: :temporary)
    ]

    {:ok, children, strategy: :one_for_one, subscribe_to: [{Counter, max_demand: 50}]}
  end
end

defmodule Printer do
  def start_link(event) do
    Task.start_link(fn ->
      IO.inspect {self(), event}
    end)
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
      worker(Counter, [0]),
      # We can add as many consumer supervisors as consumers as we want!
      worker(Consumer, [], id: 1)
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

# Start the app and wait forever
App.start
Process.sleep(:infinity)
