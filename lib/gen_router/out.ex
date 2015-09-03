defmodule GenRouter.Out do
  @moduledoc """
  Specifies the outgoing part of a router.
  """

  # TODO: Define __using__

  @doc """
  Invoked when the router is started.
  """
  @callback init(args :: term) ::
            {:ok, state :: term}

  @doc """
  Invoked when a sink asks for data.

  Must return a non negative integer (>= 0) to signal the
  demand upstream. 0 means no demand.

  It may optionally return a list of events to dispatch.
  Particularly useful when events have been buffering
  and they are now ready to be dispatched once the demand
  arrived.
  """
  @callback handle_demand(demand :: pos_integer, sink :: {pid, reference}, state :: term) ::
            {:ok, non_neg_integer, new_state :: term} |
            {:ok, non_neg_integer, [event], new_state :: term} |
            {:error, reason :: term, new_state :: term} |
            {:error, reason :: term, [event], new_state :: term} |
            {:stop, reason :: term, new_state :: term} |
            {:stop, reason :: term, [event], new_state :: term}

  @doc """
  Invoked when a sink cancels subscription or crashes.
  """
  @callback handle_down(reason :: term, sink :: {pid, reference}, state :: term) ::
            {:ok, new_state :: term} |
            {:stop, reason :: term, new_state :: term}

  @doc """
  Specifies to which process(es) an event should be dispatched to.

  Returns a list with references that identify existing sinks.
  """
  @callback handle_dispatch(event :: term, state :: term) ::
            {:ok, [reference], new_state :: term} |
            {:stop, reason :: term, [reference], new_state :: term} |
            {:stop, reason :: term, new_state :: term}

  @doc """
  Called when the router is about to terminate, useful for cleaning up.

  It must return `:ok`. If part of a supervision tree, terminate only gets
  called if the router is set to trap exits using `Process.flag/2` *and*
  the shutdown strategy of the Supervisor is a timeout value, not `:brutal_kill`.
  The callback is also not invoked if links are broken unless trapping exits.
  For such reasons, we usually recommend important clean-up rules to happen
  in separated processes either by use of monitoring or by links themselves.
  """
  @callback terminate(reason :: :normal | :shutdown | {:shutdown, term} | term, state :: term) ::
            term()

  @doc """
  Called when the router code is being upgraded live (hot code swapping).
  """
  @callback code_change(old_vsn :: term | {:down, term}, state :: term, extra :: term) ::
            {:ok, new_state :: term} |
            {:error, reason :: term}
end
