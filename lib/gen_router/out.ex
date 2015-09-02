defmodule GenRouter.Out do
  @moduledoc """
  Specifies the outgoing part of a router.
  """

  @doc """
  Invoked when the router is started.
  """
  @callback init(args :: term) ::
            {:ok, state :: term} |
            {:stop, reason :: term} |
            :ignore

  @doc """
  Invoked when a sink asks for data.

  Must return a non negative integer (>= 0) to signal the
  demand upstream. 0 means no demand.
  """
  @callback handle_demand(demand :: pos_integer, sink :: {pid, reference}, state :: term) ::
            {:ok, non_neg_integer, new_state :: term} |
            {:error, reason :: term, new_state :: term} |
            {:stop, reason :: term, new_state :: term}

  @doc """
  Invoked when a sink cancels subscription or crashes.
  """
  @callback handle_down(reason :: term, sink :: {pid, reference}, state :: term) ::
            {:ok, new_state :: term} |
            {:stop, reason :: term, new_state :: term}

  @doc """
  Specifies to which process(es) an event should be dispatched to.
  """
  @callback handle_dispatch(event :: term, state :: term) ::
            {:ok, [pid], new_state :: term} |
            {:stop, reason :: term, [pid], new_state :: term} |
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
            :ok

  @doc """
  Called when the router code is being upgraded live (hot code swapping).
  """
  @callback code_change(old_vsn :: term | {:down, term}, state :: term, extra :: term) ::
            {:ok, new_state :: term} |
            {:error, reason :: term}
end
