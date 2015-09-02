defmodule GenRouter.In do
  @moduledoc """
  Specifies the incoming part of a router.
  """

  # TODO: Define __using__

  @doc """
  Invoked when the router is started.
  """
  @callback init(args :: term) ::
            {:ok, state :: term} |
            {:ok, state :: term, timeout | :hibernate} |
            {:stop, reason :: term} |
            :ignore

  @doc """
  Invoked when there is demand from downstream.

  This is usually where you ask the upstream sources for data.
  """
  @callback handle_demand(demand :: pos_integer, state :: term) ::
            {:dispatch, [event :: term], new_state :: term} |
            {:noreply, new_state :: term} |
            {:noreply, new_state :: term, timeout | :hibernate} |
            {:stop, reason :: term, new_state :: term}

  @doc """
  Invokes whenever there is a call into the router.
  """
  @callback handle_call(request :: term, from :: {pid, reference}, state :: term) ::
            {:reply, reply :: term, new_state :: term} |
            {:reply, reply :: term, new_state :: term, timeout | :hibernate} |
            {:noreply, new_state :: term} |
            {:noreply, new_state :: term, timeout | :hibernate} |
            {:stop, reason :: term, reply :: term, new_state :: term} |
            {:stop, reason :: term, new_state :: term}

  @doc """
  Invoked to handle all other messages received by the router process.
  """
  @callback handle_info(info :: :timeout | term, state :: term) ::
            {:dispatch, [event :: term], new_state :: term} |
            {:noreply, new_state :: term} |
            {:noreply, new_state :: term, timeout | :hibernate} |
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
