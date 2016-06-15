defmodule GenStage.Dispatcher do
  @moduledoc """
  This module defines the behaviour used by `:producer` and
  `:producer_consumer` to dispatch events.
  """

  @doc """
  Called on initialization with the options given on `c:GenStage.init/1`.
  """
  @callback init(opts :: Keyword.t) :: {:ok, state} when state: any

  @doc """
  Called every time the producer gets a new subscriber.
  """
  @callback subscribe(from :: {pid, reference}, state :: term) ::
    {:ok, demand :: non_neg_integer, new_state} when new_state: term

  @doc """
  Called every time a subscription is cancelled or the consumer goes down.

  It is guaranteed the reference given in `from` points to a reference
  previously given in subscribe.
  """
  @callback cancel(from :: {pid, reference}, state :: term) ::
    {:ok, demand :: non_neg_integer, new_state} when new_state: term

  @doc """
  Called every time a consumer sends demand.

  The demand will always be a positive integer (more than 0).
  This callback must return the `actual_demand` as part of its
  return tuple. The returned demand is then sent to producers.

  It is guaranteed the reference given in `from` points to a
  reference previously given in subscribe.
  """
  @callback ask(demand :: pos_integer, from :: {pid, reference}, state :: term) ::
    {:ok, actual_demand :: non_neg_integer, new_state} when new_state: term

  @doc """
  Called every time a producer wants to dispatch an event.

  The events will always be a non empty list. This callback must
  return events it could not effectively deliver as part of its
  return tuple. Any `leftover_events` will be stored by producers
  in their overflown buffer.

  It is important to emphasize that `leftover_events` can happen
  in any dispatcher implementation. After all, a consumer can
  subscribe, ask for events and crash. Eventually the events
  the consumer asked will be delivered while the consumer no longer
  exists, meaning they must be returned as left_over events until
  another consumer subscribes.

  It is guaranteed the reference given in `from` points to a
  reference previously given in subscribe.
  """
  @callback dispatch(events :: nonempty_list(term), state :: term) ::
    {:ok, leftover_events :: [term], new_state} when new_state: term
end