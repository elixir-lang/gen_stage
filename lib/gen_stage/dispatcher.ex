defmodule GenStage.Dispatcher do
  @moduledoc """
  This module defines the behaviour used by `:producer` and
  `:producer_consumer` to dispatch events.

  When using a `:producer` or `:producer_consumer`, the dispatcher
  may be configured on init as follows:

      {:producer, state, dispatcher: GenStage.BroadcastDispatcher}

  Some dispatchers may require options to be given on initialization,
  those can be done with a tuple:

      {:producer, state, dispatcher: {GenStage.PartitionDispatcher, partitions: 0..3}}

  Elixir ships with the following dispatcher implementations:

    * `GenStage.DemandDispatcher` - dispatches the given batch of
      events to the consumer with the biggest demand in a FIFO
      ordering. This is the default dispatcher.

    * `GenStage.BroadcastDispatcher` - dispatches all events to all
      consumers. The demand is only sent upstream once all consumers
      ask for data.

    * `GenStage.PartitionDispatcher` - dispatches all events to a
      fixed amount of consumers that works as partitions according
      to a hash function.

  """

  @typedoc "Options used by `init/1`"
  @type options :: keyword

  @doc """
  Called on initialization with the options given on `c:GenStage.init/1`.
  """
  @callback init(opts :: options) :: {:ok, state} when state: any

  @doc """
  Called every time the producer gets a new subscriber.
  """
  @callback subscribe(opts :: keyword(), from :: {pid, reference}, state :: term) ::
              {:ok, demand :: non_neg_integer, new_state} | {:error, term}
            when new_state: term

  @doc """
  Called every time a subscription is cancelled or the consumer goes down.

  It is guaranteed the reference given in `from` points to a reference
  previously given in subscribe.
  """
  @callback cancel(from :: {pid, reference}, state :: term) ::
              {:ok, demand :: non_neg_integer, new_state}
            when new_state: term

  @doc """
  Called every time a consumer sends demand.

  The demand will always be a positive integer (more than 0).
  This callback must return the `actual_demand` as part of its
  return tuple. The returned demand is then sent to producers.

  It is guaranteed the reference given in `from` points to a
  reference previously given in subscribe.
  """
  @callback ask(demand :: pos_integer, from :: {pid, reference}, state :: term) ::
              {:ok, actual_demand :: non_neg_integer, new_state}
            when new_state: term

  @doc """
  Called every time a producer wants to dispatch an event.

  The events will always be a non empty list. This callback may
  receive more events than previously asked and therefore must
  return events it cannot not effectively deliver as part of its
  return tuple. Any `leftover_events` will be stored by producers
  in their buffer.

  It is important to emphasize that `leftover_events` can happen
  in any dispatcher implementation. After all, a consumer can
  subscribe, ask for events and crash. Eventually the events
  the consumer asked will be delivered while the consumer no longer
  exists, meaning they must be returned as left_over events until
  another consumer subscribes.

  It is guaranteed the reference given in `from` points to a
  reference previously given in subscribe. It is also recommended
  for events to be sent with `Process.send/3` and the `[:noconnect]`
  option as the consumers are all monitored by the producer. For
  example:

      Process.send(consumer, {:"$gen_consumer", {self(), consumer_ref}, events}, [:noconnect])

  """
  @callback dispatch(events :: nonempty_list(term), length :: pos_integer, state :: term) ::
              {:ok, leftover_events :: [term], new_state}
            when new_state: term

  @doc """
  Used to send an info message to the current process.

  In case the dispatcher is doing buffering, the message must
  only be sent after all currently buffered consumer messages are
  delivered.
  """
  @callback info(msg :: term, state :: term) :: {:ok, new_state} when new_state: term
end
