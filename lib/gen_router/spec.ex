defmodule GenRouter.Spec do
  @moduledoc """
  Convenience functions for sending `GenRouter` protocol messages.

  The messages between producer and consumer are as follows:

    * `{:"$gen_subscribe", {pid, ref}, {count, options}}` -
      used to subscribe to and ask data from a producer. Once this
      message is received, the producer MUST monitor the consumer (`pid`)
      and emit data up to the counter. `subscribe/5` is a convenience
      function to send this message.

    * `{:"$gen_ask", {pid, ref}, count}` -
      used to ask data from a producer. The `ref` must be the `ref` sent
      in a prior `:"$gen_subscribe"` message. The producer MUST emit
      data up to the counter to the `pid` in the original
      `:"$gen_subscribe"` message - even if it does not match the `pid`
      in the `:"gen_ask"` message. The producer MUST send a reply (detailed
      below), even if it does not know the given reference, in which case
      the reply MUST be an `:eos`. Following messages will increase the
      counter kept by the producer. `ask/3` is a convenience function to
      send this message.

    * `{:"$gen_route", {pid, ref}, [event]}` -
      used to send data to a consumer. The third argument is a non-empty
      list of events. The `ref` is the same used when asked for the
      data. `route/3` is a convenience function to send this message.

    * `{:"$gen_route", {pid, ref}, {:eos, reason}}` -
      signals the end of the "event stream". Reason may
      be `:done`, `:halted`, `:cancelled` or even `{:error,
      reason}` (in case `handle_up/2` returns error).
      `route/3` is a convenience function to send this message.

    * `{:"$gen_unsubscribe", {pid, ref}, reason}` -
      cancels the current producer/consumer relationship. The producer MUST
      send a `:"$gen_route"` `:eos` message as a reply (detailed below)
      to the original subscriber, if it does not know the given
      reference the reply is sent to `pid`. However there is no
      guarantee the message will be received (for example, the producer
      may crash just before sending the confirmation). For
      such, it is recomended for the producer to be monitored.
      `unsubscribe/3` is a convenience function to send
      this message.
  """

  # TODO: Introduce both {:eos, :done | :halted} and {:error, reason}

  @doc """
  Send a subscription request to a producer and ask for events.

  Sends `{:"$gen_subscribe", {self(), ref}, {demand, opts}}` to `producer`.

  `ref` is the subscription stream reference and must be unique. It
  will be included in all future messages concerning the subscribed stream.

  If a consumer is already subscribed to the producer with the same reference,
  the message
  `{:"$gen_route", {producer_pid, ref}, {:eos, {:error, :already_subscribed}}}`
  is sent to the caller.

      producer = GenServer.whereis(:producer) || raise "no process named :producer"
      ref = Process.monitor(producer)
      GenRouter.Spec.subscribe(producer, self, ref, 1)
      receive do
        {:"$gen_route", {_producer, ^ref}, [event]} -> event
        {:DOWN, ^ref, :process, _producer, reason}  -> exit(reason)
      end
  """
  @spec subscribe(GenRouter.router, pid, reference, pos_integer, Keyword.t) :: :ok
  def subscribe(producer, pid, ref, demand, opts \\ []) do
    producer = whereis(producer)
    _ = send(producer, {:"$gen_subscribe", {pid, ref}, {demand, opts}})
    :ok
  end

  @doc """
  Ask for events from a producer using an existing subscription stream.

  Sends `{:"$gen_ask", {self(), ref}, demand}` to `producer`.

  `ref` is the reference of an existing subscription. If
  subscription stream does not exist on the producer the message
  `{:"$gen_route", {producer_pid, ref}, {:eos, {:error, :not_found}}}`
  is sent to the caller.

      GenRouter.Spec.ask(producer, ref, 1)
      receive do
        {:"$gen_route", {_producer_pid, ^ref}, [event]} -> event
      end
  """
  @spec ask(GenRouter.router, reference, pos_integer) :: :ok
  def ask(producer, ref, demand) do
    producer = whereis(producer)
    _ = send(producer, {:"$gen_ask", {self(), ref}, demand})
    :ok
  end

  @doc """
  Send events or notification of the end of a stream to a consumer.

  Sends `{:"$gen_route", {self(), ref}, msg}` to `consumer`.

  `ref` is the reference of the subscription stream that asked for
  events. If the subscription exists the `consumer` must be the process
  that sent the `:"$gen_subscribe"` request with reference `ref`. This
  may not be the same process that sent the `:"$gen_ask"` request.

      receive do
        {:"$gen_subscribe", {consumer, ^ref}, {1, _opts}} ->
          GenRouter.Spec.route(consumer, ref, [:hello_consumer])
      end
      receive do
        {:"$gen_ask", {_sender, ^ref}, 1} ->
          GenRouter.Spec.route(consumer, ref, [:hello_again])
      end
  """
  @spec route(GenRouter.router, reference, [any, ...] | {:eos, any}) :: :ok
  def route(consumer, ref, [_|_] = events) do
    consumer = whereis(consumer)
    _ = send(consumer, {:"$gen_route", {self(), ref}, events})
    :ok
  end
  def route(consumer, ref, {:eos, _} = eos) do
    consumer = whereis(consumer)
    _ = send(consumer, {:"$gen_route", {self(), ref}, eos})
    :ok
  end

  @doc """
  Cancel a subscription to a producer.

  Sends `{:"$gen_unsubscribe", {self(), ref}, reason}` to `producer`.

  `ref` is the reference of an existing subscription. If the stream is
  cancelled the message
  `{:"$gen_route", {producer_pid, ref}, {:eos, :cancelled}}` is sent to the
  subscribed process. If the subscription does not exist then the producer
  sends `{:"$gen_route", {producer_pid, ref}, {:eos, {:error, :not_found}}}`
  to the caller.

      GenRouter.Spec.unsubscribe(producer, ref, :done)
      receive do
        {:"$gen_route", {_producer_pid, ^ref}, {:eos, :cancelled} -> :ok
      end
  """
  @spec unsubscribe(GenRouter.router, reference, any) :: :ok
  def unsubscribe(producer, ref, reason) do
    producer = whereis(producer)
    _ = send(producer, {:"$gen_unsubscribe", {self(), ref}, reason})
    :ok
  end

  ## Helpers

  defp whereis(router) do
    case GenServer.whereis(router) do
      nil   -> raise ArgumentError, "no process associated with #{inspect(router)}"
      proc -> proc
    end
  end
end
