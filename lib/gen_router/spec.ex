defmodule GenRouter.Spec do
  @moduledoc """
  Convenience functions for sending `GenRouter` protocol messages.

  The messages between source and sink are as follows:

    * `{:"$gen_subscribe", {pid, ref}, {count, options}}` -
      used to subscribe to and ask data from a source. Once this
      message is received, the source MUST monitor the sink (`pid`)
      and emit data up to the counter. `subscribe/5` is a convenience
      function to send this message.

    * `{:"$gen_ask", {pid, ref}, count}` -
      used to ask data from a source. The `ref` must be the `ref` sent
      in a prior `:"$gen_subscribe"` message. The source MUST emit
      data up to the counter to the `pid` in the original
      `:"$gen_subscribe"` message - even if it does not match the `pid`
      in the `:"gen_ask"` message. The source MUST send a reply (detailed
      below), even if it does not know the given reference, in which case
      the reply MUST be an `:eos`. Following messages will increase the
      counter kept by the source. `ask/3` is a convenience function to
      send this message.

    * `{:"$gen_route", {pid, ref}, [event]}` -
      used to send data to a sink. The third argument is a non-empty
      list of events. The `ref` is the same used when asked for the
      data. `route/3` is a convenience function to send this message.

    * `{:"$gen_route", {pid, ref}, {:eos, reason}}` -
      signals the end of the "event stream". Reason may
      be `:done`, `:halted`, `:cancelled` or even `{:error,
      reason}` (in case `handle_up/2` returns error).
      `route/3` is a convenience function to send this message.

    * `{:"$gen_unsubscribe", {pid, ref}, reason}` -
      cancels the current source/sink relationship. The source MUST
      send a `:"$gen_route"` `:eos` message as a reply (detailed below)
      to the original subscriber, if it does not know the given
      reference the reply is sent to `pid`. However there is no
      guarantee the message will be received (for example, the source
      may crash just before sending the confirmation). For
      such, it is recomended for the source to be monitored.
      `unsubscribe/3` is a convenience function to send
      this message.
  """

  @doc """
  Send a subscription request to a source and ask for events.

  Sends `{:"$gen_subscribe", {self(), ref}, {demand, opts}}` to `source`.

  `ref` is the subscription stream reference and must be unique. It
  will be included in all future messages concerning the subscribed stream.

  If a sink is already subscribed to the source with the same reference,
  the message
  `{:"$gen_route", {source_pid, ref}, {:eos, {:error, :already_subscribed}}}`
  is sent to the caller.

      source = GenServer.whereis(:source) || raise "no process named :source"
      ref = Process.monitor(source)
      GenRouter.Spec.subscribe(source, self, ref, 1)
      receive do
        {:"$gen_route", {_source, ^ref}, [event]} -> event
        {:DOWN, ^ref, :process, _source, reason}  -> exit(reason)
      end
  """
  @spec subscribe(GenRouter.router, pid, reference, pos_integer, Keyword.t) :: :ok
  def subscribe(source, pid, ref, demand, opts \\ []) do
    source = whereis(source)
    _ = send(source, {:"$gen_subscribe", {pid, ref}, {demand, opts}})
    :ok
  end

  @doc """
  Ask for events from a source using an existing subscription stream.

  Sends `{:"$gen_ask", {self(), ref}, demand}` to `source`.

  `ref` is the reference of an existing subscription. If
  subscription stream does not exist on the source the message
  `{:"$gen_route", {source_pid, ref}, {:eos, {:error, :not_found}}}`
  is sent to the caller.

      GenRouter.Spec.ask(source, ref, 1)
      receive do
        {:"$gen_route", {_source_pid, ^ref}, [event]} -> event
      end
  """
  @spec ask(GenRouter.router, reference, pos_integer) :: :ok
  def ask(source, ref, demand) do
    source = whereis(source)
    _ = send(source, {:"$gen_ask", {self(), ref}, demand})
    :ok
  end

  @doc """
  Send events or notification of the end of a stream to a sink.

  Sends `{:"$gen_route", {self(), ref}, msg}` to `sink`.

  `ref` is the reference of the subscription stream that asked for
  events. If the subscription exists the `sink` must be the process
  that sent the `:"$gen_subscribe"` request with reference `ref`. This
  may not be the same process that sent the `:"$gen_ask"` request.

      receive do
        {:"$gen_subscribe", {sink, ^ref}, {1, _opts}} ->
          GenRouter.Spec.route(sink, ref, [:hello_sink])
      end
      receive do
        {:"$gen_ask", {_sender, ^ref}, 1} ->
          GenRouter.Spec.route(sink, ref, [:hello_again])
      end
  """
  @spec route(GenRouter.router, reference, [any, ...] | {:eos, any}) :: :ok
  def route(sink, ref, [_|_] = events) do
    sink = whereis(sink)
    _ = send(sink, {:"$gen_route", {self(), ref}, events})
    :ok
  end
  def route(sink, ref, {:eos, _} = eos) do
    sink = whereis(sink)
    _ = send(sink, {:"$gen_route", {self(), ref}, eos})
    :ok
  end

  @doc """
  Cancel a subscription to a source.

  Sends `{:"$gen_unsubscribe", {self(), ref}, reason}` to `source`.

  `ref` is the reference of an existing subscription. If the stream is
  cancelled the message
  `{:"$gen_route", {source_pid, ref}, {:eos, :cancelled}}` is sent to the
  subscribed process. If the subscription does not exist then the source
  sends `{:"$gen_route", {source_pid, ref}, {:eos, {:error, :not_found}}}`
  to the caller.

      GenRouter.Spec.unsubscribe(source, ref, :done)
      receive do
        {:"$gen_route", {_source_pid, ^ref}, {:eos, :cancelled} -> :ok
      end
  """
  @spec unsubscribe(GenRouter.router, reference, any) :: :ok
  def unsubscribe(source, ref, reason) do
    source = whereis(source)
    _ = send(source, {:"$gen_unsubscribe", {self(), ref}, reason})
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
