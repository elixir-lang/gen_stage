defmodule GenStage.Utils do
  @moduledoc false

  @doc """
  Validates the argument is a list.
  """
  def validate_list(opts, key, default) do
    {value, opts} = Keyword.pop(opts, key, default)

    if is_list(value) do
      {:ok, value, opts}
    else
      {:error, "expected #{inspect(key)} to be a list, got: #{inspect(value)}"}
    end
  end

  @doc """
  Validates the given option is one of the values.
  """
  def validate_in(opts, key, default, values) do
    {value, opts} = Keyword.pop(opts, key, default)

    if value in values do
      {:ok, value, opts}
    else
      {:error, "expected #{inspect(key)} to be one of #{inspect(values)}, got: #{inspect(value)}"}
    end
  end

  @doc """
  Validates an integer.
  """
  def validate_integer(opts, key, default, min, max, infinity?) do
    {value, opts} = Keyword.pop(opts, key, default)

    cond do
      value == :infinity and infinity? ->
        {:ok, value, opts}

      not is_integer(value) ->
        error_message = "expected #{inspect(key)} to be an integer, got: #{inspect(value)}"
        {:error, error_message}

      value < min ->
        error_message =
          "expected #{inspect(key)} to be equal to or greater than #{min}, got: #{inspect(value)}"

        {:error, error_message}

      value > max ->
        error_message =
          "expected #{inspect(key)} to be equal to or less than #{max}, got: #{inspect(value)}"

        {:error, error_message}

      true ->
        {:ok, value, opts}
    end
  end

  @doc """
  Validates there are no options left.
  """
  def validate_no_opts(opts) do
    if opts == [] do
      :ok
    else
      {:error, "unknown options #{inspect(opts)}"}
    end
  end

  @doc """
  Helper to check if a shutdown is transient.
  """
  defmacro is_transient_shutdown(value) do
    quote do
      unquote(value) == :normal or unquote(value) == :shutdown or
        (is_tuple(unquote(value)) and tuple_size(unquote(value)) == 2 and
           elem(unquote(value), 0) == :shutdown)
    end
  end

  @doc """
  Returns the name of the current process or self.
  """
  def self_name() do
    case :erlang.process_info(self(), :registered_name) do
      {:registered_name, name} when is_atom(name) -> name
      _ -> self()
    end
  end

  @doc """
  Splits a list of events into messages configured by min, max, and demand.
  """
  def split_batches(events, from, min, max, demand) do
    split_batches(events, from, min, max, demand, demand, [])
  end

  defp split_batches([], _from, _min, _max, _old_demand, new_demand, batches) do
    {new_demand, :lists.reverse(batches)}
  end

  defp split_batches(events, from, min, max, old_demand, new_demand, batches) do
    {events, batch, batch_size} = split_events(events, max - min, 0, [])

    # Adjust the batch size to whatever is left of the demand in case of excess.
    {old_demand, batch_size} =
      case old_demand - batch_size do
        diff when diff < 0 ->
          error_msg = 'GenStage consumer ~tp has received ~tp events in excess from: ~tp~n'
          :error_logger.error_msg(error_msg, [self_name(), abs(diff), from])
          {0, old_demand}

        diff ->
          {diff, batch_size}
      end

    # In case we've reached min, we will ask for more events.
    {new_demand, batch_size} =
      case new_demand - batch_size do
        diff when diff <= min ->
          {max, max - diff}

        diff ->
          {diff, 0}
      end

    split_batches(events, from, min, max, old_demand, new_demand, [{batch, batch_size} | batches])
  end

  defp split_events(events, limit, limit, acc), do: {events, :lists.reverse(acc), limit}
  defp split_events([], _limit, counter, acc), do: {[], :lists.reverse(acc), counter}

  defp split_events([event | events], limit, counter, acc) do
    split_events(events, limit, counter + 1, [event | acc])
  end
end
