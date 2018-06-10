defmodule GenStage.Buffer do
  # The buffer stores temporary, which is implicitly discarded,
  # and permanent data, which are explicitly discarded.
  #
  # Data is always delivered in the order they are buffered.
  # The temporary data is stored in a queue. Permanent data
  # is stored in a wheel for performance and to avoid discards.
  @moduledoc false

  @opaque t() :: {:queue.queue(), non_neg_integer(), wheel()}
  @typep wheel() :: {non_neg_integer(), pos_integer(), map()} | pos_integer() | reference()

  @doc """
  Builds a new buffer.
  """
  def new(size) when size > 0 do
    {:queue.new(), 0, init_wheel(size)}
  end

  @doc """
  Returns the estimate size of the buffer data.

  It does not count data on the wheel.
  """
  def estimate_size({_, count, _}) do
    count
  end

  @doc """
  Stores the temporary entries.

  `keep` controls which side to keep, `:first` or `:last`.

  It returns a new buffer, the amount of discarded messages and
  any permanent entry that had to be emitted while discarding.
  """
  def store_temporary({queue, counter, infos}, temps, keep) when is_list(temps) do
    {{excess, queue, counter}, perms, infos} =
      store_temporary(keep, temps, queue, counter, capacity_wheel(infos), infos)

    {{queue, counter, infos}, excess, perms}
  end

  defp store_temporary(_keep, temps, _queue, 0, :infinity, infos),
    do: {{0, :queue.from_list(temps), length(temps)}, [], infos}

  defp store_temporary(_keep, temps, queue, counter, :infinity, infos),
    do: {queue_infinity(temps, queue, counter), [], infos}

  defp store_temporary(:first, temps, queue, counter, max, infos),
    do: {queue_first(temps, queue, counter, max), [], infos}

  defp store_temporary(:last, temps, queue, counter, max, infos),
    do: queue_last(temps, queue, 0, counter, max, [], infos)

  ## Infinity

  defp queue_infinity([], queue, counter),
    do: {0, queue, counter}

  defp queue_infinity([temp | temps], queue, counter),
    do: queue_infinity(temps, :queue.in(temp, queue), counter + 1)

  ## First

  defp queue_first([], queue, counter, _max),
    do: {0, queue, counter}

  defp queue_first(temps, queue, max, max),
    do: {length(temps), queue, max}

  defp queue_first([temp | temps], queue, counter, max),
    do: queue_first(temps, :queue.in(temp, queue), counter + 1, max)

  ## Last

  defp queue_last([], queue, excess, counter, _max, perms, wheel),
    do: {{excess, queue, counter}, perms, wheel}

  defp queue_last([temp | temps], queue, excess, max, max, perms, wheel) do
    queue = :queue.in(temp, :queue.drop(queue))

    case pop_and_increment_wheel(wheel) do
      {:ok, new_perms, wheel} ->
        queue_last(temps, queue, excess + 1, max, max, perms ++ new_perms, wheel)

      {:error, wheel} ->
        queue_last(temps, queue, excess + 1, max, max, perms, wheel)
    end
  end

  defp queue_last([temp | temps], queue, excess, counter, max, perms, wheel),
    do: queue_last(temps, :queue.in(temp, queue), excess, counter + 1, max, perms, wheel)

  @doc """
  Puts the permanent entry in the buffer unless the buffer is empty.
  """
  def store_permanent_unless_empty(buffer, perm) do
    case buffer do
      {_queue, 0, _infos} ->
        :empty

      {queue, count, infos} when is_reference(infos) ->
        {:ok, {:queue.in({infos, perm}, queue), count + 1, infos}}

      {queue, count, infos} ->
        {:ok, {queue, count, put_wheel(infos, count, perm)}}
    end
  end

  @doc """
  Take count temporary from the buffer or until we find a permanent.

  Returns `:empty` if nothing was taken.
  """
  def take_count_or_until_permanent({_queue, buffer, _infos}, counter)
      when buffer == 0 or counter == 0 do
    :empty
  end

  def take_count_or_until_permanent({queue, buffer, infos}, counter) do
    take_count_or_until_permanent(counter, [], queue, buffer, infos)
  end

  defp take_count_or_until_permanent(0, temps, queue, buffer, infos) do
    {:ok, {queue, buffer, infos}, 0, :lists.reverse(temps), []}
  end

  defp take_count_or_until_permanent(counter, temps, queue, 0, infos) do
    {:ok, {queue, 0, infos}, counter, :lists.reverse(temps), []}
  end

  defp take_count_or_until_permanent(counter, temps, queue, buffer, infos)
       when is_reference(infos) do
    {{:value, value}, queue} = :queue.out(queue)

    case value do
      {^infos, perm} ->
        {:ok, {queue, buffer - 1, infos}, counter, :lists.reverse(temps), [perm]}

      temp ->
        take_count_or_until_permanent(counter - 1, [temp | temps], queue, buffer - 1, infos)
    end
  end

  defp take_count_or_until_permanent(counter, temps, queue, buffer, infos) do
    {{:value, temp}, queue} = :queue.out(queue)

    case pop_and_increment_wheel(infos) do
      {:ok, perms, infos} ->
        {:ok, {queue, buffer - 1, infos}, counter - 1, :lists.reverse([temp | temps]), perms}

      {:error, infos} ->
        take_count_or_until_permanent(counter - 1, [temp | temps], queue, buffer - 1, infos)
    end
  end

  ## Wheel helpers

  defp init_wheel(:infinity), do: make_ref()
  defp init_wheel(size), do: size

  defp capacity_wheel(ref) when is_reference(ref), do: :infinity
  defp capacity_wheel({_, max, _}), do: max
  defp capacity_wheel(max), do: max

  defp put_wheel({pos, max, wheel}, count, perm) do
    {pos, max, Map.update(wheel, rem(pos + count - 1, max), [perm], &[perm | &1])}
  end

  defp put_wheel(max, count, perm) do
    {0, max, %{rem(count - 1, max) => [perm]}}
  end

  defp pop_and_increment_wheel({pos, max, wheel}) do
    new_pos = rem(pos + 1, max)

    case :maps.take(pos, wheel) do
      {perms, wheel} ->
        maybe_triplet = if wheel == %{}, do: max, else: {new_pos, max, wheel}
        {:ok, perms, maybe_triplet}

      :error ->
        {:error, {new_pos, max, wheel}}
    end
  end

  defp pop_and_increment_wheel(max) do
    {:error, max}
  end
end
