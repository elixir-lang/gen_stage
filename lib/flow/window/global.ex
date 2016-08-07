alias Experimental.Flow

defmodule Flow.Window.Global do
  @moduledoc false

  defstruct [:trigger, periodically: []]

  def materialize(_window, reducer_acc, reducer_fun, reducer_trigger) do
    acc = reducer_acc

    fun =
      cond do
        is_function(reducer_fun, 3) ->
          reducer_fun
        is_function(reducer_fun, 4) ->
          fn events, acc, index ->
            reducer_fun.(events, acc, index, {:global, :global, :placeholder})
          end
        is_function(reducer_fun, 5) ->
          reducer_fun # TODO: Fix me
      end

    trigger =
      fn acc, index, op, name ->
        reducer_trigger.(acc, index, op, {:global, :global, name})
      end

    {acc, fun, trigger}
  end
end
