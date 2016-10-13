alias Experimental.Flow

defmodule Flow.Window.Count do
  @moduledoc false

  @enforce_keys [:count]
  defstruct [:count, :trigger, periodically: []]

  def materialize(%{count: count}, reducer_acc, reducer_fun, reducer_trigger) do
    acc = reducer_acc
    fun = reducer_fun
    trigger = reducer_trigger
    {acc, fun, trigger}
  end
end
