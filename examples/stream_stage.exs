# Usage: mix run examples/stream_stage.exs /usr/share/dict/words
#
# It expects a file as argument. It will return the longest
# lines in the file. For example above, the longest word.
alias Experimental.GenStage

# We expect a filename
file = Enum.at(System.argv, 0) || raise ArgumentError, "expected a filename to run script"

{:ok, stage} =
  File.read!(file)
  |> String.split("\n")
  |> Stream.map(&{String.length(&1), &1})
  |> GenStage.from_enumerable(consumers: :permanent)

GenStage.stream([stage])
|> Enum.sort(&>=/2)
|> Enum.take(10)
|> IO.inspect()
