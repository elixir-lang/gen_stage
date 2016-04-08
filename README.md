# GenBroker

Coming soon.

## Installation

GenBroker requires Erlang 18 and Elixir v1.2.

  1. Add `:gen_broker` to your list of dependencies in mix.exs:

        def deps do
          [{:gen_broker, "~> 0.0.1"}]
        end

  2. Ensure `:gen_broker` is started before your application:

        def application do
          [applications: [:gen_broker]]
        end

## License

Same as Elixir.
