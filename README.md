# GenRouter

**TODO: Add description**

## Installation

GenRouter requires Erlang 18 as it is meant to be included in
Elixir v1.3 or later.

  1. Add gen_router to your list of dependencies in mix.exs:

        def deps do
          [{:gen_router, "~> 0.0.1"}]
        end

  2. Ensure gen_router is started before your application:

        def application do
          [applications: [:gen_router]]
        end
