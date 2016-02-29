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

## TODO

  * Discuss naming convention:
    * `GenRouter.*Source`
    * `GenRouter.*Sink`
    * `GenRouter.*In` and `GenRouter.*Out`

  * See if we can remove subscribers from inside GenRouter

  * Revisit why we need both :"$gen_subscribe" and :"$gen_ask"

  * What happens if a producer dies?

  * What happens if a consumer dies?