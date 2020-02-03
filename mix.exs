defmodule GenStage.Mixfile do
  use Mix.Project

  @version "1.0.0"

  def project do
    [
      app: :gen_stage,
      version: @version,
      elixir: "~> 1.5",
      package: package(),
      description: "Producer and consumer actors with back-pressure for Elixir",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: [
        main: "GenStage",
        source_ref: "v#{@version}",
        source_url: "https://github.com/elixir-lang/gen_stage"
      ]
    ]
  end

  def application do
    [applications: [:logger]]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.12", only: :docs}
    ]
  end

  defp package do
    %{
      licenses: ["Apache 2"],
      maintainers: ["José Valim", "James Fish"],
      links: %{"GitHub" => "https://github.com/elixir-lang/gen_stage"}
    }
  end
end
