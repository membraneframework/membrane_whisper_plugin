defmodule Membrane.Whisper.Mixfile do
  use Mix.Project

  @version "0.1.0"
  @github_url "https://github.com/membraneframework/membrane_whisper_plugin"

  def project do
    [
      app: :membrane_whisper_plugin,
      version: @version,
      elixir: "~> 1.13",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: dialyzer(),

      # hex
      description: "Membrane plugin for OpenAI's Whisper model",
      package: package(),

      # docs
      name: "Membrane Whisper Plugin",
      source_url: @github_url,
      docs: docs(),
      homepage_url: "https://membrane.stream"
    ]
  end

  def application, do: []

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:membrane_core, "~> 1.0"},
      {:membrane_raw_audio_format, "~> 0.12.0"},
      {:bumblebee, git: "https://github.com/kidq330/bumblebee.git", branch: "kidq330/wave2vec2"},
      nx_backend_dep(),
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:dialyxir, ">= 0.0.0", only: :dev, runtime: false},
      {:credo, ">= 0.0.0", only: :dev, runtime: false},
      {:membrane_file_plugin, "~> 0.17.0", only: :test},
      {:membrane_raw_audio_parser_plugin, "~> 0.4.0", only: :test}
    ]
  end

  # EMLX (MLX) on Apple Silicon, EXLA everywhere else
  defp nx_backend_dep do
    if apple_silicon?() do
      {:emlx, "~> 0.4.0"}
    else
      {:exla, ">= 0.0.0"}
    end
  end

  defp apple_silicon? do
    :os.type() == {:unix, :darwin} and
      :erlang.system_info(:system_architecture)
      |> List.to_string()
      |> String.starts_with?("aarch64")
  end

  defp dialyzer() do
    opts = [
      flags: [:error_handling]
    ]

    if System.get_env("CI") == "true" do
      # Store PLTs in cacheable directory for CI
      [plt_local_path: "priv/plts", plt_core_path: "priv/plts"] ++ opts
    else
      opts
    end
  end

  defp package do
    [
      maintainers: ["Membrane Team"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @github_url,
        "Membrane Framework Homepage" => "https://membrane.stream"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "LICENSE"],
      formatters: ["html"],
      source_ref: "v#{@version}",
      nest_modules_by_prefix: [Membrane.Whisper]
    ]
  end
end
