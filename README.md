# Membrane Whisper Plugin

[![Hex.pm](https://img.shields.io/hexpm/v/membrane_whisper_plugin.svg)](https://hex.pm/packages/membrane_whisper_plugin)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat)](https://hexdocs.pm/membrane_whisper_plugin)
[![CircleCI](https://circleci.com/gh/membraneframework/membrane_whisper_plugin.svg?style=svg)](https://circleci.com/gh/membraneframework/membrane_whisper_plugin)

A Membrane plugin for producing transcripts from audio streams containing human speech, based on OpenAI's Whisper model.

It's a part of the [Membrane Framework](https://membrane.stream).

## Installation

The package can be installed by adding `membrane_whisper_plugin` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:membrane_whisper_plugin, "~> 0.1.1"}
  ]
end
```

## Examples

For a demo streaming and transcribing a static `.mp4`, see `examples/live_mp4_processing.exs`:

```sh
$ elixir examples/live_mp4_processing.exs
```

You can also try it out yourself - see `examples/live_mic_processing.exs` which uses `portaudio` to stream your microphone's input directly to Whisper and prints the resulting transcript directly to console:

```sh
$ elixir examples/live_mic_processing.exs
```

## Copyright and License

Copyright 2026, [Software Mansion](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane_whisper_plugin)

[![Software Mansion](https://logo.swmansion.com/logo?color=white&variant=desktop&width=200&tag=membrane-github)](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane_whisper_plugin)

Licensed under the [Apache License, Version 2.0](LICENSE)
