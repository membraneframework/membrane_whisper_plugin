Mix.install(
  [
    {:membrane_whisper_plugin, path: Path.join(__DIR__, "..")},
    {:membrane_portaudio_plugin, "~> 0.19.4"},
    {:membrane_core, "~> 1.0"},
    {:exla, "~> 0.10"}
  ],
  config: [
    nx: [
      default_backend: EXLA.Backend
    ]
  ]
)

Logger.configure(level: :info)

defmodule Whisper.Demo.Mic.TranscriptPrinter do
  use Membrane.Filter

  def_input_pad :input,
    accepted_format: _any

  def_output_pad :output,
    accepted_format: _any

  @impl true
  def handle_buffer(:input, buffer, _ctx, state), do: {[forward: buffer], state}

  @impl true
  def handle_event(:input, %Membrane.Whisper.TranscriptEvent{text: text}, _ctx, state) do
    IO.puts(text)
    {[], state}
  end
end

defmodule Whisper.Demo.Mic.LivePipeline do
  use Membrane.Pipeline

  defp setup_serving do
    hf_repo = "openai/whisper-tiny"

    {:ok, whisper} = Bumblebee.load_model({:hf, hf_repo})
    {:ok, featurizer} = Bumblebee.load_featurizer({:hf, hf_repo})
    {:ok, tokenizer} = Bumblebee.load_tokenizer({:hf, hf_repo})
    {:ok, generation_config} = Bumblebee.load_generation_config({:hf, hf_repo})

    Bumblebee.Audio.speech_to_text_whisper(
      whisper,
      featurizer,
      tokenizer,
      generation_config,
      stream: true,
      chunk_num_seconds: 10
    )
  end

  @impl true
  def handle_init(_ctx, _opts) do
    spec =
      child(:mic_source, %Membrane.PortAudio.Source{
        sample_format: :f32le,
        channels: 1,
        sample_rate: 16_000,
        latency: :low
      })
      |> via_in(:input, toilet_capacity: 1_000)
      |> child(:whisper, %Membrane.Whisper.TranscriberFilter{
        serving: setup_serving()
      })
      |> child(:transcript_printer, Whisper.Demo.Mic.TranscriptPrinter)
      |> child(:sink, Membrane.Fake.Sink)

    {[spec: spec], %{}}
  end

  @impl true
  def handle_child_notification(:processing_finished, :boombox_sink, _ctx, state) do
    {[terminate: :normal], state}
  end
end

{:ok, supervisor, _pipeline} = Membrane.Pipeline.start_link(Whisper.Demo.Mic.LivePipeline, [])
Process.monitor(supervisor)

receive do
  {:DOWN, _ref, :process, _pid, _reason} -> :ok
end
