# Two-pass keyword spotting over a media file:
#   pass 1: Whisper (`Membrane.Whisper.TranscriberFilter`) transcribes the audio
#           with segment-level timestamps,
#   pass 2: `Membrane.Whisper.KeywordSpotterFilter` localizes the watched keywords
#           within the segments via Wav2Vec2 CTC forced alignment (~20 ms resolution).
#
# Usage:
#   elixir examples/keyword_spotting.exs <media-file-or-url> <keyword> [keyword...]
#
# Set WHISPER_REPO to swap the Whisper model (default openai/whisper-tiny) and
# CTC_REPO to swap the CTC model (default facebook/wav2vec2-base-960h).

# EMLX (MLX) on Apple Silicon, EXLA everywhere else
apple_silicon? =
  :os.type() == {:unix, :darwin} and
    :erlang.system_info(:system_architecture) |> List.to_string() |> String.starts_with?("aarch64")

{nx_backend_dep, nx_backend} =
  if apple_silicon?,
    do: {{:emlx, "~> 0.4.0"}, {EMLX.Backend, device: :gpu}},
    else: {{:exla, "~> 0.12"}, EXLA.Backend}

Mix.install(
  [
    {:membrane_whisper_plugin, path: Path.join(__DIR__, "..")},
    # override the bumblebee constraint of boombox's transitive `image` dependency
    {:bumblebee, path: Path.join(__DIR__, "../../bumblebee"), override: true},
    {:membrane_transcoder_plugin, "~> 0.3.2"},
    {:membrane_core, "~> 1.0"},
    {:membrane_raw_audio_format, "~> 0.12.0"},
    {:membrane_ffmpeg_swresample_plugin, "~> 0.20.5"},
    {:boombox, "~> 0.2.8"},
    nx_backend_dep
  ],
  config: [
    nx: [
      default_backend: nx_backend
    ]
  ]
)

Logger.configure(level: :info)

defmodule Whisper.Demo.KeywordSpotting.Printer do
  use Membrane.Sink

  def_input_pad :input,
    accepted_format: _any

  @impl true
  def handle_buffer(:input, _buffer, _ctx, state), do: {[], state}

  @impl true
  def handle_event(:input, %Membrane.Whisper.TranscriptEvent{} = event, _ctx, state) do
    IO.puts("[#{fmt(event.start_timestamp_seconds)} - #{fmt(event.end_timestamp_seconds)}]#{event.text}")
    {[], state}
  end

  @impl true
  def handle_event(:input, %Membrane.Whisper.KeywordEvent{} = event, _ctx, state) do
    tag = if event.ctc_only?, do: ", CTC-only", else: ""

    IO.puts(
      ~s{>>> "#{event.keyword}" @ #{fmt(event.start_timestamp_seconds)} - } <>
        ~s{#{fmt(event.end_timestamp_seconds)} (confidence #{event.confidence}#{tag})}
    )

    {[], state}
  end

  @impl true
  def handle_event(_pad, _event, _ctx, state), do: {[], state}

  defp fmt(nil), do: "?"
  defp fmt(seconds), do: :io_lib.format("~.2fs", [seconds * 1.0]) |> IO.iodata_to_binary()
end

defmodule Whisper.Demo.KeywordSpotting.Pipeline do
  use Membrane.Pipeline

  defp setup_whisper_serving do
    hf_repo = System.get_env("WHISPER_REPO", "openai/whisper-tiny")

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
      chunk_num_seconds: 10,
      context_num_seconds: 0,
      timestamps: :segments
    )
  end

  defp load_ctc_model do
    hf_repo = System.get_env("CTC_REPO", "facebook/wav2vec2-base-960h")

    {:ok, model_info} = Bumblebee.load_model({:hf, hf_repo})
    {:ok, featurizer} = Bumblebee.load_featurizer({:hf, hf_repo})
    {model_info, featurizer}
  end

  @impl true
  def handle_init(_ctx, %{input: input, keywords: keywords}) do
    {ctc_model_info, ctc_featurizer} = load_ctc_model()

    spec =
      child(:source, %Boombox.Bin{input: input})
      |> via_out(:output, options: [kind: :audio])
      |> child(:transcoder, %Membrane.Transcoder{
        output_stream_format: Membrane.RawAudio
      })
      |> child(:resampler, %Membrane.FFmpeg.SWResample.Converter{
        output_stream_format: %Membrane.RawAudio{
          sample_format: :f32le,
          channels: 1,
          sample_rate: 16_000
        }
      })
      |> child(:whisper, %Membrane.Whisper.TranscriberFilter{
        serving: setup_whisper_serving()
      })
      |> child(:keyword_spotter, %Membrane.Whisper.KeywordSpotterFilter{
        keywords: keywords,
        model_info: ctc_model_info,
        featurizer: ctc_featurizer
      })
      |> child(:printer, Whisper.Demo.KeywordSpotting.Printer)

    {[spec: spec], %{}}
  end

  @impl true
  def handle_element_end_of_stream(:printer, :input, _ctx, state) do
    {[terminate: :normal], state}
  end

  @impl true
  def handle_element_end_of_stream(_child, _pad, _ctx, state) do
    {[], state}
  end
end

case System.argv() do
  [input | keywords] when keywords != [] ->
    {:ok, supervisor, _pipeline} =
      Membrane.Pipeline.start_link(Whisper.Demo.KeywordSpotting.Pipeline, %{
        input: input,
        keywords: keywords
      })

    Process.monitor(supervisor)

    receive do
      {:DOWN, _ref, :process, _pid, _reason} -> :ok
    end

  _usage ->
    IO.puts("usage: elixir examples/keyword_spotting.exs <media-file-or-url> <keyword> [keyword...]")
    System.halt(1)
end
