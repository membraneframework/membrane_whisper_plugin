Mix.install(
  [
    {:membrane_whisper_plugin, path: Path.join(__DIR__, "..")},
    {:membrane_transcoder_plugin, "~> 0.3.2"},
    {:membrane_core, "~> 1.0"},
    {:membrane_raw_audio_format, "~> 0.12.0"},
    {:membrane_ffmpeg_swresample_plugin, "~> 0.20.5"},
    {:boombox, "~> 0.2.8"},
    {:exla, "~> 0.10"}
  ],
  config: [
    nx: [
      default_backend: EXLA.Backend
    ]
  ]
)

Logger.configure(level: :info)

defmodule Whisper.Debug.ChunkSink do
  @moduledoc """
  Sink that accumulates audio buffers between consecutive
  `Membrane.Whisper.TranscriptEvent`s, then writes each chunk as a pair of
  files in `output_dir`:

  - `chunk_NNN.raw` — concatenated raw f32le 16 kHz mono PCM for that chunk
  - `chunk_NNN.txt` — the Whisper transcript for that chunk

  The invariant being validated: the TranscriptEvent arrives *after* all
  buffers that belong to the corresponding audio chunk, so every `.raw` file
  should be fully populated before its `.txt` counterpart is written.
  """

  use Membrane.Sink

  alias Membrane.Whisper.TranscriptEvent

  def_input_pad :input,
    flow_control: :auto,
    accepted_format: %Membrane.RawAudio{sample_format: :f32le, channels: 1, sample_rate: 16_000}

  def_options output_dir: [
                spec: Path.t(),
                description: "Directory where chunk_NNN.{raw,txt} pairs will be written"
              ]

  @impl true
  def handle_init(_ctx, opts) do
    File.mkdir_p!(opts.output_dir)
    {[], %{output_dir: opts.output_dir, chunk_index: 0, pending_payloads: []}}
  end

  @impl true
  def handle_buffer(:input, buffer, _ctx, state) do
    {[], %{state | pending_payloads: [buffer.payload | state.pending_payloads]}}
  end

  # Each TranscriptEvent flushes all buffers accumulated since the previous one.
  @impl true
  def handle_event(:input, %TranscriptEvent{text: text, start_timestamp_seconds: ts_start, end_timestamp_seconds: ts_end}, _ctx, state) do
    %{output_dir: dir, chunk_index: idx, pending_payloads: payloads} = state

    IO.puts("[chunk #{idx}] #{length(payloads)} buffer(s) → #{inspect(text)}")

    prefix = chunk_prefix(dir, idx)
    raw_data = payloads |> Enum.reverse() |> :erlang.iolist_to_binary()
    File.write!(prefix <> ".raw", raw_data)
    File.write!(prefix <> ".txt", "#{text}\n#{ts_start}\n#{ts_end}")

    {[], %{state | chunk_index: idx + 1, pending_payloads: []}}
  end

  def handle_event(pad, event, ctx, state), do: super(pad, event, ctx, state)

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    if state.pending_payloads != [] do
      IO.puts(
        "[warning] #{length(state.pending_payloads)} buffer(s) arrived without a matching " <>
          "TranscriptEvent — invariant violated"
      )
    end

    IO.puts("Done — #{state.chunk_index} chunk(s) written to #{state.output_dir}/")
    {[], state}
  end

  defp chunk_prefix(dir, index),
    do: Path.join(dir, "chunk_" <> String.pad_leading("#{index}", 3, "0"))
end

defmodule Whisper.Debug.Pipeline do
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
      chunk_num_seconds: 8,
      timestamps: :segments,
      context_num_seconds: 0
    )
  end

  @impl true
  def handle_init(_ctx, {input_file, output_dir}) do
    spec =
      child(:source, %Boombox.Bin{input: input_file})
      |> via_out(:output, options: [kind: :audio])
      |> child(:transcoder, %Membrane.Transcoder{output_stream_format: Membrane.RawAudio})
      |> child(:resampler, %Membrane.FFmpeg.SWResample.Converter{
        output_stream_format: %Membrane.RawAudio{
          sample_format: :f32le,
          channels: 1,
          sample_rate: 16_000
        }
      })
#      |> child(:debug, %Membrane.Debug.Filter{
#        handle_buffer: fn %Membrane.Buffer{payload: buf} -> buf |> byte_size() |> Membrane.RawAudio.bytes_to_time(%Membrane.RawAudio{
#            sample_format: :f32le, channels: 1, sample_rate: 16_000
#          }) |> Membrane.Time.pretty_duration() |> IO.puts() end
#      })
      |> child(:whisper, %Membrane.Whisper.TranscriberFilter{
           serving: setup_serving(),
         })
      |> child(:sink, %Whisper.Debug.ChunkSink{output_dir: output_dir})

    {[spec: spec], %{}}
  end

  @impl true
  def handle_element_end_of_stream(:sink, :input, _ctx, state) do
    {[terminate: :normal], state}
  end
end

input_file = Path.expand(Path.join([__DIR__, "..", "sherlock_librivox.mp4"]))
output_dir = Path.expand(Path.join([__DIR__, "..", "transcript_chunks"]))

IO.puts("Reading: #{input_file}")
IO.puts("Writing: #{output_dir}/")

{:ok, supervisor, _pipeline} =
  Membrane.Pipeline.start_link(Whisper.Debug.Pipeline, {input_file, output_dir})

Process.monitor(supervisor)

receive do
  {:DOWN, _ref, :process, _pid, _reason} -> :ok
end
