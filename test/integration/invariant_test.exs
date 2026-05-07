defmodule Membrane.Whisper.Integration.InvariantTest do
  use ExUnit.Case, async: false
  @moduletag timeout: :infinity

  import Membrane.ChildrenSpec

  alias Membrane.Testing.Pipeline
  alias Membrane.Whisper.TranscriptEvent

  @bytes_per_second 16_000 * 4

  defmodule InvariantSink do
    use Membrane.Sink

    alias Membrane.Whisper.TranscriptEvent

    def_input_pad :input,
      flow_control: :auto,
      accepted_format: %Membrane.RawAudio{
        sample_format: :f32le,
        channels: 1,
        sample_rate: 16_000
      }

    def_options test_pid: [spec: pid()]

    @impl true
    def handle_init(_ctx, opts) do
      {[], %{test_pid: opts.test_pid, bytes_received: 0}}
    end

    @impl true
    def handle_buffer(:input, buffer, _ctx, state) do
      {[], %{state | bytes_received: state.bytes_received + byte_size(buffer.payload)}}
    end

    @impl true
    def handle_event(:input, %TranscriptEvent{} = event, _ctx, state) do
      send(state.test_pid, {:transcript, event, state.bytes_received})
      {[], state}
    end

    @impl true
    def handle_event(pad, event, ctx, state), do: super(pad, event, ctx, state)

    @impl true
    def handle_end_of_stream(:input, _ctx, state) do
      send(state.test_pid, {:eos, state.bytes_received})
      {[], state}
    end
  end

  defp load_whisper_serving do
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
      defn_options: [compiler: EXLA],
      stream: true,
      chunk_num_seconds: 10,
      timestamps: :segments
    )
  end

  setup do
    {:ok, serving: load_whisper_serving()}
  end

  @input_file "test/fixtures/sherlock_1min.raw"

  test "each TranscriptEvent arrives after all audio it covers has been forwarded", ctx do
    ra_format = %Membrane.RawAudio{sample_format: :f32le, channels: 1, sample_rate: 16_000}
    test_pid = self()

    spec =
      child(:source, %Membrane.File.Source{location: @input_file})
      |> child(:parser, %Membrane.RawAudioParser{stream_format: ra_format})
      |> child(:whisper_filter, %Membrane.Whisper.TranscriberFilter{serving: ctx.serving})
      |> child(:sink, %InvariantSink{test_pid: test_pid})

    {:ok, _supervisor_pid, _pipeline_pid} = Pipeline.start(spec: spec)

    events = collect_until_eos([])

    assert events != [], "expected at least one TranscriptEvent"

    for {%TranscriptEvent{end_timestamp_seconds: end_ts} = event, bytes_received} <- events,
        not is_nil(end_ts) do
      required_bytes = round(end_ts * @bytes_per_second)

      assert bytes_received >= required_bytes,
             """
             Invariant violated: TranscriptEvent arrived before its audio was fully forwarded.
               text:           #{inspect(event.text)}
               end_ts:         #{end_ts}s
               required bytes: #{required_bytes} (#{end_ts}s * #{@bytes_per_second}B/s)
               bytes received: #{bytes_received}
               shortfall:      #{required_bytes - bytes_received} bytes
             """
    end
  end

  defp collect_until_eos(acc) do
    receive do
      {:transcript, event, bytes_received} ->
        collect_until_eos([{event, bytes_received} | acc])

      {:eos, _bytes} ->
        Enum.reverse(acc)
    after
      120_000 -> flunk("timed out waiting for transcripts / EOS")
    end
  end
end
