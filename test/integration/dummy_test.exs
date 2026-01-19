defmodule Membrane.Whisper.Integration.DummyTest do
  use ExUnit.Case, async: false

  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec

  alias Membrane.RawAudio
  alias Membrane.Testing.Pipeline

  def load_whisper_serving do
    {:ok, whisper} = Bumblebee.load_model({:hf, "openai/whisper-tiny"})
    {:ok, featurizer} = Bumblebee.load_featurizer({:hf, "openai/whisper-tiny"})
    {:ok, tokenizer} = Bumblebee.load_tokenizer({:hf, "openai/whisper-tiny"})
    {:ok, generation_config} = Bumblebee.load_generation_config({:hf, "openai/whisper-tiny"})

    serving =
      Bumblebee.Audio.speech_to_text_whisper(
        whisper,
        featurizer,
        tokenizer,
        generation_config,
        defn_options: [compiler: EXLA],
        stream: true,
        chunk_num_seconds: 10,
        client_batch_size: 1,
        timestamps: :segments
      )

    serving
  end

  @input_file "test/fixtures/sherlock_1min.raw"
  @output_file "test/fixtures/output.raw"

  test "Audio buffers are forwarded without change and at least one non-empty transcript is sent as event" do
    ra_format = %RawAudio{channels: 1, sample_rate: 16_000, sample_format: :f32le}

    spec = [
      child(:source, %Membrane.File.Source{location: @input_file})
      |> child(:parser, %Membrane.RawAudioParser{stream_format: ra_format})
      |> child(:whisper_filter, %Membrane.Whisper{
        input_stream_format: ra_format,
        output_stream_format: ra_format,
        serving: load_whisper_serving()
      })
      |> child(:tee, Membrane.Tee)
      |> child(:sink, %Membrane.File.Sink{location: @output_file}),
      get_child(:tee) |> child(:testing_sink, Membrane.Testing.Sink)
    ]

    {:ok, _supervisor_pid, pipeline_pid} = Pipeline.start(spec: spec)

    assert_end_of_stream(pipeline_pid, :sink, :input, 20_000)

    # Check if at least one transcript was generated
    assert_sink_event(pipeline_pid, :testing_sink, %Membrane.Whisper.TranscriptEvent{
      whisper_output: %{
        text: text
      }
    })

    assert is_bitstring(text) and String.length(text) > 0

    assert File.read!(@input_file) == File.read!(@output_file)
  end
end
