defmodule Membrane.Whisper.Integration.DummyTest do
  use ExUnit.Case, async: false

  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec

  alias Membrane.RawAudio
  alias Membrane.Testing.Pipeline
  alias Membrane.Whisper.TranscriptEvent

  def load_whisper_serving do
    whisper_local_dir = "./priv/openai/whisper-tiny/"
    {:ok, whisper} = Bumblebee.load_model({:local, whisper_local_dir})
    {:ok, featurizer} = Bumblebee.load_featurizer({:local, whisper_local_dir})
    {:ok, tokenizer} = Bumblebee.load_tokenizer({:local, whisper_local_dir})
    {:ok, generation_config} = Bumblebee.load_generation_config({:local, whisper_local_dir})

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
      |> child(:whisper_filter, %Membrane.Whisper.TranscriberFilter{
        serving: load_whisper_serving()
      })
      |> child(:tee, Membrane.Tee)
      |> child(:sink, %Membrane.File.Sink{location: @output_file}),
      get_child(:tee) |> child(:testing_sink, Membrane.Testing.Sink)
    ]

    {:ok, _supervisor_pid, pipeline_pid} = Pipeline.start(spec: spec)

    assert_end_of_stream(pipeline_pid, :sink, :input, 20_000)

    [
      assert_sink_event(pipeline_pid, :testing_sink, %TranscriptEvent{
        text:
          " Adventure 1, a scandal in Bohemia from the adventures of Sherlock Holmes by Sir Arthur Conan Doyle.",
        start_timestamp_seconds: +0.0,
        end_timestamp_seconds: 7.0
      }),
      assert_sink_event(pipeline_pid, :testing_sink, %TranscriptEvent{
        text:
          " This is a Librevox recording. All Librevox recordings are in the public domain, for more information or to volunteer, please visit librivox.org.",
        start_timestamp_seconds: 7.0,
        end_timestamp_seconds: 19.89
      }),
      assert_sink_event(pipeline_pid, :testing_sink, %TranscriptEvent{
        text: " Recording by Ruth Golden. A scan. recording by rescoating, a scandal in Bohemia.",
        start_timestamp_seconds: 19.89,
        end_timestamp_seconds: 25.0
      }),
      assert_sink_event(pipeline_pid, :testing_sink, %TranscriptEvent{
        text:
          " To Sherlock Holmes, she is always the woman. I have seldom heard him mention her under any other name. In his eyes she eclipses and predominates the whole of her sex. It was not that he felt any emotion akin to love for iron-eyedler. All emotions and that one particularly were apparent to his cold, precise, but admirably balanced mind. He was, I take it.",
        start_timestamp_seconds: 25.0,
        end_timestamp_seconds: 59.89399999999998
      })
    ]

    assert File.read!(@input_file) == File.read!(@output_file)
    File.rm!(@output_file)
  end
end
