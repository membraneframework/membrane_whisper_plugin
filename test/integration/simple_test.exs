defmodule Membrane.Whisper.Integration.SimpleTest do
  use ExUnit.Case, async: false

  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec

  alias Membrane.Testing.Pipeline
  alias Membrane.Whisper.TranscriptEvent

  @spec load_whisper_serving() :: Nx.Serving.t()
  defp load_whisper_serving do
    hf_repo = "openai/whisper-tiny"

    {:ok, whisper} = Bumblebee.load_model({:hf, hf_repo})
    {:ok, featurizer} = Bumblebee.load_featurizer({:hf, hf_repo})
    {:ok, tokenizer} = Bumblebee.load_tokenizer({:hf, hf_repo})
    {:ok, generation_config} = Bumblebee.load_generation_config({:hf, hf_repo})

    serving =
      Bumblebee.Audio.speech_to_text_whisper(
        whisper,
        featurizer,
        tokenizer,
        generation_config,
        defn_options: [compiler: EXLA],
        stream: true,
        chunk_num_seconds: 10,
        context_num_seconds: 0,
        timestamps: :segments
      )

    serving
  end

  setup do
    {:ok, serving: load_whisper_serving()}
  end

  @input_file "test/fixtures/sherlock_1min.raw"
  @output_file "test/fixtures/output.raw"

  test "audio buffers are forwarded without change and transcripts are sent as events", ctx do
    ra_format = %Membrane.RawAudio{sample_format: :f32le, channels: 1, sample_rate: 16_000}

    spec = [
      child(:source, %Membrane.File.Source{location: @input_file})
      |> child(:parser, %Membrane.RawAudioParser{stream_format: ra_format})
      |> child(:whisper_filter, %Membrane.Whisper.TranscriberFilter{
        serving: ctx.serving
      })
      |> child(:tee, Membrane.Tee)
      |> child(:sink, %Membrane.File.Sink{location: @output_file}),
      get_child(:tee) |> child(:testing_sink, Membrane.Testing.Sink)
    ]

    {:ok, _supervisor_pid, pipeline_pid} = Pipeline.start(spec: spec)

    assert_end_of_stream(pipeline_pid, :sink, :input, 20_000)

#    [
#      assert_sink_event(pipeline_pid, :testing_sink, %TranscriptEvent{
#        text:
#          " Adventure 1, a scandal in Bohemia from the adventures of Sherlock Holmes by Sir Arthur Conan Doyle.",
#        start_timestamp_seconds: +0.0,
#        end_timestamp_seconds: 7.0
#      }),
#      assert_sink_event(pipeline_pid, :testing_sink, %TranscriptEvent{
#        text:
#          " This is a Librevox recording. All Librevox recordings are in the public domain, for more information or to volunteer, please visit librivox.org.",
#        start_timestamp_seconds: 7.0,
#        end_timestamp_seconds: 19.89
#      }),
#      assert_sink_event(pipeline_pid, :testing_sink, %TranscriptEvent{
#        text: " Recording by Ruth Golden. A scan. recording by rescoating, a scandal in Bohemia.",
#        start_timestamp_seconds: 19.89,
#        end_timestamp_seconds: 25.0
#      }),
#      assert_sink_event(pipeline_pid, :testing_sink, %TranscriptEvent{
#        text:
#          " To Sherlock Holmes, she is always the woman. I have seldom heard him mention her under any other name. In his eyes she eclipses and predominates the whole of her sex. It was not that he felt any emotion akin to love for iron-eyedler. All emotions and that one particularly were apparent to his cold, precise, but admirably balanced mind. He was, I take it.",
#        start_timestamp_seconds: 25.0,
#        end_timestamp_seconds: 59.89399999999998
#      })
#    ]

    assert File.read!(@input_file) == File.read!(@output_file)
    File.rm!(@output_file)
  end
end
