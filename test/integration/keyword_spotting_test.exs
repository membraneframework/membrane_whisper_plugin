defmodule Membrane.Whisper.Integration.KeywordSpottingTest do
  use ExUnit.Case, async: false

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.Testing.Pipeline
  alias Membrane.Whisper.KeywordEvent

  @moduletag timeout: 300_000

  # 2s of silence, the word "elixir" (ground truth 2.0s - 2.8s), 2s of silence
  @input_file "test/fixtures/keyword_padded.raw"

  @whisper_repo "openai/whisper-tiny"
  @ctc_repo "facebook/wav2vec2-base-960h"

  defp load_whisper_serving do
    {:ok, whisper} = Bumblebee.load_model({:hf, @whisper_repo})
    {:ok, featurizer} = Bumblebee.load_featurizer({:hf, @whisper_repo})
    {:ok, tokenizer} = Bumblebee.load_tokenizer({:hf, @whisper_repo})
    {:ok, generation_config} = Bumblebee.load_generation_config({:hf, @whisper_repo})

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
    {:ok, model_info} = Bumblebee.load_model({:hf, @ctc_repo})
    {:ok, featurizer} = Bumblebee.load_featurizer({:hf, @ctc_repo})
    {model_info, featurizer}
  end

  test "a spoken keyword is localized with word-level precision and absent keywords are rejected" do
    {model_info, featurizer} = load_ctc_model()
    ra_format = %Membrane.RawAudio{sample_format: :f32le, channels: 1, sample_rate: 16_000}

    spec =
      child(:source, %Membrane.File.Source{location: @input_file})
      |> child(:parser, %Membrane.RawAudioParser{stream_format: ra_format})
      |> child(:whisper_filter, %Membrane.Whisper.TranscriberFilter{
        serving: load_whisper_serving()
      })
      |> child(:keyword_spotter, %Membrane.Whisper.KeywordSpotterFilter{
        keywords: ["elixir", "python"],
        model_info: model_info,
        featurizer: featurizer
      })
      |> child(:sink, Membrane.Testing.Sink)

    pipeline_pid = Pipeline.start_link_supervised!(spec: spec)

    assert_sink_event(pipeline_pid, :sink, %KeywordEvent{keyword: "elixir"} = event, 120_000)
    assert_in_delta event.start_timestamp_seconds, 2.0, 0.5
    assert_in_delta event.end_timestamp_seconds, 2.8, 0.5
    assert event.confidence >= 0.5

    assert_end_of_stream(pipeline_pid, :sink, :input, 120_000)
    refute_sink_event(pipeline_pid, :sink, %KeywordEvent{keyword: "python"}, 0)
  end
end
