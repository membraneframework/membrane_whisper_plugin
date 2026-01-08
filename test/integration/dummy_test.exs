defmodule Membrane.Whisper.Integration.DummyTest do
  use ExUnit.Case, async: false

  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec
  import Membrane.Whisper

  alias Membrane.RawAudio
  alias Membrane.Testing.Pipeline
  alias Membrane.File.Sink
  alias Membrane.File.Source
  alias Membrane.File.SeekSourceEvent
  alias Membrane.Buffer

  @input_file "test/fixtures/input.raw"
  @output_file "test/fixtures/output.raw"

  test "dummy test" do
    ra_format = %RawAudio{channels: 1, sample_rate: 16_000, sample_format: :f32le}
    spec = [
      child(:source, %Membrane.File.Source{location: @input_file}) |>
      child(:parser, %Membrane.RawAudioParser{stream_format: ra_format}) |>
      child(:whisper_filter, %Membrane.Whisper{
        input_stream_format: ra_format,
        output_stream_format: ra_format
      })
      |> child(:sink, %Sink{location: @output_file})
    ]

    {:ok, _supervisor_pid, pipeline_pid} = Pipeline.start(spec: spec)


    assert_end_of_stream(pipeline_pid, :sink)
    assert File.read!(@input_file) == File.read!(@output_file)
  end

end
