defmodule Membrane.Whisper do
  @moduledoc """
    TODO
  """

  use Membrane.Filter
  # import Mockery.Macro

  require Membrane.Logger

  # alias __MODULE__.Native
  alias Membrane.RawAudio

  @supported_sample_format [:f32le]
  @supported_channels [1]

  def_output_pad :output,
    accepted_format: %RawAudio{sample_format: format, channels: channels} when format in @supported_sample_format and channels in @supported_channels

  def_input_pad :input,
    accepted_format: %RawAudio{sample_format: format, channels: channels} when format in @supported_sample_format and channels in @supported_channels

  def_options input_stream_format: [
                spec: RawAudio.t() | nil,
                default: nil,
                description: """
                Stream format for the input pad. If set to nil (default value),
                stream format is assumed to be received through the pad. If explicitly set to some
                stream format, it cannot be changed by stream format received through the pad.
                """
              ],
              output_stream_format: [
                spec: RawAudio.t(),
                description: """
                Audio stream format for output pad
                """
              ]

  @impl true
  def handle_buffer(_pad, buffer, _ctx, state) do
   {[buffer: {:output, buffer}], state}
  end

  @impl true
  def handle_init(_ctx, %__MODULE__{} = options) do
    case options.input_stream_format do
      %RawAudio{} -> :ok
      nil -> :ok
      _other -> raise ":input_stream_format must be nil or %RawAudio{}"
    end

    case options.output_stream_format do
      %RawAudio{} -> :ok
      _other -> raise ":output_stream_format must be %RawAudio{}"
    end

    state =
      options
      |> Map.from_struct()
      |> Map.merge(%{
        queue: <<>>,
        input_stream_format_provided?: options.input_stream_format != nil,
        pts_queue: [],
        last_valid_pts: nil
      })

    {[], state}
  end

  @impl true
  def handle_setup(_ctx, %{input_stream_format_provided?: false} = state), do: {[], state}

  def handle_setup(_ctx, state) do
    {[], state}
  end

  @impl true
  def handle_stream_format(:input, %RawAudio{} = stream_format, _ctx, state) do
    state = %{
      state
      | input_stream_format: stream_format,
        queue: <<>>,
        pts_queue: [],
        last_valid_pts: nil
    }

    {[stream_format: {:output, state.output_stream_format}], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    {[stream_format: {:output, state.output_stream_format}], state}
  end

  def handle_end_of_stream(:input, _ctx, state) do
    {[end_of_stream: :output], state}
  end

end
