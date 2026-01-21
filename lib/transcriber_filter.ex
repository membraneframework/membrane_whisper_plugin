defmodule Membrane.Whisper.TranscriberFilter do
  @moduledoc """
  Element that wraps a `Bumblebee.Audio.speech_to_text_whisper` serving, producing transcripts of the input audio.

  The serving must be provided by the user. For details on the configuration of the serving, see the description of the `serving` option of this element.
  """

  use Membrane.Filter

  require Membrane.Logger

  alias Membrane.RawAudio

  def_output_pad :output,
    accepted_format: %RawAudio{sample_format: :f32le, channels: 1}

  def_input_pad :input,
    flow_control: :manual,
    demand_unit: :buffers,
    accepted_format: %RawAudio{sample_format: :f32le, channels: 1}

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
              ],
              serving: [
                spec: Nx.Serving.t(),
                description: """
                The result of a call to `Bumblebee.Audio.speech_to_text_whisper`, with the following options set:

                ```elixir
                  Bumblebee.Audio.speech_to_text_whisper(
                    ...,
                    stream: true,
                    chunk_num_seconds: chunk_num_seconds
                  )
                ```
                The options `chunk_num_seconds` and `stream` correspond to enabling input and output streaming.
                """
              ]

  @impl true
  def handle_buffer(:input = _pad, buffer, _ctx, state) do
    send(state.serving_pid, {:serving_receive, buffer.payload})
    {[buffer: {:output, buffer}], state}
  end

  @impl true
  def handle_init(ctx, %__MODULE__{serving: serving} = options) do
    case options.input_stream_format do
      %RawAudio{} -> :ok
      nil -> :ok
      _other -> raise ":input_stream_format must be nil or %RawAudio{}"
    end

    case options.output_stream_format do
      %RawAudio{} -> :ok
      _other -> raise ":output_stream_format must be %RawAudio{}"
    end

    {:ok, server} =
      Membrane.UtilitySupervisor.start_link_child(
        ctx.utility_supervisor,
        {Membrane.Whisper.ServingServer, [serving: serving]}
      )

    state =
      options
      |> Map.from_struct()
      |> Map.merge(%{
        server_pid: server,
        serving_pid: nil,
        finished?: false
      })

    {[], state}
  end

  @impl true
  def handle_stream_format(:input, %RawAudio{} = _stream_format, _ctx, state) do
    {[stream_format: {:output, state.output_stream_format}], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    :ok = GenServer.cast(state.server_pid, {:serving_start, self()})
    {[stream_format: {:output, state.output_stream_format}], Map.delete(state, :server_pid)}
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {[], %{state | finished?: true}}
  end

  @impl true
  def handle_info({:serving_output, whisper_output}, _ctx, state) do
    {[event: {:output, %Membrane.Whisper.TranscriptEvent{whisper_output: whisper_output}}], state}
  end

  @impl true
  def handle_info({:serving_pid, pid}, _ctx, state) do
    {[], %{state | serving_pid: pid}}
  end

  @impl true
  def handle_info(:serving_ready, _ctx, state) do
    if state.finished? do
      send(state.serving_pid, :halt)
      {[], state}
    else
      {[demand: {:input, 1}], state}
    end
  end

  @impl true
  def handle_info(:serving_finished, _ctx, state) do
    {[end_of_stream: :output], state}
  end
end
