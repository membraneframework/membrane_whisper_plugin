defmodule Membrane.Whisper.Filter do
  @moduledoc """
  Element that wraps a `Bumblebee.Audio.speech_to_text_whisper` serving, producing transcripts of the input audio.

  The serving must be provided by the user. For details on the configuration of the serving, see the description of the `serving` option of this element.
  """

  use Membrane.Filter

  require Membrane.Logger

  alias Membrane.RawAudio

  def_output_pad :output,
    accepted_format:
      %RawAudio{sample_format: :f32le, channels: 1}

  def_input_pad :input,
    accepted_format:
      %RawAudio{sample_format: :f32le, channels: 1}

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
  def handle_buffer(_pad, buffer, _ctx, state) do
    new_state =
      case state.serving_status do
        {:ready, pid} ->
          send(pid, {:serving_receive, buffer.payload})
          %{state | serving_status: :busy}

        :busy ->
          # NOTE: store a Stream in state and concat instead?
          %{state | acc: state.acc ++ [buffer.payload]}
      end

    {[buffer: {:output, buffer}], new_state}
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

    :ok = GenServer.cast(server, {:serving_start, self()})

    state =
      options
      |> Map.from_struct()
      |> Map.merge(%{
        serving_status: :busy,
        acc: [],
        finished?: false
      })

    {[], state}
  end

  @impl true
  def handle_stream_format(:input, %RawAudio{} = stream_format, _ctx, state) do
    state = %{state | input_stream_format: stream_format}
    {[stream_format: {:output, state.output_stream_format}], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    {[stream_format: {:output, state.output_stream_format}], state}
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
  def handle_info({:serving_ready, pid}, _ctx, state) do
    # Fires when serving is ready to process another buffer
    case {state.acc, state.finished?} do
      # Filter is waiting for more buffers to provide the serving with.
      # Saving PID to state, will send a buffer in `handle_buffer` as soon as it becomes available.
      {[], false} ->
        {[], %{state | serving_status: {:ready, pid}}}

      # Received EOS and no more buffers are to be processed.
      # Signal to the Stream running in `ServingServer`
      # that it can flush the rest of the transcript.
      {[], true} ->
        send(pid, :halt)
        {[], state}

      # Buffer ready for processing, send and wait for next ping from serving
      {[buffer | rest], _finished?} ->
        send(pid, {:serving_receive, buffer})
        {[], %{state | acc: rest, serving_status: :busy}}
    end
  end

  @impl true
  def handle_info(:serving_finished, _ctx, state) do
    {[end_of_stream: :output], state}
  end
end
