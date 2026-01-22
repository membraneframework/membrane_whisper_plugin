defmodule Membrane.Whisper.TranscriberFilter do
  @moduledoc """
  Element that wraps a `Bumblebee.Audio.speech_to_text_whisper/4` serving, producing transcripts of the input audio.

  The serving must be provided by the user. For details on the configuration of the serving, see the description of the `serving` option of this element.
  """

  use Membrane.Filter

  require Membrane.Logger

  alias Membrane.RawAudio

  def_input_pad :input,
    flow_control: :manual,
    demand_unit: :buffers,
    accepted_format: %RawAudio{sample_format: :f32le, channels: 1}

  def_output_pad :output,
    accepted_format: %RawAudio{sample_format: :f32le, channels: 1}

  def_options serving: [
                spec: Nx.Serving.t(),
                description: """
                The result of a call to `Bumblebee.Audio.speech_to_text_whisper/4`, with the following options set:
                - `:stream`: Must be `true`. Enables output streaming, e.g. it makes calls to `Nx.Serving.run/2` return an Elixir Stream that will return outputs from Whisper.
                - `:chunk_num_seconds`: Must be set. Enables long-form transcription by splitting the input into chunks of the given length. This means that calls to the serving with `Nx.Serving.run/2` will only accept enumerable input when `:chunk_num_seconds` is set.

                Example of creating a compatible serving:
                ```elixir
                  hf_repo = "openai/whisper-tiny"

                  {:ok, whisper} = Bumblebee.load_model({:hf, hf_repo})
                  {:ok, featurizer} = Bumblebee.load_featurizer({:hf, hf_repo})
                  {:ok, tokenizer} = Bumblebee.load_tokenizer({:hf, hf_repo})
                  {:ok, generation_config} = Bumblebee.load_generation_config({:hf, hf_repo})

                  serving = Bumblebee.Audio.speech_to_text_whisper(
                    whisper,
                    featurizer,
                    tokenizer,
                    generation_config,
                    stream: true,
                    chunk_num_seconds: 10
                  )

                  serving_with_timestamps = Bumblebee.Audio.speech_to_text_whisper(
                    whisper,
                    featurizer,
                    tokenizer,
                    generation_config,
                    stream: true,
                    chunk_num_seconds: 10,
                    timestamps: :segments
                  )
                  ```
                """
              ]

  @impl true
  def handle_init(ctx, %__MODULE__{serving: serving} = options) do
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
  def handle_playing(_ctx, state) do
    :ok = GenServer.cast(state.server_pid, {:serving_start, self()})
    {[], Map.delete(state, :server_pid)}
  end

  @impl true
  def handle_buffer(:input = _pad, buffer, _ctx, state) do
    send(state.serving_pid, {:serving_receive, buffer.payload})
    {[buffer: {:output, buffer}], state}
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
  def handle_info({:serving_output, whisper_output}, _ctx, state) do
    {[event: {:output, struct!(Membrane.Whisper.TranscriptEvent, whisper_output)}], state}
  end

  @impl true
  def handle_info(:serving_finished, _ctx, state) do
    {[end_of_stream: :output], state}
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {[], %{state | finished?: true}}
  end
end
