defmodule Membrane.Whisper.TranscriberFilter do
  @moduledoc """
  Element that wraps a `Bumblebee.Audio.speech_to_text_whisper/4` serving, producing transcripts of the input audio.

  The transcripts are sent via the `:output` pad along with the audio buffers, as `Membrane.Whisper.TranscriptEvent` events.
  A sequence of audio buffers is followed by an event containing the transcript for said sequence, e.g.:
  `<audio frames 0s - 10s> <event with transciption of 0s-10s> <audio frames 10s-20s> <event with transcription of 10s-20s> <audio frames 20s-30s> ...`

  The serving must be provided by the user. For details on the configuration of the serving, see the description of the `serving` option of this element.
  """

  use Membrane.Filter

  require Membrane.Logger

  alias Membrane.RawAudio

  def_input_pad :input,
    flow_control: :manual,
    demand_unit: :buffers,
    accepted_format: %RawAudio{sample_format: :f32le, channels: 1, sample_rate: 16_000}

  def_output_pad :output,
    flow_control: :manual,
    demand_unit: :buffers,
    accepted_format: %RawAudio{sample_format: :f32le, channels: 1, sample_rate: 16_000}

  def_options serving: [
                spec: Nx.Serving.t(),
                description: """
                The result of a call to `Bumblebee.Audio.speech_to_text_whisper/4`, with the following options set:
                - `:stream`: Must be `true`. Enables output streaming, e.g. it makes calls to `Nx.Serving.run/2` return an Elixir Stream that will return outputs from Whisper.
                - `:chunk_num_seconds`: Must be set. Enables long-form transcription by splitting the input into chunks of the given length. This means that calls to the serving with `Nx.Serving.run/2` will only accept enumerable input when `:chunk_num_seconds` is set.
                - `:context_num_seconds`: Must be set to 0 for proper synchronisation with streamed audio.

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
                    chunk_num_seconds: 10,
                    context_num_seconds: 0
                  )
                  ```

                  One could also enable timestamp prediction by the model, though this will impact the result by producing transcripts of segments of varying length,
                  e.g. a transcript of 7 seconds of audio followed by a transcript of 13 seconds of audio, etc.
                  ```elixir
                  hf_repo = "openai/whisper-tiny"

                  {:ok, whisper} = Bumblebee.load_model({:hf, hf_repo})
                  {:ok, featurizer} = Bumblebee.load_featurizer({:hf, hf_repo})
                  {:ok, tokenizer} = Bumblebee.load_tokenizer({:hf, hf_repo})
                  {:ok, generation_config} = Bumblebee.load_generation_config({:hf, hf_repo})

                  serving_with_timestamps = Bumblebee.Audio.speech_to_text_whisper(
                    whisper,
                    featurizer,
                    tokenizer,
                    generation_config,
                    stream: true,
                    chunk_num_seconds: 10,
                    context_num_seconds: 0,
                    timestamps: :segments
                  )
                  ```

                  For other useful options, see `Bumblebee.Audio.speech_to_text_whisper/4`.
                """
              ]

  @impl true
  def handle_init(_ctx, options) do
    state =
      options
      |> Map.from_struct()
      |> Map.merge(%{
        server_pid: nil,
        serving_pid: nil,
        serving_demand?: false,
        output_demand?: false,
        finished?: false
      })

    {[], state}
  end

  @impl true
  def handle_setup(ctx, %{serving: serving} = state) do
    {:ok, server_pid} =
      Membrane.UtilitySupervisor.start_child(
        ctx.utility_supervisor,
        {Membrane.Whisper.ModelServer, %{serving: serving, parent_pid: self()}}
      )

    Process.monitor(server_pid)

    {[], %{state | server_pid: server_pid}}
  end

  @impl true
  def handle_demand(:output = _pad, _size, :buffers, ctx, state) do
    state = %{state | output_demand?: true}
    actions = maybe_demand(state.output_demand?, state.serving_demand?, ctx.pads)
    {actions, state}
  end

  @impl true
  def handle_buffer(
        :input = _pad,
        buffer,
        _ctx,
        %{serving_demand?: true, output_demand?: true} = state
      ) do
    send(state.serving_pid, {:serving_receive, buffer.payload})

    {[buffer: {:output, buffer}, redemand: :output],
     %{state | serving_demand?: false, output_demand?: false}}
  end

  @impl true
  def handle_info(:serving_demand, ctx, %{finished?: false} = state) do
    state = %{state | serving_demand?: true}
    actions = maybe_demand(state.output_demand?, state.serving_demand?, ctx.pads)
    {actions, state}
  end

  @impl true
  def handle_info(:serving_demand, _ctx, %{finished?: true} = state) do
    send(state.serving_pid, :halt)
    {[], state}
  end

  @impl true
  def handle_info({:serving_pid, pid}, _ctx, state) do
    {[], %{state | serving_pid: pid}}
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
  def handle_info({:DOWN, _ref, :process, server_pid, reason}, _ctx, %{finished?: finished?, server_pid: server_pid}) do
    if finished? do
      {[end_of_stream: :output], state}
    else
      raise "Unexpected serving exit with reason: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {[], %{state | finished?: true}}
  end

  @spec maybe_demand(
          output_demand? :: boolean(),
          serving_demand? :: boolean(),
          pads :: %{atom() => Membrane.Element.PadData.t()}
        ) :: list(Membrane.Element.Action.demand())
  # We only demand on the input pad if it doesn't already have an awaiting demand to satisfy
  defp maybe_demand(true, true, %{input: %Membrane.Element.PadData{demand: 0}}),
    do: [demand: {:input, 1}]

  defp maybe_demand(_output_demand?, _serving_demand?, _input_demand), do: []
end
