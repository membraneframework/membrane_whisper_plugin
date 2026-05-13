defmodule Membrane.Whisper.TranscriberFilter do
  @moduledoc """
  Element that wraps a `Bumblebee.Audio.speech_to_text_whisper/4` serving, producing transcripts of the input audio.

  The transcripts are sent via the `:output` pad along with the audio buffers, as `Membrane.Whisper.TranscriptEvent` events.
  A sequence of audio buffers is followed by an event containing the transcript for said sequence, e.g.:
  `<audio frames 0s - 10s> <event with transciption of 0s-10s> <audio frames 10s-20s> <event with transcription of 10s-20s> <audio frames 20s-30s> ...`

  The serving must be provided by the user. For details on the configuration of the serving, see the description of the `serving` option of this element.

  ## Audio/transcript ordering guarantee

  Each `Membrane.Whisper.TranscriptEvent` is emitted *after* all audio buffers it
  corresponds to have been forwarded downstream. Audio is buffered internally and
  released in sync with each transcript event:

  - With `timestamps: :segments` in the serving, the `end_timestamp_seconds` field
    of each event is used as an absolute byte offset into the buffered audio.
    This is correct regardless of how many windows `Nx.Serving` has pre-consumed
    during inference (e.g. due to JIT warm-up).

  - Without timestamps, all audio accumulated since the previous event is released
    together with the event. This requires `context_num_seconds: 0` in the serving;
    with the default overlap (`chunk_num_seconds / 6`) the `at_least_2?` guard in
    Bumblebee delays the first transcript until two windows have accumulated,
    causing excess audio to be paired with it.

        Bumblebee.Audio.speech_to_text_whisper(whisper, featurizer, tokenizer,
          generation_config, stream: true, chunk_num_seconds: 8,
          context_num_seconds: 0)

  ## Latency

  Audio is held until its transcript arrives, introducing a delay of approximately
  `chunk_num_seconds` before any audio reaches downstream elements.
  """

  use Membrane.Filter

  alias Membrane.RawAudio

  # f32le @ 16 kHz mono
  @bytes_per_second 16_000 * 4

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
        finished?: false,
        audio_buffer: [],
        released_bytes: 0,
        pending_events: [],
        drain_target: nil,
        drain_event: nil,
        serving_finished?: false
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
  def handle_stream_format(:input, format, _ctx, state) do
    {[stream_format: {:output, format}], state}
  end

  @impl true
  def handle_demand(:output = _pad, _size, :buffers, ctx, state) do
    state = %{state | output_demand?: true}
    {drain_actions, state} = drain_for_demand(state, [])

    if Enum.any?(drain_actions, &match?({:buffer, _}, &1)) do
      send(self(), :continue_drain)
    end

    input_actions =
      if drain_actions == [] and state.serving_demand? do
        case ctx.pads do
          %{input: %{demand: 0}} -> [demand: {:input, 1}]
          _ -> []
        end
      else
        []
      end

    {drain_actions ++ input_actions, state}
  end

  @impl true
  def handle_buffer(:input = _pad, buffer, _ctx, %{serving_demand?: true} = state) do
    send(state.serving_pid, {:serving_receive, buffer.payload})
    {[], %{state | serving_demand?: false, audio_buffer: state.audio_buffer ++ [buffer]}}
  end

  @impl true
  def handle_info(:serving_demand, ctx, %{finished?: false} = state) do
    state = %{state | serving_demand?: true}

    actions =
      case ctx.pads do
        %{input: %{demand: 0}} -> [demand: {:input, 1}]
        _ -> []
      end

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
  def handle_info(:continue_drain, _ctx, state) do
    {[redemand: :output], state}
  end

  @impl true
  def handle_info({:serving_output, whisper_output}, _ctx, state) do
    event = struct!(Membrane.Whisper.TranscriptEvent, whisper_output)

    target_bytes =
      case event.end_timestamp_seconds do
        nil -> state.released_bytes + buffer_bytes(state.audio_buffer)
        end_ts -> round(end_ts * @bytes_per_second)
      end

    state = %{state | pending_events: state.pending_events ++ [{target_bytes, event}]}
    drain(state)
  end

  @impl true
  def handle_info(:serving_finished, _ctx, state) do
    drain(%{state | serving_finished?: true})
  end

  @impl true
  def handle_info(
        {:DOWN, _ref, :process, server_pid, :normal},
        _ctx,
        %{server_pid: server_pid} = state
      ) do
    {[], state}
  end

  @impl true
  def handle_info(
        {:DOWN, _ref, :process, server_pid, reason},
        _ctx,
        %{finished?: finished?, server_pid: server_pid} = state
      ) do
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

  # No downstream demand — nothing to do.
  defp drain(%{output_demand?: false} = state), do: {[], state}

  # Active drain in progress — emit the next buffer toward drain_target.
  defp drain(%{drain_target: target} = state) when is_integer(target) do
    emit_up_to(state, target)
  end

  # Start draining the next pending event.
  defp drain(%{pending_events: [{target, event} | rest]} = state) do
    drain(%{state | drain_target: target, drain_event: event, pending_events: rest})
  end

  # All events drained, serving done — flush remaining audio one buffer at a time.
  defp drain(%{serving_finished?: true, audio_buffer: [buf | rest]} = state) do
    state = %{state |
      audio_buffer: rest,
      released_bytes: state.released_bytes + byte_size(buf.payload),
      output_demand?: false
    }

    {[buffer: {:output, buf}, redemand: :output], state}
  end

  # All events drained, buffer empty — end of stream.
  defp drain(%{serving_finished?: true, audio_buffer: []} = state) do
    {[end_of_stream: :output], %{state | output_demand?: false}}
  end

  # Waiting for transcript events.
  defp drain(state), do: {[], state}

  defp drain_for_demand(state, acc) do
    {actions, state} = drain(state)
    actions = Keyword.delete(actions, :redemand)
    has_buffer = Enum.any?(actions, &match?({:buffer, _}, &1))
    has_eos = Enum.any?(actions, &match?({:end_of_stream, _}, &1))
    all_actions = acc ++ actions

    cond do
      actions == [] -> {all_actions, state}
      has_buffer or has_eos -> {all_actions, state}
      true -> drain_for_demand(%{state | output_demand?: true}, all_actions)
    end
  end

  # Emit one buffer (splitting if needed) toward the target byte offset.
  defp emit_up_to(state, target) do
    remaining = target - state.released_bytes

    cond do
      remaining <= 0 ->
        event = state.drain_event
        state = %{state | drain_target: nil, drain_event: nil, output_demand?: false}
        {[event: {:output, event}, redemand: :output], state}

      state.audio_buffer == [] and state.serving_finished? ->
        # Target exceeds available audio (e.g. last segment timestamp rounds past EOF).
        event = state.drain_event
        state = %{state | drain_target: nil, drain_event: nil, output_demand?: false}
        {[event: {:output, event}, redemand: :output], state}

      state.audio_buffer == [] ->
        # Audio not yet fully buffered — wait.
        {[], state}

      true ->
        [%Membrane.Buffer{payload: payload} = buf | rest] = state.audio_buffer
        buf_size = byte_size(payload)

        if buf_size <= remaining do
          state = %{state |
            audio_buffer: rest,
            released_bytes: state.released_bytes + buf_size,
            output_demand?: false
          }

          {[buffer: {:output, buf}, redemand: :output], state}
        else
          <<kept::binary-size(remaining), tail::binary>> = payload

          state = %{state |
            audio_buffer: [%{buf | payload: tail} | rest],
            released_bytes: state.released_bytes + remaining,
            output_demand?: false
          }

          {[buffer: {:output, %{buf | payload: kept}}, redemand: :output], state}
        end
    end
  end

  defp buffer_bytes(buffer) do
    Enum.reduce(buffer, 0, &(byte_size(&1.payload) + &2))
  end
end
