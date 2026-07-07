defmodule Membrane.Whisper.KeywordSpotterFilter do
  @moduledoc """
  Element that localizes watched keywords in the audio stream with word-level precision.

  It consumes `Membrane.Whisper.TranscriptEvent`s produced upstream by
  `Membrane.Whisper.TranscriberFilter` and, for each watched keyword, runs a CTC forced
  alignment (`Membrane.Whisper.CTCAligner`) of the keyword against frame
  log-probabilities computed by a Wav2Vec2 CTC model over the audio of the corresponding
  segment. This two-pass design localizes keywords at the model's frame resolution
  (~20 ms for `facebook/wav2vec2-base-960h`), which Whisper's segment-level timestamps
  cannot provide.

  Detection is two-tier:

    * if the keyword appears in the segment's transcript (whole-word, case-insensitive
      match), the alignment result is emitted unconditionally,
    * otherwise the segment is still CTC-scanned and a result is emitted only when its
      confidence reaches `:fallback_threshold` — this rescues keywords that Whisper
      misheard.

  Detections are sent as `Membrane.Whisper.KeywordEvent`s on the `:output` pad. Audio
  buffers and all other events are forwarded unchanged. When `:notify_parent?` is set,
  each detection is additionally sent to the parent as a `{:keyword_detected, event}`
  notification.

  The element keeps a ring buffer of recent raw audio (see `:buffer_duration_seconds`).
  Whisper timestamps are seconds of audio since the start of the stream and the ring
  buffer keeps a matching sample counter, so a segment can only be aligned while its
  audio is still buffered.
  """

  use Membrane.Filter

  require Membrane.Logger

  alias Membrane.RawAudio
  alias Membrane.Whisper.{CTCAligner, CtcModelServer, KeywordEvent, TranscriptEvent}

  @sample_rate 16_000
  @bytes_per_sample 4

  # vocab.json of facebook/wav2vec2-base-960h
  @default_vocab %{
    "<pad>" => 0,
    "<s>" => 1,
    "</s>" => 2,
    "<unk>" => 3,
    "|" => 4,
    "E" => 5,
    "T" => 6,
    "A" => 7,
    "O" => 8,
    "N" => 9,
    "I" => 10,
    "H" => 11,
    "S" => 12,
    "R" => 13,
    "D" => 14,
    "L" => 15,
    "U" => 16,
    "M" => 17,
    "W" => 18,
    "C" => 19,
    "F" => 20,
    "G" => 21,
    "Y" => 22,
    "P" => 23,
    "B" => 24,
    "V" => 25,
    "K" => 26,
    "'" => 27,
    "X" => 28,
    "J" => 29,
    "Q" => 30,
    "Z" => 31
  }

  def_input_pad :input,
    accepted_format: %RawAudio{sample_format: :f32le, channels: 1, sample_rate: 16_000}

  def_output_pad :output,
    accepted_format: %RawAudio{sample_format: :f32le, channels: 1, sample_rate: 16_000}

  def_options keywords: [
                spec: [String.t()],
                description: """
                The keywords to watch for. Each keyword must be expressible in the CTC
                vocabulary (for the default vocabulary: letters, apostrophes and spaces,
                case-insensitive).
                """
              ],
              model_info: [
                spec: Bumblebee.model_info(),
                description: """
                The result of a call to `Bumblebee.load_model/2` for a Wav2Vec2 model with
                the `:for_ctc` architecture, e.g.

                ```elixir
                {:ok, model_info} = Bumblebee.load_model({:hf, "facebook/wav2vec2-base-960h"})
                ```
                """
              ],
              featurizer: [
                spec: Bumblebee.Featurizer.t(),
                description: """
                The result of a call to `Bumblebee.load_featurizer/2` for the same model
                repository as `:model_info`.
                """
              ],
              vocab: [
                spec: %{String.t() => non_neg_integer()},
                default: @default_vocab,
                description: """
                The CTC vocabulary of the model: a map from token to its index in the model's
                logits (the contents of the repository's `vocab.json`). Defaults to the
                vocabulary of `facebook/wav2vec2-base-960h`.
                """
              ],
              blank_token: [
                spec: String.t(),
                default: "<pad>",
                description: "The token of `:vocab` used as the CTC blank."
              ],
              word_delimiter_token: [
                spec: String.t(),
                default: "|",
                description: "The token of `:vocab` used as the word delimiter."
              ],
              buffer_duration_seconds: [
                spec: pos_integer(),
                default: 90,
                description: """
                How many seconds of the most recent audio are kept for alignment. Must cover
                the Whisper serving's `:chunk_num_seconds` plus the delay of transcription,
                otherwise segments will be dropped from keyword spotting.
                """
              ],
              fallback_threshold: [
                spec: float(),
                default: 0.5,
                description: """
                Minimal confidence (0..1) for emitting a detection when the keyword does not
                appear in the segment's transcript (the CTC-only fallback tier).
                """
              ],
              slice_pad_seconds: [
                spec: float(),
                default: 1.0,
                description: """
                How much audio around the Whisper segment is included in the alignment, as a
                margin for imprecise segment timestamps.
                """
              ],
              notify_parent?: [
                spec: boolean(),
                default: false,
                description: """
                Whether to also send each detection to the parent as a
                `{:keyword_detected, Membrane.Whisper.KeywordEvent.t()}` notification.
                """
              ]

  @impl true
  def handle_init(_ctx, options) do
    state =
      options
      |> Map.from_struct()
      |> Map.merge(%{
        keyword_ids: Map.new(options.keywords, &{&1, keyword_ids!(&1, options)}),
        blank_id: Map.fetch!(options.vocab, options.blank_token),
        frame_seconds: Enum.product(options.model_info.spec.conv_strides) / @sample_rate,
        server_pid: nil,
        audio_queue: :queue.new(),
        first_sample: 0,
        end_sample: 0,
        pending_requests: 0,
        finished?: false
      })

    {[], state}
  end

  @impl true
  def handle_setup(ctx, state) do
    {:ok, server_pid} =
      Membrane.UtilitySupervisor.start_child(
        ctx.utility_supervisor,
        {CtcModelServer,
         %{model_info: state.model_info, featurizer: state.featurizer, parent_pid: self()}}
      )

    Process.monitor(server_pid)

    {[], %{state | server_pid: server_pid}}
  end

  @impl true
  def handle_buffer(:input, buffer, _ctx, state) do
    {[buffer: {:output, buffer}], store_samples(state, buffer.payload)}
  end

  @impl true
  def handle_event(:input, %TranscriptEvent{} = event, _ctx, state) do
    state = Enum.reduce(state.keywords, state, &request_alignment(&2, event, &1))
    {[forward: event], state}
  end

  @impl true
  def handle_event(_pad, event, _ctx, state), do: {[forward: event], state}

  @impl true
  def handle_end_of_stream(:input, _ctx, %{pending_requests: 0} = state) do
    {[end_of_stream: :output], %{state | finished?: true}}
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {[], %{state | finished?: true}}
  end

  @impl true
  def handle_info({:ctc_log_probs, meta, lp_rows}, _ctx, state) do
    state = %{state | pending_requests: state.pending_requests - 1}
    token_ids = Map.fetch!(state.keyword_ids, meta.keyword)

    detection_actions =
      case CTCAligner.locate(lp_rows, token_ids, state.blank_id) do
        nil -> []
        {first_frame, last_frame, avg_lp} -> emit(state, meta, first_frame, last_frame, avg_lp)
      end

    eos_actions =
      if state.finished? and state.pending_requests == 0,
        do: [end_of_stream: :output],
        else: []

    {detection_actions ++ eos_actions, state}
  end

  @impl true
  def handle_info(
        {:DOWN, _ref, :process, server_pid, reason},
        _ctx,
        %{server_pid: server_pid} = state
      ) do
    if reason == :normal do
      {[], state}
    else
      raise "Unexpected CTC model server exit with reason: #{inspect(reason)}"
    end
  end

  defp request_alignment(state, %TranscriptEvent{} = event, keyword) do
    seg_start = event.start_timestamp_seconds || 0.0
    seg_stop = event.end_timestamp_seconds || state.end_sample / @sample_rate

    from_sample =
      max(round((seg_start - state.slice_pad_seconds) * @sample_rate), state.first_sample)

    to_sample = min(round((seg_stop + state.slice_pad_seconds) * @sample_rate), state.end_sample)

    if to_sample > from_sample do
      meta = %{
        keyword: keyword,
        gated?: contains_word?(event.text, keyword),
        from_seconds: from_sample / @sample_rate,
        segment_text: event.text
      }

      slice = extract_slice(state, from_sample, to_sample)
      CtcModelServer.request_log_probs(state.server_pid, meta, slice)
      %{state | pending_requests: state.pending_requests + 1}
    else
      Membrane.Logger.warning(
        "Skipping keyword spotting of #{inspect(keyword)} in segment " <>
          "#{inspect(seg_start)}s - #{inspect(seg_stop)}s: audio no longer buffered"
      )

      state
    end
  end

  defp emit(state, meta, first_frame, last_frame, avg_lp) do
    confidence = :math.exp(avg_lp)

    if meta.gated? or confidence >= state.fallback_threshold do
      event = %KeywordEvent{
        keyword: meta.keyword,
        start_timestamp_seconds: meta.from_seconds + first_frame * state.frame_seconds,
        end_timestamp_seconds: meta.from_seconds + (last_frame + 1) * state.frame_seconds,
        confidence: Float.round(confidence, 3),
        segment_text: meta.segment_text,
        ctc_only?: not meta.gated?
      }

      notify_actions =
        if state.notify_parent?, do: [notify_parent: {:keyword_detected, event}], else: []

      [event: {:output, event}] ++ notify_actions
    else
      []
    end
  end

  defp store_samples(state, payload) do
    end_sample = state.end_sample + div(byte_size(payload), @bytes_per_sample)
    queue = :queue.in({state.end_sample, payload}, state.audio_queue)
    min_sample = end_sample - state.buffer_duration_seconds * @sample_rate
    {queue, first_sample} = drop_expired(queue, min_sample, state.first_sample)

    %{state | audio_queue: queue, end_sample: end_sample, first_sample: first_sample}
  end

  # Drops queue entries that lie entirely before min_sample. The remaining front
  # entry may still start before min_sample - it is kept whole.
  defp drop_expired(queue, min_sample, first_sample) do
    case :queue.peek(queue) do
      {:value, {chunk_start, payload}} ->
        chunk_end = chunk_start + div(byte_size(payload), @bytes_per_sample)

        if chunk_end <= min_sample do
          drop_expired(:queue.drop(queue), min_sample, chunk_end)
        else
          {queue, max(first_sample, chunk_start)}
        end

      :empty ->
        {queue, first_sample}
    end
  end

  defp extract_slice(state, from_sample, to_sample) do
    state.audio_queue
    |> :queue.to_list()
    |> Enum.flat_map(fn {chunk_start, payload} ->
      chunk_end = chunk_start + div(byte_size(payload), @bytes_per_sample)
      low = max(from_sample, chunk_start)
      high = min(to_sample, chunk_end)

      if low < high do
        [
          binary_part(
            payload,
            (low - chunk_start) * @bytes_per_sample,
            (high - low) * @bytes_per_sample
          )
        ]
      else
        []
      end
    end)
    |> IO.iodata_to_binary()
  end

  # keyword -> CTC token ids (for the default vocab: uppercase letters, ' and
  # word-delimited spaces)
  defp keyword_ids!(keyword, options) do
    keyword
    |> String.upcase()
    |> String.trim()
    |> String.replace(~r/\s+/, options.word_delimiter_token)
    |> String.graphemes()
    |> Enum.map(fn grapheme ->
      Map.get(options.vocab, grapheme) ||
        raise ArgumentError,
              "keyword #{inspect(keyword)} contains #{inspect(grapheme)}, " <>
                "which is not present in the CTC vocabulary"
    end)
  end

  defp contains_word?(text, keyword) do
    Regex.match?(~r/\b#{Regex.escape(keyword)}\b/iu, text || "")
  end
end
