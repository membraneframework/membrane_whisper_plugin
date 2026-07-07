defmodule Membrane.Whisper.CTCAligner do
  @moduledoc """
  CTC forced alignment of a token sequence against frame log-probabilities.

  The target sequence is wrapped in wildcard "star" states (matching any frame at
  the cost of that frame's best token), so the sequence may sit anywhere in the
  audio. The best path is found with a Viterbi dynamic program over the standard
  CTC state graph (blanks interleaved with labels, plus the CTC skip rule).

  Used by `Membrane.Whisper.KeywordSpotterFilter` to localize keywords within
  Whisper segments, but has no dependencies on Membrane or any model runtime —
  it operates on plain lists of log-probabilities.
  """

  @neg_inf -1.0e30

  @typedoc "Log-probabilities of every vocabulary token in a single frame."
  @type frame_log_probs :: [float()]

  @typedoc "Index of a token in the vocabulary."
  @type token_id :: non_neg_integer()

  @doc """
  Locates the token sequence `token_ids` within `lp_rows`, a list of frames
  (each a list of vocabulary log-probabilities).

  Returns `{start_frame, end_frame, avg_token_log_prob}` for the best alignment
  (frame indices are inclusive), or `nil` when no valid alignment exists or the
  best path does not pass through the token sequence at all.

  `:math.exp(avg_token_log_prob)` is a 0..1 confidence measure of the alignment.
  """
  @spec locate([frame_log_probs()], [token_id(), ...], token_id()) ::
          {non_neg_integer(), non_neg_integer(), float()} | nil
  def locate(lp_rows, token_ids, blank_id) do
    n = length(token_ids)
    units = [:star | token_ids] ++ [:star]
    ext = Enum.flat_map(units, &[:blank, &1]) ++ [:blank]
    s_count = length(ext)
    ext_t = List.to_tuple(ext)

    emissions = Enum.map(lp_rows, &frame_emissions(&1, ext_t, s_count, blank_id))

    with [first | rest] <- emissions,
         {final, bps} <- viterbi(first, rest, ext_t, s_count),
         {:ok, end_s} <- best_end_state(final, s_count) do
      path = backtrack(bps, end_s)
      collect_keyword_span(path, emissions, n)
    else
      _no_alignment -> nil
    end
  end

  defp frame_emissions(row, ext_t, s_count, blank_id) do
    row_t = List.to_tuple(row)
    blank_lp = elem(row_t, blank_id)
    star_lp = Enum.max(row)

    for s <- 0..(s_count - 1) do
      case elem(ext_t, s) do
        :blank -> blank_lp
        :star -> star_lp
        id -> elem(row_t, id)
      end
    end
    |> List.to_tuple()
  end

  defp viterbi(first, rest, ext_t, s_count) do
    init =
      for s <- 0..(s_count - 1) do
        if s in [0, 1], do: {elem(first, s), -1}, else: {@neg_inf, -1}
      end
      |> List.to_tuple()

    Enum.reduce(rest, {init, []}, fn em, {prev, bps} ->
      row =
        for s <- 0..(s_count - 1) do
          {best, best_prev} =
            candidates(s, ext_t)
            |> Enum.map(fn p -> {prev |> elem(p) |> elem(0), p} end)
            |> Enum.max_by(&elem(&1, 0))

          {best + elem(em, s), best_prev}
        end
        |> List.to_tuple()

      {row, [row | bps]}
    end)
  end

  # stay, step, and the CTC skip rule (over a blank, between distinct labels)
  defp candidates(s, ext_t) do
    skip? = s >= 2 and elem(ext_t, s) != :blank and elem(ext_t, s) != elem(ext_t, s - 2)
    Enum.filter([s, s - 1] ++ if(skip?, do: [s - 2], else: []), &(&1 >= 0))
  end

  defp best_end_state(final, s_count) do
    end_s = Enum.max_by([s_count - 1, s_count - 2], fn s -> final |> elem(s) |> elem(0) end)
    score = final |> elem(end_s) |> elem(0)
    if score > @neg_inf / 2, do: {:ok, end_s}, else: :no_valid_path
  end

  # bps holds rows for t = T-1 .. 1 (newest first); each bp points to the state at t-1
  defp backtrack(bps, end_s) do
    {path, _state} =
      Enum.reduce(bps, {[end_s], end_s}, fn row, {acc, s} ->
        prev_s = row |> elem(s) |> elem(1)
        {[prev_s | acc], prev_s}
      end)

    path
  end

  # ext layout: [b, u0, b, u1, ...] — odd s maps to unit div(s, 2); tokens are units 1..n
  defp collect_keyword_span(path, emissions, n) do
    hits =
      Enum.zip(path, emissions)
      |> Enum.with_index()
      |> Enum.filter(fn {{s, _em}, _t} -> rem(s, 2) == 1 and div(s, 2) in 1..n end)

    case hits do
      [] ->
        nil

      hits ->
        frames = Enum.map(hits, fn {_state_em, t} -> t end)
        total = hits |> Enum.map(fn {{s, em}, _t} -> elem(em, s) end) |> Enum.sum()
        {Enum.min(frames), Enum.max(frames), total / length(hits)}
    end
  end
end
