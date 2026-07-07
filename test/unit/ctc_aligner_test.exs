defmodule Membrane.Whisper.CTCAlignerTest do
  use ExUnit.Case, async: true

  alias Membrane.Whisper.CTCAligner

  # synthetic vocab: 0 = blank, 1 = A, 2 = B, 3 = C; keyword "AB" = [1, 2]
  @blank_id 0
  @keyword_ids [1, 2]

  # log-probs of a frame where `best` has probability 0.9 and the rest share the remainder
  defp frame(best) do
    hi = :math.log(0.90)
    lo = :math.log(0.90 / 3)
    for token_id <- 0..3, do: if(token_id == best, do: hi, else: lo)
  end

  test "locates a planted keyword within tolerance" do
    rows =
      for t <- 0..19 do
        cond do
          t in 8..9 -> frame(1)
          t in 11..12 -> frame(2)
          true -> frame(@blank_id)
        end
      end

    assert {first_frame, last_frame, avg_lp} = CTCAligner.locate(rows, @keyword_ids, @blank_id)
    assert first_frame in 8..9
    assert last_frame in 11..12
    assert :math.exp(avg_lp) > 0.5
  end

  test "scores blank audio below the detection threshold" do
    rows = for _t <- 0..19, do: frame(@blank_id)

    case CTCAligner.locate(rows, @keyword_ids, @blank_id) do
      nil -> :ok
      {_first_frame, _last_frame, avg_lp} -> assert :math.exp(avg_lp) < 0.5
    end
  end

  test "returns nil for empty input" do
    assert CTCAligner.locate([], @keyword_ids, @blank_id) == nil
  end
end
