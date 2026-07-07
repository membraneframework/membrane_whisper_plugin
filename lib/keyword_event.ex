defmodule Membrane.Whisper.KeywordEvent do
  @moduledoc """
  Event sent by `Membrane.Whisper.KeywordSpotterFilter` when a watched keyword
  was localized in the audio stream.

  Fields:
    * `:keyword` — the detected keyword, as passed in the element's options,
    * `:start_timestamp_seconds`, `:end_timestamp_seconds` — location of the spoken
      keyword, in seconds of audio since the start of the stream,
    * `:confidence` — average per-frame probability (0..1) of the keyword's tokens
      along the alignment path,
    * `:segment_text` — transcript of the Whisper segment the keyword was localized in,
    * `:ctc_only?` — `true` when the keyword was absent from the transcript (Whisper
      misheard it) and was found by the CTC fallback scan instead.
  """

  @derive Membrane.EventProtocol
  defstruct [
    :keyword,
    :start_timestamp_seconds,
    :end_timestamp_seconds,
    :confidence,
    :segment_text,
    ctc_only?: false
  ]

  @type t :: %__MODULE__{
          keyword: String.t(),
          start_timestamp_seconds: float(),
          end_timestamp_seconds: float(),
          confidence: float(),
          segment_text: String.t() | nil,
          ctc_only?: boolean()
        }
end
