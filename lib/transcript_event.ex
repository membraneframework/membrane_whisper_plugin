defmodule Membrane.Whisper.TranscriptEvent do
  @moduledoc """
  Event used to send the transcript and the timestamps predicted by Whisper.
  """

  @derive Membrane.EventProtocol
  defstruct [:text, :start_timestamp_seconds, :end_timestamp_seconds]
end
