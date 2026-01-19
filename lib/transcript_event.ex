defmodule Membrane.Whisper.TranscriptEvent do
  @moduledoc """
  Event used to send the transcript and the timestamps predicted by Whisper.
  """

  @derive Membrane.EventProtocol
  defstruct [:whisper_output]
end
