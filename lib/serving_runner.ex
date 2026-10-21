defmodule Membrane.Whisper.ServingRunner do
  @moduledoc false
  # Spawns a task which is used to convert the audio data
  # to a representation expected by the Whisper serving.

  # Bumblebee's Whisper streaming API expects an Elixir Stream input, and produces an Elixir Stream output.
  # To provide an Elixir Stream, the serving is wrapped in a separate process with its own mailbox
  # that `Membrane.Whisper.TranscriberFilter` can `send` buffers to.

  # When a transcript is ready it is sent back to `Membrane.Whisper.TranscriberFilter`.

  # Graceful termination is handled by halting the Stream
  # to flush the rest of the transcript from the serving.

  @spec child_spec({Nx.Serving.t(), parent :: pid()}) :: Supervisor.child_spec()
  def child_spec({serving, parent}) do
    %{
      id: __MODULE__,
      start: {Task, :start_link, [__MODULE__, :run, [serving, parent]]},
      restart: :temporary
    }
  end

  @spec run(Nx.Serving.t(), parent :: pid()) :: :ok
  def run(serving, parent) do
    input =
      Stream.resource(
        fn ->
          send(parent, {:serving_pid, self()})
          :ok
        end,
        fn acc ->
          send(parent, :serving_demand)

          receive do
            {:serving_receive, buffer} -> {[Nx.from_binary(buffer, :f32)], acc}
            :halt -> {:halt, acc}
          end
        end,
        fn _acc -> :ok end
      )

    serving
    |> Nx.Serving.run(input)
    |> Enum.each(&send(parent, {:serving_output, &1}))

    send(parent, :serving_finished)
    :ok
  end
end
