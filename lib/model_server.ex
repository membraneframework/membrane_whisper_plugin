defmodule Membrane.Whisper.ModelServer do
  @moduledoc false

  # This GenServer is used to convert the audio data to a representation expected by the Whisper serving.

  # Bumblebee's Whisper streaming API expects an Elixir Stream input, and produces an Elixir Stream output.
  # To provide an Elixir Stream, the serving is wrapped in a separate process with its own mailbox
  # that `Membrane.Whisper.TranscriberFilter` can `send` buffers to.

  # When a transcript is ready it is sent back to `Membrane.Whisper.TranscriberFilter`.

  # Graceful termination is handled by halting the Stream to flush the rest of the transcript from the serving.

  use GenServer

  @spec start_link(%{serving: Nx.Serving.t(), parent_pid: pid()}) ::
          :ignore | {:error, any()} | {:ok, pid()}
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    {:ok, opts, {:continue, :serving_start}}
  end

  @impl true
  def handle_info({:EXIT, _pid, _reason}, state) do
    {:stop, :normal, state}
  end

  @impl true
  def handle_continue(:serving_start, %{serving: serving, parent_pid: parent_pid} = state) do
    stream =
      Stream.resource(
        fn ->
          send(parent_pid, {:serving_pid, self()})
          :ok
        end,
        fn state ->
          send(parent_pid, :serving_demand)

          receive do
            {:serving_receive, buffer} -> {[Nx.from_binary(buffer, :f32)], state}
            :halt -> {:halt, state}
          end
        end,
        fn state -> state end
      )

    try do
      Nx.Serving.run(serving, stream)
      |> Enum.each(fn output ->
        send(parent_pid, {:serving_output, output})
      end)
    catch
      # TODO: This can be removed along with the exit trap when
      # https://github.com/elixir-nx/bumblebee/pull/454 is released
      :exit, {{%ArgumentError{}, _}, {Nx.Serving, :streaming, []}} -> :ok
    end

    # Processing only finishes if the Stream received an explicit `:halt` from the filter.
    # Sending a message back so the filter knows it can EOS.
    send(parent_pid, :serving_finished)
    {:noreply, state}
  end
end
