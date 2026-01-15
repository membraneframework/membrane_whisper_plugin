defmodule Membrane.Whisper do
  @moduledoc """
    TODO
  """

  use Membrane.Filter
  # import Mockery.Macro

  require Membrane.Logger

  # alias __MODULE__.Native
  alias Membrane.Whisper.ServingServer
  alias Membrane.RawAudio

  @supported_sample_format [:f32le]
  @supported_channels [1]

  def_output_pad :output,
    accepted_format:
      %RawAudio{sample_format: format, channels: channels}
      when format in @supported_sample_format and channels in @supported_channels

  def_input_pad :input,
    accepted_format:
      %RawAudio{sample_format: format, channels: channels}
      when format in @supported_sample_format and channels in @supported_channels

  def_options input_stream_format: [
                spec: RawAudio.t() | nil,
                default: nil,
                description: """
                Stream format for the input pad. If set to nil (default value),
                stream format is assumed to be received through the pad. If explicitly set to some
                stream format, it cannot be changed by stream format received through the pad.
                """
              ],
              output_stream_format: [
                spec: RawAudio.t(),
                description: """
                Audio stream format for output pad
                """
              ],
              serving: [
                spec: Nx.Serving.t(),
                description: """
                  Bumblebee.Audio.speech_to_text_whisper
                """
              ]

  defmodule ServingServer do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(_opts) do
      serving = Agent.get(Whisper.Serving, & &1)

      {:ok,
       %{
         serving: serving,
         parent_filter_pid: nil
       }}
    end

    @impl true
    def handle_call({:start_serving_stream_mf}, {parent_filter_pid, _alias}, state) do

      stream =
        Stream.repeatedly(fn ->
          send(parent_filter_pid, {:serving_stream_pid, self()})

          receive do
            {:handle_buffer, buffer} -> Nx.from_binary(buffer, :f32)
          end
        end)

      GenServer.cast(self(), {:start_serving_stream, parent_filter_pid, stream})
      {:reply, :ok, state}
    end

    @impl true
    def handle_cast({:start_serving_stream, pf_pid, stream}, state) do
      IO.puts("starting stream to serving")

      Nx.Serving.run(
        state.serving,
        stream
      )
      |> Enum.each(fn text ->
        send(pf_pid, {:receive_whisper_text, text})
      end)

      {:noreply, state}
    end
  end

  @impl true
  # @spec handle_buffer(any(), any(), any(), %{
  #         :serving_status =>
  #           :waiting | {:ready, pid()},
  #         :any => list(binary()),
  #         optional(any()) => any()
  #       }) ::
  #         {[],
  #          %{
  #            optional(:serving_status) =>
  #              :waiting | {:ready, atom() | pid() | port() | reference() | {any(), any()}},
  #            optional(any()) => any()
  #          }}
  def handle_buffer(_pad, buffer, _ctx, state) do
    # IO.inspect(byte_size(buffer.payload))
    new_state =
      case state.serving_status do
        {:ready, pid} ->
          send(pid, {:handle_buffer, buffer.payload})
          %{state | serving_status: :busy}

        :busy ->
          %{state | acc: state.acc ++ [buffer.payload]}
      end

    {[], new_state}
  end

  @impl true
  def handle_init(ctx, %__MODULE__{serving: serving} = options) do
    case options.input_stream_format do
      %RawAudio{} -> :ok
      nil -> :ok
      _other -> raise ":input_stream_format must be nil or %RawAudio{}"
    end

    case options.output_stream_format do
      %RawAudio{} -> :ok
      _other -> raise ":output_stream_format must be %RawAudio{}"
    end

    {:ok, server} =
      Membrane.UtilitySupervisor.start_link_child(ctx.utility_supervisor, ServingServer, [serving: serving])

    :ok = GenServer.call(server, {:start_serving_stream_mf})

    state =
      options
      |> Map.from_struct()
      |> Map.merge(%{
        serving_status: :busy,
        acc: [],
        finished: false
      })

    {[], state}
  end

  @impl true
  def handle_stream_format(:input, %RawAudio{} = stream_format, _ctx, state) do
    state = %{state | input_stream_format: stream_format}
    {[stream_format: {:output, state.output_stream_format}], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    {[stream_format: {:output, state.output_stream_format}], state}
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {[], %{state | finished: true}}
  end

  @impl true
  def handle_info({:receive_whisper_text, text}, _ctx, state) do
    IO.inspect(text)
    {[], state}
  end

  @impl true
  def handle_info({:serving_stream_pid, pid}, _ctx, state) do
    case {state.acc, state.finished} do
      {[], false} ->
        {[], %{state | serving_status: {:ready, pid}}}

      {[], true} ->
        # pipeline sent end_of_stream AND GenServer is :ready, e.g. not processing anything -> we are free to send EOS
        {[
          # end_of_stream: :output
          ], state}

      {[buffer | rest], _finished} ->
        send(pid, {:handle_buffer, buffer})
        {[], %{state | acc: rest}}
    end
  end
end
