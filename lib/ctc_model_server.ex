defmodule Membrane.Whisper.CtcModelServer do
  @moduledoc false

  # This GenServer owns the Wav2Vec2 CTC model and computes frame log-probabilities
  # of audio slices on behalf of `Membrane.Whisper.KeywordSpotterFilter`.

  # Requests are asynchronous (casts), so the filter is never blocked on model
  # inference; each response is sent back to the filter as a
  # `{:ctc_log_probs, meta, lp_rows}` message, with `meta` passed through verbatim.

  use GenServer

  @spec start_link(%{
          model_info: Bumblebee.model_info(),
          featurizer: Bumblebee.Featurizer.t(),
          parent_pid: pid()
        }) :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @spec request_log_probs(pid(), term(), binary()) :: :ok
  def request_log_probs(server, meta, samples_binary) do
    GenServer.cast(server, {:log_probs, meta, samples_binary})
  end

  @impl true
  def init(%{model_info: model_info, featurizer: featurizer, parent_pid: parent_pid}) do
    {_init_fn, predict_fn} = Axon.build(model_info.model)

    state = %{
      predict_fn: predict_fn,
      params: model_info.params,
      featurizer: featurizer,
      parent_pid: parent_pid
    }

    {:ok, state}
  end

  @impl true
  def handle_cast({:log_probs, meta, samples_binary}, state) do
    audio = Nx.from_binary(samples_binary, :f32)
    inputs = Bumblebee.apply_featurizer(state.featurizer, audio)
    %{logits: logits} = state.predict_fn.(state.params, inputs)

    lp_rows =
      logits[0]
      |> log_softmax()
      |> Nx.to_list()

    send(state.parent_pid, {:ctc_log_probs, meta, lp_rows})
    {:noreply, state}
  end

  defp log_softmax(logits) do
    shifted = Nx.subtract(logits, Nx.reduce_max(logits, axes: [-1], keep_axes: true))
    lse = shifted |> Nx.exp() |> Nx.sum(axes: [-1], keep_axes: true) |> Nx.log()
    Nx.subtract(shifted, lse)
  end
end
