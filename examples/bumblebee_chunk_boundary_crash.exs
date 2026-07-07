# EMLX (MLX) on Apple Silicon, EXLA everywhere else
apple_silicon? =
  :os.type() == {:unix, :darwin} and
    :erlang.system_info(:system_architecture) |> List.to_string() |> String.starts_with?("aarch64")

{nx_backend_dep, nx_backend, defn_options} =
  if apple_silicon?,
    do: {{:emlx, "~> 0.4.0"}, {EMLX.Backend, device: :gpu}, []},
    else: {{:exla, "~> 0.12"}, EXLA.Backend, [compiler: EXLA]}

Mix.install(
  [
    {:bumblebee, "~> 0.7"},
    nx_backend_dep
  ],
  config: [nx: [default_backend: nx_backend]]
)

hf_repo = "openai/whisper-tiny"

{:ok, whisper} = Bumblebee.load_model({:hf, hf_repo})
{:ok, featurizer} = Bumblebee.load_featurizer({:hf, hf_repo})
{:ok, tokenizer} = Bumblebee.load_tokenizer({:hf, hf_repo})
{:ok, generation_config} = Bumblebee.load_generation_config({:hf, hf_repo})

serving =
  Bumblebee.Audio.speech_to_text_whisper(
    whisper,
    featurizer,
    tokenizer,
    generation_config,
    stream: false,
    chunk_num_seconds: 10,
    context_num_seconds: 0,
    defn_options: defn_options
  )

sample_rate = 16_000
chunk_num_seconds = 20
audio = Nx.broadcast(0.0, {chunk_num_seconds * sample_rate})

serving
|> Nx.Serving.run(audio)
|> Enum.to_list()
