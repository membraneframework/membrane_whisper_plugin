import Config

apple_silicon? =
  :os.type() == {:unix, :darwin} and
    :erlang.system_info(:system_architecture)
    |> List.to_string()
    |> String.starts_with?("aarch64")

if apple_silicon? do
  config :nx, default_backend: {EMLX.Backend, device: :gpu}
else
  has_gpu? = System.find_executable("nvidia-smi") != nil
  platform_for_exla_cuda_client = if has_gpu?, do: :cuda, else: :host

  config :exla, :clients,
    cuda: [
      platform: platform_for_exla_cuda_client,
      memory_fraction: 0.5
    ],
    default: [platform: :host]

  config :nx, default_backend: {EXLA.Backend, client: :cuda}
end
