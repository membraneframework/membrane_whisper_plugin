import Config

has_gpu? = System.find_executable("nvidia-smi") != nil
platform_for_exla_cuda_client = if has_gpu?, do: :cuda, else: :host

config :exla, :clients,
  cuda: [
    platform: platform_for_exla_cuda_client,
    memory_fraction: 0.5
  ],
  default: [platform: :host]

config :nx, default_backend: {EXLA.Backend, client: :cuda}
