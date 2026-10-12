# best-peer

A peer is one binary on a machine with a GPU that puts that machine behind a
[Behavioral State Protocol](https://behavioralstate.io) door. An agent that knows only the door's manifest can read the
hardware state, deploy a model, follow its lifecycle on the event stream, run a prompt against the endpoint the event
named, run a job and undeploy. Nothing is written for the agent beyond the manifest. Peers find, name and vouch for
each other through BEST doors too, so the same word carries to the end game: a network of peers and no centre.

best-peer is an implementation of BEST written in Rust from the spec text alone, sharing no code with any other, so
the spec is proven portable and its gaps surface. It wraps an existing runtime (containers with the NVIDIA container
toolkit running Ollama or vLLM) and builds no engine of its own. Inference tokens stay on the runtime's
OpenAI-compatible endpoint; BEST carries the control path only.

The plan is the epic [behavioralstate/spec#27](https://github.com/behavioralstate/spec/issues/27) and its slices, on the
[Behavioralstate board](https://github.com/orgs/Amarcode-Studios/projects/4). The board is the plan: no plan file lives
here. How the work is made and shipped is [SDLC.md](SDLC.md).

Done means `best-validate` green on the served manifest and an agent running the whole lifecycle from the manifest
alone, with the transcript kept in this repository.

## Layout

| Crate | What it owns |
|---|---|
| `crates/door` | The BEST surface, from the spec text: the manifest, the catalogues and schema documents, command ingest with refusals that say what to send and an idempotent `id`, the event log on disk with its paged history and its live stream, queries, workflows, and sign-in by the device flow with the guidance verbatim. Who approves is an `Approver` the peer chooses. |
| `crates/gpu` | The accelerator abstraction: a vendor-neutral snapshot of the host's devices (`Gpu`), each naming the compute APIs it offers (CUDA, ROCm, oneAPI, Metal, Vulkan, CPU) so a workload asks for an API and memory, never for a vendor. Ships the `Fake` tests run against. |
| `crates/runtime` | The container runtime abstraction (`Runtime`): pull with progress, start with devices handed over by id and the ports published, stop, observe, and what the engine does on its own told as events. Ships the `Fake`. |
| `crates/engine` | The inference engine abstraction (`Engine`): which engines serve a model (Ollama, vLLM), what to run for each, and how a served model is made ready, with progress. Ships the `Fake`. |
| `crates/peer` | The `best-peer` binary: the door over the three abstractions, with the peer's own commands, queries and events, its state folded from the events it told, and `best-peer approve`, the operator's side of sign-in. |
| `crates/host` | The host itself as a device: a `Gpu` adapter reporting the CPU and system memory, so a peer with no accelerator serves the whole lifecycle. |
| `crates/docker` | The `Runtime` adapter over the Docker Engine API: NVIDIA devices handed over through the container toolkit by their NVML UUID. |
| `crates/native` | The `Runtime` adapter for engines already running on the host outside any container (an installed Ollama): a deploy binds to the engine's own port; jobs need Docker. |
| `crates/engine-http` | The `Engine` adapter over the engines' own HTTP APIs. |

The three adapters are thin and proven on a host by hand, with the proof on the slice's issue; they are never
unit-tested and CI runs on the fakes. Accelerator adapters (NVML, ROCm) are separate crates too, and join the same way.

## Run

```bash
cargo run -p best-peer
curl http://127.0.0.1:7777/.well-known/best
npx -y @behavioralstate/best-validate http://127.0.0.1:7777
```

`BEST_PEER_LISTEN` (default `127.0.0.1:7777`) is where it listens; `BEST_PEER_ENDPOINT` is the consumer-facing base URL
the manifest advertises, required when the peer listens on every interface. `BEST_PEER_DATA` (default `~/.best-peer`)
holds the event log, the credentials as hashes, and the admin token.

The container engine is Docker on the same host, reached through its socket or named pipe; the peer refuses to start
without it unless `BEST_PEER_RUNTIME` says otherwise: `native` deploys to an engine already running on the host
outside any container (an installed Ollama; no jobs), `fake` serves the whole surface against the in-memory fakes
(what CI runs).
`BEST_PEER_OLLAMA_IMAGE` and `BEST_PEER_VLLM_IMAGE` pin or mirror the engines' images; `BEST_PEER_READY_SECONDS`
(default 900) is how long an engine may take to answer after its container started, `BEST_PEER_LOAD_SECONDS`
(default 3600) how long a model may take to load. `BEST_PEER_ENGINE_HOST` (default `127.0.0.1`) is where the peer
itself reaches the engines' published ports; the endpoint a `DeploymentReady` event names uses the door's host, and
the ports are bound to every interface only when the door listens on one that is not loopback.

## Deploy

Everything an agent needs is in the catalogues. `get-hardware-state` says what devices are here and what each offers;
`get-workloads` says what already holds memory on them. `deploy-model` names a model, an engine (`ollama` runs on
CUDA, ROCm or CPU; `vllm` on CUDA) and the device memory it needs; the peer places it on a device offering an API the
engine runs on with that much room after what is already promised, or refuses at once with the reason and what to do.
The facts follow on the event stream under the command's correlation id, in order: `ImagePulling`, `ImagePulled`,
`DeploymentStarted`, `WeightsLoading`, then `DeploymentReady` with the OpenAI-compatible endpoint prompts go to, or
`DeploymentFailed`. What the engine does on its own later (`OutOfMemory`, `DeploymentEnded`) is told under the same
correlation id. `run-job` runs an image once and tells `JobStarted` and `JobFinished` with the exit code;
`undeploy-model` and `stop-job` end them. Prompts never pass through the door: BEST carries the control path only.

The peer's state is the fold of the events it told. After a restart it is rebuilt from the log and squared with what
the engine still runs; a workload that ended while the peer was down is told so then.

## Sign in

An agent signs a person in the way every BEST service is signed in to: it reads the manifest, posts to
`deviceAuthorizationUrl`, gives the person the link, and polls `tokenUrl`. The published connector does this by itself.

Who may use a peer is decided by whoever controls the machine it runs on, as with SSH keys: no accounts, no passwords,
no identity provider. The page the person opens shows the code and says what to do; on the peer's machine:

```bash
best-peer approve ABCD-EFGH
```

That presents the machine's admin token, which only its owner can read, and the agent's next poll gets its credential:
a key shown once, kept hashed, bound to the connection name the person gave. An operator who wants an identity
provider instead plugs one in behind the `Approver` trait; the door never knows which stands behind it.
