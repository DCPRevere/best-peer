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
alone, with the transcript kept in this repository: [acceptance/lifecycle-from-the-manifest.md](acceptance/lifecycle-from-the-manifest.md)
is that record, made with the published connector and a client that had nothing but the person's sentence.

## Layout

| Crate | What it owns |
|---|---|
| `crates/door` | The BEST surface, from the spec text: the manifest, the catalogues and schema documents, command ingest with refusals that say what to send and an idempotent `id`, the event log on disk with its paged history and its live stream, queries, workflows, and sign-in by the device flow with the guidance verbatim. Who approves is an `Approver` the peer chooses. |
| `crates/gpu` | The accelerator abstraction: a vendor-neutral snapshot of the host's devices (`Gpu`), each naming the compute APIs it offers (CUDA, ROCm, oneAPI, Metal, Vulkan, CPU) so a workload asks for an API and memory, never for a vendor. Ships the `Fake` tests run against. |
| `crates/runtime` | The container runtime abstraction (`Runtime`): pull with progress, start with devices handed over by id and the ports published, stop, observe, and what the engine does on its own told as events. Ships the `Fake`. |
| `crates/engine` | The inference engine abstraction (`Engine`): which engines serve a model (Ollama, vLLM), what to run for each, and how a served model is made ready, with progress. Ships the `Fake`. |
| `crates/peer` | The `best-peer` binary: the door over the three abstractions, with the peer's own commands, queries and events, its state folded from the events it told, and `best-peer approve`, the operator's side of sign-in. |
| `crates/host` | The host itself as a device: a `Gpu` adapter reporting the CPU and system memory, so a peer with no accelerator serves the whole lifecycle. |
| `crates/nvml` | The `Gpu` adapter for NVIDIA cards through NVML: memory, temperature, power, utilisation and the CUDA version, read live; the peer refuses to start without the driver unless told the host has none. |
| `crates/docker` | The `Runtime` adapter over the Docker Engine API: NVIDIA devices handed over through the container toolkit by their NVML UUID. |
| `crates/native` | The `Runtime` adapter for engines already running on the host outside any container (an installed Ollama): a deploy binds to the engine's own port; jobs need Docker. |
| `crates/engine-http` | The `Engine` adapter over the engines' own HTTP APIs. |

The three adapters are thin and proven on a host by hand, with the proof on the slice's issue; they are never
unit-tested and CI runs on the fakes. Accelerator adapters (NVML, ROCm) are separate crates too, and join the same way.

## Run

```bash
cargo run -p best-peer                        # on a host with the NVIDIA driver
BEST_PEER_GPU=none cargo run -p best-peer     # on any other machine: the CPU is the one device
curl http://127.0.0.1:7777/.well-known/best
npx -y @behavioralstate/best-validate http://127.0.0.1:7777
```

Or take a [release](https://github.com/behavioralstate/best-peer/releases): one binary per machine (Linux x86_64 on
glibc 2.35 or newer; Windows x86_64; macOS Apple silicon), with `SHA256SUMS` and a build provenance attestation
(`gh attestation verify <archive> --repo behavioralstate/best-peer`). `best-peer --version` says which it is.

`BEST_PEER_LISTEN` (default `127.0.0.1:7777`) is where it listens; `BEST_PEER_ENDPOINT` is the consumer-facing base URL
the manifest advertises, required when the peer listens on every interface. `BEST_PEER_DATA` (default `~/.best-peer`)
holds the event log, the credentials as hashes, and the admin token.

`BEST_PEER_GPU` (default `nvml`) is how the peer sees the hardware: through the NVIDIA driver's own library, every
card with its memory, temperature, power, utilisation and the CUDA version the driver offers, read live at every
query, and the host CPU with the system memory listed after the cards; a host without the driver is refused at
start, with the fix. `none` says this host has no accelerator, and the CPU is the one device.

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
`undeploy-model` and `stop-job` end them.

The peer's state is the fold of the events it told. After a restart it is rebuilt from the log and squared with what
the engine still runs; a workload that ended while the peer was down is told so then.

## The data path

BEST carries the control path: the commands, the facts they produce and the state those facts fold to. The
conversation with the model is not on it. `DeploymentReady` names the endpoint, an OpenAI-compatible base URL on the
door's host, and the model name to put in the request; from then on a client prompts the engine there directly,
with the engine's own API, and the door neither sees, proxies nor records a token. Two reasons. Streaming tokens
through a protocol made for facts would add a hop and a format to every answer for nothing: a token is not a fact
about the machine, and every client already speaks the API the engines expose. And the boundary keeps the door
honest about what it promises: its events say what the machine runs, never what the model answered.

Where the boundary lies: the door's credential ends at the door. The endpoint answers whoever reaches its port, with
whatever access the engine itself enforces (none, in Ollama as installed), on the interfaces the peer binds the
container's port to, or, in `native` mode, the ones the installed engine listens on. The door promises that the
endpoint serves the model from `DeploymentReady` until the deployment ends: an `undeploy-model` accepted (its
phase is `ending` in `get-workloads` from then, and `Undeployed` follows), or `DeploymentEnded` or `OutOfMemory`
telling what the engine did on its own, within 30 s of it where the engine told nothing itself (an installed engine
that stopped). What is said at the endpoint, how fast and to whom, is the engine's.

## Commands over time

Every command of this peer follows one pattern, and nothing else is needed to drive it:

1. `POST /commands` answers `201` with the correlation id, once the command passed its schema and the peer's
   current state (a deploy no device has the memory for is refused here, not failed later). From that moment the
   command ends in a completing event, through a restart if need be: a command accepted and not yet run when the
   peer stops is run when it starts again, through the same state check, or failed with a `CommandFailed` that
   says why; one whose workload had already started and did not survive the restart is failed with the fix.
2. Everything that happens is an event carrying that correlation id, in the order it happened. The command's
   schema document lists what it `produces` and, among those, what `completes` it: the events after which the
   command's outcome is known (`DeploymentReady` for a deploy, or `DeploymentFailed`, `DeploymentEnded` or
   `OutOfMemory` when it ended before it was ready; `JobStarted` or `JobFailed` for a job), so a consumer stops
   waiting for the command and a restart does not run it again; the others are
   progress on the way (`ImagePulling`, `WeightsLoading`, with `progress_pct` where the engine says).
   `CommandFailed` completes any command.
3. What the command made then has a life of its own, told under the same correlation id (`JobFinished` with the
   exit code, `OutOfMemory`, `DeploymentEnded`), so one filter follows it from the first event to the last.
4. A consumer that was not listening catches up with `GET /events?correlationId=...`, following `nextCursor` to
   the end, and follows from there with `GET /events/stream` and `Last-Event-ID`. There is no other channel for
   progress or outcome, and nothing to poll.

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
