# best-peer

A peer is one binary on a machine with a GPU that puts that machine behind a
[Behavioral State Protocol](https://behavioralstate.io) door. An agent that knows only the door's manifest can read the
hardware state, deploy a model, follow its lifecycle on the event stream, run a prompt against the endpoint the event
named, run a job and undeploy. Nothing is written for the agent beyond the manifest. Peers find, name and vouch for
each other through BEST doors too, so the same word carries to the end game: a network of peers and no centre.

best-peer is an implementation of BEST written in Rust from the spec text alone, sharing no code with any other, so
the spec is proven portable and its gaps surface. It wraps an existing runtime (containers with the NVIDIA container toolkit running
Ollama or vLLM) and builds no engine of its own. Inference tokens stay on the runtime's OpenAI-compatible endpoint;
BEST carries the control path only.

The plan is the epic [behavioralstate/spec#27](https://github.com/behavioralstate/spec/issues/27) and its slices, on the
[Behavioralstate board](https://github.com/orgs/Amarcode-Studios/projects/4). The board is the plan: no plan file lives
here. How the work is made and shipped is [SDLC.md](SDLC.md).

Done means `best-validate` green on the served manifest and an agent running the whole lifecycle from the manifest
alone, with the transcript kept in this repository.

## Layout

| Crate | What it owns |
|---|---|
| `crates/gpu` | The accelerator abstraction: a vendor-neutral snapshot of the host's devices (`Gpu`), each naming the compute APIs it offers (CUDA, ROCm, oneAPI, Metal, Vulkan, CPU) so a workload asks for an API and memory, never for a vendor. Ships the `Fake` tests run against. |
| `crates/runtime` | The container runtime abstraction (`Runtime`): pull, start, stop, observe, with devices handed over by id and the lifecycle told as events. Ships the `Fake`. |
| `crates/peer` | The `best-peer` binary: the door (axum), built on the two abstractions; serves `/.well-known/best`. |

Adapters for real hardware and engines (NVML, ROCm, Docker through bollard) are separate crates, thin, and proven on a
host by hand; they are never unit-tested and CI never needs a GPU.

## Run

```bash
cargo run -p best-peer
curl http://127.0.0.1:7777/.well-known/best
npx -y @behavioralstate/best-validate http://127.0.0.1:7777
```

`BEST_PEER_LISTEN` (default `127.0.0.1:7777`) is where it listens; `BEST_PEER_ENDPOINT` is the consumer-facing base URL
the manifest advertises when it differs.
