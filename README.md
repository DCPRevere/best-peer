# best-pod

A pod is a self-contained peer: one binary on a machine with a GPU that puts that machine behind a
[Behavioral State Protocol](https://behavioralstate.io) door. An agent that knows only the door's manifest can read the
hardware state, deploy a model, follow its lifecycle on the event stream, run a prompt against the endpoint the event
named, run a job and undeploy. Nothing is written for the agent beyond the manifest. It is not a Kubernetes pod.

best-pod is the second, independent implementation of BEST, written in Rust from the spec text, so the spec is proven
portable and its gaps surface. It wraps an existing runtime (containers with the NVIDIA container toolkit running
Ollama or vLLM) and builds no engine of its own. Inference tokens stay on the runtime's OpenAI-compatible endpoint;
BEST carries the control path only.

The plan is the epic [behavioralstate/spec#27](https://github.com/behavioralstate/spec/issues/27) and its slices, on the
[Behavioralstate board](https://github.com/orgs/Amarcode-Studios/projects/4). The board is the plan: no plan file lives
here.

Done means `best-validate` green on the served manifest and an agent running the whole lifecycle from the manifest
alone, with the transcript kept in this repository.
