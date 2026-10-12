# How best-peer is built and shipped

> This is the one place the lifecycle of this repository is written down. Open work of any kind (a slice, a bug, a
> decision, a spec gap) is an issue on the Behavioralstate board, never a file: the epic
> [behavioralstate/spec#27](https://github.com/behavioralstate/spec/issues/27) and its sub-issues are the plan.

## 1. Intake

- Work is a slice of the epic, or an issue filed next to it on the same board, with the problem, the outcome from the
  agent's side, numbered decisions, and a "Done when" list. A spec gap met while implementing is an entry on the
  spec follow-ups issue, never a silent patch and never a spec change up front.
- Ambiguity is resolved by reading the spec and the running code, never by guessing.

## 2. Design rules every change is held to

- BEST is the surface: a capability exists only when it has a schema, a catalogue entry and a description an agent
  can act on; a refusal is a 400 that says what to do; sign-in is the spec's. Nothing is invented before the
  implementation proves the spec lacks it.
- Facts are events; an event is versioned, never mutated; a redelivery is idempotent; nothing is clamped silently.
- Low-level dependencies sit behind abstractions this workspace owns (`Gpu`, `Runtime`, later `Transport`); tests use
  the fakes, never the dependency; an adapter is thin and is proven on a host by hand, with the proof on the slice's
  issue. CI never needs a GPU.
- The accelerator abstraction is vendor-neutral: a workload asks for a compute API and memory, never for a vendor.
- The inference data path stays on the runtime's OpenAI-compatible endpoint; BEST carries the control path only.
- Instructions and this file state the current rule, not its history; the history is in git.

## 3. Build

- Tests first where one applies, against the fakes: the lifecycle, the refusals, the events, the manifest.
- `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`; then the served
  manifest through `best-validate`, anonymous and then signed in: an agent registered through the device flow,
  admitted with the real `best-peer approve`, and the validator probing every active capability with that credential.
  CI runs the same, and a surface the spec refuses fails the build.

## 4. Ship

- **Every change is made on `main`.** No feature branches, no pull requests: one commit per piece of work, the subject
  saying what changed and why, ending with the assistant's attribution line.
- **Before the push, the review gate**: `/best-review`, the owner-run review of everything `main` holds that origin
  does not. Every finding is fixed on `main` with a test or set aside with its reason in the report; nothing is dropped
  silently.
- **The push is the release.** There is no deployment yet: a peer is built from `main` and run on a host by hand.

## 5. What we do not do

- No release versions until a peer is installed somewhere: the sha is the version.
- No force-pushes, no push with an unfixed review finding or a red check.
- No plan file, no `TODO`, no dated narrative: open work is an issue.
