//! What this peer offers through its door: the hardware and the workloads as queries, the model lifecycle as
//! commands, each outcome as events. The schemas and descriptions here are everything an agent gets; the behaviour
//! is in [`crate::lifecycle`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use best_peer_door::{BestError, Catalogue, CommandDef, EventDef, QueryDef};
use best_peer_gpu::Gpu;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::lifecycle::Peer;

pub fn catalogue(peer: Arc<Peer>) -> Catalogue {
    let mut c = Catalogue::new()
        .query(hardware_query(peer.gpu.clone()))
        .query(workloads_query(peer.clone()))
        .command(pull_image(peer.clone()))
        .command(deploy_model(peer.clone()))
        .command(undeploy_model(peer.clone()))
        .command(run_job(peer.clone()))
        .command(stop_job(peer));
    for e in event_defs() {
        c = c.event(e);
    }
    c
}

/// What completes each command: the events after which its outcome is known, so a consumer stops waiting for the
/// command and a restart does not run it again. What the command made then has a life of its own, told under the
/// same correlation id (a deployment's end, a job's end). `CommandFailed` completes any command.
pub fn completions() -> HashMap<String, HashSet<String>> {
    let set = |types: &[&str]| types.iter().map(|t| t.to_string()).collect::<HashSet<_>>();
    HashMap::from([
        ("PullImage".to_string(), set(&["ImagePulled", "ImagePullFailed"])),
        (
            "DeployModel".to_string(),
            set(&["DeploymentReady", "DeploymentFailed", "DeploymentEnded", "OutOfMemory"]),
        ),
        ("UndeployModel".to_string(), set(&["Undeployed"])),
        ("RunJob".to_string(), set(&["JobStarted", "JobFailed"])),
        ("StopJob".to_string(), set(&["JobStopped"])),
    ])
}

fn completes(command_type: &str) -> Vec<String> {
    let mut v: Vec<String> = completions()
        .remove(command_type)
        .unwrap_or_default()
        .into_iter()
        .collect();
    v.sort();
    v
}

/// Event type to absolute dataschema URI, for the peer's own emitters: the same map the door builds.
pub fn event_dataschemas(endpoint: &str) -> HashMap<String, String> {
    let mut c = Catalogue::new();
    for e in event_defs() {
        c = c.event(e);
    }
    c.event_dataschemas(endpoint)
}

fn hardware_query(gpu: Arc<dyn Gpu>) -> QueryDef {
    QueryDef {
        schema: "get-hardware-state".into(),
        version: "1.0".into(),
        description: "The accelerators on this host right now: each device's compute APIs with their versions, its memory total and free, temperature, power and utilisation, and the driver. Read it before deploying: a model needs an API the device offers and memory it has free; get-workloads says what already holds memory.".into(),
        parameters: None,
        response: json!({
            "type": "object",
            "required": ["devices", "observed_at"],
            "properties": {
                "driver": { "type": ["string", "null"], "description": "The accelerator driver's version, when there is one" },
                "observed_at": { "type": "string", "format": "date-time" },
                "devices": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["id", "vendor", "name", "memory", "compute"],
                        "properties": {
                            "id": { "type": "string", "description": "Stable device id, the value a command names a device by" },
                            "vendor": { "description": "Who made it; informative, never what a workload asks for" },
                            "name": { "type": "string" },
                            "memory": { "type": "object", "properties": { "total_bytes": { "type": "integer" }, "free_bytes": { "type": "integer" } } },
                            "compute": { "type": "array", "description": "The compute APIs the device offers, with versions: what a workload asks for", "items": { "type": "object", "properties": { "api": { "type": "string" }, "version": { "type": "string" } } } },
                            "temperature_c": { "type": ["number", "null"] },
                            "power_w": { "type": ["number", "null"] },
                            "utilisation_pct": { "type": ["integer", "null"] }
                        }
                    }
                }
            }
        }),
        handler: Arc::new(move |_params| {
            let gpu = gpu.clone();
            Box::pin(async move {
                // A real adapter talks to a driver synchronously; that never blocks the door's workers.
                tokio::task::spawn_blocking(move || hardware_state(gpu.as_ref()))
                    .await
                    .map_err(|e| BestError::internal(format!("the hardware query did not complete: {e}")))?
            })
        }),
    }
}

fn workloads_query(peer: Arc<Peer>) -> QueryDef {
    QueryDef {
        schema: "get-workloads".into(),
        version: "1.0".into(),
        description: "What this peer runs now: each deployment with its model, engine, device, the memory promised to it, its inference endpoint once ready and its phase (starting, ready, ending); each job with its image, device and phase (running, stopping). The ids here are what undeploy-model and stop-job name.".into(),
        parameters: None,
        response: json!({
            "type": "object",
            "required": ["deployments", "jobs"],
            "properties": {
                "deployments": { "type": "array", "items": { "type": "object", "required": ["deployment", "model", "engine", "device", "memory_bytes", "phase"], "properties": {
                    "deployment": { "type": "string" }, "model": { "type": "string" }, "engine": { "type": "string", "enum": ["ollama", "vllm"] },
                    "device": { "type": "string" }, "memory_bytes": { "type": "integer" },
                    "endpoint": { "type": ["string", "null"], "description": "The OpenAI-compatible base URL, once the deployment is ready" },
                    "phase": { "type": "string", "enum": ["starting", "ready", "ending"] }
                } } },
                "jobs": { "type": "array", "items": { "type": "object", "required": ["job", "image", "phase"], "properties": {
                    "job": { "type": "string" }, "image": { "type": "string" }, "device": { "type": ["string", "null"] },
                    "memory_bytes": { "type": "integer" }, "phase": { "type": "string", "enum": ["running", "stopping"] }
                } } }
            }
        }),
        handler: Arc::new(move |_params| {
            let peer = peer.clone();
            Box::pin(async move { Ok(peer.workloads()) })
        }),
    }
}

fn pull_image(peer: Arc<Peer>) -> CommandDef {
    CommandDef {
        schema: "pull-image".into(),
        version: "1.0".into(),
        command_type: "PullImage".into(),
        description: "Makes a container image present on this peer, so a later deploy or job can start at once. The outcome is an ImagePulled or ImagePullFailed event with this command's correlation id.".into(),
        data_schema: json!({
            "type": "object",
            "required": ["image"],
            "properties": {
                "image": { "type": "string", "minLength": 1, "description": "The image reference as the container engine understands it, name and tag" }
            },
            "additionalProperties": false
        }),
        produces: vec!["ImagePulling".into(), "ImagePulled".into(), "ImagePullFailed".into()],
        completes: completes("PullImage"),
        impact: None,
        admit: None,
        handler: Arc::new(move |cmd, emitter| {
            let peer = peer.clone();
            Box::pin(async move { peer.pull_image(cmd, emitter).await })
        }),
    }
}

fn deploy_model(peer: Arc<Peer>) -> CommandDef {
    let admit = peer.clone();
    CommandDef {
        schema: "deploy-model".into(),
        version: "1.0".into(),
        command_type: "DeployModel".into(),
        description: "Serves a model on this peer with the engine named, on a device that offers an API the engine runs on and has the memory asked for free. Refused at once, with the reason and what to do, when no device fits. Otherwise the facts follow on the stream with this command's correlation id: ImagePulling and ImagePulled, DeploymentStarted, WeightsLoading, then DeploymentReady with the OpenAI-compatible endpoint to send prompts to, or DeploymentFailed, which complete it. Later, OutOfMemory or DeploymentEnded if the engine ends it or stops on its own, under the same correlation id.".into(),
        data_schema: json!({
            "type": "object",
            "required": ["model", "engine", "memory_bytes"],
            "properties": {
                "model": { "type": "string", "minLength": 1, "description": "The model as the engine names it: the name in the engine's own library or registry, tag included where the engine uses one" },
                "engine": { "type": "string", "enum": ["ollama", "vllm"], "description": "ollama runs on cuda, rocm or cpu; vllm on cuda" },
                "memory_bytes": { "type": "integer", "minimum": 1, "description": "The device memory the model needs; the deploy is placed where that much is free after what is already promised" },
                "device": { "type": "string", "description": "A device id from get-hardware-state, to pin the placement; otherwise the fitting device with the most room" }
            },
            "additionalProperties": false
        }),
        produces: vec![
            "ImagePulling".into(),
            "ImagePulled".into(),
            "DeploymentStarted".into(),
            "WeightsLoading".into(),
            "DeploymentReady".into(),
            "DeploymentFailed".into(),
            "OutOfMemory".into(),
            "DeploymentEnded".into(),
        ],
        completes: completes("DeployModel"),
        impact: None,
        admit: Some(Arc::new(move |cmd, key| {
            let peer = admit.clone();
            Box::pin(async move { peer.admit_deploy(cmd, key).await })
        })),
        handler: Arc::new(move |cmd, emitter| {
            let peer = peer.clone();
            Box::pin(async move { peer.deploy(cmd, emitter).await })
        }),
    }
}

fn undeploy_model(peer: Arc<Peer>) -> CommandDef {
    let admit = peer.clone();
    CommandDef {
        schema: "undeploy-model".into(),
        version: "1.0".into(),
        command_type: "UndeployModel".into(),
        description: "Stops a deployment and frees its device memory. Refused when no such deployment is live. The outcome is an Undeployed event with this command's correlation id.".into(),
        data_schema: json!({
            "type": "object",
            "required": ["deployment"],
            "properties": {
                "deployment": { "type": "string", "minLength": 1, "description": "The deployment id from DeploymentStarted or get-workloads" }
            },
            "additionalProperties": false
        }),
        produces: vec!["Undeployed".into(), "CommandFailed".into()],
        completes: completes("UndeployModel"),
        impact: None,
        admit: Some(Arc::new(move |cmd, _key| {
            let peer = admit.clone();
            Box::pin(async move { peer.admit_undeploy(cmd) })
        })),
        handler: Arc::new(move |cmd, emitter| {
            let peer = peer.clone();
            Box::pin(async move { peer.undeploy(cmd, emitter).await })
        }),
    }
}

fn run_job(peer: Arc<Peer>) -> CommandDef {
    let admit = peer.clone();
    CommandDef {
        schema: "run-job".into(),
        version: "1.0".into(),
        command_type: "RunJob".into(),
        description: "Runs an image once, with the arguments given, and tells its end. With memory_bytes the job is placed on a device with that much room and handed that device; without, it runs with no device. The facts follow with this command's correlation id: ImagePulling and ImagePulled, then JobStarted or JobFailed, which complete the command; the job's own end follows as JobFinished with the exit code, or JobFailed.".into(),
        data_schema: json!({
            "type": "object",
            "required": ["image"],
            "properties": {
                "image": { "type": "string", "minLength": 1, "description": "The image to run, as the container engine understands it" },
                "args": { "type": "array", "items": { "type": "string" }, "description": "The command line passed to the image's entrypoint" },
                "memory_bytes": { "type": "integer", "minimum": 1, "description": "Device memory the job needs; omit to run it with no device" },
                "device": { "type": "string", "description": "A device id from get-hardware-state, to pin the placement; refused without memory_bytes" }
            },
            "additionalProperties": false
        }),
        produces: vec![
            "ImagePulling".into(),
            "ImagePulled".into(),
            "JobStarted".into(),
            "JobFinished".into(),
            "JobFailed".into(),
        ],
        completes: completes("RunJob"),
        impact: None,
        admit: Some(Arc::new(move |cmd, key| {
            let peer = admit.clone();
            Box::pin(async move { peer.admit_run_job(cmd, key).await })
        })),
        handler: Arc::new(move |cmd, emitter| {
            let peer = peer.clone();
            Box::pin(async move { peer.run_job(cmd, emitter).await })
        }),
    }
}

fn stop_job(peer: Arc<Peer>) -> CommandDef {
    let admit = peer.clone();
    CommandDef {
        schema: "stop-job".into(),
        version: "1.0".into(),
        command_type: "StopJob".into(),
        description: "Stops a running job. Refused when no such job is running. The outcome is a JobStopped event with this command's correlation id.".into(),
        data_schema: json!({
            "type": "object",
            "required": ["job"],
            "properties": {
                "job": { "type": "string", "minLength": 1, "description": "The job id from JobStarted or get-workloads" }
            },
            "additionalProperties": false
        }),
        produces: vec!["JobStopped".into(), "CommandFailed".into()],
        completes: completes("StopJob"),
        impact: None,
        admit: Some(Arc::new(move |cmd, _key| {
            let peer = admit.clone();
            Box::pin(async move { peer.admit_stop_job(cmd) })
        })),
        handler: Arc::new(move |cmd, emitter| {
            let peer = peer.clone();
            Box::pin(async move { peer.stop_job(cmd, emitter).await })
        }),
    }
}

fn event(schema: &str, event_type: &str, description: &str, required: &[&str], properties: Value) -> EventDef {
    EventDef {
        schema: schema.into(),
        version: "1.0".into(),
        event_type: event_type.into(),
        description: description.into(),
        data_schema: json!({ "type": "object", "required": required, "properties": properties }),
    }
}

/// Every event this peer tells, with its schema. The stream is the only channel for progress and outcomes.
pub fn event_defs() -> Vec<EventDef> {
    let s = |d: &str| json!({ "type": "string", "description": d });
    let n = |d: &str| json!({ "type": "integer", "description": d });
    vec![
        event(
            "image-pulling",
            "ImagePulling",
            "The image is being pulled; progress in percent where the engine says.",
            &["image"],
            json!({ "image": s("The image being pulled"), "progress_pct": { "type": ["integer", "null"] } }),
        ),
        event(
            "image-pulled",
            "ImagePulled",
            "The image is present on the peer.",
            &["image"],
            json!({ "image": s("The image now present") }),
        ),
        event(
            "image-pull-failed",
            "ImagePullFailed",
            "The image could not be made present; the reason says why.",
            &["image", "reason"],
            json!({ "image": s("The image asked for"), "reason": s("What the engine said") }),
        ),
        event(
            "deployment-started",
            "DeploymentStarted",
            "The engine's container runs on the device named; the model is not yet served.",
            &["deployment", "model", "engine", "device", "memory_bytes", "container"],
            json!({ "deployment": s("The id later commands name"), "model": s("The model"), "engine": s("ollama or vllm"), "device": s("The device it was placed on"), "memory_bytes": n("The memory promised to it"), "container": s("The engine's container id"), "port": n("The host port the engine is reached at") }),
        ),
        event(
            "weights-loading",
            "WeightsLoading",
            "The engine is making the model present; progress in percent where it says.",
            &["deployment", "model"],
            json!({ "deployment": s("The deployment"), "model": s("The model"), "progress_pct": { "type": ["integer", "null"] } }),
        ),
        event(
            "deployment-ready",
            "DeploymentReady",
            "The model answers at the endpoint: an OpenAI-compatible base URL to send prompts to, outside this door, with the engine's own API and access. The model name goes in the request.",
            &["deployment", "model", "engine", "endpoint"],
            json!({ "deployment": s("The deployment"), "model": s("The model name to put in the request"), "engine": s("ollama or vllm"), "endpoint": s("The OpenAI-compatible base URL, such as http://host:41434/v1; this door's credential is not presented there") }),
        ),
        event(
            "deployment-failed",
            "DeploymentFailed",
            "The deployment did not reach ready, or failed after; its container is stopped. The reason says what happened and the fix what to do.",
            &["deployment", "model", "reason", "fix"],
            json!({ "deployment": s("The deployment"), "model": s("The model"), "reason": s("What happened"), "fix": s("What to do") }),
        ),
        event(
            "deployment-ended",
            "DeploymentEnded",
            "The engine ended the deployment on its own: its container exited.",
            &["deployment", "model", "reason"],
            json!({ "deployment": s("The deployment"), "model": s("The model"), "exit_code": { "type": ["integer", "null"] }, "reason": s("What happened") }),
        ),
        event(
            "undeployed",
            "Undeployed",
            "The deployment is stopped and its memory free.",
            &["deployment"],
            json!({ "deployment": s("The deployment"), "model": s("The model it served") }),
        ),
        event(
            "out-of-memory",
            "OutOfMemory",
            "The device ran out of memory under the deployment; the engine ended it.",
            &["deployment", "model", "device", "memory_bytes", "fix"],
            json!({ "deployment": s("The deployment"), "model": s("The model"), "device": s("The device"), "memory_bytes": n("What was promised to it"), "fix": s("What to do") }),
        ),
        event(
            "job-started",
            "JobStarted",
            "The job's container runs.",
            &["job", "image", "container"],
            json!({ "job": s("The id stop-job names"), "image": s("The image"), "device": { "type": ["string", "null"] }, "memory_bytes": n("The memory promised to it"), "container": s("The engine's container id") }),
        ),
        event(
            "job-finished",
            "JobFinished",
            "The job's container exited; the exit code is the engine's.",
            &["job", "exit_code"],
            json!({ "job": s("The job"), "exit_code": n("The container's exit code") }),
        ),
        event(
            "job-failed",
            "JobFailed",
            "The job did not start, or the engine ended it; the reason says what happened and the fix what to do.",
            &["job", "reason", "fix"],
            json!({ "job": s("The job"), "image": s("The image"), "reason": s("What happened"), "fix": s("What to do") }),
        ),
        event(
            "job-stopped",
            "JobStopped",
            "The job was stopped on request.",
            &["job"],
            json!({ "job": s("The job") }),
        ),
        event(
            "command-failed",
            "CommandFailed",
            "A command that was accepted could not be carried out; the reason says why. The state is as it was.",
            &["type", "reason"],
            json!({ "type": s("The command's type"), "reason": s("What happened") }),
        ),
    ]
}

fn hardware_state(gpu: &dyn Gpu) -> Result<Value, BestError> {
    let host = gpu
        .snapshot()
        .map_err(|e| BestError::internal(format!("the accelerator library failed: {e}")))?;
    let observed = OffsetDateTime::from(host.observed_at)
        .format(&Rfc3339)
        .unwrap_or_default();
    let devices: Vec<Value> = host
        .devices
        .iter()
        .map(|d| {
            json!({
                "id": d.id.0,
                "vendor": d.vendor,
                "name": d.name,
                "memory": { "total_bytes": d.memory.total_bytes, "free_bytes": d.memory.free_bytes },
                "compute": d.compute,
                "temperature_c": d.temperature_c,
                "power_w": d.power_w,
                "utilisation_pct": d.utilisation_pct
            })
        })
        .collect();
    Ok(json!({ "driver": host.driver, "observed_at": observed, "devices": devices }))
}
