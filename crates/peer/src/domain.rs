//! What this peer offers through its door, over the two abstractions: the hardware as a query, the image pull as a
//! command with its outcome as events. The model lifecycle (deploy, run, stop) joins here in its own slice.

use std::sync::Arc;

use best_peer_door::{BestError, Catalogue, CommandDef, EventDef, QueryDef};
use best_peer_gpu::Gpu;
use best_peer_runtime::{ImageRef, Runtime};
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub fn catalogue(gpu: Arc<dyn Gpu>, runtime: Arc<dyn Runtime>) -> Catalogue {
    Catalogue::new()
        .query(QueryDef {
            schema: "get-hardware-state".into(),
            version: "1.0".into(),
            description: "The accelerators on this host right now: each device's compute APIs with their versions, its memory total and free, temperature, power and utilisation, and the driver. Read it before deploying: a model needs an API the device offers and memory it has free.".into(),
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
        })
        .command(CommandDef {
            schema: "pull-image".into(),
            version: "1.0".into(),
            command_type: "PullImage".into(),
            description: "Makes a container image present on this peer, so a later deploy or job can start at once. The outcome is an ImagePulled or ImagePullFailed event with this command's correlation id.".into(),
            data_schema: json!({
                "type": "object",
                "required": ["image"],
                "properties": {
                    "image": { "type": "string", "minLength": 1, "description": "The image reference as the engine understands it, such as ollama/ollama:0.6" }
                },
                "additionalProperties": false
            }),
            produces: vec!["ImagePulled".into(), "ImagePullFailed".into()],
            impact: None,
            handler: Arc::new(move |cmd, emitter| {
                let runtime = runtime.clone();
                Box::pin(async move {
                    let image = ImageRef(cmd.data["image"].as_str().unwrap_or_default().to_string());
                    match runtime.pull(&image).await {
                        Ok(()) => {
                            emitter.emit("ImagePulled", json!({ "image": image.0 }));
                        }
                        Err(e) => {
                            emitter.emit("ImagePullFailed", json!({ "image": image.0, "reason": e.to_string() }));
                        }
                    }
                })
            }),
        })
        .event(EventDef {
            schema: "image-pulled".into(),
            version: "1.0".into(),
            event_type: "ImagePulled".into(),
            description: "The image is present on the peer.".into(),
            data_schema: json!({ "type": "object", "required": ["image"], "properties": { "image": { "type": "string" } } }),
        })
        .event(EventDef {
            schema: "image-pull-failed".into(),
            version: "1.0".into(),
            event_type: "ImagePullFailed".into(),
            description: "The image could not be made present; the reason says why.".into(),
            data_schema: json!({ "type": "object", "required": ["image", "reason"], "properties": { "image": { "type": "string" }, "reason": { "type": "string" } } }),
        })
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
