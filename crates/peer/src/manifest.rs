//! The discovery manifest, `GET /.well-known/best`: the one entry to everything this peer offers.
//!
//! It is built from the spec's manifest root (`version`, `services`, `capabilities`) and nothing else; every
//! capability is `planned` until the slice that serves it lands, so a validator skips it and a consumer knows not to
//! call it. The BEST version is the spec version this peer is written against.

use serde_json::{json, Value};

/// The spec version this peer implements.
pub const BEST_VERSION: &str = "0.9.17";

/// The service every capability of this peer belongs to. A peer resolves from the name its operator gives it, so the
/// standard agents service key is used until a name exists to reverse.
pub const SERVICE: &str = "io.best.agents";

/// The manifest for a peer whose consumer-facing base URL is `endpoint`.
pub fn manifest(endpoint: &str) -> Value {
    let cap = |name: &str, description: &str, page: &str| {
        json!({
            "name": name,
            "version": BEST_VERSION,
            "service": SERVICE,
            "description": description,
            "spec": format!("https://behavioralstate.io/specs/agents/{page}"),
            "schema": format!("https://behavioralstate.io/v1/schemas/agents/{page}.json"),
            "status": "planned"
        })
    };
    json!({
        "best": {
            "version": BEST_VERSION,
            "services": {
                SERVICE: {
                    "version": BEST_VERSION,
                    "description": "A peer: a GPU host that deploys models and runs jobs on request and tells their lifecycle as events.",
                    "spec": "https://behavioralstate.io/specs/agents",
                    "http": { "endpoint": endpoint }
                }
            },
            "capabilities": [
                cap("io.best.agents.commands", "Deploy and undeploy models, run and stop jobs, pull images.", "commands"),
                cap("io.best.agents.events", "The lifecycle of every model and job, as it happens.", "events"),
                cap("io.best.agents.queries", "The host's accelerators and what is deployed and running on them.", "queries"),
                cap("io.best.agents.workflows", "Recipes from an empty host to a model answering prompts.", "workflows")
            ]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_manifest_has_the_root_the_spec_requires_and_nothing_callable_yet() {
        let m = manifest("http://127.0.0.1:7777");
        let best = &m["best"];
        assert_eq!(best["version"], BEST_VERSION);
        assert_eq!(best["services"][SERVICE]["http"]["endpoint"], "http://127.0.0.1:7777");
        let caps = best["capabilities"].as_array().unwrap();
        assert_eq!(caps.len(), 4);
        for c in caps {
            assert_eq!(c["status"], "planned");
            assert_eq!(c["service"], SERVICE);
            assert!(c["spec"]
                .as_str()
                .unwrap()
                .starts_with("https://behavioralstate.io/specs/agents/"));
            assert!(c["schema"].as_str().unwrap().ends_with(".json"));
            assert!(c["description"].as_str().unwrap().len() <= 500);
        }
    }
}
