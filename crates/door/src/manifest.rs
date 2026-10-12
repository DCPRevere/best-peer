//! `GET /.well-known/best`: the one entry to everything the door offers, built from what the door actually has. A
//! capability is `active` only when its routes answer; the sign-in guidance's manifest text rides in the
//! authentication block, verbatim.

use serde_json::{json, Value};

use crate::guidance;
use crate::{Door, BEST_VERSION};

pub fn manifest(door: &Door) -> Value {
    let cap = |name: &str, description: &str, page: &str, status: &str| {
        json!({
            "name": name,
            "version": BEST_VERSION,
            "service": door.service,
            "description": description,
            "spec": format!("https://behavioralstate.io/specs/agents/{page}"),
            "schema": format!("https://behavioralstate.io/v1/schemas/agents/{page}.json"),
            "status": status
        })
    };
    let active = |has: bool| if has { "active" } else { "planned" };
    let mut events = cap(
        "io.best.agents.events",
        "What the peer did, as it happened: every command's outcome, correlated to it.",
        "events",
        "active",
    );
    events["endpoints"] = json!([
        { "method": "GET", "path": "/events", "description": "What has already happened, filterable, paged" },
        { "method": "GET", "path": "/events/stream", "description": "What happens next, as server-sent events" }
    ]);
    events["push"] = json!({ "sse": true, "mcp": false });
    json!({
        "best": {
            "version": BEST_VERSION,
            "authentication": {
                "type": "apiKey",
                "scheme": crate::auth::AUTH_HEADER,
                "in": "header",
                "tokenUrl": door.auth.token_url(),
                "deviceAuthorizationUrl": door.auth.device_authorization_url(),
                "note": guidance::MANIFEST_TEXT
            },
            "services": {
                door.service.as_str(): {
                    "version": BEST_VERSION,
                    "description": door.description,
                    "spec": "https://behavioralstate.io/specs/agents",
                    "http": { "endpoint": door.endpoint }
                }
            },
            "capabilities": [
                cap("io.best.agents.commands", "What this peer can be asked to do; the catalogue lists each command.", "commands", active(door.catalogue.has_commands())),
                events,
                cap("io.best.agents.queries", "What this peer knows right now; the catalogue lists each query.", "queries", active(door.catalogue.has_queries())),
                cap("io.best.agents.workflows", "Recipes that chain this peer's commands and queries; none is published yet.", "workflows", "planned")
            ]
        }
    })
}
