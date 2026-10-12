//! The BEST surface of a peer, written from the spec text (behavioralstate.io, SPEC.md) and from nothing else.
//!
//! A [`Door`] is what a peer offers through the protocol: a [`Catalogue`] of commands, queries and events with their
//! schemas and handlers, an [`EventLog`] every fact is appended to and streamed from, and an [`Auth`] that admits
//! agents through the device flow and checks their credential on every call. [`router`] turns a door into the HTTP
//! routes the spec names; [`manifest`] describes it at `/.well-known/best`.
//!
//! What the door does not know: which hardware or engine stands behind the handlers, and who approves a sign-in. The
//! first two are the peer's abstractions; the last is an [`Approver`] the peer chooses.

pub mod auth;
pub mod catalogue;
pub mod envelope;
pub mod error;
pub mod guidance;
pub mod log;
pub mod manifest;
pub mod router;

pub use auth::{Approver, Auth, FakeApprover, LocalApprover};
pub use catalogue::{Catalogue, CommandDef, Emitter, EventDef, QueryDef};
pub use envelope::Envelope;
pub use error::BestError;
pub use log::{EventLog, Filter};
pub use router::router;

use std::collections::HashMap;
use std::sync::Arc;

/// The BEST spec version this surface is written against.
pub const BEST_VERSION: &str = "0.9.17";

/// Everything the routes need. Built once by the peer, shared by every handler.
pub struct Door {
    /// The consumer-facing base URL every capability path is appended to.
    pub endpoint: String,
    /// The service key in the manifest (`services`), and the `source` of every event the door emits.
    pub service: String,
    /// What the service is, in a sentence, for the manifest.
    pub description: String,
    pub catalogue: Catalogue,
    pub log: Arc<EventLog>,
    pub auth: Arc<Auth>,
    /// Event type → absolute dataschema URI, for every event the catalogue declares.
    pub event_dataschemas: Arc<HashMap<String, String>>,
}

impl Door {
    pub fn new(
        endpoint: &str,
        service: &str,
        description: &str,
        catalogue: Catalogue,
        log: Arc<EventLog>,
        auth: Arc<Auth>,
    ) -> Self {
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let event_dataschemas = Arc::new(catalogue.event_dataschemas(&endpoint));
        Self {
            endpoint,
            service: service.into(),
            description: description.into(),
            catalogue,
            log,
            auth,
            event_dataschemas,
        }
    }

    /// The absolute `dataschema` URI of a command, query or event schema document on this door.
    pub fn schema_url(&self, kind: &str, schema: &str, version: &str) -> String {
        format!("{}/{kind}/{schema}/{version}", self.endpoint)
    }

    /// An emitter for the handler of a command with this correlation id.
    pub fn emitter(&self, correlation: &str) -> Emitter {
        Emitter::new(
            self.log.clone(),
            &self.service,
            correlation,
            self.event_dataschemas.clone(),
        )
    }
}
