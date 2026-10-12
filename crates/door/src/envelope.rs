//! The BEST envelope: a CloudEvents 1.0 message, restricted as the spec's profile says (SPEC.md, Wire Format).
//! Commands and events share it; `correlationid` is the one extension BEST defines, and unknown extensions are kept,
//! never rejected.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::error::BestError;

/// The most characters an `id` or `correlationid` may have: an identifier, not a payload.
const MAX_ID: usize = 256;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub specversion: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlationid: Option<String>,
    pub source: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub datacontenttype: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataschema: Option<String>,
    pub time: String,
    pub data: Value,
    /// Extension attributes BEST does not define; a consumer ignores them, a server keeps them.
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}

impl Envelope {
    /// A new event from this door, now, correlated to `correlationid` when it results from a command.
    pub fn event(
        source: &str,
        r#type: &str,
        data: Value,
        correlationid: Option<String>,
        dataschema: Option<String>,
    ) -> Self {
        Self {
            specversion: "1.0".into(),
            id: uuid::Uuid::new_v4().to_string(),
            correlationid,
            source: source.into(),
            r#type: r#type.into(),
            datacontenttype: "application/json".into(),
            dataschema,
            time: now_rfc3339(),
            data,
            extensions: BTreeMap::new(),
        }
    }

    /// The command-side rules of the profile: every required attribute present and well-formed. The error names the
    /// attribute and what it must be.
    pub fn validate_command(&self) -> Result<(), BestError> {
        let refuse = |attr: &'static str, what: &str| {
            Err(
                BestError::bad_request("INVALID_ENVELOPE", format!("{attr} must be {what}"))
                    .with_details(serde_json::json!({ "attribute": attr })),
            )
        };
        if self.specversion != "1.0" {
            return refuse("specversion", "\"1.0\"");
        }
        if self.id.trim().is_empty() || self.id.len() > MAX_ID {
            return refuse(
                "id",
                "a non-empty unique message id of at most 256 characters; it is the idempotency key",
            );
        }
        if self.source.trim().is_empty() || self.source.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return refuse("source", "a URI-reference naming the caller's origin, with no spaces");
        }
        if self
            .correlationid
            .as_deref()
            .is_some_and(|c| c.trim().is_empty() || c.len() > MAX_ID)
        {
            return refuse(
                "correlationid",
                "omitted, or a non-empty identifier of at most 256 characters",
            );
        }
        if !is_pascal_case(&self.r#type) {
            return refuse(
                "type",
                "the command type in PascalCase, as the catalogue states it (commandType)",
            );
        }
        if self.datacontenttype != "application/json" {
            return refuse("datacontenttype", "\"application/json\"");
        }
        match &self.dataschema {
            Some(d) if !d.trim().is_empty() => {}
            _ => return refuse("dataschema", "the catalogue entry's dataschema URI for this command"),
        }
        if OffsetDateTime::parse(&self.time, &Rfc3339).is_err() {
            return refuse("time", "an ISO 8601 timestamp, such as 2026-10-12T10:30:00Z");
        }
        if !self.data.is_object() {
            return refuse("data", "a JSON object, the command's payload");
        }
        Ok(())
    }

    /// The correlation identifier this message carries: its own, or its id when it carries none (the server adopts the
    /// command's id, SPEC.md, Command Results and Correlation).
    pub fn effective_correlation(&self) -> String {
        self.correlationid.clone().unwrap_or_else(|| self.id.clone())
    }
}

/// PascalCase: starts with an upper-case letter, then letters and digits only.
pub fn is_pascal_case(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_uppercase()) && chars.all(|c| c.is_ascii_alphanumeric())
}

pub fn now_rfc3339() -> String {
    OffsetDateTime::now_utc().format(&Rfc3339).expect("rfc3339")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn command() -> Envelope {
        serde_json::from_value(json!({
            "specversion": "1.0",
            "id": "a1b2c3",
            "source": "https://agent.example/claude",
            "type": "PullImage",
            "datacontenttype": "application/json",
            "dataschema": "http://peer.test/commands/pull-image/1.0",
            "time": "2026-10-12T10:30:00Z",
            "data": { "image": "ollama/ollama:0.6" },
            "traceparent": "00-abc-def-01"
        }))
        .unwrap()
    }

    #[test]
    fn a_conformant_command_passes_and_keeps_unknown_extensions() {
        let c = command();
        c.validate_command().unwrap();
        assert_eq!(c.extensions["traceparent"], "00-abc-def-01");
        assert_eq!(c.effective_correlation(), "a1b2c3");
        let back: Value = serde_json::to_value(&c).unwrap();
        assert_eq!(back["traceparent"], "00-abc-def-01");
        assert!(
            back.get("correlationid").is_none(),
            "an absent correlationid is not serialised as null"
        );
    }

    #[test]
    fn each_profile_rule_is_refused_by_attribute_name() {
        type Break = Box<dyn Fn(&mut Envelope)>;
        let cases: Vec<(&str, Break)> = vec![
            ("specversion", Box::new(|e| e.specversion = "0.3".into())),
            ("id", Box::new(|e| e.id = " ".into())),
            ("source", Box::new(|e| e.source = "".into())),
            ("source", Box::new(|e| e.source = "not a uri".into())),
            ("correlationid", Box::new(|e| e.correlationid = Some("".into()))),
            ("id", Box::new(|e| e.id = "x".repeat(257))),
            ("type", Box::new(|e| e.r#type = "pull-image".into())),
            ("datacontenttype", Box::new(|e| e.datacontenttype = "text/plain".into())),
            ("dataschema", Box::new(|e| e.dataschema = None)),
            ("time", Box::new(|e| e.time = "yesterday".into())),
            ("data", Box::new(|e| e.data = json!([1]))),
        ];
        for (attr, break_it) in cases {
            let mut c = command();
            break_it(&mut c);
            let err = c.validate_command().unwrap_err();
            assert_eq!(err.code, "INVALID_ENVELOPE", "{attr}");
            assert_eq!(err.details["attribute"], attr, "{attr}: {}", err.message);
            assert!(err.message.starts_with(attr), "{attr}: {}", err.message);
        }
    }

    #[test]
    fn an_event_is_correlated_when_it_results_from_a_command() {
        let e = Envelope::event(
            "io.best.agents",
            "ImagePulled",
            json!({"image": "x"}),
            Some("a1b2c3".into()),
            None,
        );
        assert_eq!(e.specversion, "1.0");
        assert_eq!(e.correlationid.as_deref(), Some("a1b2c3"));
        assert!(is_pascal_case(&e.r#type));
        OffsetDateTime::parse(&e.time, &Rfc3339).unwrap();
    }

    #[test]
    fn pascal_case_is_strict() {
        assert!(is_pascal_case("PullImage"));
        assert!(is_pascal_case("A1"));
        assert!(!is_pascal_case("pullImage"));
        assert!(!is_pascal_case("Pull-Image"));
        assert!(!is_pascal_case(""));
    }
}
