//! The BEST error format (`error.json`): `{ "error": { "code", "message", "details" } }` on every endpoint but the two
//! OAuth ones. A refusal says what to do: the message names the field and the fix, the details carry what a program
//! needs to act.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct BestError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub details: Value,
}

impl BestError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            details: json!({}),
        }
    }

    pub fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    pub fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    pub fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "This surface needs a credential: send it in the X-Api-Key header. Register through the manifest's deviceAuthorizationUrl if you have none.",
        )
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "CONFLICT", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", message)
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }

    pub fn body(&self) -> Value {
        json!({ "error": { "code": self.code, "message": self.message, "details": self.details } })
    }
}

impl std::fmt::Display for BestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}: {}", self.status.as_u16(), self.code, self.message)
    }
}

impl std::error::Error for BestError {}

impl IntoResponse for BestError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body())).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_body_is_the_spec_shape_and_nothing_else() {
        let e = BestError::bad_request("SCHEMA_VALIDATION", "data.model is required")
            .with_details(json!({"field": "data.model"}));
        let b = e.body();
        assert_eq!(b["error"]["code"], "SCHEMA_VALIDATION");
        assert_eq!(b["error"]["message"], "data.model is required");
        assert_eq!(b["error"]["details"]["field"], "data.model");
        assert_eq!(b.as_object().unwrap().len(), 1);
        assert_eq!(b["error"].as_object().unwrap().len(), 3);
    }
}
