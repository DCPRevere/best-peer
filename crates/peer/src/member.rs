//! A member of the cloud this peer fronts: another peer's door, reached through the transport with the credential
//! that door issued to this peer by its own sign-in. Everything here is the BEST surface as a consumer uses it (the
//! manifest, queries, commands, the event stream, the inference endpoint): the cloud is an agent to its members.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{header, Method, Request, Response, StatusCode};
use best_peer_door::auth::AUTH_HEADER;
use best_peer_door::{BestError, Envelope};
use futures::{Stream, StreamExt};
use serde_json::{json, Value};

use crate::transport::Transport;

/// Why a member did not do what was asked: it could not be reached, or it refused, with its answer.
#[derive(Debug)]
pub enum Refusal {
    Unreachable(String),
    Refused { status: StatusCode, body: Value },
}

impl Refusal {
    /// The member's refusal as this door's: the member named, the member's code and message kept.
    pub fn relayed(&self, member: &str) -> BestError {
        match self {
            Refusal::Unreachable(why) => BestError::new(
                StatusCode::BAD_GATEWAY,
                "PEER_UNREACHABLE",
                format!("peer {member} did not answer: {why}"),
            ),
            Refusal::Refused { status, body } => {
                let code = body["error"]["code"].as_str().unwrap_or("REFUSED").to_string();
                let message = body["error"]["message"].as_str().unwrap_or("refused").to_string();
                BestError::new(
                    *status,
                    "PEER_REFUSED",
                    format!("peer {member} refused ({code}): {message}"),
                )
                .with_details(json!({ "peer": member, "code": code, "details": body["error"]["details"] }))
            }
        }
    }
}

pub struct Member {
    pub name: String,
    pub endpoint: String,
    key: Mutex<Option<String>>,
    pub transport: Arc<dyn Transport>,
}

impl Member {
    pub fn new(name: &str, endpoint: &str, key: Option<String>, transport: Arc<dyn Transport>) -> Self {
        Self {
            name: name.into(),
            endpoint: endpoint.trim_end_matches('/').into(),
            key: Mutex::new(key),
            transport,
        }
    }

    pub fn key(&self) -> Option<String> {
        self.key.lock().unwrap().clone()
    }

    pub fn set_key(&self, key: String) {
        *self.key.lock().unwrap() = Some(key);
    }

    /// Admitted by this member: it issued the credential this peer holds for it.
    pub fn admitted(&self) -> bool {
        self.key.lock().unwrap().is_some()
    }

    fn request(&self, method: Method, path: &str, body: Option<(&str, String)>) -> Request<Body> {
        let mut req = Request::builder()
            .method(method)
            .uri(format!("{}{path}", self.endpoint))
            .header(header::ACCEPT, "application/json");
        if let Some(key) = self.key() {
            req = req.header(AUTH_HEADER, key);
        }
        match body {
            Some((content_type, text)) => req.header(header::CONTENT_TYPE, content_type).body(Body::from(text)),
            None => req.body(Body::empty()),
        }
        .expect("a request to a member is well formed")
    }

    async fn exchange(&self, req: Request<Body>) -> Result<Value, Refusal> {
        let res = self.transport.call(req).await.map_err(Refusal::Unreachable)?;
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), 8 << 20)
            .await
            .map_err(|e| Refusal::Unreachable(format!("the answer could not be read: {e}")))?;
        let body: Value = serde_json::from_slice(&bytes).unwrap_or_default();
        if status.is_success() {
            Ok(body)
        } else {
            Err(Refusal::Refused { status, body })
        }
    }

    pub async fn get(&self, path: &str) -> Result<Value, Refusal> {
        self.exchange(self.request(Method::GET, path, None)).await
    }

    /// A query of the member, with this peer's credential.
    pub async fn query(&self, schema: &str) -> Result<Value, Refusal> {
        self.get(&format!("/queries/{schema}")).await
    }

    /// A command sent to the member, as the envelope is; its `created` answer, or the refusal.
    pub async fn send(&self, envelope: &Envelope) -> Result<Value, Refusal> {
        let text = serde_json::to_string(envelope).expect("an envelope serialises");
        self.exchange(self.request(Method::POST, "/commands", Some(("application/cloudevents+json", text))))
            .await
    }

    /// A form posted to one of the member's sign-in routes, no credential: the device flow is public.
    pub async fn sign_in_form(&self, path: &str, fields: &[(&str, &str)]) -> Result<Value, Refusal> {
        let text = serde_urlencoded::to_string(fields).expect("a form encodes");
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("{}{path}", self.endpoint))
            .header(header::ACCEPT, "application/json")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(text))
            .expect("a form request is well formed");
        self.exchange(req).await
    }

    /// The member's event stream from after `last` (its event id), as it comes: each event's id and envelope. Ends
    /// when the member closes it; the caller reconnects with the last id it saw.
    pub async fn stream(&self, last: Option<&str>) -> Result<impl Stream<Item = (String, Envelope)>, Refusal> {
        let mut req = self.request(Method::GET, "/events/stream", None);
        req.headers_mut()
            .insert(header::ACCEPT, "text/event-stream".parse().unwrap());
        if let Some(id) = last {
            if let Ok(v) = id.parse() {
                req.headers_mut().insert("last-event-id", v);
            }
        }
        let res = self.transport.call(req).await.map_err(Refusal::Unreachable)?;
        if !res.status().is_success() {
            let status = res.status();
            let bytes = axum::body::to_bytes(res.into_body(), 1 << 20).await.unwrap_or_default();
            return Err(Refusal::Refused {
                status,
                body: serde_json::from_slice(&bytes).unwrap_or_default(),
            });
        }
        Ok(sse_events(res.into_body()))
    }

    /// A request passed to the member's inference endpoint for `deployment`, with this peer's credential in place
    /// of the caller's; the answer as it comes.
    pub async fn forward(&self, mut req: Request<Body>) -> Result<Response<Body>, String> {
        if let Some(key) = self.key() {
            req.headers_mut().insert(
                AUTH_HEADER,
                key.parse().map_err(|_| "the credential is not a header value")?,
            );
        }
        self.transport.call(req).await
    }
}

/// Server-sent events parsed from a body as it streams: the `id` and the `data` of each event, the data as an
/// envelope. Comments (keep-alives) and events without an envelope are skipped.
fn sse_events(body: Body) -> impl Stream<Item = (String, Envelope)> {
    let mut buffer = String::new();
    body.into_data_stream()
        .take_while(|chunk| futures::future::ready(chunk.is_ok()))
        .flat_map(move |chunk| {
            buffer.push_str(&String::from_utf8_lossy(&chunk.unwrap_or_default()));
            let mut out = Vec::new();
            while let Some(end) = buffer.find("\n\n") {
                let block = buffer[..end].to_string();
                buffer.drain(..end + 2);
                let (mut id, mut data) = (None, String::new());
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("id:") {
                        id = Some(v.trim().to_string());
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data.push_str(v.trim_start());
                    }
                }
                if let (Some(id), Ok(envelope)) = (id, serde_json::from_str::<Envelope>(&data)) {
                    out.push((id, envelope));
                }
            }
            futures::stream::iter(out)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_stream_is_parsed_event_by_event_across_chunks() {
        let e1 = serde_json::to_string(&Envelope::event("s", "A", json!({}), None, None)).unwrap();
        let e2 = serde_json::to_string(&Envelope::event("s", "B", json!({}), None, None)).unwrap();
        let text = format!(": keepalive\n\nid: one\ndata: {e1}\n\nid: two\ndata: {e2}\n\n");
        let (head, tail) = text.split_at(text.len() / 2);
        let chunks: Vec<Result<axum::body::Bytes, std::io::Error>> = vec![
            Ok(axum::body::Bytes::from(head.to_string())),
            Ok(axum::body::Bytes::from(tail.to_string())),
        ];
        let body = Body::from_stream(futures::stream::iter(chunks));
        let got: Vec<(String, String)> = sse_events(body).map(|(id, e)| (id, e.r#type)).collect().await;
        assert_eq!(
            got,
            [
                ("one".to_string(), "A".to_string()),
                ("two".to_string(), "B".to_string())
            ]
        );
    }
}
