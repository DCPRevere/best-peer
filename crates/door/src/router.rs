//! The HTTP routes the spec names (SPEC.md, HTTP Transport and the four capabilities), over a [`Door`]. The manifest
//! and the two OAuth endpoints are public; everything else needs the credential in the `X-Api-Key` header.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::{self, Stream, StreamExt};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio_stream::wrappers::BroadcastStream;

use crate::auth::{DeviceRequest, OAuthError, TokenRequest, AUTH_HEADER};
use crate::envelope::Envelope;
use crate::error::BestError;
use crate::log::{Accept, Filter};
use crate::{manifest, Door};

type Shared = Arc<Door>;

pub fn router(door: Shared) -> Router {
    let public = Router::new()
        .route("/.well-known/best", get(well_known))
        .route("/auth/device", post(device))
        .route("/auth/token", post(token))
        .route("/auth/verify", get(verify))
        .route("/auth/approve", post(approve));
    let protected = Router::new()
        .route("/commands", get(command_catalogue).post(ingest))
        .route("/commands/:schema/:version", get(command_document))
        .route("/events", get(list_events))
        .route("/events/stream", get(stream_events))
        .route("/events/:schema/:version", get(event_document))
        .route("/queries", get(query_catalogue))
        .route("/queries/:schema/:version", get(query_document))
        .route("/queries/:schema", get(execute_query))
        .route("/workflows", get(workflow_index))
        .route("/workflows/:id", get(workflow))
        .layer(middleware::from_fn_with_state(door.clone(), require_key));
    public.merge(protected).with_state(door)
}

async fn require_key(State(door): State<Shared>, req: Request, next: Next) -> Response {
    let key = req
        .headers()
        .get(AUTH_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .unwrap_or("");
    if key.is_empty() || door.auth.authenticate(key).is_none() {
        return BestError::unauthorized().into_response();
    }
    next.run(req).await
}

async fn well_known(State(door): State<Shared>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/json")],
        Json(manifest::manifest(&door)),
    )
}

// ----- sign-in -----------------------------------------------------------------------------------------------------

impl IntoResponse for OAuthError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::BAD_REQUEST);
        (status, [(header::CACHE_CONTROL, "no-store")], Json(self.body())).into_response()
    }
}

fn form<T: serde::de::DeserializeOwned>(headers: &HeaderMap, body: &Bytes) -> Result<T, OAuthError> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !ct.starts_with("application/x-www-form-urlencoded") {
        return Err(OAuthError::invalid_request(
            "the request must be form-encoded: Content-Type application/x-www-form-urlencoded",
        ));
    }
    serde_urlencoded::from_bytes(body)
        .map_err(|e| OAuthError::invalid_request(format!("the form could not be read: {e}")))
}

async fn device(State(door): State<Shared>, headers: HeaderMap, body: Bytes) -> Result<Response, OAuthError> {
    let req: DeviceRequest = form(&headers, &body)?;
    let answer = door.auth.start_device(req)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(answer)).into_response())
}

async fn token(State(door): State<Shared>, headers: HeaderMap, body: Bytes) -> Result<Response, OAuthError> {
    let req: TokenRequest = form(&headers, &body)?;
    let answer = door.auth.token(req)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(answer)).into_response())
}

async fn verify(State(door): State<Shared>, Query(q): Query<HashMap<String, String>>) -> Html<String> {
    Html(door.auth.verification_page(q.get("user_code").map(String::as_str)))
}

#[derive(serde::Deserialize)]
struct ApproveForm {
    user_code: String,
    proof: String,
}

async fn approve(State(door): State<Shared>, headers: HeaderMap, body: Bytes) -> Result<Response, OAuthError> {
    let f: ApproveForm = form(&headers, &body)?;
    door.auth.approve(f.user_code.trim(), f.proof.trim()).await?;
    Ok(Json(json!({ "approved": true })).into_response())
}

// ----- commands ----------------------------------------------------------------------------------------------------

async fn command_catalogue(State(door): State<Shared>) -> Json<Value> {
    Json(door.catalogue.command_catalogue(&door.endpoint))
}

async fn command_document(
    State(door): State<Shared>,
    Path((schema, version)): Path<(String, String)>,
) -> Result<Response, BestError> {
    let def = door.catalogue.command_by_schema(&schema, &version).ok_or_else(|| {
        BestError::not_found(
            "SCHEMA_NOT_FOUND",
            format!(
                "Unknown command schema '{schema}' version '{version}': GET /commands lists the ones this door accepts"
            ),
        )
    })?;
    Ok((
        [(header::CONTENT_TYPE, "application/schema+json")],
        Json(door.catalogue.command_document(&door.endpoint, def)),
    )
        .into_response())
}

async fn ingest(State(door): State<Shared>, headers: HeaderMap, body: Bytes) -> Result<Response, BestError> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");
    let mime = ct.split(';').next().unwrap_or("").trim();
    if mime != "application/json" && mime != "application/cloudevents+json" {
        return Err(BestError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UNSUPPORTED_MEDIA_TYPE",
            format!("Content-Type must be application/cloudevents+json or application/json, not {mime}"),
        ));
    }
    let value: Value = serde_json::from_slice(&body)
        .map_err(|e| BestError::bad_request("INVALID_JSON", format!("the body is not JSON: {e}")))?;
    let envelope: Envelope = serde_json::from_value(value)
        .map_err(|e| BestError::bad_request("INVALID_ENVELOPE", format!("the body is not a BEST envelope: {e}")))?;
    envelope.validate_command()?;
    let def = door.catalogue.command_by_type(&envelope.r#type).ok_or_else(|| {
        BestError::bad_request(
            "UNKNOWN_TYPE",
            format!("type {} is not a command this door accepts: GET /commands lists them, and commandType is the value to send", envelope.r#type),
        )
        .with_details(json!({ "accepted": door.catalogue.command_types() }))
    })?;
    let expected = door.schema_url("commands", &def.schema, &def.version);
    if envelope.dataschema.as_deref() != Some(expected.as_str()) {
        return Err(BestError::bad_request(
            "DATASCHEMA_MISMATCH",
            format!(
                "dataschema must be {expected}, the catalogue's value for {}",
                def.command_type
            ),
        )
        .with_details(json!({ "expected": expected })));
    }
    door.catalogue.validate_data(&envelope.r#type, &envelope.data)?;

    let correlation = envelope.effective_correlation();
    let hash = hex::encode(Sha256::digest(
        serde_json::to_vec(&json!({ "type": envelope.r#type, "data": envelope.data })).unwrap(),
    ));
    match door.log.accept_command(&envelope.id, &hash) {
        Accept::Conflict => {
            return Err(BestError::conflict(format!(
                "id {} was already used for a different command: a new command needs a new id",
                envelope.id
            )))
        }
        Accept::Duplicate => {}
        Accept::New => {
            let emitter = door.emitter(&correlation);
            tokio::spawn((def.handler)(envelope.clone(), emitter));
        }
    }
    Ok((
        StatusCode::CREATED,
        Json(json!({ "id": envelope.id, "correlationId": correlation })),
    )
        .into_response())
}

// ----- events ------------------------------------------------------------------------------------------------------

fn filter_from(q: &HashMap<String, String>) -> Result<Filter, BestError> {
    let bad = |param: &str, what: &str| {
        BestError::bad_request("INVALID_PARAMETER", format!("{param} must be {what}"))
            .with_details(json!({ "parameter": param }))
    };
    let time = |param: &str| -> Result<Option<OffsetDateTime>, BestError> {
        q.get(param)
            .map(|v| OffsetDateTime::parse(v, &Rfc3339).map_err(|_| bad(param, "an ISO 8601 timestamp")))
            .transpose()
    };
    Ok(Filter {
        r#type: q.get("type").cloned(),
        correlation_id: q.get("correlationId").cloned(),
        source: q.get("source").cloned(),
        from: time("from")?,
        to: time("to")?,
        limit: q
            .get("limit")
            .map(|v| v.parse::<usize>().map_err(|_| bad("limit", "a positive number")))
            .transpose()?,
        after: q
            .get("after")
            .map(|v| {
                v.parse::<u64>()
                    .map_err(|_| bad("after", "a cursor from a previous response's nextCursor"))
            })
            .transpose()?,
    })
}

async fn list_events(
    State(door): State<Shared>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Json<Value>, BestError> {
    let filter = filter_from(&q)?;
    let (events, next) = door.log.list(&filter);
    let mut body = json!({ "events": events });
    if let Some(n) = next {
        body["nextCursor"] = json!(n);
    }
    Ok(Json(body))
}

async fn stream_events(
    State(door): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, BestError> {
    let filter = filter_from(&q)?;
    let last = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let rx = door.log.subscribe();
    let replay = match last {
        Some(id) => door.log.since_id(&id),
        None => Vec::new(),
    };
    let f1 = filter.clone();
    let replayed = stream::iter(replay.into_iter().filter(move |s| f1.matches(s)));
    let live = BroadcastStream::new(rx).filter_map(move |r| {
        let keep = r.ok().filter(|s| filter.matches(s));
        async move { keep }
    });
    let out = replayed.chain(live).map(|s| {
        Ok(SseEvent::default()
            .id(s.envelope.id.clone())
            .data(serde_json::to_string(&s.envelope).unwrap_or_default()))
    });
    Ok(Sse::new(out).keep_alive(KeepAlive::default().text("keepalive")))
}

async fn event_document(
    State(door): State<Shared>,
    Path((schema, version)): Path<(String, String)>,
) -> Result<Response, BestError> {
    let def = door.catalogue.event_by_schema(&schema, &version).ok_or_else(|| {
        BestError::not_found(
            "SCHEMA_NOT_FOUND",
            format!("Unknown event schema '{schema}' version '{version}'"),
        )
    })?;
    Ok((
        [(header::CONTENT_TYPE, "application/schema+json")],
        Json(door.catalogue.event_document(&door.endpoint, def)),
    )
        .into_response())
}

// ----- queries -----------------------------------------------------------------------------------------------------

async fn query_catalogue(State(door): State<Shared>) -> Json<Value> {
    Json(door.catalogue.query_catalogue(&door.endpoint))
}

fn query_not_found(schema: &str) -> BestError {
    BestError::not_found(
        "SCHEMA_NOT_FOUND",
        format!("Unknown query schema '{schema}': GET /queries lists the ones this door answers"),
    )
}

async fn query_document(
    State(door): State<Shared>,
    Path((schema, version)): Path<(String, String)>,
) -> Result<Response, BestError> {
    let def = door
        .catalogue
        .query_by_schema(&schema)
        .filter(|q| q.version == version)
        .ok_or_else(|| query_not_found(&schema))?;
    Ok((
        [(header::CONTENT_TYPE, "application/schema+json")],
        Json(door.catalogue.query_document(def)),
    )
        .into_response())
}

async fn execute_query(
    State(door): State<Shared>,
    Path(schema): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, BestError> {
    let def = door
        .catalogue
        .query_by_schema(&schema)
        .ok_or_else(|| query_not_found(&schema))?;
    Ok(Json((def.handler)(params).await?))
}

// ----- workflows ---------------------------------------------------------------------------------------------------

async fn workflow_index() -> Json<Value> {
    Json(json!({ "workflows": [] }))
}

async fn workflow(Path(id): Path<String>) -> BestError {
    BestError::not_found(
        "WORKFLOW_NOT_FOUND",
        format!("Unknown workflow '{id}': GET /workflows lists the published recipes"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Auth, FakeApprover};
    use crate::catalogue::{Catalogue, CommandDef, EventDef, QueryDef};
    use crate::log::EventLog;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    const EP: &str = "http://peer.test";

    fn door() -> Shared {
        let catalogue = Catalogue::new()
            .command(CommandDef {
                schema: "pull-image".into(),
                version: "1.0".into(),
                command_type: "PullImage".into(),
                description: "Makes an image present on the peer.".into(),
                data_schema: json!({ "type": "object", "required": ["image"], "properties": { "image": { "type": "string", "minLength": 1 } }, "additionalProperties": false }),
                produces: vec!["ImagePulled".into()],
                impact: None,
                handler: Arc::new(|cmd, emitter| {
                    Box::pin(async move {
                        emitter.emit("ImagePulled", json!({ "image": cmd.data["image"] }));
                    })
                }),
            })
            .event(EventDef {
                schema: "image-pulled".into(),
                version: "1.0".into(),
                event_type: "ImagePulled".into(),
                description: "An image is present.".into(),
                data_schema: json!({ "type": "object", "properties": { "image": { "type": "string" } } }),
            })
            .query(QueryDef {
                schema: "get-peer".into(),
                version: "1.0".into(),
                description: "Who this peer is.".into(),
                parameters: None,
                response: json!({ "type": "object", "properties": { "name": { "type": "string" } } }),
                handler: Arc::new(|p| Box::pin(async move { Ok(json!({ "name": "peer", "echo": p.get("x") })) })),
            });
        let auth = Arc::new(Auth::ephemeral(EP, Box::new(FakeApprover { proof: "ok".into() })));
        Arc::new(Door::new(
            EP,
            "io.best.agents",
            "A test peer.",
            catalogue,
            Arc::new(EventLog::ephemeral()),
            auth,
        ))
    }

    async fn call(app: &Router, req: Request) -> (StatusCode, HeaderMap, Value) {
        let res = app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let headers = res.headers().clone();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into()));
        (status, headers, body)
    }

    fn get(path: &str, key: Option<&str>) -> Request {
        let mut b = Request::get(path);
        if let Some(k) = key {
            b = b.header(AUTH_HEADER, k);
        }
        b.body(Body::empty()).unwrap()
    }

    fn post_json(path: &str, key: &str, ct: &str, body: Value) -> Request {
        Request::post(path)
            .header(AUTH_HEADER, key)
            .header(header::CONTENT_TYPE, ct)
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn post_form(path: &str, fields: &[(&str, &str)]) -> Request {
        Request::post(path)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(serde_urlencoded::to_string(fields).unwrap()))
            .unwrap()
    }

    /// Registers through the device flow and returns a working key.
    async fn sign_in(app: &Router) -> String {
        let (s, _, a) = call(
            app,
            post_form("/auth/device", &[("client_id", "test"), ("connection", "my-peer")]),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{a}");
        let (s, _, _) = call(
            app,
            post_form(
                "/auth/approve",
                &[("user_code", a["user_code"].as_str().unwrap()), ("proof", "ok")],
            ),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let (s, h, t) = call(
            app,
            post_form(
                "/auth/token",
                &[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                    ("device_code", a["device_code"].as_str().unwrap()),
                    ("client_id", "test"),
                ],
            ),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{t}");
        assert_eq!(h[header::CACHE_CONTROL], "no-store");
        t["access_token"].as_str().unwrap().to_string()
    }

    fn command(id: &str, image: Value) -> Value {
        json!({
            "specversion": "1.0", "id": id, "source": "https://agent.test/claude", "type": "PullImage",
            "datacontenttype": "application/json", "dataschema": format!("{EP}/commands/pull-image/1.0"),
            "time": "2026-10-12T10:30:00Z", "data": { "image": image }
        })
    }

    #[tokio::test]
    async fn the_manifest_is_public_and_everything_else_needs_the_key() {
        let app = router(door());
        let (s, h, m) = call(&app, get("/.well-known/best", None)).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(h[header::CONTENT_TYPE], "application/json");
        assert_eq!(
            m["best"]["authentication"]["deviceAuthorizationUrl"],
            format!("{EP}/auth/device")
        );
        assert_eq!(m["best"]["authentication"]["note"], crate::guidance::MANIFEST_TEXT);
        for path in [
            "/commands",
            "/events",
            "/events/stream",
            "/queries",
            "/queries/get-peer",
            "/workflows",
        ] {
            let (s, _, b) = call(&app, get(path, None)).await;
            assert_eq!(s, StatusCode::UNAUTHORIZED, "{path}");
            assert_eq!(b["error"]["code"], "UNAUTHORIZED", "{path}");
        }
        let (s, _, _) = call(&app, get("/commands", Some("bp_wrong"))).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn sign_in_end_to_end_then_the_catalogue_opens() {
        let app = router(door());
        let key = sign_in(&app).await;
        let (s, _, c) = call(&app, get("/commands", Some(&key))).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(c["commands"][0]["commandType"], "PullImage");
        let (s, _, e) = call(
            &app,
            post_form("/auth/device", &[("client_id", "test"), ("connection", "Not One Word")]),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_eq!(e["error"], "invalid_request");
        assert!(e["error_description"].as_str().unwrap().contains("lowercase"));
        let (s, _, e) = call(
            &app,
            Request::post("/auth/device")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_eq!(e["error"], "invalid_request");
        let (s, _, e) = call(
            &app,
            post_form(
                "/auth/token",
                &[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                    ("device_code", "best-validate-unknown"),
                    ("client_id", "x"),
                ],
            ),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_eq!(e["error"], "invalid_grant");
    }

    #[tokio::test]
    async fn a_command_is_recorded_correlated_and_its_events_are_readable() {
        let d = door();
        let app = router(d.clone());
        let key = sign_in(&app).await;
        let (s, _, b) = call(
            &app,
            post_json(
                "/commands",
                &key,
                "application/cloudevents+json",
                command("cmd-1", json!("ollama/ollama:0.6")),
            ),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        assert_eq!(b["id"], "cmd-1");
        assert_eq!(b["correlationId"], "cmd-1");
        tokio::task::yield_now().await;
        for _ in 0..50 {
            if !d.log.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let (s, _, e) = call(&app, get("/events?correlationId=cmd-1", Some(&key))).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(e["events"][0]["type"], "ImagePulled");
        assert_eq!(e["events"][0]["correlationid"], "cmd-1");
        assert_eq!(e["events"][0]["dataschema"], format!("{EP}/events/image-pulled/1.0"));
        assert_eq!(e["events"][0]["source"], "io.best.agents");
        // The same id again is ignored safely; the same id with another payload is a conflict.
        let (s, _, _) = call(
            &app,
            post_json(
                "/commands",
                &key,
                "application/json",
                command("cmd-1", json!("ollama/ollama:0.6")),
            ),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED);
        let (s, _, c) = call(
            &app,
            post_json(
                "/commands",
                &key,
                "application/json",
                command("cmd-1", json!("other:1")),
            ),
        )
        .await;
        assert_eq!(s, StatusCode::CONFLICT);
        assert_eq!(c["error"]["code"], "CONFLICT");
        assert_eq!(d.log.len(), 1, "the duplicate ran no handler");
    }

    #[tokio::test]
    async fn every_refusal_says_what_to_send_instead() {
        let app = router(door());
        let key = sign_in(&app).await;
        let mut unknown = command("c2", json!("x"));
        unknown["type"] = json!("Explode");
        let (s, _, e) = call(&app, post_json("/commands", &key, "application/json", unknown)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_eq!(e["error"]["code"], "UNKNOWN_TYPE");
        assert_eq!(e["error"]["details"]["accepted"][0], "PullImage");
        let (s, _, e) = call(
            &app,
            post_json("/commands", &key, "application/json", command("c3", json!(""))),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_eq!(e["error"]["code"], "SCHEMA_VALIDATION");
        assert_eq!(e["error"]["details"]["errors"][0]["path"], "data/image");
        let mut wrong = command("c4", json!("x"));
        wrong["dataschema"] = json!("https://elsewhere.test/schema");
        let (s, _, e) = call(&app, post_json("/commands", &key, "application/json", wrong)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_eq!(e["error"]["code"], "DATASCHEMA_MISMATCH");
        let (s, _, e) = call(&app, post_json("/commands", &key, "text/plain", json!({}))).await;
        assert_eq!(s, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(e["error"]["code"], "UNSUPPORTED_MEDIA_TYPE");
        let (s, _, e) = call(
            &app,
            post_json("/commands", &key, "application/json", json!({ "specversion": "1.0" })),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_eq!(e["error"]["code"], "INVALID_ENVELOPE");
        let (s, _, e) = call(&app, get("/commands/nope/1.0", Some(&key))).await;
        assert_eq!(s, StatusCode::NOT_FOUND);
        assert_eq!(e["error"]["code"], "SCHEMA_NOT_FOUND");
        let (s, h, doc) = call(&app, get("/commands/pull-image/1.0", Some(&key))).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(h[header::CONTENT_TYPE], "application/schema+json");
        assert_eq!(doc["commandType"], "PullImage");
    }

    #[tokio::test]
    async fn queries_are_catalogued_documented_executed_and_unknown_ones_are_404() {
        let app = router(door());
        let key = sign_in(&app).await;
        let (_, _, c) = call(&app, get("/queries", Some(&key))).await;
        assert_eq!(c["queries"][0]["dataschema"], format!("{EP}/queries/get-peer/1.0"));
        let (s, _, d) = call(&app, get("/queries/get-peer/1.0", Some(&key))).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(d["response"]["type"], "object");
        let (s, _, r) = call(&app, get("/queries/get-peer?x=1", Some(&key))).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(r["echo"], "1");
        let (s, _, e) = call(&app, get("/queries/nope", Some(&key))).await;
        assert_eq!(s, StatusCode::NOT_FOUND);
        assert_eq!(e["error"]["code"], "SCHEMA_NOT_FOUND");
        let (s, _, e) = call(&app, get("/events?limit=zero", Some(&key))).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_eq!(e["error"]["details"]["parameter"], "limit");
        let (s, _, w) = call(&app, get("/workflows", Some(&key))).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(w["workflows"], json!([]));
        let (s, _, _) = call(&app, get("/workflows/nope", Some(&key))).await;
        assert_eq!(s, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn the_stream_replays_after_the_last_seen_id_then_follows_live() {
        let d = door();
        let app = router(d.clone());
        let key = sign_in(&app).await;
        let first = d.log.append(Envelope::event(
            "io.best.agents",
            "A",
            json!({}),
            Some("c".into()),
            None,
        ));
        let _second = d.log.append(Envelope::event(
            "io.best.agents",
            "B",
            json!({}),
            Some("c".into()),
            None,
        ));
        let res = app
            .clone()
            .oneshot(
                Request::get("/events/stream?correlationId=c")
                    .header(AUTH_HEADER, &key)
                    .header("last-event-id", &first.envelope.id)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(res.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream"));
        let mut body = res.into_body().into_data_stream();
        let replay = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let replay = String::from_utf8_lossy(&replay);
        assert!(replay.contains("\"type\":\"B\""), "{replay}");
        assert!(!replay.contains("\"type\":\"A\""), "{replay}");
        d.log.append(Envelope::event(
            "io.best.agents",
            "Other",
            json!({}),
            Some("not-c".into()),
            None,
        ));
        let live = d.log.append(Envelope::event(
            "io.best.agents",
            "C",
            json!({}),
            Some("c".into()),
            None,
        ));
        let frame = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let frame = String::from_utf8_lossy(&frame);
        assert!(frame.contains(&format!("id: {}", live.envelope.id)), "{frame}");
        assert!(frame.contains("\"type\":\"C\""), "{frame}");
        assert!(!frame.contains("Other"), "the filter holds on the live part: {frame}");
    }
}
