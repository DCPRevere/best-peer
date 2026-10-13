//! The inference endpoint, behind the door's credential. `DeploymentReady` names a base URL on this door, under the
//! deployment; a request there with the credential that drove the deploy, as the door's own header or as the
//! `Authorization: Bearer` the OpenAI-compatible clients send, goes to the engine as it is and the answer comes back
//! as it comes, streaming included. The door reads the headers it checks and nothing else: no token is recorded, and
//! its events still say what the machine runs, never what the model answered. Without the credential the answer is
//! the door's 401; a deployment that is not ready, is ending or is gone answers 404 with what to do; an engine that
//! does not answer is 502 with what to do.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use best_peer_door::auth::AUTH_HEADER;
use best_peer_door::error::BestError;
use best_peer_door::Auth;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::lifecycle::Peer;

#[derive(Clone)]
struct Gateway {
    peer: Arc<Peer>,
    auth: Arc<Auth>,
    client: Client<HttpConnector, Body>,
}

/// The route `DeploymentReady` names: everything under `/deployments/{deployment}/` goes to that deployment's engine.
pub fn router(peer: Arc<Peer>, auth: Arc<Auth>) -> Router {
    let client = Client::builder(TokioExecutor::new()).build_http();
    Router::new()
        .route("/deployments/:deployment/*rest", any(pass_through))
        .with_state(Gateway { peer, auth, client })
}

async fn pass_through(
    State(gw): State<Gateway>,
    Path((deployment, rest)): Path<(String, String)>,
    req: Request,
) -> Response {
    forward(&gw, &deployment, &rest, req)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

async fn forward(gw: &Gateway, deployment: &str, rest: &str, req: Request) -> Result<Response, BestError> {
    let key = credential(req.headers());
    if key.is_empty() || gw.auth.authenticate(key).is_none() {
        return Err(BestError::unauthorized());
    }
    let upstream = gw.peer.upstream_of(deployment)?;
    let query = req.uri().query().map(|q| format!("?{q}")).unwrap_or_default();
    let uri: Uri = format!("{upstream}/{rest}{query}")
        .parse()
        .map_err(|e| BestError::bad_request("BAD_REQUEST", format!("the path cannot be forwarded: {e}")))?;
    let (mut parts, body) = req.into_parts();
    parts.uri = uri;
    // The engine gets the request as the client made it, less what is the door's: the host it was sent to and the
    // credential, which ends here.
    parts.headers.remove(header::HOST);
    parts.headers.remove(AUTH_HEADER);
    parts.headers.remove(header::AUTHORIZATION);
    let res = gw
        .client
        .request(Request::from_parts(parts, body))
        .await
        .map_err(|e| {
            BestError::new(
                StatusCode::BAD_GATEWAY,
                "ENGINE_UNREACHABLE",
                format!(
                    "the engine behind {deployment} did not answer: {e}. If it stopped, DeploymentEnded follows within 30 s; else read the engine's logs on the host"
                ),
            )
        })?;
    Ok(res.map(Body::new))
}

/// The credential, from the door's own header or the `Authorization: Bearer` the OpenAI-compatible clients send.
fn credential(headers: &HeaderMap) -> &str {
    let own = headers
        .get(AUTH_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .unwrap_or("");
    if !own.is_empty() {
        return own;
    }
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().strip_prefix("Bearer "))
        .map(str::trim)
        .unwrap_or("")
}
