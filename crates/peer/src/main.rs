//! best-peer: one binary that puts a GPU host behind a BEST door.
//!
//! The door is built against two abstractions the workspace owns, [`best_peer_gpu::Gpu`] and
//! [`best_peer_runtime::Runtime`]; which adapters stand behind them is decided here and nowhere else. Until the
//! adapter slices land, the peer runs on the fakes, which is enough to serve the manifest and prove the shape.

mod manifest;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{extract::State, http::header, response::IntoResponse, routing::get, Json, Router};
use best_peer_gpu::Gpu;
use best_peer_runtime::Runtime;

/// What every handler can reach.
#[derive(Clone)]
pub struct Peer {
    pub endpoint: String,
    pub gpu: Arc<dyn Gpu>,
    pub runtime: Arc<dyn Runtime>,
}

/// The door's routes. Everything the peer serves hangs off this router, so a test can drive it without a socket.
pub fn router(peer: Peer) -> Router {
    Router::new()
        .route("/.well-known/best", get(well_known))
        .with_state(peer)
}

async fn well_known(State(peer): State<Peer>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/json")],
        Json(manifest::manifest(&peer.endpoint)),
    )
}

/// Where to listen and what to advertise, from `BEST_PEER_LISTEN` (ip:port, default loopback) and
/// `BEST_PEER_ENDPOINT` (the consumer-facing base URL). The endpoint defaults to the listen address only when that
/// address is one a consumer can use: a peer listening on every interface has no address of its own to advertise,
/// so it refuses to start until told what it is called.
pub fn config(listen: Option<&str>, endpoint: Option<&str>) -> Result<(SocketAddr, String), String> {
    let listen_text = listen.unwrap_or("127.0.0.1:7777");
    let listen: SocketAddr = listen_text
        .parse()
        .map_err(|_| format!("BEST_PEER_LISTEN must be ip:port, not {listen_text:?}"))?;
    let endpoint = match endpoint {
        Some(e) => e.trim_end_matches('/').to_string(),
        None if listen.ip().is_unspecified() => {
            return Err(format!(
                "BEST_PEER_LISTEN is {listen}, an address no consumer can reach: set BEST_PEER_ENDPOINT to the URL this peer is reached at"
            ))
        }
        None => format!("http://{listen}"),
    };
    Ok((listen, endpoint))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let (listen, endpoint) = match config(
        std::env::var("BEST_PEER_LISTEN").ok().as_deref(),
        std::env::var("BEST_PEER_ENDPOINT").ok().as_deref(),
    ) {
        Ok(c) => c,
        Err(why) => {
            eprintln!("{why}");
            std::process::exit(2);
        }
    };
    let peer = Peer {
        endpoint,
        gpu: Arc::new(best_peer_gpu::Fake::new().with_driver("fake")),
        runtime: Arc::new(best_peer_runtime::Fake::new(vec![], vec![])),
    };
    let listener = tokio::net::TcpListener::bind(listen).await.expect("bind");
    tracing::info!(%listen, endpoint = %peer.endpoint, "best-peer serving its manifest");
    axum::serve(listener, router(peer))
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await
        .expect("serve");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn peer() -> Peer {
        Peer {
            endpoint: "http://peer.test".into(),
            gpu: Arc::new(best_peer_gpu::Fake::new()),
            runtime: Arc::new(best_peer_runtime::Fake::new(vec![], vec![])),
        }
    }

    #[tokio::test]
    async fn the_well_known_manifest_is_public_json_with_the_best_root() {
        let res = router(peer())
            .oneshot(Request::get("/.well-known/best").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()[header::CONTENT_TYPE], "application/json");
        let body: serde_json::Value =
            serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(
            body["best"]["services"][manifest::SERVICE]["http"]["endpoint"],
            "http://peer.test"
        );
    }

    #[test]
    fn the_endpoint_defaults_to_the_listen_address_only_when_a_consumer_can_reach_it() {
        let (listen, endpoint) = config(None, None).unwrap();
        assert_eq!(listen.to_string(), "127.0.0.1:7777");
        assert_eq!(endpoint, "http://127.0.0.1:7777");
        let (_, endpoint) = config(Some("0.0.0.0:7777"), Some("https://peer.example.com/")).unwrap();
        assert_eq!(endpoint, "https://peer.example.com");
        let why = config(Some("0.0.0.0:7777"), None).unwrap_err();
        assert!(why.contains("BEST_PEER_ENDPOINT"), "{why}");
        let why = config(Some("[::]:7777"), None).unwrap_err();
        assert!(why.contains("BEST_PEER_ENDPOINT"), "{why}");
        let why = config(Some("localhost:7777"), None).unwrap_err();
        assert!(why.contains("ip:port"), "{why}");
    }

    #[tokio::test]
    async fn the_door_is_built_on_the_abstractions_not_on_an_adapter() {
        let p = peer();
        assert!(p.gpu.snapshot().unwrap().devices.is_empty());
        assert!(p.runtime.running().await.unwrap().is_empty());
    }
}
