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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let listen: SocketAddr = std::env::var("BEST_PEER_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:7777".into())
        .parse()
        .expect("BEST_PEER_LISTEN is host:port");
    let endpoint = std::env::var("BEST_PEER_ENDPOINT").unwrap_or_else(|_| format!("http://{listen}"));
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

    #[tokio::test]
    async fn the_door_is_built_on_the_abstractions_not_on_an_adapter() {
        let p = peer();
        assert!(p.gpu.snapshot().unwrap().devices.is_empty());
        assert!(p.runtime.running().await.unwrap().is_empty());
    }
}
