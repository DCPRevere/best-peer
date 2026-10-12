//! best-peer: one binary that puts a GPU host behind a BEST door.
//!
//! The door is built against two abstractions the workspace owns, [`best_peer_gpu::Gpu`] and
//! [`best_peer_runtime::Runtime`]; which adapters stand behind them is decided here and nowhere else. Until the
//! adapter slices land, the peer runs on the fakes, which is enough to serve the whole surface and prove its shape.
//!
//! `best-peer` serves; `best-peer approve <code>` admits an agent that asked, on the word of whoever controls this
//! machine.

mod domain;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use best_peer_door::{Auth, Door, EventLog, LocalApprover};
use best_peer_gpu::Gpu;
use best_peer_runtime::Runtime;

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

/// The peer's data directory: `BEST_PEER_DATA`, else `.best-peer` in the home directory. Never a relative path:
/// the server and the operator's shell must resolve the same directory, or `best-peer approve` finds no token.
fn data_dir() -> PathBuf {
    if let Ok(d) = std::env::var("BEST_PEER_DATA") {
        return PathBuf::from(d);
    }
    match std::env::home_dir() {
        Some(h) => h.join(".best-peer"),
        None => fail("no home directory to keep the peer's data in: set BEST_PEER_DATA to an absolute path"),
    }
}

/// The door of this peer over the given hardware and engine.
pub fn door(endpoint: &str, gpu: Arc<dyn Gpu>, runtime: Arc<dyn Runtime>, log: Arc<EventLog>, auth: Arc<Auth>) -> Door {
    Door::new(
        endpoint,
        "io.best.agents",
        "A peer: a GPU host that deploys models and runs jobs on request and tells their lifecycle as events.",
        domain::catalogue(gpu, runtime),
        log,
        auth,
    )
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None => serve(),
        Some("approve") => approve(args.get(1).map(String::as_str)),
        Some(other) => {
            eprintln!("unknown command {other:?}: run with no arguments to serve, or `best-peer approve <code>`");
            std::process::exit(2);
        }
    }
}

#[tokio::main]
async fn serve() {
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
    let dir = data_dir();
    let approver = LocalApprover::open(&dir)
        .unwrap_or_else(|e| fail(&format!("cannot keep the admin token in {}: {e}", dir.display())));
    let auth = Arc::new(
        Auth::open(&dir, &endpoint, Box::new(approver))
            .unwrap_or_else(|e| fail(&format!("cannot keep credentials in {}: {e}", dir.display()))),
    );
    let log = Arc::new(
        EventLog::open(&dir).unwrap_or_else(|e| fail(&format!("cannot keep the event log in {}: {e}", dir.display()))),
    );
    let gpu: Arc<dyn Gpu> = Arc::new(best_peer_gpu::Fake::new().with_driver("fake"));
    let runtime: Arc<dyn Runtime> = Arc::new(best_peer_runtime::Fake::new(vec![], vec![]));
    let door = Arc::new(door(&endpoint, gpu, runtime, log, auth));
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .unwrap_or_else(|e| fail(&format!("cannot listen on {listen}: {e}")));
    if door.endpoint.starts_with("http://") && !listen.ip().is_loopback() {
        tracing::warn!(endpoint = %door.endpoint, "the endpoint is cleartext and not loopback: credentials will travel unencrypted; put TLS in front before anyone but you uses it");
    }
    tracing::info!(%listen, endpoint = %door.endpoint, data = %dir.display(), "best-peer is serving its door");
    let server = axum::serve(listener, best_peer_door::router(door)).with_graceful_shutdown(async {
        tokio::signal::ctrl_c().await.ok();
    });
    // An open event stream never ends by itself, so a graceful stop would wait for it forever: a few seconds, then out.
    let deadline = async {
        tokio::signal::ctrl_c().await.ok();
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    };
    tokio::select! {
        r = server => r.unwrap_or_else(|e| fail(&format!("serving failed: {e}"))),
        _ = deadline => tracing::info!("stopping with event streams still open"),
    }
}

/// `best-peer approve <code>`: presents this machine's admin token to the peer running on it, for the code the
/// person was shown. The token goes to the listen address only, never to the public endpoint.
fn approve(code: Option<&str>) {
    let Some(code) = code else {
        fail("usage: best-peer approve <code>, the code the agent showed the person")
    };
    let dir = data_dir();
    let token = std::fs::read_to_string(dir.join("admin-token"))
        .map(|t| t.trim().to_string())
        .unwrap_or_else(|e| {
            fail(&format!(
                "no admin token in {}: is the peer running on this machine with the same BEST_PEER_DATA? ({e})",
                dir.display()
            ))
        });
    let listen = std::env::var("BEST_PEER_LISTEN").unwrap_or_else(|_| "127.0.0.1:7777".into());
    let url = format!("http://{listen}/auth/approve");
    match ureq::post(&url).send_form([("user_code", code), ("proof", token.as_str())]) {
        Ok(mut res) => {
            let admitted: serde_json::Value = res
                .body_mut()
                .read_to_string()
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let a = &admitted["admitted"];
            let who = match a["agent_label"].as_str() {
                Some(l) => format!("{l} ({})", a["client_id"].as_str().unwrap_or("?")),
                None => a["client_id"].as_str().unwrap_or("?").to_string(),
            };
            println!(
                "admitted {who} as connection {} with a {} credential: the agent's next poll gets it",
                a["connection"].as_str().unwrap_or("(the host's name)"),
                a["lifetime"].as_str().unwrap_or("durable")
            );
        }
        Err(ureq::Error::StatusCode(403)) => fail(&format!(
            "the peer refused the admin token in {}: it is not the token of the peer listening on {listen}",
            dir.display()
        )),
        Err(ureq::Error::StatusCode(status)) => fail(&format!(
            "the peer refused ({status}): is {code} a live code shown by an agent in the last ten minutes?"
        )),
        Err(e) => fail(&format!("cannot reach the peer at {url}: {e}")),
    }
}

fn fail(why: &str) -> ! {
    eprintln!("{why}");
    std::process::exit(2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Request, StatusCode};
    use best_peer_door::FakeApprover;
    use http_body_util::BodyExt;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    fn peer() -> (Arc<Door>, Arc<best_peer_runtime::Fake>) {
        let gpu = Arc::new(
            best_peer_gpu::Fake::new()
                .with_driver("fake 1.0")
                .with_device(best_peer_gpu::Fake::cuda_card("gpu-0", "Card A", 24, "12.4")),
        );
        let runtime = Arc::new(best_peer_runtime::Fake::new(vec!["gpu-0".into()], vec![]));
        runtime.registry_has(best_peer_runtime::ImageRef("ollama/ollama:0.6".into()));
        let auth = Arc::new(Auth::ephemeral(
            "http://peer.test",
            Box::new(FakeApprover { proof: "ok".into() }),
        ));
        let d = door(
            "http://peer.test",
            gpu,
            runtime.clone(),
            Arc::new(EventLog::ephemeral()),
            auth,
        );
        (Arc::new(d), runtime)
    }

    async fn body(res: axum::response::Response) -> Value {
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap()
    }

    async fn key(app: &axum::Router) -> String {
        let form = |f: &[(&str, &str)]| {
            Request::post("/auth/device")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(serde_urlencoded::to_string(f).unwrap()))
                .unwrap()
        };
        let a = body(app.clone().oneshot(form(&[("client_id", "test")])).await.unwrap()).await;
        let approve = Request::post("/auth/approve")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(
                serde_urlencoded::to_string([("user_code", a["user_code"].as_str().unwrap()), ("proof", "ok")])
                    .unwrap(),
            ))
            .unwrap();
        assert_eq!(app.clone().oneshot(approve).await.unwrap().status(), StatusCode::OK);
        let token = Request::post("/auth/token")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(
                serde_urlencoded::to_string([
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                    ("device_code", a["device_code"].as_str().unwrap()),
                    ("client_id", "test"),
                ])
                .unwrap(),
            ))
            .unwrap();
        body(app.clone().oneshot(token).await.unwrap()).await["access_token"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn the_endpoint_defaults_to_the_listen_address_only_when_a_consumer_can_reach_it() {
        let (listen, endpoint) = config(None, None).unwrap();
        assert_eq!(listen.to_string(), "127.0.0.1:7777");
        assert_eq!(endpoint, "http://127.0.0.1:7777");
        let (_, endpoint) = config(Some("0.0.0.0:7777"), Some("https://peer.example.com/")).unwrap();
        assert_eq!(endpoint, "https://peer.example.com");
        assert!(config(Some("0.0.0.0:7777"), None)
            .unwrap_err()
            .contains("BEST_PEER_ENDPOINT"));
        assert!(config(Some("[::]:7777"), None)
            .unwrap_err()
            .contains("BEST_PEER_ENDPOINT"));
        assert!(config(Some("localhost:7777"), None).unwrap_err().contains("ip:port"));
    }

    #[tokio::test]
    async fn the_hardware_state_query_answers_from_the_gpu_abstraction() {
        let (d, _) = peer();
        let app = best_peer_door::router(d);
        let k = key(&app).await;
        let res = app
            .clone()
            .oneshot(
                Request::get("/queries/get-hardware-state")
                    .header("X-Api-Key", &k)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let b = body(res).await;
        assert_eq!(b["driver"], "fake 1.0");
        assert_eq!(b["devices"][0]["id"], "gpu-0");
        assert_eq!(b["devices"][0]["compute"][0]["api"], "cuda");
        assert_eq!(b["devices"][0]["memory"]["free_bytes"], 24u64 * 1024 * 1024 * 1024);
    }

    #[tokio::test]
    async fn pull_image_runs_through_the_runtime_abstraction_and_tells_the_outcome_as_an_event() {
        let (d, _) = peer();
        let app = best_peer_door::router(d.clone());
        let k = key(&app).await;
        let send = |id: &str, image: &str| {
            Request::post("/commands")
                .header("X-Api-Key", &k)
                .header(header::CONTENT_TYPE, "application/cloudevents+json")
                .body(Body::from(
                    json!({
                        "specversion": "1.0", "id": id, "source": "https://agent.test", "type": "PullImage",
                        "datacontenttype": "application/json", "dataschema": "http://peer.test/commands/pull-image/1.0",
                        "time": "2026-10-12T10:30:00Z", "data": { "image": image }
                    })
                    .to_string(),
                ))
                .unwrap()
        };
        assert_eq!(
            app.clone()
                .oneshot(send("c1", "ollama/ollama:0.6"))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        assert_eq!(
            app.clone()
                .oneshot(send("c2", "nobody/nothing:1"))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        for _ in 0..100 {
            if d.log.len() >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let (events, _) = d.log.list(&best_peer_door::Filter::default());
        let by_corr = |c: &str| events.iter().find(|e| e.correlationid.as_deref() == Some(c)).unwrap();
        assert_eq!(by_corr("c1").r#type, "ImagePulled");
        assert_eq!(by_corr("c1").data["image"], "ollama/ollama:0.6");
        assert_eq!(by_corr("c2").r#type, "ImagePullFailed");
        assert!(by_corr("c2").data["reason"]
            .as_str()
            .unwrap()
            .contains("nobody/nothing:1"));
    }
}
