//! best-peer: one binary that puts a GPU host behind a BEST door.
//!
//! The door is built against three abstractions the workspace owns, [`best_peer_gpu::Gpu`],
//! [`best_peer_runtime::Runtime`] and [`best_peer_engine::Engine`]; which adapters stand behind them is decided here
//! and nowhere else. The host's own CPU and memory are always a device; the container engine is Docker through its
//! API, or the fakes when `BEST_PEER_RUNTIME=fake` (CI, a machine with no engine).
//!
//! `best-peer` serves; `best-peer approve <code>` admits an agent that asked, on the word of whoever controls this
//! machine.

mod domain;
mod lifecycle;
mod state;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use best_peer_door::{Auth, Door, EventLog, LocalApprover};
use best_peer_engine::Engine;
use best_peer_gpu::Gpu;
use best_peer_runtime::Runtime;
use lifecycle::{Images, Patience, Peer};

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

/// The service key in the manifest, and the `source` of every event.
const SERVICE: &str = "io.best.agents";

/// The peer over the given hardware, container engine and inference engine, its state rebuilt from the log and
/// its ears on the engines' events.
#[allow(clippy::too_many_arguments)]
pub async fn peer(
    endpoint: &str,
    engine_host: &str,
    gpu: Arc<dyn Gpu>,
    runtime: Arc<dyn Runtime>,
    engine: Arc<dyn Engine>,
    log: Arc<EventLog>,
    images: Images,
    patience: Patience,
) -> Arc<Peer> {
    // Subscribed before the rebuild, so an exit told while the state is being rebuilt is not lost.
    let events = runtime.events();
    let p = Arc::new(Peer::new(
        endpoint,
        engine_host,
        SERVICE,
        gpu,
        runtime,
        engine,
        log,
        images,
        patience,
    ));
    p.rebuild().await;
    p.follow(events);
    p
}

/// The door of this peer.
pub fn door(endpoint: &str, peer: Arc<Peer>, auth: Arc<Auth>) -> Door {
    Door::new(
        endpoint,
        SERVICE,
        "A peer: a GPU host that deploys models and runs jobs on request and tells their lifecycle as events.",
        domain::catalogue(peer.clone()),
        peer.log.clone(),
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

/// The container engine and the inference engine's client, by `BEST_PEER_RUNTIME`: `docker` (the default) talks to
/// the Docker Engine on this host and refuses to start when it cannot; `native` deploys to engines already running
/// on this host outside any container (an installed Ollama), with no jobs; `fake` serves the whole surface against
/// the in-memory fakes, for CI and for a machine with no engine.
async fn engines(bind_ip: &str, engine_host: &str) -> (Arc<dyn Runtime>, Arc<dyn Engine>) {
    match std::env::var("BEST_PEER_RUNTIME").as_deref() {
        Ok("native") => (
            Arc::new(best_peer_native::NativeRuntime::new(engine_host)),
            Arc::new(best_peer_engine_http::HttpEngine::new()),
        ),
        Ok("fake") => {
            tracing::warn!("BEST_PEER_RUNTIME=fake: no workload runs; the lifecycle is told but nothing happens");
            (
                Arc::new(best_peer_runtime::Fake::new(vec![], vec![]).with_open_registry()),
                Arc::new(best_peer_engine::Fake::new()),
            )
        }
        Ok("docker") | Err(_) => {
            let docker = best_peer_docker::DockerRuntime::connect(bind_ip)
                .await
                .unwrap_or_else(|e| {
                    fail(&format!(
                        "{e}; set BEST_PEER_RUNTIME=native to deploy to an engine already running here, or fake to serve without one"
                    ))
                });
            (Arc::new(docker), Arc::new(best_peer_engine_http::HttpEngine::new()))
        }
        Ok(other) => fail(&format!(
            "BEST_PEER_RUNTIME must be docker, native or fake, not {other:?}"
        )),
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
    let gpu: Arc<dyn Gpu> = Arc::new(best_peer_host::HostCpu::new());
    // The engines' ports are bound where the door is reached: this machine only, unless the door is open to others.
    let bind_ip = if listen.ip().is_loopback() {
        "127.0.0.1"
    } else {
        "0.0.0.0"
    };
    let engine_host = std::env::var("BEST_PEER_ENGINE_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let (runtime, engine) = engines(bind_ip, &engine_host).await;
    let images = Images {
        ollama: std::env::var("BEST_PEER_OLLAMA_IMAGE").ok(),
        vllm: std::env::var("BEST_PEER_VLLM_IMAGE").ok(),
    };
    let seconds = |name: &str, default: u64| {
        Duration::from_secs(std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default))
    };
    let patience = Patience {
        ready: seconds("BEST_PEER_READY_SECONDS", 900),
        load: seconds("BEST_PEER_LOAD_SECONDS", 3600),
        ..Patience::default()
    };
    let peer = peer(&endpoint, &engine_host, gpu, runtime, engine, log, images, patience).await;
    let door = Arc::new(door(&endpoint, peer, auth));
    // What the last run accepted and never completed is run now, before anyone can send more.
    best_peer_door::resume(&door).await;
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
        tokio::time::sleep(Duration::from_secs(3)).await;
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
    use best_peer_door::{Envelope, FakeApprover, Filter};
    use best_peer_runtime::ContainerId;
    use http_body_util::BodyExt;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    const EP: &str = "http://peer.test";
    const GB: u64 = 1024 * 1024 * 1024;

    struct Bed {
        door: Arc<Door>,
        peer: Arc<Peer>,
        gpu: Arc<best_peer_gpu::Fake>,
        runtime: Arc<best_peer_runtime::Fake>,
        engine: Arc<best_peer_engine::Fake>,
        app: axum::Router,
        key: String,
    }

    /// One 24 GiB CUDA card.
    fn card() -> Arc<best_peer_gpu::Fake> {
        Arc::new(
            best_peer_gpu::Fake::new()
                .with_driver("fake 1.0")
                .with_device(best_peer_gpu::Fake::cuda_card("gpu-0", "Card A", 24, "12.4")),
        )
    }

    /// A peer over the fakes: the hardware given, a registry that serves the engines' images, and a signed-in
    /// agent. `log` carries a previous run's log across a restart.
    async fn bed(gpu: Arc<best_peer_gpu::Fake>, log: Arc<EventLog>, runtime: Arc<best_peer_runtime::Fake>) -> Bed {
        for image in ["ollama/ollama", "vllm/vllm-openai", "busybox", "ollama/ollama:0.6"] {
            runtime.registry_has(best_peer_runtime::ImageRef(image.into()));
        }
        let engine = Arc::new(best_peer_engine::Fake::new());
        let peer = peer(
            EP,
            "engines.local",
            gpu.clone(),
            runtime.clone(),
            engine.clone(),
            log,
            Images::default(),
            Patience {
                ready: Duration::from_secs(1),
                load: Duration::from_secs(1),
                unload: Duration::from_secs(1),
            },
        )
        .await;
        let auth = Arc::new(Auth::ephemeral(EP, Box::new(FakeApprover { proof: "ok".into() })));
        let door = Arc::new(super::door(EP, peer.clone(), auth));
        best_peer_door::resume(&door).await;
        let app = best_peer_door::router(door.clone());
        let key = key(&app).await;
        Bed {
            door,
            peer,
            gpu,
            runtime,
            engine,
            app,
            key,
        }
    }

    async fn fresh() -> Bed {
        bed(
            card(),
            Arc::new(EventLog::ephemeral()),
            Arc::new(best_peer_runtime::Fake::new(vec!["gpu-0".into()], vec![])),
        )
        .await
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

    impl Bed {
        /// Sends a command and answers the status and body.
        async fn send(&self, id: &str, r#type: &str, schema: &str, data: Value) -> (StatusCode, Value) {
            let req = Request::post("/commands")
                .header("X-Api-Key", &self.key)
                .header(header::CONTENT_TYPE, "application/cloudevents+json")
                .body(Body::from(
                    json!({
                        "specversion": "1.0", "id": id, "source": "https://agent.test", "type": r#type,
                        "datacontenttype": "application/json", "dataschema": format!("{EP}/commands/{schema}/1.0"),
                        "time": "2026-10-12T10:30:00Z", "data": data
                    })
                    .to_string(),
                ))
                .unwrap();
            let res = self.app.clone().oneshot(req).await.unwrap();
            let status = res.status();
            (status, body(res).await)
        }

        async fn query(&self, schema: &str) -> Value {
            let res = self
                .app
                .clone()
                .oneshot(
                    Request::get(format!("/queries/{schema}"))
                        .header("X-Api-Key", &self.key)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            body(res).await
        }

        fn log_of(&self, correlation: &str) -> Vec<Envelope> {
            self.door
                .log
                .list(&Filter {
                    correlation_id: Some(correlation.into()),
                    ..Default::default()
                })
                .0
        }

        /// The events with `correlation`, oldest first, once one of type `until` is among them.
        async fn events_of(&self, correlation: &str, until: &str) -> Vec<Envelope> {
            for _ in 0..400 {
                let events = self.log_of(correlation);
                if events.iter().any(|e| e.r#type == until) {
                    return events;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            panic!(
                "no {until} for {correlation}; got {:?}",
                types(&self.log_of(correlation))
            );
        }

        async fn deploy(&self, id: &str, model: &str, engine: &str, gb: u64) -> (StatusCode, Value) {
            self.send(
                id,
                "DeployModel",
                "deploy-model",
                json!({ "model": model, "engine": engine, "memory_bytes": gb * GB }),
            )
            .await
        }

        /// Deploys on ollama and waits for ready; answers the deployment id.
        async fn deployed(&self, id: &str, model: &str, gb: u64) -> String {
            let (s, b) = self.deploy(id, model, "ollama", gb).await;
            assert_eq!(s, StatusCode::CREATED, "{b}");
            let events = self.events_of(id, "DeploymentReady").await;
            events.last().unwrap().data["deployment"].as_str().unwrap().to_string()
        }

        fn container_of(&self, deployment: &str) -> ContainerId {
            self.peer.state.lock().unwrap().deployments[deployment]
                .container
                .clone()
        }
    }

    fn types(events: &[Envelope]) -> Vec<String> {
        events.iter().map(|e| e.r#type.clone()).collect()
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
        let bed = fresh().await;
        let b = bed.query("get-hardware-state").await;
        assert_eq!(b["driver"], "fake 1.0");
        assert_eq!(b["devices"][0]["id"], "gpu-0");
        assert_eq!(b["devices"][0]["compute"][0]["api"], "cuda");
        assert_eq!(b["devices"][0]["memory"]["free_bytes"], 24 * GB);
    }

    #[tokio::test]
    async fn pull_image_runs_through_the_runtime_abstraction_and_tells_the_outcome_as_an_event() {
        let bed = fresh().await;
        let (s, _) = bed
            .send("c1", "PullImage", "pull-image", json!({ "image": "ollama/ollama:0.6" }))
            .await;
        assert_eq!(s, StatusCode::CREATED);
        let (s, _) = bed
            .send("c2", "PullImage", "pull-image", json!({ "image": "nobody/nothing:1" }))
            .await;
        assert_eq!(s, StatusCode::CREATED);
        let ok = bed.events_of("c1", "ImagePulled").await;
        assert_eq!(ok.last().unwrap().data["image"], "ollama/ollama:0.6");
        let failed = bed.events_of("c2", "ImagePullFailed").await;
        assert!(failed.last().unwrap().data["reason"]
            .as_str()
            .unwrap()
            .contains("nobody/nothing:1"));
    }

    #[tokio::test]
    async fn a_deploy_is_every_transition_on_the_stream_then_a_prompt_endpoint_and_an_undeploy_frees_the_device() {
        let bed = fresh().await;
        let (s, b) = bed.deploy("d1", "llama3.2:3b", "ollama", 8).await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        assert_eq!(b["correlationId"], "d1");
        let events = bed.events_of("d1", "DeploymentReady").await;
        assert_eq!(
            types(&events),
            vec![
                "ImagePulling",
                "ImagePulled",
                "DeploymentStarted",
                "WeightsLoading",
                "WeightsLoading",
                "DeploymentReady"
            ],
            "{events:#?}"
        );
        let started = &events[2].data;
        assert_eq!(started["device"], "gpu-0");
        assert_eq!(started["engine"], "ollama");
        assert_eq!(started["memory_bytes"], 8 * GB);
        assert!(started["deployment"].as_str().unwrap().starts_with("dep-"));
        assert_eq!(events[3].data["progress_pct"], 25);
        let ready = &events[5].data;
        assert_eq!(
            ready["endpoint"], "http://peer.test:41434/v1",
            "the host of the door, the port the engine published"
        );
        assert_eq!(ready["deployment"], started["deployment"]);
        assert!(events.iter().all(|e| e.dataschema.as_deref().unwrap().starts_with(EP)));
        // The container runs the recipe on the device, and the engine loaded the model at the base the event names.
        let container = ContainerId(started["container"].as_str().unwrap().into());
        let spec = bed.runtime.spec_of(&container).unwrap();
        assert_eq!(spec.image.0, "ollama/ollama");
        assert_eq!(spec.devices, vec!["gpu-0".into()]);
        assert_eq!(spec.ports, vec![11434]);
        assert_eq!(
            bed.engine.loaded_at("http://engines.local:41434"),
            vec!["llama3.2:3b"],
            "the peer reaches the engine where its port is bound, not at the door's public host"
        );
        let w = bed.query("get-workloads").await;
        assert_eq!(w["deployments"][0]["phase"], "ready");
        assert_eq!(w["deployments"][0]["endpoint"], "http://peer.test:41434/v1");

        let dep = started["deployment"].as_str().unwrap();
        let (s, b) = bed
            .send("u1", "UndeployModel", "undeploy-model", json!({ "deployment": dep }))
            .await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let events = bed.events_of("u1", "Undeployed").await;
        assert_eq!(types(&events), vec!["Undeployed"]);
        assert_eq!(events[0].data["model"], "llama3.2:3b");
        assert_eq!(
            bed.engine.unloaded_at("http://engines.local:41434"),
            vec!["llama3.2:3b"],
            "the engine is asked to free the model before its container goes"
        );
        assert!(bed.runtime.running().await.unwrap().is_empty());
        assert!(bed.query("get-workloads").await["deployments"]
            .as_array()
            .unwrap()
            .is_empty());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            types(&bed.log_of("d1")).last().unwrap(),
            "DeploymentReady",
            "the container's exit on undeploy is not an ending of its own"
        );
        // The memory is free again: a deploy of most of the card fits now.
        assert_eq!(
            bed.deploy("d2", "llama3.2:3b", "ollama", 20).await.0,
            StatusCode::CREATED
        );
        // And a deployment that is gone is refused by name.
        let (s, b) = bed
            .send("u2", "UndeployModel", "undeploy-model", json!({ "deployment": dep }))
            .await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_eq!(b["error"]["code"], "NO_SUCH_DEPLOYMENT");
        assert!(b["error"]["message"].as_str().unwrap().contains("get-workloads"));
    }

    #[tokio::test]
    async fn a_deploy_the_device_cannot_hold_is_refused_with_what_holds_the_memory_and_what_to_do() {
        let bed = fresh().await;
        let dep = bed.deployed("d1", "big:70b", 20).await;
        // 4 GiB left of 24: the driver still says 24 free (the fake never moves it); the promise is what counts.
        let (s, b) = bed.deploy("d2", "small:3b", "ollama", 8).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "INSUFFICIENT_MEMORY");
        let msg = b["error"]["message"].as_str().unwrap();
        assert!(msg.contains("UndeployModel"), "{msg}");
        assert!(msg.contains("gpu-0 (Card A)"), "{msg}");
        let device = &b["error"]["details"]["devices"][0];
        assert_eq!(device["promised_bytes"], 20 * GB);
        assert_eq!(device["room_bytes"], 4 * GB);
        assert_eq!(device["holders"][0]["deployment"], dep);
        assert!(bed.log_of("d2").is_empty(), "a refused deploy told nothing");
        // The driver's free memory counts too: when it says less than the promises leave, that is the room.
        bed.gpu.set_free(&"gpu-0".into(), 2 * GB).unwrap();
        let (s, b) = bed.deploy("d3", "tiny:1b", "ollama", 3).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["details"]["devices"][0]["room_bytes"], 2 * GB);
        assert_eq!(bed.deploy("d4", "tiny:1b", "ollama", 2).await.0, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn an_engine_no_device_can_run_and_a_device_that_is_not_here_are_refused_by_name() {
        let cpu = best_peer_gpu::Device {
            compute: vec![best_peer_gpu::Compute::Cpu],
            ..best_peer_gpu::Fake::cuda_card("cpu-0", "Host", 64, "0")
        };
        let bed = bed(
            Arc::new(best_peer_gpu::Fake::new().with_device(cpu)),
            Arc::new(EventLog::ephemeral()),
            Arc::new(best_peer_runtime::Fake::new(vec!["cpu-0".into()], vec![])),
        )
        .await;
        // vllm runs on cuda only; a host with only a cpu device cannot run it.
        let (s, b) = bed.deploy("v1", "Qwen/Qwen2.5-0.5B", "vllm", 1).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "NO_DEVICE_FOR_ENGINE");
        assert_eq!(b["error"]["details"]["wanted"][0], "cuda");
        // ollama runs on cpu: placed there, with no device handed to the container.
        let (s, b) = bed.deploy("o1", "llama3.2:3b", "ollama", 1).await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let events = bed.events_of("o1", "DeploymentReady").await;
        assert_eq!(events[2].data["device"], "cpu-0");
        let container = ContainerId(events[2].data["container"].as_str().unwrap().into());
        assert!(bed.runtime.spec_of(&container).unwrap().devices.is_empty());
        let (s, b) = bed
            .send(
                "p1",
                "DeployModel",
                "deploy-model",
                json!({ "model": "m", "engine": "ollama", "memory_bytes": 1, "device": "gpu-9" }),
            )
            .await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "NO_SUCH_DEVICE");
        assert_eq!(b["error"]["details"]["devices"][0], "cpu-0");
    }

    #[tokio::test]
    async fn a_deploy_that_fails_tells_why_and_what_to_do_and_leaves_nothing_running() {
        let bed = fresh().await;
        bed.engine.missing("nobody/none:1b");
        let (s, _) = bed.deploy("f1", "nobody/none:1b", "ollama", 1).await;
        assert_eq!(s, StatusCode::CREATED);
        let events = bed.events_of("f1", "DeploymentFailed").await;
        assert_eq!(
            types(&events),
            vec!["ImagePulling", "ImagePulled", "DeploymentStarted", "DeploymentFailed"]
        );
        let failed = &events[3].data;
        assert!(failed["reason"].as_str().unwrap().contains("nobody/none:1b"));
        assert!(failed["fix"].as_str().unwrap().contains("check the name"));
        assert!(
            bed.runtime.running().await.unwrap().is_empty(),
            "the container is stopped"
        );
        assert!(bed.query("get-workloads").await["deployments"]
            .as_array()
            .unwrap()
            .is_empty());
        // The memory it was promised is free again: the whole card can be asked for.
        let dep = bed.deployed("f2", "llama3.2:3b", 24).await;
        let (s, _) = bed
            .send("u", "UndeployModel", "undeploy-model", json!({ "deployment": dep }))
            .await;
        assert_eq!(s, StatusCode::CREATED);
        bed.events_of("u", "Undeployed").await;
        // An engine that never comes up is a failure that says so.
        bed.engine.down("http://engines.local:41434");
        let (s, _) = bed.deploy("f3", "llama3.2:3b", "ollama", 1).await;
        assert_eq!(s, StatusCode::CREATED);
        let events = bed.events_of("f3", "DeploymentFailed").await;
        assert!(events.last().unwrap().data["reason"]
            .as_str()
            .unwrap()
            .contains("did not answer"));
        assert!(bed.runtime.running().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn what_the_engine_does_on_its_own_is_told_under_the_deploys_correlation() {
        let bed = fresh().await;
        let dep = bed.deployed("d1", "llama3.2:3b", 8).await;
        let dep2 = bed.deployed("d2", "llama3.2:3b", 8).await;
        bed.runtime.out_of_memory(&bed.container_of(&dep)).unwrap();
        let events = bed.events_of("d1", "OutOfMemory").await;
        let oom = &events.last().unwrap().data;
        assert_eq!(oom["deployment"], dep);
        assert_eq!(oom["device"], "gpu-0");
        assert!(oom["fix"].as_str().unwrap().contains("memory_bytes"));
        // Failed once it was ready: the deploy completed with DeploymentReady, so this is the deployment's end.
        bed.runtime.fail(&bed.container_of(&dep2), "exit 137").unwrap();
        let events = bed.events_of("d2", "DeploymentEnded").await;
        assert_eq!(events.last().unwrap().data["reason"], "exit 137");
        assert!(!events.iter().any(|e| e.r#type == "DeploymentFailed"));
        assert!(bed.query("get-workloads").await["deployments"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(
            bed.deploy("d3", "llama3.2:3b", "ollama", 24).await.0,
            StatusCode::CREATED,
            "all memory is free again"
        );
    }

    #[tokio::test]
    async fn a_job_runs_once_and_its_end_is_told_and_a_stop_ends_it_early() {
        let bed = fresh().await;
        let (s, b) = bed
            .send(
                "j1",
                "RunJob",
                "run-job",
                json!({ "image": "busybox", "args": ["echo", "hi"], "memory_bytes": 4 * GB }),
            )
            .await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let events = bed.events_of("j1", "JobStarted").await;
        assert_eq!(types(&events), vec!["ImagePulling", "ImagePulled", "JobStarted"]);
        let started = &events[2].data;
        assert_eq!(started["device"], "gpu-0");
        let job = started["job"].as_str().unwrap().to_string();
        assert!(job.starts_with("job-"));
        let container = ContainerId(started["container"].as_str().unwrap().into());
        assert_eq!(bed.runtime.spec_of(&container).unwrap().args, vec!["echo", "hi"]);
        assert_eq!(bed.query("get-workloads").await["jobs"][0]["phase"], "running");
        // The engine ends it: the exit is told, and the memory is free.
        bed.runtime.stop(&container).await.unwrap();
        let events = bed.events_of("j1", "JobFinished").await;
        assert_eq!(events.last().unwrap().data["exit_code"], 0);
        assert!(bed.query("get-workloads").await["jobs"].as_array().unwrap().is_empty());
        let (s, b) = bed.send("s0", "StopJob", "stop-job", json!({ "job": job })).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "NO_SUCH_JOB");

        // A job with no memory asked runs with no device, and a stop ends it on request: JobStopped, not JobFinished.
        let (s, _) = bed
            .send(
                "j2",
                "RunJob",
                "run-job",
                json!({ "image": "busybox", "args": ["sleep", "100"] }),
            )
            .await;
        assert_eq!(s, StatusCode::CREATED);
        let events = bed.events_of("j2", "JobStarted").await;
        let started = &events.last().unwrap().data;
        assert!(started["device"].is_null());
        let container = ContainerId(started["container"].as_str().unwrap().into());
        assert!(bed.runtime.spec_of(&container).unwrap().devices.is_empty());
        let job = started["job"].as_str().unwrap();
        let (s, _) = bed.send("s1", "StopJob", "stop-job", json!({ "job": job })).await;
        assert_eq!(s, StatusCode::CREATED);
        let events = bed.events_of("s1", "JobStopped").await;
        assert_eq!(types(&events), vec!["JobStopped"]);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            types(&bed.log_of("j2")),
            vec!["ImagePulling", "ImagePulled", "JobStarted"],
            "no JobFinished for a stop"
        );
        // A device without the memory to place it by is refused, not ignored.
        let (s, b) = bed
            .send(
                "j4",
                "RunJob",
                "run-job",
                json!({ "image": "busybox", "device": "gpu-0" }),
            )
            .await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "DEVICE_NEEDS_MEMORY");
        assert_eq!(b["error"]["details"]["field"], "memory_bytes");
        // A missing image fails the job with the fix.
        let (s, _) = bed
            .send("j3", "RunJob", "run-job", json!({ "image": "nobody/nothing:1" }))
            .await;
        assert_eq!(s, StatusCode::CREATED);
        let events = bed.events_of("j3", "JobFailed").await;
        assert!(events.last().unwrap().data["fix"]
            .as_str()
            .unwrap()
            .contains("registry"));
    }

    #[tokio::test]
    async fn after_a_restart_the_state_is_the_logs_and_what_died_meanwhile_is_told() {
        let log = Arc::new(EventLog::ephemeral());
        let runtime = Arc::new(best_peer_runtime::Fake::new(vec!["gpu-0".into()], vec![]));
        let first = bed(card(), log.clone(), runtime.clone()).await;
        let kept = first.deployed("d1", "kept:1b", 4).await;
        let lost = first.deployed("d2", "lost:1b", 4).await;
        let (s, _) = first
            .send(
                "j1",
                "RunJob",
                "run-job",
                json!({ "image": "busybox", "memory_bytes": GB }),
            )
            .await;
        assert_eq!(s, StatusCode::CREATED);
        let job_container = ContainerId(
            first.events_of("j1", "JobStarted").await.last().unwrap().data["container"]
                .as_str()
                .unwrap()
                .into(),
        );
        let lost_container = first.container_of(&lost);
        // A job that keeps running across the restart: its command is complete, and must not be run again.
        let (s, _) = first
            .send(
                "j2",
                "RunJob",
                "run-job",
                json!({ "image": "busybox", "args": ["sleep", "1000"] }),
            )
            .await;
        assert_eq!(s, StatusCode::CREATED);
        first.events_of("j2", "JobStarted").await;
        // The peer dies. While it is down the engine loses one deployment and the job, and nobody hears.
        first.peer.halt();
        drop(first);
        runtime.stop(&lost_container).await.unwrap();
        runtime.stop(&job_container).await.unwrap();
        // A new peer over the same log and the same engine.
        let again = bed(card(), log.clone(), runtime.clone()).await;
        let w = again.query("get-workloads").await;
        assert_eq!(w["deployments"].as_array().unwrap().len(), 1, "{w}");
        assert_eq!(w["deployments"][0]["deployment"], kept);
        assert_eq!(w["deployments"][0]["phase"], "ready");
        assert_eq!(w["jobs"].as_array().unwrap().len(), 1, "{w}");
        assert_eq!(w["jobs"][0]["phase"], "running");
        assert_eq!(
            types(&again.log_of("j2")).iter().filter(|t| *t == "JobStarted").count(),
            1,
            "the running job was not started a second time"
        );
        let ended = again.events_of("d2", "DeploymentEnded").await;
        assert!(ended.last().unwrap().data["reason"]
            .as_str()
            .unwrap()
            .contains("not observed"));
        assert_eq!(
            again.engine.unloaded_at("http://engines.local:41434"),
            vec!["lost:1b"],
            "an engine outside any container is asked to let the lost model go"
        );
        let failed = again.events_of("j1", "JobFailed").await;
        assert!(failed.last().unwrap().data["reason"]
            .as_str()
            .unwrap()
            .contains("not observed"));
        // A deploy accepted just before the crash, never run: it runs now, under its own correlation.
        let owed_log = log.clone();
        let envelope: Envelope = serde_json::from_value(json!({
            "specversion": "1.0", "id": "d9", "source": "https://agent.test", "type": "DeployModel",
            "datacontenttype": "application/json", "dataschema": format!("{EP}/commands/deploy-model/1.0"),
            "time": "2026-10-12T10:30:00Z", "data": { "model": "owed:1b", "engine": "ollama", "memory_bytes": GB }
        }))
        .unwrap();
        owed_log.accept_command(
            "some-sender",
            "d9",
            &best_peer_door::payload_hash(&envelope),
            "d9",
            &envelope,
        );
        again.peer.halt();
        drop(again);
        let third = bed(card(), log.clone(), runtime.clone()).await;
        let ready = third.events_of("d9", "DeploymentReady").await;
        assert_eq!(ready.last().unwrap().data["model"], "owed:1b");
        assert!(
            third.door.log.unfinished().is_empty(),
            "nothing is owed once it completed"
        );
        let again = third;
        // The kept deployment still holds its memory, and can still be undeployed.
        let (s, b) = again.deploy("d3", "more:1b", "ollama", 21).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(
            b["error"]["details"]["devices"][0]["promised_bytes"],
            5 * GB,
            "the kept deployment and the one owed after the restart"
        );
        let (s, _) = again
            .send("u1", "UndeployModel", "undeploy-model", json!({ "deployment": kept }))
            .await;
        assert_eq!(s, StatusCode::CREATED);
        again.events_of("u1", "Undeployed").await;
        let still = runtime.running().await.unwrap();
        assert_eq!(
            still.len(),
            2,
            "the deployment owed after the restart and the running job"
        );
        assert_eq!(again.query("get-workloads").await["deployments"][0]["model"], "owed:1b");
    }
}
