//! best-peer: one binary that puts a GPU host behind a BEST door.
//!
//! The door is built against three abstractions the workspace owns, [`best_peer_gpu::Gpu`],
//! [`best_peer_runtime::Runtime`] and [`best_peer_engine::Engine`]; which adapters stand behind them is decided here
//! and nowhere else. The host's own CPU and memory are always a device; the container engine is Docker through its
//! API, or the fakes when `BEST_PEER_RUNTIME=fake` (CI, a machine with no engine).
//!
//! `best-peer` serves; `best-peer approve <code>` admits an agent that asked, on the word of whoever controls this
//! machine. With `BEST_PEER_MEMBERS` the same binary is a cloud door: it fronts the peers named and places on them.

mod cloud;
mod domain;
mod endpoint;
mod lifecycle;
mod member;
mod state;
mod tls;
mod transport;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use best_peer_door::{Auth, Door, EventLog, LocalApprover};
use best_peer_engine::Engine;
use best_peer_gpu::Gpu;
use best_peer_runtime::Runtime;
use lifecycle::{Images, Patience, Peer};
use tls::Tls;

/// Where to listen, what to advertise and whether TLS serves it, from `BEST_PEER_LISTEN` (ip:port, default
/// loopback), `BEST_PEER_ENDPOINT` (the consumer-facing base URL) and `BEST_PEER_TLS_CERT` with `BEST_PEER_TLS_KEY`
/// (the operator's certificate; an `https://` endpoint without them is served with one the peer makes). The endpoint
/// defaults to the listen address only when that address is one a consumer can use: a peer listening on every
/// interface has no address of its own to advertise, so it refuses to start until told what it is called.
pub fn config(
    listen: Option<&str>,
    endpoint: Option<&str>,
    cert: Option<&str>,
    key: Option<&str>,
) -> Result<(SocketAddr, String, Tls), String> {
    let listen_text = listen.unwrap_or("127.0.0.1:7777");
    let listen: SocketAddr = listen_text
        .parse()
        .map_err(|_| format!("BEST_PEER_LISTEN must be ip:port, not {listen_text:?}"))?;
    let endpoint = match endpoint {
        Some(e) if e.starts_with("http://") || e.starts_with("https://") => e.trim_end_matches('/').to_string(),
        Some(e) => {
            return Err(format!(
                "BEST_PEER_ENDPOINT must be a base URL, http:// or https://, not {e:?}"
            ))
        }
        None if listen.ip().is_unspecified() => {
            return Err(format!(
                "BEST_PEER_LISTEN is {listen}, an address no consumer can reach: set BEST_PEER_ENDPOINT to the URL this peer is reached at"
            ))
        }
        None if cert.is_some() => format!("https://{listen}"),
        None => format!("http://{listen}"),
    };
    let tls = Tls::decide(&endpoint, cert, key)?;
    Ok((listen, endpoint, tls))
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

/// The door of a cloud: the same surface, placed across the members.
pub fn cloud_door(endpoint: &str, cloud: Arc<cloud::Cloud>, auth: Arc<Auth>) -> Door {
    Door::new(
        endpoint,
        SERVICE,
        "A cloud: a door that deploys models and runs jobs on the GPU peers behind it, placing each where there is room, and tells their lifecycle as events with the peer named.",
        domain::cloud_catalogue(cloud.clone()),
        cloud.log.clone(),
        auth,
    )
}

/// The cloud over the members `BEST_PEER_MEMBERS` names, reached through HTTP with the trust `BEST_PEER_TRUST`
/// adds (the PEM files of members' own certificates), its members' credentials kept in the data directory.
async fn cloud_app(
    members: &str,
    endpoint: &str,
    dir: &std::path::Path,
    log: Arc<EventLog>,
    auth: Arc<Auth>,
) -> axum::Router {
    let list = cloud::parse_members(members).unwrap_or_else(|why| fail(&why));
    let trust: Vec<PathBuf> = std::env::var("BEST_PEER_TRUST")
        .ok()
        .map(|t| {
            t.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(PathBuf::from)
                .collect()
        })
        .unwrap_or_default();
    let transport: Arc<dyn transport::Transport> =
        Arc::new(transport::Http::new(&trust).unwrap_or_else(|why| fail(&why)));
    let members = list
        .iter()
        .map(|(name, member_endpoint)| Arc::new(member::Member::new(name, member_endpoint, None, transport.clone())))
        .collect();
    let cloud = Arc::new(cloud::Cloud::new(
        endpoint,
        SERVICE,
        log,
        Arc::new(domain::cloud_event_dataschemas(endpoint)),
        Arc::new(domain::completions()),
        members,
        Some(dir.join("members.json")),
    ));
    cloud.rebuild();
    let door = Arc::new(cloud_door(endpoint, cloud.clone(), auth.clone()));
    best_peer_door::resume(&door).await;
    cloud.follow();
    tracing::info!(members = %list.iter().map(|(n, e)| format!("{n}={e}")).collect::<Vec<_>>().join(","), "this peer is a cloud door over its members");
    best_peer_door::router(door).merge(endpoint::router(cloud, auth))
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None => serve(),
        Some("approve") => approve(&args[1..]),
        Some("--version" | "-V" | "version") => println!("best-peer {}", env!("CARGO_PKG_VERSION")),
        Some(other) => {
            eprintln!(
                "unknown command {other:?}: run with no arguments to serve, `best-peer approve <code>`, or `best-peer --version`"
            );
            std::process::exit(2);
        }
    }
}

/// Whether a host name or address is this machine's loopback.
fn is_loopback(host: &str) -> bool {
    host == "localhost" || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// The hardware the peer places workloads on, by `BEST_PEER_GPU`: `nvml` (the default) reads the NVIDIA cards
/// through the driver's own library and refuses to start without it, since a GPU host that cannot see its cards is
/// misconfigured; `none` says this host has no accelerator, and the CPU with the system memory is the one device.
/// With cards, the host CPU is listed after them, so a CPU deploy is placed there too.
fn accelerator(kind: Option<&str>) -> Result<Arc<dyn Gpu>, String> {
    let host = best_peer_host::HostCpu::new();
    match kind {
        None | Some("nvml") => {
            let cards = best_peer_nvml::NvmlGpu::init().map_err(|e| match e {
                best_peer_gpu::GpuError::NoDriver(_) => {
                    format!("{e}; this peer needs the NVIDIA driver, or BEST_PEER_GPU=none to serve on the CPU alone")
                }
                other => format!(
                    "the NVIDIA driver is here but could not be used: {other}; fix that, or BEST_PEER_GPU=none serves on the CPU alone"
                ),
            })?;
            Ok(Arc::new(best_peer_gpu::Combined::new(vec![
                Box::new(cards),
                Box::new(host),
            ])))
        }
        Some("none") => Ok(Arc::new(host)),
        Some(other) => Err(format!("BEST_PEER_GPU must be nvml or none, not {other:?}")),
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
    let (listen, endpoint, tls) = match config(
        std::env::var("BEST_PEER_LISTEN").ok().as_deref(),
        std::env::var("BEST_PEER_ENDPOINT").ok().as_deref(),
        std::env::var("BEST_PEER_TLS_CERT").ok().as_deref(),
        std::env::var("BEST_PEER_TLS_KEY").ok().as_deref(),
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
            .map(|a| a.with_scopes(domain::SCOPES))
            .unwrap_or_else(|e| fail(&format!("cannot keep credentials in {}: {e}", dir.display()))),
    );
    let log = Arc::new(
        EventLog::open(&dir).unwrap_or_else(|e| fail(&format!("cannot keep the event log in {}: {e}", dir.display()))),
    );
    let app = match std::env::var("BEST_PEER_MEMBERS").ok() {
        Some(members) => cloud_app(&members, &endpoint, &dir, log, auth).await,
        None => peer_app(&endpoint, log, auth).await,
    };
    let serving = tls::serving(&tls, &dir, &tls::names_for(&endpoint, &listen)).unwrap_or_else(|why| fail(&why));
    let listener = std::net::TcpListener::bind(listen)
        .and_then(|l| l.set_nonblocking(true).map(|()| l))
        .unwrap_or_else(|e| fail(&format!("cannot listen on {listen}: {e}")));
    // Where the door is and what it serves, for `best-peer approve` on this machine.
    tls::Record {
        listen,
        endpoint: endpoint.clone(),
        certificate: serving.as_ref().map(|s| s.certificate.clone()),
    }
    .write(&dir)
    .unwrap_or_else(|e| fail(&format!("cannot record the door in {}: {e}", dir.display())));
    match &serving {
        Some(s) if s.made => {
            tracing::info!(certificate = %s.file.display(), "https with a certificate this peer made at this start: a client that pins it reads this file; one that pinned an earlier file reads it again")
        }
        Some(s) if tls == Tls::Own => {
            tracing::info!(certificate = %s.file.display(), "https with a certificate this peer made: a client that pins it reads this file")
        }
        Some(s) => tracing::info!(certificate = %s.file.display(), "https with the operator's certificate"),
        None if !listen.ip().is_loopback() => {
            tracing::warn!(%endpoint, "the endpoint is cleartext and not loopback: credentials will travel unencrypted; make BEST_PEER_ENDPOINT https:// before anyone but you uses it")
        }
        None => {}
    }
    tracing::info!(%listen, %endpoint, data = %dir.display(), "best-peer is serving its door");
    let handle = axum_server::Handle::new();
    // An open event stream never ends by itself, so a graceful stop would wait for it forever: a few seconds, then out.
    let stop = handle.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("stopping: open connections have three seconds");
        stop.graceful_shutdown(Some(Duration::from_secs(3)));
    });
    serve_on(listener, app, serving.map(|s| s.config), handle)
        .await
        .unwrap_or_else(|e| fail(&format!("serving failed: {e}")));
}

/// The peer over this host's hardware and engines, its door and its endpoints.
async fn peer_app(endpoint: &str, log: Arc<EventLog>, auth: Arc<Auth>) -> axum::Router {
    let gpu = accelerator(std::env::var("BEST_PEER_GPU").ok().as_deref()).unwrap_or_else(|why| fail(&why));
    // The engines' ports are bound where the peer reaches them, whatever the door listens on: a prompt reaches an
    // engine through the door, with the door's credential. On this machine that is loopback; a container engine on
    // another host (BEST_PEER_ENGINE_HOST not loopback) publishes them on that host's interfaces, as loopback there
    // is not here.
    let engine_host = std::env::var("BEST_PEER_ENGINE_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let bind_ip = if is_loopback(&engine_host) {
        "127.0.0.1"
    } else {
        "0.0.0.0"
    };
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
    let peer = peer(endpoint, &engine_host, gpu, runtime, engine, log, images, patience).await;
    let door = Arc::new(door(endpoint, peer.clone(), auth.clone()));
    // What the last run accepted and never completed is run now, before anyone can send more.
    best_peer_door::resume(&door).await;
    best_peer_door::router(door).merge(endpoint::router(peer, auth))
}

/// Serves `app` on `listener`, over TLS when a configuration is given, until `handle` says to stop.
async fn serve_on(
    listener: std::net::TcpListener,
    app: axum::Router,
    tls: Option<Arc<rustls::ServerConfig>>,
    handle: axum_server::Handle<SocketAddr>,
) -> std::io::Result<()> {
    let service = app.into_make_service();
    match tls {
        Some(config) => {
            let config = axum_server::tls_rustls::RustlsConfig::from_config(config);
            axum_server::from_tcp_rustls(listener, config)?
                .handle(handle)
                .serve(service)
                .await
        }
        None => axum_server::from_tcp(listener)?.handle(handle).serve(service).await,
    }
}

/// An amount of memory as a person writes it: `40G`, `512M`, `1T`, `40GB`, or bytes.
fn parse_memory(text: &str) -> Result<u64, String> {
    let t = text.trim().to_ascii_uppercase();
    let t = t.strip_suffix('B').unwrap_or(&t);
    let (digits, unit) = match t.chars().last() {
        Some(c) if c.is_ascii_alphabetic() => (&t[..t.len() - 1], Some(c)),
        _ => (t, None),
    };
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("{text:?} is not an amount of memory: write 40G, 512M, 1T or a number of bytes"))?;
    let factor: u64 = match unit {
        None => 1,
        Some('K') => 1 << 10,
        Some('M') => 1 << 20,
        Some('G') => 1 << 30,
        Some('T') => 1 << 40,
        Some(u) => return Err(format!("{u} is not a unit: K, M, G or T")),
    };
    n.checked_mul(factor)
        .ok_or_else(|| format!("{text:?} is more memory than exists"))
}

/// `best-peer approve <code>`: presents this machine's admin token to the peer running on it, for the code the
/// person was shown. The token goes to the listen address only, never to the public endpoint, and over TLS to the
/// certificate the peer serves and no other, when it serves one.
fn approve(args: &[String]) {
    const USAGE: &str = "usage: best-peer approve <code> [--may read,deploy,jobs | all] [--memory 40G]: the code the agent showed the person; what the credential may do (all when not said); the most device memory its workloads may hold together (no share when not said)";
    let Some(code) = args.first() else { fail(USAGE) };
    let (mut may, mut memory) = (None::<String>, None::<u64>);
    let mut rest = args[1..].iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--may" => {
                may = Some(
                    rest.next()
                        .cloned()
                        .unwrap_or_else(|| fail("--may needs the scopes, comma-separated, or all")),
                )
            }
            "--memory" => {
                let amount = rest
                    .next()
                    .unwrap_or_else(|| fail("--memory needs an amount, such as 40G, 512M or a number of bytes"));
                memory = Some(parse_memory(amount).unwrap_or_else(|why| fail(&why)));
            }
            other => fail(&format!("unknown argument {other:?}. {USAGE}")),
        }
    }
    let dir = data_dir();
    let token = std::fs::read_to_string(dir.join("admin-token"))
        .map(|t| t.trim().to_string())
        .unwrap_or_else(|e| {
            fail(&format!(
                "no admin token in {}: is the peer running on this machine with the same BEST_PEER_DATA? ({e})",
                dir.display()
            ))
        });
    // Where the running peer is and what it serves, as it recorded at start.
    let record = tls::Record::read(&dir).unwrap_or_else(|e| {
        fail(&format!(
            "no record of a running peer in {}: is the peer running on this machine with the same BEST_PEER_DATA? ({e})",
            dir.display()
        ))
    });
    let addr = record.dial();
    let mut form: Vec<(&str, String)> = vec![("user_code", code.clone()), ("proof", token)];
    if let Some(m) = &may {
        form.push(("may", m.clone()));
    }
    if let Some(b) = memory {
        form.push(("memory_bytes", b.to_string()));
    }
    let body = serde_urlencoded::to_string(&form).unwrap_or_else(|e| fail(&format!("cannot encode the request: {e}")));
    let req = axum::http::Request::post("/auth/approve")
        .header(axum::http::header::HOST, addr.to_string())
        .header(axum::http::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|e| fail(&format!("cannot make the request: {e}")));
    // The door's answer is read whatever its status: a refusal says why, in its body, and that is what the person
    // at the machine needs to see (a scope the door does not have, a code that expired).
    let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|e| fail(&format!("cannot start: {e}")));
    let (status, answer) = runtime
        .block_on(tls::call(addr, record.certificate.as_deref(), req))
        .unwrap_or_else(|why| fail(&why));
    match status.as_u16() {
        200 => {}
        403 => fail(&format!(
            "the peer refused the admin token in {}: it is not the token of the peer listening on {}",
            dir.display(),
            record.listen
        )),
        status => fail(&format!(
            "the peer refused ({status}): {}",
            answer["error_description"]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| format!("is {code} a live code shown by an agent in the last ten minutes?"))
        )),
    }
    let a = &answer["admitted"];
    let who = match a["agent_label"].as_str() {
        Some(l) => format!("{l} ({})", a["client_id"].as_str().unwrap_or("?")),
        None => a["client_id"].as_str().unwrap_or("?").to_string(),
    };
    let may = match a["grant"]["scopes"].as_array() {
        Some(s) => s.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>().join(","),
        None => "all".into(),
    };
    let share = match a["grant"]["memory_bytes"].as_u64() {
        Some(b) => format!("{b} bytes of device memory at most"),
        None => "no share set".into(),
    };
    println!(
        "admitted {who} as connection {} with a {} credential that may {may}, {share}: the agent's next poll gets it",
        a["connection"].as_str().unwrap_or("(the host's name)"),
        a["lifetime"].as_str().unwrap_or("durable")
    );
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

    #[test]
    fn the_hardware_switch_names_its_values_and_none_is_the_host_alone() {
        let host = accelerator(Some("none")).unwrap().snapshot().unwrap();
        assert_eq!(host.devices.len(), 1);
        assert_eq!(host.devices[0].id.0, "cpu-0");
        assert!(host.driver.is_none());
        match accelerator(Some("rocm")) {
            Err(why) => assert!(why.contains("nvml or none"), "{why}"),
            Ok(_) => panic!("an unknown value was accepted"),
        }
    }
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
        bed_at(gpu, log, runtime, "engines.local").await
    }

    /// The same, with the engines reached at `engine_host`: `127.0.0.1` for a test that stands in for one.
    async fn bed_at(
        gpu: Arc<best_peer_gpu::Fake>,
        log: Arc<EventLog>,
        runtime: Arc<best_peer_runtime::Fake>,
        engine_host: &str,
    ) -> Bed {
        bed_with(
            gpu,
            log,
            runtime,
            Arc::new(best_peer_engine::Fake::new()),
            engine_host,
            None,
        )
        .await
    }

    /// The same, with the inference engine given too, and the credentials of a previous run when this bed is a
    /// restart: on a host `Auth::open` reads them back from disk.
    async fn bed_with(
        gpu: Arc<best_peer_gpu::Fake>,
        log: Arc<EventLog>,
        runtime: Arc<best_peer_runtime::Fake>,
        engine: Arc<best_peer_engine::Fake>,
        engine_host: &str,
        auth: Option<Arc<Auth>>,
    ) -> Bed {
        for image in ["ollama/ollama", "vllm/vllm-openai", "busybox", "ollama/ollama:0.6"] {
            runtime.registry_has(best_peer_runtime::ImageRef(image.into()));
        }
        let peer = peer(
            EP,
            engine_host,
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
        let auth = auth.unwrap_or_else(|| {
            Arc::new(Auth::ephemeral(EP, Box::new(FakeApprover { proof: "ok".into() })).with_scopes(domain::SCOPES))
        });
        let door = Arc::new(super::door(EP, peer.clone(), auth.clone()));
        best_peer_door::resume(&door).await;
        let app = best_peer_door::router(door.clone()).merge(endpoint::router(peer.clone(), auth));
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
        key_with(app, None, None).await
    }

    /// A credential approved with `may` and `memory`, as `best-peer approve --may --memory` would.
    async fn key_with(app: &axum::Router, may: Option<&str>, memory: Option<u64>) -> String {
        let form = |f: &[(&str, &str)]| {
            Request::post("/auth/device")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(serde_urlencoded::to_string(f).unwrap()))
                .unwrap()
        };
        let a = body(app.clone().oneshot(form(&[("client_id", "test")])).await.unwrap()).await;
        let memory = memory.map(|m| m.to_string());
        let mut fields = vec![("user_code", a["user_code"].as_str().unwrap()), ("proof", "ok")];
        if let Some(m) = may {
            fields.push(("may", m));
        }
        if let Some(m) = &memory {
            fields.push(("memory_bytes", m));
        }
        let approve = Request::post("/auth/approve")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(serde_urlencoded::to_string(fields).unwrap()))
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

    // ----- the cloud door --------------------------------------------------------------------------------------------

    const CLOUD_EP: &str = "http://cloud.test";

    /// A member's door reached in-process, that can be made to stop answering.
    struct Flaky {
        inner: transport::InProcess,
        down: Arc<std::sync::atomic::AtomicBool>,
    }

    impl transport::Transport for Flaky {
        fn call(
            &self,
            req: Request<Body>,
        ) -> futures::future::BoxFuture<'static, Result<axum::response::Response, String>> {
            if self.down.load(std::sync::atomic::Ordering::SeqCst) {
                return Box::pin(async { Err("connection refused".to_string()) });
            }
            self.inner.call(req)
        }
    }

    struct CloudBed {
        cloud: Arc<cloud::Cloud>,
        app: axum::Router,
        key: String,
        members: Vec<Bed>,
        down: Vec<Arc<std::sync::atomic::AtomicBool>>,
    }

    /// A cloud over member peers in this process, each having admitted the cloud (its key is the one the member
    /// bed signed in with), and an agent signed in to the cloud.
    async fn cloud_bed(members: Vec<(&str, Bed)>) -> CloudBed {
        let mut down = Vec::new();
        let mut list = Vec::new();
        let mut beds = Vec::new();
        for (name, bed) in members {
            let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let transport: Arc<dyn transport::Transport> = Arc::new(Flaky {
                inner: transport::InProcess {
                    router: bed.app.clone(),
                },
                down: flag.clone(),
            });
            list.push(Arc::new(member::Member::new(
                name,
                EP,
                Some(bed.key.clone()),
                transport,
            )));
            down.push(flag);
            beds.push(bed);
        }
        let cloud = Arc::new(cloud::Cloud::new(
            CLOUD_EP,
            SERVICE,
            Arc::new(EventLog::ephemeral()),
            Arc::new(domain::cloud_event_dataschemas(CLOUD_EP)),
            Arc::new(domain::completions()),
            list,
            None,
        ));
        let auth = Arc::new(
            Auth::ephemeral(CLOUD_EP, Box::new(FakeApprover { proof: "ok".into() })).with_scopes(domain::SCOPES),
        );
        let door = Arc::new(cloud_door(CLOUD_EP, cloud.clone(), auth.clone()));
        let app = best_peer_door::router(door).merge(endpoint::router(cloud.clone(), auth));
        cloud.follow();
        let key = key(&app).await;
        CloudBed {
            cloud,
            app,
            key,
            members: beds,
            down,
        }
    }

    impl CloudBed {
        async fn send(&self, id: &str, r#type: &str, schema: &str, data: Value) -> (StatusCode, Value) {
            let req = Request::post("/commands")
                .header("X-Api-Key", &self.key)
                .header(header::CONTENT_TYPE, "application/cloudevents+json")
                .body(Body::from(
                    json!({
                        "specversion": "1.0", "id": id, "source": "https://agent.test", "type": r#type,
                        "datacontenttype": "application/json", "dataschema": format!("{CLOUD_EP}/commands/{schema}/1.0"),
                        "time": "2026-10-13T10:30:00Z", "data": data
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
            self.cloud
                .log
                .list(&Filter {
                    correlation_id: Some(correlation.into()),
                    ..Default::default()
                })
                .0
        }

        async fn events_of(&self, correlation: &str, until: &str) -> Vec<Envelope> {
            for _ in 0..600 {
                let events = self.log_of(correlation);
                if events.iter().any(|e| e.r#type == until) {
                    return events;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            panic!(
                "no {until} for {correlation} on the cloud; got {:?}",
                types(&self.log_of(correlation))
            );
        }

        async fn deploy(&self, id: &str, gb: u64) -> (StatusCode, Value) {
            self.send(
                id,
                "DeployModel",
                "deploy-model",
                json!({ "model": "llama3.2:3b", "engine": "ollama", "memory_bytes": gb * GB }),
            )
            .await
        }
    }

    /// Two members: one with a 24 GiB card whose engine is an echo on loopback, one with an 8 GiB card.
    async fn two_members() -> CloudBed {
        let port = echo_engine().await;
        let a = bed_at(
            card(),
            Arc::new(EventLog::ephemeral()),
            Arc::new(best_peer_runtime::Fake::new(vec!["gpu-0".into()], vec![]).publishes(11434, port)),
            "127.0.0.1",
        )
        .await;
        let small = Arc::new(
            best_peer_gpu::Fake::new()
                .with_driver("fake 1.0")
                .with_device(best_peer_gpu::Fake::cuda_card("gpu-0", "Card B", 8, "12.4")),
        );
        let b = bed(
            small,
            Arc::new(EventLog::ephemeral()),
            Arc::new(best_peer_runtime::Fake::new(vec!["gpu-0".into()], vec![])),
        )
        .await;
        cloud_bed(vec![("a", a), ("b", b)]).await
    }

    #[tokio::test]
    async fn a_cloud_lists_every_members_devices_places_where_the_room_is_and_relays_the_facts_with_the_member_named() {
        let c = two_members().await;
        let hs = c.query("get-hardware-state").await;
        let ids: Vec<&str> = hs["devices"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["a/gpu-0", "b/gpu-0"]);
        assert_eq!(hs["devices"][0]["peer"], "a");
        assert_eq!(hs["peers"][0]["state"], "up");
        assert_eq!(hs["peers"][1]["peer"], "b");

        // 16 GiB fits the 24 GiB card alone.
        let (s, b) = c.deploy("d1", 16).await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let events = c.events_of("d1", "DeploymentReady").await;
        assert_eq!(
            types(&events),
            vec![
                "ImagePulling",
                "ImagePulling",
                "ImagePulled",
                "DeploymentStarted",
                "WeightsLoading",
                "WeightsLoading",
                "DeploymentReady"
            ],
            "{events:#?}"
        );
        assert!(
            events.iter().all(|e| e.data["peer"] == "a"),
            "every fact names the member"
        );
        assert!(events
            .iter()
            .all(|e| e.source == SERVICE && e.dataschema.as_deref().unwrap().starts_with(CLOUD_EP)));
        let started = &events[3].data;
        assert_eq!(
            started["device"], "a/gpu-0",
            "the device is named as the cloud names it"
        );
        let dep = started["deployment"].as_str().unwrap().to_string();
        assert_eq!(
            events[6].data["endpoint"],
            format!("{CLOUD_EP}/deployments/{dep}/v1"),
            "the endpoint is on the cloud's door"
        );
        let w = c.query("get-workloads").await;
        assert_eq!(w["deployments"][0]["peer"], "a");
        assert_eq!(w["deployments"][0]["phase"], "ready");
        assert_eq!(w["deployments"][0]["endpoint"], events[6].data["endpoint"]);
        // The member holds it too, under its own name for the device.
        let on_a = c.members[0].query("get-workloads").await;
        assert_eq!(on_a["deployments"][0]["deployment"], dep);
        assert_eq!(on_a["deployments"][0]["device"], "gpu-0");

        // A prompt to the cloud's endpoint reaches the model through the member, with the member's credential, not
        // the caller's, and the engine sees neither.
        let res = c
            .app
            .clone()
            .oneshot(
                Request::post(format!("/deployments/{dep}/v1/chat/completions"))
                    .header(header::AUTHORIZATION, format!("Bearer {}", c.key))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"llama3.2:3b","messages":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let seen = body(res).await;
        assert_eq!(seen["path"], "/v1/chat/completions");
        assert_eq!(seen["saw_credential"], false);
        let res = c
            .app
            .clone()
            .oneshot(
                Request::get(format!("/deployments/{dep}/v1/models"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // Another 16 GiB fits nowhere: the refusal names every member's room.
        let (s, b) = c.deploy("d2", 16).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "INSUFFICIENT_MEMORY");
        let named: Vec<&str> = b["error"]["details"]["devices"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["id"].as_str().unwrap())
            .collect();
        assert_eq!(named, ["a/gpu-0", "b/gpu-0"]);
        assert_eq!(b["error"]["details"]["devices"][0]["room_bytes"], 8 * GB);
        // 8 GiB fits both at 8 GiB of room: the first member listed takes it, every time.
        let (s, b) = c.deploy("d3", 8).await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let events = c.events_of("d3", "DeploymentReady").await;
        assert_eq!(events[3].data["device"], "a/gpu-0");
        // Pinned to the other member's card.
        let (s, b) = c
            .send(
                "d4",
                "DeployModel",
                "deploy-model",
                json!({ "model": "llama3.2:3b", "engine": "ollama", "memory_bytes": 4 * GB, "device": "b/gpu-0" }),
            )
            .await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let events = c.events_of("d4", "DeploymentReady").await;
        assert_eq!(events[3].data["peer"], "b");

        // An undeploy through the cloud ends it on the member, and the cloud's stream says so.
        let (s, b) = c
            .send("u1", "UndeployModel", "undeploy-model", json!({ "deployment": dep }))
            .await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let events = c.events_of("u1", "Undeployed").await;
        assert_eq!(events.last().unwrap().data["peer"], "a");
        let w = c.query("get-workloads").await;
        assert_eq!(w["deployments"].as_array().unwrap().len(), 2);
        assert!(c.members[0].query("get-workloads").await["deployments"]
            .as_array()
            .unwrap()
            .iter()
            .all(|d| d["deployment"] != dep));
    }

    #[tokio::test]
    async fn a_member_that_stops_answering_is_told_as_lost_with_its_workloads_ended_and_as_back_when_it_answers() {
        let c = two_members().await;
        let (s, b) = c.deploy("d1", 16).await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let events = c.events_of("d1", "DeploymentReady").await;
        let dep = events[3].data["deployment"].as_str().unwrap().to_string();
        assert_eq!(
            types(&c.log_of("peer-a")),
            vec!["PeerJoined"],
            "the first answer is a fact too"
        );

        c.down[0].store(true, std::sync::atomic::Ordering::SeqCst);
        c.cloud.reconcile().await;
        let lost = c.log_of("peer-a");
        assert_eq!(types(&lost), vec!["PeerJoined", "PeerLost"], "{lost:#?}");
        assert!(lost[1].data["reason"].as_str().unwrap().contains("connection refused"));
        let events = c.events_of("d1", "DeploymentEnded").await;
        let ended = &events.last().unwrap().data;
        assert_eq!(ended["deployment"], dep);
        assert!(
            ended["reason"].as_str().unwrap().contains("peer a is unreachable"),
            "{ended}"
        );
        assert!(c.query("get-workloads").await["deployments"]
            .as_array()
            .unwrap()
            .is_empty());
        // Nothing is placed on a lost member: its devices are gone from the state, and the refusal says so.
        let hs = c.query("get-hardware-state").await;
        assert_eq!(hs["peers"][0]["state"], "unreachable");
        let (s, b) = c.deploy("d2", 16).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "INSUFFICIENT_MEMORY");
        assert_eq!(b["error"]["details"]["devices"][0]["id"], "b/gpu-0");

        // The member still runs what the cloud told as ended: the cloud sees no room there until it ends it.
        c.down[0].store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            c.members[0].query("get-workloads").await["deployments"][0]["deployment"],
            dep
        );
        let hs = c.query("get-hardware-state").await;
        assert_eq!(
            hs["devices"][0]["memory"]["free_bytes"],
            8 * GB,
            "the member's promise is counted"
        );
        c.cloud.reconcile().await;
        let events = c.events_of("peer-a", "Undeployed").await;
        assert_eq!(
            types(&events),
            vec!["PeerJoined", "PeerLost", "PeerJoined", "Undeployed"],
            "{events:#?}"
        );
        assert_eq!(events[3].data["deployment"], dep);
        assert!(c.members[0].query("get-workloads").await["deployments"]
            .as_array()
            .unwrap()
            .is_empty());
        let (s, b) = c.deploy("d3", 16).await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        assert_eq!(c.events_of("d3", "DeploymentReady").await[3].data["peer"], "a");
    }

    #[tokio::test]
    async fn a_pull_through_the_cloud_reaches_every_member_and_is_told_once_when_all_have_it() {
        let c = two_members().await;
        let (s, b) = c
            .send("p1", "PullImage", "pull-image", json!({ "image": "ollama/ollama:0.6" }))
            .await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let events = c.events_of("p1", "ImagePulled").await;
        let pulled: Vec<&Envelope> = events.iter().filter(|e| e.r#type == "ImagePulled").collect();
        assert_eq!(pulled.len(), 1, "{events:#?}");
        assert!(
            pulled[0].data["peer"].is_null(),
            "the outcome is the cloud's, not one member's"
        );
        let progressed: std::collections::BTreeSet<&str> = events
            .iter()
            .filter(|e| e.r#type == "ImagePulling")
            .filter_map(|e| e.data["peer"].as_str())
            .collect();
        assert_eq!(progressed.into_iter().collect::<Vec<_>>(), ["a", "b"]);
        let (s, b) = c
            .send("p2", "PullImage", "pull-image", json!({ "image": "nobody/nothing:1" }))
            .await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let events = c.events_of("p2", "ImagePullFailed").await;
        let reason = events.last().unwrap().data["reason"].as_str().unwrap().to_string();
        assert!(reason.contains("a:") && reason.contains("b:"), "{reason}");
    }

    #[test]
    fn the_endpoint_defaults_to_the_listen_address_only_when_a_consumer_can_reach_it() {
        let (listen, endpoint, tls) = config(None, None, None, None).unwrap();
        assert_eq!(listen.to_string(), "127.0.0.1:7777");
        assert_eq!(endpoint, "http://127.0.0.1:7777");
        assert_eq!(tls, Tls::Off);
        let (_, endpoint, tls) = config(Some("0.0.0.0:7777"), Some("https://peer.example.com/"), None, None).unwrap();
        assert_eq!(endpoint, "https://peer.example.com");
        assert_eq!(tls, Tls::Own);
        assert!(config(Some("0.0.0.0:7777"), None, None, None)
            .unwrap_err()
            .contains("BEST_PEER_ENDPOINT"));
        assert!(config(Some("[::]:7777"), None, None, None)
            .unwrap_err()
            .contains("BEST_PEER_ENDPOINT"));
        assert!(config(Some("localhost:7777"), None, None, None)
            .unwrap_err()
            .contains("ip:port"));
        assert!(config(None, Some("peer.example.com"), None, None)
            .unwrap_err()
            .contains("https://"));
    }

    #[test]
    fn a_certificate_makes_the_default_endpoint_https() {
        let (_, endpoint, tls) = config(None, None, Some("c.pem"), Some("k.pem")).unwrap();
        assert_eq!(endpoint, "https://127.0.0.1:7777");
        assert!(matches!(tls, Tls::Given { .. }));
        let why = config(None, Some("http://127.0.0.1:7777"), Some("c.pem"), Some("k.pem")).unwrap_err();
        assert!(why.contains("https://"), "{why}");
    }

    /// The door served over https from a loopback port, with the certificate the peer makes for itself: the manifest
    /// names https everywhere, `approve` reaches it pinned to that certificate, and any other certificate is refused.
    #[tokio::test]
    async fn the_door_serves_https_with_its_own_certificate_and_approve_pins_it() {
        let dir = tempfile::tempdir().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let listen = listener.local_addr().unwrap();
        let endpoint = format!("https://{listen}");
        let served = tls::serving(&Tls::Own, dir.path(), &tls::names_for(&endpoint, &listen))
            .unwrap()
            .unwrap();
        let (config, cert) = (served.config, served.certificate.as_str());
        let peer = peer(
            &endpoint,
            "engines.local",
            card(),
            Arc::new(best_peer_runtime::Fake::new(vec!["gpu-0".into()], vec![])),
            Arc::new(best_peer_engine::Fake::new()),
            Arc::new(EventLog::ephemeral()),
            Images::default(),
            Patience::default(),
        )
        .await;
        let auth = Arc::new(Auth::ephemeral(
            &endpoint,
            Box::new(FakeApprover { proof: "ok".into() }),
        ));
        let door = Arc::new(super::door(&endpoint, peer.clone(), auth.clone()));
        let app = best_peer_door::router(door).merge(endpoint::router(peer, auth));
        let handle = axum_server::Handle::new();
        tokio::spawn(serve_on(listener, app, Some(config), handle.clone()));
        handle.listening().await;

        let get = |path: &str| {
            Request::get(path)
                .header(header::HOST, listen.to_string())
                .body(Body::empty())
                .unwrap()
        };
        let (status, manifest) = tls::call(listen, Some(cert), get("/.well-known/best")).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        let auth_block = &manifest["best"]["authentication"];
        assert_eq!(auth_block["deviceAuthorizationUrl"], format!("{endpoint}/auth/device"));
        assert_eq!(auth_block["tokenUrl"], format!("{endpoint}/auth/token"));
        assert_eq!(manifest["best"]["services"][SERVICE]["http"]["endpoint"], endpoint);

        // The operator's side, as `best-peer approve` does it: the device flow's code approved over the pinned link.
        let form = |path: &str, pairs: &[(&str, &str)]| {
            Request::post(path)
                .header(header::HOST, listen.to_string())
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(serde_urlencoded::to_string(pairs).unwrap()))
                .unwrap()
        };
        let (status, asked) = tls::call(
            listen,
            Some(cert),
            form("/auth/device", &[("client_id", "agent"), ("connection", "box")]),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK, "{asked}");
        assert!(asked["verification_uri"].as_str().unwrap().starts_with("https://"));
        let code = asked["user_code"].as_str().unwrap();
        let (status, admitted) = tls::call(
            listen,
            Some(cert),
            form("/auth/approve", &[("user_code", code), ("proof", "ok")]),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK, "{admitted}");
        assert_eq!(admitted["admitted"]["client_id"], "agent");

        // Another certificate, even one made the same way, is not the peer's.
        let other = tempfile::tempdir().unwrap();
        let other_cert = tls::serving(&Tls::Own, other.path(), &tls::names_for(&endpoint, &listen))
            .unwrap()
            .unwrap()
            .certificate;
        let why = tls::call(listen, Some(&other_cert), get("/.well-known/best"))
            .await
            .unwrap_err();
        assert!(why.contains("does not serve the certificate"), "{why}");
        handle.shutdown();
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
                "ImagePulling",
                "ImagePulled",
                "DeploymentStarted",
                "WeightsLoading",
                "WeightsLoading",
                "DeploymentReady"
            ],
            "{events:#?}"
        );
        let started = &events[3].data;
        assert_eq!(started["device"], "gpu-0");
        assert_eq!(started["engine"], "ollama");
        assert_eq!(started["memory_bytes"], 8 * GB);
        assert!(started["deployment"].as_str().unwrap().starts_with("dep-"));
        assert_eq!(events[1].data["progress_pct"], 100, "a pull that completed is 100");
        assert_eq!(events[4].data["progress_pct"], 25);
        let ready = &events[6].data;
        assert_eq!(
            ready["endpoint"],
            format!("{EP}/deployments/{}/v1", started["deployment"].as_str().unwrap()),
            "on the door, under the deployment"
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
        assert_eq!(w["deployments"][0]["endpoint"], ready["endpoint"]);

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

    /// An engine stand-in on a loopback port of its own: answers every request with what reached it.
    async fn echo_engine() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let app = axum::Router::new().fallback(|req: axum::extract::Request| async move {
            let (parts, body) = req.into_parts();
            let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap();
            axum::Json(json!({
                "method": parts.method.as_str(),
                "path": parts.uri.path(),
                "query": parts.uri.query(),
                "saw_credential": parts.headers.contains_key("X-Api-Key") || parts.headers.contains_key(header::AUTHORIZATION),
                "headers": parts.headers.keys().map(|k| k.as_str()).collect::<Vec<_>>(),
                "body": String::from_utf8_lossy(&bytes),
            }))
        });
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        port
    }

    /// A loopback port nothing listens on.
    fn closed_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[tokio::test]
    async fn the_endpoint_is_on_the_door_behind_its_credential_and_passes_prompts_through_to_the_engine() {
        let port = echo_engine().await;
        let bed = bed_at(
            card(),
            Arc::new(EventLog::ephemeral()),
            Arc::new(best_peer_runtime::Fake::new(vec!["gpu-0".into()], vec![]).publishes(11434, port)),
            "127.0.0.1",
        )
        .await;
        let dep = bed.deployed("d1", "llama3.2:3b", 8).await;
        assert_eq!(
            bed.query("get-workloads").await["deployments"][0]["endpoint"],
            format!("{EP}/deployments/{dep}/v1")
        );
        let path = format!("/deployments/{dep}/v1/chat/completions?stream=true");
        let prompt = |headers: Vec<(&'static str, String)>| {
            let mut req = Request::post(&path).header(header::CONTENT_TYPE, "application/json");
            for (name, value) in headers {
                req = req.header(name, value);
            }
            req.body(Body::from(r#"{"model":"llama3.2:3b","messages":[]}"#))
                .unwrap()
        };
        // No credential: the door's refusal, and nothing reaches the engine.
        let res = bed.app.clone().oneshot(prompt(vec![])).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body(res).await["error"]["code"], "UNAUTHORIZED");
        // The door's own header: the request reaches the engine as the client made it, less the credential.
        let res = bed
            .app
            .clone()
            .oneshot(prompt(vec![("X-Api-Key", bed.key.clone())]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let seen = body(res).await;
        assert_eq!(seen["method"], "POST");
        assert_eq!(seen["path"], "/v1/chat/completions");
        assert_eq!(seen["query"], "stream=true");
        assert_eq!(seen["body"], r#"{"model":"llama3.2:3b","messages":[]}"#);
        assert_eq!(seen["saw_credential"], false, "the credential ends at the door");
        // The header the OpenAI-compatible clients send, in whichever case.
        for scheme in ["Bearer", "bearer"] {
            let res = bed
                .app
                .clone()
                .oneshot(prompt(vec![("authorization", format!("{scheme} {}", bed.key))]))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK, "{scheme}");
        }
        // What the client encoded reaches the engine encoded, and what governs one connection stops at the hop.
        let res = bed
            .app
            .clone()
            .oneshot(
                Request::get(format!("/deployments/{dep}/v1/a%20b/models%3Ffoo=bar"))
                    .header("X-Api-Key", &bed.key)
                    .header("connection", "close, x-secret")
                    .header("x-secret", "s")
                    .header("te", "trailers")
                    .header("x-request-id", "r1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let seen = body(res).await;
        assert_eq!(seen["path"], "/v1/a%20b/models%3Ffoo=bar");
        assert_eq!(seen["query"], Value::Null);
        let names: Vec<&str> = seen["headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(names.contains(&"x-request-id"), "{names:?}");
        // Host is the engine's, set by the door's own client, not the door's as the client sent it.
        for gone in ["connection", "x-secret", "te", "x-api-key"] {
            assert!(!names.contains(&gone), "{gone} crossed: {names:?}");
        }
        // The deployment's base itself, with or without the slash, is the engine's root.
        for base in [format!("/deployments/{dep}"), format!("/deployments/{dep}/")] {
            let res = bed
                .app
                .clone()
                .oneshot(
                    Request::get(&base)
                        .header("X-Api-Key", &bed.key)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK, "{base}");
            assert_eq!(body(res).await["path"], "/", "{base}");
        }
        let res = bed
            .app
            .clone()
            .oneshot(Request::get("/deployments/dep-nope").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            StatusCode::UNAUTHORIZED,
            "the credential comes before the lookup"
        );
        // A wrong credential is refused like a missing one.
        let res = bed
            .app
            .clone()
            .oneshot(prompt(vec![("authorization", "Bearer bp_nope".into())]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        // A deployment nobody made.
        let res = bed
            .app
            .clone()
            .oneshot(
                Request::get("/deployments/dep-nope/v1/models")
                    .header("X-Api-Key", &bed.key)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert_eq!(body(res).await["error"]["code"], "NO_SUCH_DEPLOYMENT");
        // Once an undeploy is accepted the promise ends, before the engine is gone.
        let (s, _) = bed
            .send("u1", "UndeployModel", "undeploy-model", json!({ "deployment": dep }))
            .await;
        assert_eq!(s, StatusCode::CREATED);
        bed.events_of("u1", "Undeployed").await;
        let res = bed
            .app
            .clone()
            .oneshot(prompt(vec![("X-Api-Key", bed.key.clone())]))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn an_engine_that_does_not_answer_is_a_bad_gateway_with_the_fix() {
        let bed = bed_at(
            card(),
            Arc::new(EventLog::ephemeral()),
            Arc::new(best_peer_runtime::Fake::new(vec!["gpu-0".into()], vec![]).publishes(11434, closed_port())),
            "127.0.0.1",
        )
        .await;
        let dep = bed.deployed("d1", "llama3.2:3b", 8).await;
        let res = bed
            .app
            .clone()
            .oneshot(
                Request::get(format!("/deployments/{dep}/v1/models"))
                    .header("X-Api-Key", &bed.key)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
        let b = body(res).await;
        assert_eq!(b["error"]["code"], "ENGINE_UNREACHABLE");
        assert!(
            b["error"]["message"].as_str().unwrap().contains("DeploymentEnded"),
            "{b}"
        );
    }

    #[tokio::test]
    async fn engines_that_report_every_percent_are_told_at_steps_of_ten_and_the_last_is_a_hundred() {
        let bed = bed_with(
            card(),
            Arc::new(EventLog::ephemeral()),
            Arc::new(best_peer_runtime::Fake::new(vec!["gpu-0".into()], vec![]).reporting_every_percent()),
            Arc::new(best_peer_engine::Fake::new().reporting_every_percent()),
            "engines.local",
            None,
        )
        .await;
        let (s, b) = bed.deploy("d1", "llama3.2:3b", "ollama", 8).await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let events = bed.events_of("d1", "DeploymentReady").await;
        for kind in ["ImagePulling", "WeightsLoading"] {
            let pcts: Vec<u64> = events
                .iter()
                .filter(|e| e.r#type == kind)
                .map(|e| e.data["progress_pct"].as_u64().unwrap())
                .collect();
            assert_eq!(pcts, vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100], "{kind}");
        }
        // Eleven of each, with ImagePulled, DeploymentStarted and DeploymentReady between: tens, not hundreds.
        assert_eq!(events.len(), 11 + 1 + 1 + 11 + 1, "{:?}", types(&events));
    }

    #[test]
    fn an_amount_of_memory_reads_as_a_person_writes_it() {
        assert_eq!(parse_memory("40G").unwrap(), 40 << 30);
        assert_eq!(parse_memory("40GB").unwrap(), 40 << 30);
        assert_eq!(parse_memory("512m").unwrap(), 512 << 20);
        assert_eq!(parse_memory("1T").unwrap(), 1 << 40);
        assert_eq!(parse_memory("1073741824").unwrap(), 1 << 30);
        assert!(parse_memory("lots").unwrap_err().contains("40G"));
        assert!(parse_memory("4X").unwrap_err().contains("unit"));
    }

    #[tokio::test]
    async fn a_credential_does_what_it_was_approved_for_holds_its_share_and_ends_only_its_own() {
        let bed = fresh().await;
        // A team key: may deploy, 8 GiB of device memory at most, no jobs; nothing in the door changed for the
        // owner's own key, which was approved with nothing said.
        let team = key_with(&bed.app, Some("deploy"), Some(8 * GB)).await;
        let send = |key: String, id: &'static str, r#type: &'static str, schema: &'static str, data: Value| {
            let app = bed.app.clone();
            async move {
                let req = Request::post("/commands")
                    .header("X-Api-Key", &key)
                    .header(header::CONTENT_TYPE, "application/cloudevents+json")
                    .body(Body::from(
                        json!({
                            "specversion": "1.0", "id": id, "source": "https://team.test", "type": r#type,
                            "datacontenttype": "application/json", "dataschema": format!("{EP}/commands/{schema}/1.0"),
                            "time": "2026-10-13T10:30:00Z", "data": data
                        })
                        .to_string(),
                    ))
                    .unwrap();
                let res = app.oneshot(req).await.unwrap();
                let status = res.status();
                (status, body(res).await)
            }
        };
        let query = |key: String| {
            let app = bed.app.clone();
            async move {
                let req = Request::get("/queries/get-workloads")
                    .header("X-Api-Key", &key)
                    .body(Body::empty())
                    .unwrap();
                body(app.oneshot(req).await.unwrap()).await
            }
        };
        // The share: 6 of 8 GiB fits; 4 more does not, at ingest, with what is held and the room left.
        let (s, b) = send(
            team.clone(),
            "t1",
            "DeployModel",
            "deploy-model",
            json!({ "model": "a:7b", "engine": "ollama", "memory_bytes": 6 * GB }),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        let (s, b) = send(
            team.clone(),
            "t2",
            "DeployModel",
            "deploy-model",
            json!({ "model": "b:3b", "engine": "ollama", "memory_bytes": 4 * GB }),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "SHARE_EXCEEDED");
        assert_eq!(b["error"]["details"]["room_bytes"], 2 * GB);
        assert!(bed.log_of("t2").is_empty(), "a refused deploy told nothing");
        // The device had room (24 GiB): the share, not the hardware, said no.
        let (s, _) = send(
            team.clone(),
            "t3",
            "DeployModel",
            "deploy-model",
            json!({ "model": "c:1b", "engine": "ollama", "memory_bytes": 2 * GB }),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED);
        // No jobs for this key, said at ingest with what it was approved for; the command document says so too.
        let (s, b) = send(team.clone(), "t4", "RunJob", "run-job", json!({ "image": "busybox" })).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "FORBIDDEN");
        assert!(
            b["error"]["message"].as_str().unwrap().contains("approved for deploy"),
            "{b}"
        );
        let doc = body(
            bed.app
                .clone()
                .oneshot(
                    Request::get("/commands/run-job/1.0")
                        .header("X-Api-Key", &team)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert!(doc["description"].as_str().unwrap().contains("jobs scope"), "{doc}");
        // The owner's key deploys too; the team key sees only its own, the owner's sees both with the holder.
        let owner_dep = bed.deployed("o1", "owner:7b", 4).await;
        let team_events = bed.events_of("t1", "DeploymentReady").await;
        let team_dep = team_events
            .iter()
            .find(|e| e.r#type == "DeploymentStarted")
            .unwrap()
            .data["deployment"]
            .as_str()
            .unwrap()
            .to_string();
        let mine = query(team.clone()).await;
        let ids: Vec<&str> = mine["deployments"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["deployment"].as_str().unwrap())
            .collect();
        assert!(
            ids.contains(&team_dep.as_str()) && !ids.contains(&owner_dep.as_str()),
            "{ids:?}"
        );
        assert_eq!(mine["held_bytes"], 8 * GB);
        assert_eq!(mine["share_bytes"], 8 * GB);
        let all = query(bed.key.clone()).await;
        assert_eq!(all["deployments"].as_array().unwrap().len(), 3);
        assert_eq!(all["share_bytes"], Value::Null);
        assert!(all["deployments"]
            .as_array()
            .unwrap()
            .iter()
            .all(|d| d["holder"].as_str().is_some_and(|h| h.len() == 16)));
        // Another's deployment is refused by name; its own and, for the owner, anyone's, go.
        let (s, b) = send(
            team.clone(),
            "t5",
            "UndeployModel",
            "undeploy-model",
            json!({ "deployment": owner_dep }),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "FORBIDDEN");
        assert!(
            b["error"]["message"].as_str().unwrap().contains("another credential"),
            "{b}"
        );
        // Nor its endpoint, its events, or its name in a refusal: the team reads what is the team's.
        let res = bed
            .app
            .clone()
            .oneshot(
                Request::get(format!("/deployments/{owner_dep}/v1/models"))
                    .header("X-Api-Key", &team)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body(res).await["error"]["code"], "FORBIDDEN");
        let events = |key: String, corr: &'static str| {
            let app = bed.app.clone();
            async move {
                let req = Request::get(format!("/events?correlationId={corr}"))
                    .header("X-Api-Key", &key)
                    .body(Body::empty())
                    .unwrap();
                body(app.oneshot(req).await.unwrap()).await["events"]
                    .as_array()
                    .unwrap()
                    .len()
            }
        };
        assert_eq!(
            events(team.clone(), "o1").await,
            0,
            "the owner's deploy is not the team's to read"
        );
        assert!(events(team.clone(), "t1").await > 0, "its own is");
        assert!(
            events(bed.key.clone(), "t1").await > 0,
            "and the owner reads everyone's"
        );
        let (s, b) = send(
            team.clone(),
            "t5b",
            "UndeployModel",
            "undeploy-model",
            json!({ "deployment": "dep-nope" }),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        let live: Vec<&str> = b["error"]["details"]["live"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(
            !live.contains(&owner_dep.as_str()) && live.contains(&team_dep.as_str()),
            "{live:?}"
        );
        let (s, _) = send(
            team.clone(),
            "t6",
            "UndeployModel",
            "undeploy-model",
            json!({ "deployment": team_dep.clone() }),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED);
        bed.events_of("t6", "Undeployed").await;
        let (s, _) = send(
            bed.key.clone(),
            "o2",
            "UndeployModel",
            "undeploy-model",
            json!({ "deployment": owner_dep }),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED);
        // Freed memory is room again for the share.
        bed.events_of("o2", "Undeployed").await;
        let (s, b) = send(
            team.clone(),
            "t7",
            "DeployModel",
            "deploy-model",
            json!({ "model": "d:3b", "engine": "ollama", "memory_bytes": 5 * GB }),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        // A scope the door does not have is refused at the approval, by name.
        let a = body(
            bed.app
                .clone()
                .oneshot(
                    Request::post("/auth/device")
                        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                        .body(Body::from("client_id=test"))
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        let res = bed
            .app
            .clone()
            .oneshot(
                Request::post("/auth/approve")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from(
                        serde_urlencoded::to_string([
                            ("user_code", a["user_code"].as_str().unwrap()),
                            ("proof", "ok"),
                            ("may", "deploy,admin"),
                        ])
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert!(body(res).await["error_description"].as_str().unwrap().contains("admin"));
        // The manifest lists what may be granted.
        let m = body(
            bed.app
                .clone()
                .oneshot(Request::get("/.well-known/best").body(Body::empty()).unwrap())
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(m["best"]["authentication"]["scopes"], json!(["deploy", "jobs"]));
    }

    #[tokio::test]
    async fn a_job_on_a_runtime_that_runs_none_is_refused_at_ingest_with_the_fix_and_nothing_is_told() {
        let bed = bed(
            card(),
            Arc::new(EventLog::ephemeral()),
            Arc::new(best_peer_runtime::Fake::new(vec!["gpu-0".into()], vec![]).without_jobs()),
        )
        .await;
        let (s, b) = bed.send("j1", "RunJob", "run-job", json!({ "image": "busybox" })).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "NO_CONTAINER_ENGINE");
        let msg = b["error"]["message"].as_str().unwrap();
        assert!(msg.contains("BEST_PEER_RUNTIME=docker"), "{msg}");
        assert!(bed.log_of("j1").is_empty(), "a refused job told nothing");
        // A deploy on the same runtime is still placed: only jobs are what it lacks.
        assert_eq!(
            bed.deploy("d1", "llama3.2:3b", "ollama", 8).await.0,
            StatusCode::CREATED
        );
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
        assert_eq!(events[3].data["device"], "cpu-0");
        let container = ContainerId(events[3].data["container"].as_str().unwrap().into());
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
            vec![
                "ImagePulling",
                "ImagePulling",
                "ImagePulled",
                "DeploymentStarted",
                "DeploymentFailed"
            ]
        );
        let failed = &events[4].data;
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
        assert_eq!(
            types(&events),
            vec!["ImagePulling", "ImagePulling", "ImagePulled", "JobStarted"]
        );
        let started = &events[3].data;
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
            vec!["ImagePulling", "ImagePulling", "ImagePulled", "JobStarted"],
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
        let credentials = first.door.auth.clone();
        drop(first);
        runtime.stop(&lost_container).await.unwrap();
        runtime.stop(&job_container).await.unwrap();
        // A new peer over the same log and the same engine.
        // The restart keeps the credentials, as a host's data directory does: the command accepted before it
        // is still its sender's.
        let again = bed_with(
            card(),
            log.clone(),
            runtime.clone(),
            Arc::new(best_peer_engine::Fake::new()),
            "engines.local",
            Some(credentials),
        )
        .await;
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
        // Accepted from a live credential: a restart runs it as that credential, found again in the store.
        owed_log.accept_command(
            &best_peer_door::auth::hash_key(&again.key),
            "d9",
            &best_peer_door::payload_hash(&envelope),
            "d9",
            &envelope,
        );
        again.peer.halt();
        let credentials = again.door.auth.clone();
        drop(again);
        let third = bed_with(
            card(),
            log.clone(),
            runtime.clone(),
            Arc::new(best_peer_engine::Fake::new()),
            "engines.local",
            Some(credentials),
        )
        .await;
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
