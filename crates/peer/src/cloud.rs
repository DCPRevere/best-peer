//! A peer that fronts peers: the cloud door. The same surface as a peer, where the devices are every member's,
//! a deploy or a job is placed on the member with the room (the peer's own rule, applied across members) and sent
//! there as the cloud's own command, and what the member tells about it comes back on the cloud's stream under the
//! caller's correlation id, with the member named. The cloud holds no model and runs no job: it holds the log of what
//! it placed, follows the members' streams, and squares its state with theirs on a timer, so a member that is gone is
//! told as such for every workload it held. Members are a list the operator gives; each admits the cloud by its own
//! sign-in, the device flow, with the code shown in the cloud's log for the person at that member's machine.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use best_peer_door::auth::{hash_key, holder_of_hash, Credential};
use best_peer_door::envelope::now_rfc3339;
use best_peer_door::{BestError, Emitter, Envelope, EventLog};
use best_peer_engine::Kind;
use best_peer_gpu::{Device, DeviceId, Host};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::domain::Service;
use crate::endpoint::{Route, Upstream};
use crate::lifecycle::{
    any_compute, mine, new_id, not_yours, parse, place, ready_deployment, require, within_share, workloads_view,
};
use crate::member::{Member, Refusal};
use crate::state::{JobPhase, Phase, Reservation, State};

/// How often the cloud squares its state with its members', and the most a member may be silent before its
/// workloads are told as ended: the same thirty seconds a peer gives a lost engine.
pub const RECONCILE_EVERY: Duration = Duration::from_secs(30);

/// How long a member's stream may be silent before it is reconnected: the member keeps it alive every fifteen
/// seconds, so a minute of nothing is a dead socket.
const STREAM_SILENCE: Duration = Duration::from_secs(60);

/// The events after which nothing more is said about a workload or a command.
const TERMINAL: &[&str] = &[
    "Undeployed",
    "DeploymentFailed",
    "DeploymentEnded",
    "OutOfMemory",
    "JobFinished",
    "JobFailed",
    "JobStopped",
    "CommandFailed",
];

/// A member's credential and the cloud's place on its stream, kept across restarts in `<data>/members.json`.
#[derive(Serialize, Deserialize, Clone, Default)]
struct Saved {
    name: String,
    endpoint: String,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    last_event: Option<String>,
}

/// What the cloud sent a member and still listens for, by the member and the correlation the member was given:
/// the emitter of the cloud command it came from, and what the command is.
#[derive(Clone)]
struct Issued {
    em: Emitter,
    /// The cloud command's key when this is one part of a pull fanned out to every member.
    pull: Option<String>,
    /// The workload this command ends, for an undeploy or a stop.
    ends: Option<String>,
}

/// A pull fanned out to every member: who is still to hear from, and who failed.
struct Pull {
    image: String,
    pending: HashSet<String>,
    failed: Vec<String>,
}

/// What a member's answer at a reconcile tick meant for it.
#[derive(PartialEq, Eq)]
enum Contact {
    Same,
    Joined,
    Lost,
}

pub struct Cloud {
    pub log: Arc<EventLog>,
    service: String,
    endpoint: String,
    dataschemas: Arc<HashMap<String, String>>,
    completions: Arc<HashMap<String, HashSet<String>>>,
    pub members: Vec<Arc<Member>>,
    pub state: Mutex<State>,
    issued: Mutex<HashMap<(String, String), Issued>>,
    pulls: Mutex<HashMap<String, Pull>>,
    /// Each member's last event the cloud relayed.
    cursors: Mutex<HashMap<String, String>>,
    /// Whether each member answered at the last reconcile tick; a member never asked is taken as answering.
    reachable: Mutex<HashMap<String, bool>>,
    /// Workloads a member did not list at the last tick: listed at two ticks running, they are told as ended, so
    /// a fact still on its way on the stream is not outrun.
    missing: Mutex<HashSet<String>>,
    file: Option<PathBuf>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// A member's `get-hardware-state`, as the abstraction's types.
#[derive(Deserialize)]
struct Seen {
    #[serde(default)]
    driver: Option<String>,
    devices: Vec<Device>,
}

#[derive(Deserialize)]
struct DeployData {
    model: String,
    engine: Kind,
    memory_bytes: u64,
    #[serde(default)]
    device: Option<String>,
}

#[derive(Deserialize)]
struct RunJobData {
    image: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    memory_bytes: Option<u64>,
    #[serde(default)]
    device: Option<String>,
}

impl Cloud {
    /// A cloud over `members`, its log, and the file its members' credentials and cursors are kept in (none for a
    /// cloud that forgets them at exit). A member already in the file keeps the credential it was issued.
    pub fn new(
        endpoint: &str,
        service: &str,
        log: Arc<EventLog>,
        dataschemas: Arc<HashMap<String, String>>,
        completions: Arc<HashMap<String, HashSet<String>>>,
        members: Vec<Arc<Member>>,
        file: Option<PathBuf>,
    ) -> Self {
        let mut cursors = HashMap::new();
        if let Some(saved) = file.as_ref().and_then(|f| read_saved(f)) {
            for s in saved {
                if let Some(m) = members.iter().find(|m| m.name == s.name && m.endpoint == s.endpoint) {
                    if let Some(k) = s.key {
                        m.set_key(k);
                    }
                    if let Some(id) = s.last_event {
                        cursors.insert(s.name.clone(), id);
                    }
                }
            }
        }
        Self {
            log,
            service: service.into(),
            endpoint: endpoint.trim_end_matches('/').into(),
            dataschemas,
            completions,
            members,
            state: Mutex::new(State::default()),
            issued: Mutex::new(HashMap::new()),
            pulls: Mutex::new(HashMap::new()),
            cursors: Mutex::new(cursors),
            reachable: Mutex::new(HashMap::new()),
            missing: Mutex::new(HashSet::new()),
            file,
            tasks: Mutex::new(Vec::new()),
        }
    }

    /// Folds every fact the log holds into the state. Run once, before the door opens.
    pub fn rebuild(&self) {
        let mut state = self.state.lock().unwrap();
        self.log.replay(|e| state.apply(e));
    }

    /// Writes the members' credentials and cursors, whole or not at all.
    fn save(&self) {
        let Some(file) = &self.file else { return };
        let cursors = self.cursors.lock().unwrap();
        let saved: Vec<Saved> = self
            .members
            .iter()
            .map(|m| Saved {
                name: m.name.clone(),
                endpoint: m.endpoint.clone(),
                key: m.key(),
                last_event: cursors.get(&m.name).cloned(),
            })
            .collect();
        let tmp = file.with_extension("json.tmp");
        let written = crate::tls::write_private(&tmp, &serde_json::to_vec_pretty(&saved).unwrap_or_default())
            .and_then(|()| std::fs::rename(&tmp, file));
        if let Err(e) = written {
            tracing::warn!(file = %file.display(), error = %e, "the members' credentials could not be saved");
        }
    }

    pub fn emitter(&self, correlation: &str) -> Emitter {
        Emitter::new(self.log.clone(), &self.service, correlation, self.dataschemas.clone())
            .with_completions(self.completions.clone())
    }

    /// Tells a fact of the cloud's own: appended, then folded.
    fn tell(&self, em: &Emitter, r#type: &str, data: Value) -> Envelope {
        let workload = data["deployment"].as_str().or(data["job"].as_str()).map(str::to_string);
        let e = em.emit(r#type, data);
        if let Some(w) = workload {
            em.completes_workload(&w, r#type);
        }
        self.state.lock().unwrap().apply(&e);
        e
    }

    fn member(&self, name: &str) -> Option<Arc<Member>> {
        self.members.iter().find(|m| m.name == name).cloned()
    }

    /// The name this cloud signs in to its members under: its host, one word.
    fn connection_name(&self) -> String {
        let host: String = host_of(&self.endpoint)
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .take(40)
            .collect();
        format!("cloud-{}", host.trim_matches('-'))
    }

    /// What the member names as `holder` for the workloads this cloud placed there.
    fn holder_on(&self, member: &Member) -> Option<String> {
        member.key().map(|k| holder_of_hash(&hash_key(&k)))
    }

    /// The members admitted and answering at the last tick: where anything can be placed.
    fn up(&self) -> Vec<Arc<Member>> {
        let reachable = self.reachable.lock().unwrap();
        self.members
            .iter()
            .filter(|m| m.admitted() && reachable.get(&m.name).copied().unwrap_or(true))
            .cloned()
            .collect()
    }

    /// Records whether a member answered, and tells the membership fact when that changed.
    fn contact(&self, member: &Member, now: bool, why: &str) -> Contact {
        let before = self.reachable.lock().unwrap().insert(member.name.clone(), now);
        match (before, now) {
            (Some(true), false) | (None, false) => {
                tracing::warn!(peer = %member.name, %why, "a member peer is unreachable");
                self.tell(
                    &self.emitter(&format!("peer-{}", member.name)),
                    "PeerLost",
                    json!({ "peer": member.name, "endpoint": member.endpoint, "reason": why }),
                );
                Contact::Lost
            }
            (Some(false), true) | (None, true) => {
                tracing::info!(peer = %member.name, "a member peer answers");
                self.tell(
                    &self.emitter(&format!("peer-{}", member.name)),
                    "PeerJoined",
                    json!({ "peer": member.name, "endpoint": member.endpoint }),
                );
                Contact::Joined
            }
            _ => Contact::Same,
        }
    }

    /// Every admitted member's devices as one host, each device named `<peer>/<id>`, with the members as they
    /// answered this time. A device's free memory is what the member's own placement would allow: the free memory
    /// it reports, less what the member has promised to workloads there that have not taken it yet. Whether a
    /// member is lost is the reconcile tick's to say, not a read's.
    async fn combined(&self) -> (Host, Vec<Value>) {
        let mut devices = Vec::new();
        let mut peers = Vec::new();
        for m in &self.members {
            if !m.admitted() {
                peers.push(json!({ "peer": m.name, "endpoint": m.endpoint, "state": "joining" }));
                continue;
            }
            let answered = async {
                let promised = promised_by_device(&m.query("get-workloads").await?);
                let hs = m.query("get-hardware-state").await?;
                Ok::<_, Refusal>((promised, hs))
            }
            .await;
            match answered {
                Ok((promised, hs)) => {
                    let seen: Seen = match serde_json::from_value(hs.clone()) {
                        Ok(h) => h,
                        Err(e) => {
                            tracing::warn!(peer = %m.name, error = %e, "the member's hardware state could not be read");
                            peers.push(json!({ "peer": m.name, "endpoint": m.endpoint, "state": "up", "driver": hs["driver"] }));
                            continue;
                        }
                    };
                    peers.push(json!({ "peer": m.name, "endpoint": m.endpoint, "state": "up", "driver": seen.driver }));
                    for mut d in seen.devices {
                        let held = promised.get(&d.id.0).copied().unwrap_or(0);
                        d.memory.free_bytes = d.memory.free_bytes.min(d.memory.total_bytes.saturating_sub(held));
                        devices.push(Device {
                            id: DeviceId(format!("{}/{}", m.name, d.id.0)),
                            ..d
                        });
                    }
                }
                Err(e) => {
                    peers.push(
                        json!({ "peer": m.name, "endpoint": m.endpoint, "state": "unreachable", "reason": e.text() }),
                    );
                }
            }
        }
        (
            Host {
                driver: None,
                devices,
                observed_at: std::time::SystemTime::now(),
            },
            peers,
        )
    }

    /// The member a cloud device id names, and the device's id on it.
    fn split_device(id: &str) -> Option<(&str, &str)> {
        id.split_once('/')
    }

    /// The command as the member receives it: the cloud's own, with an id and a correlation unique to the cloud
    /// command it stands for, so a retry is the same command there too, and the facts the member tells come back
    /// to the right emitter. A command of the cloud's own, for no caller, is named by the workload it ends.
    fn to_member(&self, schema_base: &str, key: &str, r#type: &str, schema: &str, data: Value) -> (Envelope, String) {
        let mc = member_correlation(key, &data);
        let envelope = Envelope {
            specversion: "1.0".into(),
            id: mc.clone(),
            correlationid: Some(mc.clone()),
            source: self.endpoint.clone(),
            r#type: r#type.into(),
            datacontenttype: "application/json".into(),
            dataschema: Some(format!("{schema_base}/commands/{schema}/1.0")),
            time: now_rfc3339(),
            data,
            extensions: Default::default(),
        };
        (envelope, mc)
    }

    fn listen_for(&self, member: &str, mc: &str, em: &Emitter, pull: Option<String>, ends: Option<String>) {
        self.issued.lock().unwrap().insert(
            (member.into(), mc.into()),
            Issued {
                em: em.clone(),
                pull,
                ends,
            },
        );
    }

    /// Sends a command to a member and listens for its facts; a refusal comes back as the cloud's error.
    async fn place_on(
        &self,
        member: &Member,
        em: &Emitter,
        r#type: &str,
        schema: &str,
        data: Value,
    ) -> Result<(), BestError> {
        let ends = data["deployment"].as_str().or(data["job"].as_str()).map(str::to_string);
        let base = member.schema_base().await;
        let (envelope, mc) = self.to_member(&base, em.command_key(), r#type, schema, data);
        self.listen_for(&member.name, &mc, em, None, ends);
        member.send(&envelope).await.map(|_| ()).map_err(|e| {
            self.issued.lock().unwrap().remove(&(member.name.clone(), mc));
            e.relayed(&member.name)
        })
    }

    /// The sender and the id of a cloud command, from its key.
    fn sender_of(key: &str) -> (&str, &str) {
        key.split_once(':').unwrap_or(("", key))
    }

    /// Whether the command with this key already made a workload the cloud knows: a command run again after a
    /// restart is already placed, and the member runs it.
    fn already_placed(&self, key: &str) -> Option<String> {
        let (sender, id) = Self::sender_of(key);
        let workload = self.log.workload_of(sender, id)?;
        let s = self.state.lock().unwrap();
        let peer = s
            .deployments
            .get(&workload)
            .and_then(|d| d.peer.clone())
            .or_else(|| s.jobs.get(&workload).and_then(|j| j.peer.clone()))?;
        Some(peer)
    }

    // ----- what the members tell ----------------------------------------------------------------------------------

    /// A fact a member told, relayed when it answers something the cloud sent there: told again on the cloud's log
    /// under the caller's correlation, with the member named and the caller as the holder, and folded into the
    /// state.
    fn relay(&self, member: &Member, id: &str, e: &Envelope) {
        let issued = e.correlationid.as_deref().and_then(|mc| {
            self.issued
                .lock()
                .unwrap()
                .get(&(member.name.clone(), mc.to_string()))
                .cloned()
        });
        if let Some(Issued { em, pull, .. }) = issued {
            let mut data = e.data.clone();
            data["peer"] = json!(member.name);
            if let Some(device) = data["device"].as_str().filter(|d| !d.is_empty() && !d.contains('/')) {
                data["device"] = json!(format!("{}/{device}", member.name));
            }
            if let Some(key) = pull {
                self.on_pull_fact(member, &key, &em, &e.r#type, data);
            } else {
                let key = em.command_key().to_string();
                let (sender, _) = Self::sender_of(&key);
                // The promise made at admission is kept until the member starts the workload, or fails to.
                if !matches!(e.r#type.as_str(), "ImagePulling" | "ImagePulled") && !key.is_empty() {
                    self.state.lock().unwrap().reserved.remove(&key);
                }
                match e.r#type.as_str() {
                    "DeploymentStarted" | "JobStarted" => {
                        if !sender.is_empty() {
                            data["holder"] = json!(holder_of_hash(sender));
                        }
                        if let Some(w) = data["deployment"].as_str().or(data["job"].as_str()) {
                            em.bind_workload(w);
                        }
                    }
                    "DeploymentReady" => {
                        if let (Some(dep), Some(engine)) = (
                            data["deployment"].as_str(),
                            serde_json::from_value::<Kind>(data["engine"].clone()).ok(),
                        ) {
                            data["endpoint"] =
                                json!(format!("{}/deployments/{dep}{}", self.endpoint, engine.openai_path()));
                        }
                    }
                    _ => {}
                }
                self.tell(&em, &e.r#type, data);
                if TERMINAL.contains(&e.r#type.as_str()) {
                    if let Some(mc) = e.correlationid.as_deref() {
                        self.issued
                            .lock()
                            .unwrap()
                            .remove(&(member.name.clone(), mc.to_string()));
                    }
                }
            }
        }
        self.cursors.lock().unwrap().insert(member.name.clone(), id.to_string());
        self.save();
    }

    /// A pull is on every member at once: progress is relayed as it comes, the outcome is told once all answered,
    /// a member that fails or is lost counted as failed.
    fn on_pull_fact(&self, member: &Member, key: &str, em: &Emitter, r#type: &str, data: Value) {
        if r#type == "ImagePulling" {
            self.tell(em, r#type, data);
            return;
        }
        let done = {
            let mut pulls = self.pulls.lock().unwrap();
            let Some(p) = pulls.get_mut(key) else { return };
            match r#type {
                "ImagePulled" => {
                    p.pending.remove(&member.name);
                }
                "ImagePullFailed" | "CommandFailed" => {
                    p.pending.remove(&member.name);
                    p.failed.push(format!(
                        "{}: {}",
                        member.name,
                        data["reason"].as_str().unwrap_or("failed")
                    ));
                }
                _ => return,
            }
            p.pending.is_empty().then(|| pulls.remove(key)).flatten()
        };
        if let Some(p) = done {
            self.issued
                .lock()
                .unwrap()
                .retain(|_, i| i.pull.as_deref() != Some(key));
            if p.failed.is_empty() {
                self.tell(em, "ImagePulled", json!({ "image": p.image }));
            } else {
                self.tell(
                    em,
                    "ImagePullFailed",
                    json!({ "image": p.image, "reason": p.failed.join("; "), "fix": "check the image name and the tag, and that every member reaches the registry" }),
                );
            }
        }
    }

    /// Follows every member for as long as the cloud runs: signs in where it is not admitted yet, or no longer is,
    /// then relays the member's stream, reconnecting from the last event it relayed; and squares the state with the
    /// members on a timer.
    pub fn follow(self: &Arc<Self>) {
        let mut tasks = self.tasks.lock().unwrap();
        for m in &self.members {
            let (me, member) = (self.clone(), m.clone());
            tasks.push(tokio::spawn(async move {
                loop {
                    if !member.admitted() {
                        me.sign_in(&member).await;
                    }
                    let last = me.cursors.lock().unwrap().get(&member.name).cloned();
                    match member.stream(last.as_deref()).await {
                        Ok(stream) => {
                            let mut stream = std::pin::pin!(stream);
                            loop {
                                match tokio::time::timeout(STREAM_SILENCE, stream.next()).await {
                                    Ok(Some((id, e))) => me.relay(&member, &id, &e),
                                    Ok(None) => {
                                        tracing::info!(peer = %member.name, "the member's stream ended; reconnecting");
                                        break;
                                    }
                                    Err(_) => {
                                        tracing::info!(peer = %member.name, "the member's stream went silent; reconnecting");
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) if e.is_unauthorized() => {
                            tracing::warn!(peer = %member.name, "the member no longer honours this cloud's credential; signing in again");
                            member.clear_key();
                            me.save();
                            continue;
                        }
                        Err(e) => {
                            tracing::debug!(peer = %member.name, why = %e.text(), "the member's stream is not open");
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }));
        }
        let me = self.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::time::sleep(RECONCILE_EVERY).await;
                me.reconcile().await;
            }
        }));
    }

    /// The device-flow sign-in with a member, until it admits this cloud: the code goes to the log for the person
    /// at that member's machine, a code that expires is asked again.
    async fn sign_in(&self, member: &Member) {
        let label = format!("the cloud door at {}", self.endpoint);
        let connection = self.connection_name();
        loop {
            let asked = match member
                .sign_in_form(
                    "/auth/device",
                    &[
                        ("client_id", "best-peer"),
                        ("connection", &connection),
                        ("agent_label", &label),
                    ],
                )
                .await
            {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!(peer = %member.name, why = %e.text(), "the member did not start the sign-in; trying again in five seconds");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let (Some(user_code), Some(device_code)) = (asked["user_code"].as_str(), asked["device_code"].as_str())
            else {
                tracing::warn!(peer = %member.name, "the member's sign-in answer has no code; trying again in five seconds");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            };
            let interval = asked["interval"].as_u64().unwrap_or(5).max(1);
            tracing::info!(peer = %member.name, code = %user_code, "to admit this cloud on {}, run on that machine: best-peer approve {user_code}", member.name);
            loop {
                tokio::time::sleep(Duration::from_secs(interval)).await;
                match member
                    .sign_in_form(
                        "/auth/token",
                        &[
                            ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                            ("device_code", device_code),
                            ("client_id", "best-peer"),
                        ],
                    )
                    .await
                {
                    Ok(token) => {
                        if let Some(key) = token["access_token"].as_str() {
                            member.set_key(key.to_string());
                            self.save();
                            tracing::info!(peer = %member.name, "admitted by the member");
                            self.contact(member, true, "");
                            return;
                        }
                    }
                    Err(Refusal::Refused { body, .. }) => match body["error"].as_str() {
                        Some("authorization_pending") | Some("slow_down") => continue,
                        Some(other) => {
                            tracing::info!(peer = %member.name, %other, "the sign-in code is no longer good; a new one follows");
                            break;
                        }
                        None => continue,
                    },
                    Err(Refusal::Unreachable(why)) => {
                        tracing::debug!(peer = %member.name, %why, "the member did not answer the token poll");
                    }
                }
            }
        }
    }

    /// Squares the state with every member, all at once: a member that does not answer is told as lost, with every
    /// workload it held told as ended and every command in flight there failed; a workload a member no longer lists
    /// at two ticks running ended while nobody watched, and is told so; a member back after being lost that still
    /// runs a workload of this cloud's, told as ended meanwhile, is asked to end it, so the two say the same.
    pub async fn reconcile(&self) {
        futures::future::join_all(
            self.members
                .iter()
                .filter(|m| m.admitted())
                .map(|m| self.reconcile_member(m)),
        )
        .await;
    }

    async fn reconcile_member(&self, m: &Arc<Member>) {
        let held: Vec<(String, bool)> = {
            let s = self.state.lock().unwrap();
            s.deployments
                .values()
                .filter(|d| d.peer.as_deref() == Some(&m.name))
                .map(|d| (d.id.clone(), true))
                .chain(
                    s.jobs
                        .values()
                        .filter(|j| j.peer.as_deref() == Some(&m.name))
                        .map(|j| (j.id.clone(), false)),
                )
                .collect()
        };
        match m.query("get-workloads").await {
            Ok(w) => {
                let contact = self.contact(m, true, "");
                let runs = |id: &str, deployment: bool| {
                    let list = if deployment { &w["deployments"] } else { &w["jobs"] };
                    let field = if deployment { "deployment" } else { "job" };
                    list.as_array()
                        .is_some_and(|l| l.iter().any(|x| x[field].as_str() == Some(id)))
                };
                for (id, deployment) in &held {
                    if runs(id, *deployment) {
                        self.missing.lock().unwrap().remove(id);
                    } else if self.missing.lock().unwrap().insert(id.clone()) {
                        // Not listed for the first time: its end may still be on its way on the stream.
                    } else {
                        self.end(
                            id,
                            *deployment,
                            &format!("peer {} no longer runs it; its end was not observed", m.name),
                        );
                    }
                }
                if contact == Contact::Joined {
                    self.reclaim(m, &w, &held).await;
                }
            }
            Err(e) if e.is_unauthorized() => {
                tracing::warn!(peer = %m.name, "the member no longer honours this cloud's credential; signing in again");
                m.clear_key();
                self.save();
            }
            Err(e) => {
                if self.contact(m, false, &e.text()) == Contact::Lost {
                    self.lost(m, &held);
                }
            }
        }
    }

    /// A member is lost: every workload it held ends now, whatever its phase, and every command in flight there
    /// that no fact can complete any more is failed.
    fn lost(&self, m: &Member, held: &[(String, bool)]) {
        let why = format!("peer {} is unreachable", m.name);
        for (id, deployment) in held {
            self.end(id, *deployment, &why);
        }
        let in_flight: Vec<((String, String), Issued)> = {
            let mut issued = self.issued.lock().unwrap();
            let keys: Vec<(String, String)> = issued.keys().filter(|(p, _)| *p == m.name).cloned().collect();
            keys.into_iter()
                .filter_map(|k| issued.remove(&k).map(|i| (k, i)))
                .collect()
        };
        for ((_, mc), i) in in_flight {
            let key = i.em.command_key().to_string();
            if let Some(pull) = &i.pull {
                self.on_pull_fact(m, pull, &i.em, "ImagePullFailed", json!({ "reason": why }));
            } else if i.ends.is_some() || key.is_empty() {
                i.em.emit(
                    "CommandFailed",
                    json!({ "type": mc, "reason": format!("{why}; what it was to end is told as ended") }),
                );
            } else {
                let (sender, id) = Self::sender_of(&key);
                if self.log.workload_of(sender, id).is_none() {
                    // Never started there: nothing of it exists to be told as ended.
                    self.state.lock().unwrap().reserved.remove(&key);
                    i.em.emit(
                        "CommandFailed",
                        json!({ "type": mc, "reason": format!("{why} before the workload started; send it again when it answers") }),
                    );
                }
            }
        }
    }

    /// A member back after being lost still runs what this cloud told as ended meanwhile: the cloud ends it there.
    async fn reclaim(&self, m: &Arc<Member>, w: &Value, held: &[(String, bool)]) {
        let mine = self.holder_on(m);
        let known = |id: &str| held.iter().any(|(h, _)| h == id);
        let stray = |list: &Value, field: &str| -> Vec<String> {
            list.as_array()
                .map(|l| {
                    l.iter()
                        .filter(|x| x["holder"].as_str() == mine.as_deref())
                        .filter_map(|x| x[field].as_str().map(str::to_string))
                        .filter(|id| !known(id))
                        .collect()
                })
                .unwrap_or_default()
        };
        for id in stray(&w["deployments"], "deployment") {
            tracing::info!(peer = %m.name, deployment = %id, "the member still runs a deployment this cloud told as ended; ending it there");
            let em = self.emitter(&format!("peer-{}", m.name));
            let _ = self
                .place_on(m, &em, "UndeployModel", "undeploy-model", json!({ "deployment": id }))
                .await;
        }
        for id in stray(&w["jobs"], "job") {
            tracing::info!(peer = %m.name, job = %id, "the member still runs a job this cloud told as ended; stopping it there");
            let em = self.emitter(&format!("peer-{}", m.name));
            let _ = self.place_on(m, &em, "StopJob", "stop-job", json!({ "job": id })).await;
        }
    }

    /// Tells the end of a workload the cloud can no longer see, once; one already ending is ended too, since
    /// whatever was ending it cannot say so any more.
    fn end(&self, id: &str, deployment: bool, reason: &str) {
        let (correlation, model_or_image) = {
            let mut s = self.state.lock().unwrap();
            if deployment {
                match s.deployments.get_mut(id) {
                    Some(d) => {
                        d.phase = Phase::Ending;
                        (d.correlation.clone(), d.model.clone())
                    }
                    None => return,
                }
            } else {
                match s.jobs.get_mut(id) {
                    Some(j) => {
                        j.phase = JobPhase::Stopping;
                        (j.correlation.clone(), j.image.0.clone())
                    }
                    None => return,
                }
            }
        };
        self.missing.lock().unwrap().remove(id);
        let em = self.emitter(&correlation);
        if deployment {
            self.tell(
                &em,
                "DeploymentEnded",
                json!({ "deployment": id, "model": model_or_image, "exit_code": null, "reason": reason }),
            );
        } else {
            self.tell(
                &em,
                "JobFailed",
                json!({ "job": id, "image": model_or_image, "reason": reason, "fix": "run it again once the peer is back" }),
            );
        }
    }

    /// A placement refused with every member's room says which members were not asked, and why.
    fn with_peers(mut e: BestError, peers: Vec<Value>) -> BestError {
        if let Some(o) = e.details.as_object_mut() {
            o.insert("peers".into(), json!(peers));
        }
        e
    }
}

#[async_trait]
impl Service for Cloud {
    async fn hardware_state(&self) -> Result<Value, BestError> {
        let (host, peers) = self.combined().await;
        let mut v = crate::domain::host_state(&host);
        if let Some(devices) = v["devices"].as_array_mut() {
            for d in devices {
                if let Some((peer, _)) = d["id"].as_str().and_then(Self::split_device) {
                    d["peer"] = json!(peer);
                }
            }
        }
        v["peers"] = json!(peers);
        Ok(v)
    }

    fn workloads(&self, who: &Credential) -> Result<Value, BestError> {
        Ok(workloads_view(&self.state.lock().unwrap(), who, &self.endpoint))
    }

    fn admit_pull(&self, who: &Credential) -> Result<(), BestError> {
        require(who, "deploy")
    }

    async fn pull_image(self: Arc<Self>, cmd: Envelope, em: Emitter) {
        let image = cmd.data["image"].as_str().unwrap_or_default().to_string();
        let members = self.up();
        if members.is_empty() {
            self.tell(
                &em,
                "ImagePullFailed",
                json!({ "image": image, "reason": "no member peer answers", "fix": "get-hardware-state lists the members and their state" }),
            );
            return;
        }
        let key = em.command_key().to_string();
        self.pulls.lock().unwrap().insert(
            key.clone(),
            Pull {
                image: image.clone(),
                pending: members.iter().map(|m| m.name.clone()).collect(),
                failed: Vec::new(),
            },
        );
        for m in members {
            let base = m.schema_base().await;
            let (envelope, mc) = self.to_member(&base, &key, "PullImage", "pull-image", json!({ "image": image }));
            self.listen_for(&m.name, &mc, &em, Some(key.clone()), None);
            if let Err(e) = m.send(&envelope).await {
                self.issued.lock().unwrap().remove(&(m.name.clone(), mc));
                self.on_pull_fact(
                    &m,
                    &key,
                    &em,
                    "ImagePullFailed",
                    json!({ "image": image, "reason": e.relayed(&m.name).message }),
                );
            }
        }
    }

    async fn admit_deploy(&self, cmd: Envelope, who: Credential, key: String) -> Result<(), BestError> {
        require(&who, "deploy")?;
        let data: DeployData = parse(&cmd.data)?;
        if self.already_placed(&key).is_some() {
            return Ok(());
        }
        let (host, peers) = self.combined().await;
        let mut state = self.state.lock().unwrap();
        within_share(&state, &who, data.memory_bytes)?;
        let (device, compute) = place(
            &host,
            &state,
            &data.engine.runs_on(),
            data.memory_bytes,
            data.device.as_deref(),
            &format!("{} on {}", data.model, data.engine),
            mine(&who).as_deref(),
        )
        .map_err(|e| Self::with_peers(e, peers))?;
        state.reserved.insert(
            key,
            Reservation {
                id: new_id("dep"),
                device: Some(device),
                compute: Some(compute),
                memory_bytes: data.memory_bytes,
                holder: who.holder(),
            },
        );
        Ok(())
    }

    async fn deploy(self: Arc<Self>, cmd: Envelope, em: Emitter) {
        let Ok(data) = parse::<DeployData>(&cmd.data) else {
            return;
        };
        // Run again after a restart: the member runs it; listen for the rest of its facts and say nothing twice.
        if let Some(peer) = self.already_placed(em.command_key()) {
            let mc = member_correlation(em.command_key(), &json!({}));
            self.listen_for(&peer, &mc, &em, None, None);
            return;
        }
        let reservation = self.state.lock().unwrap().reserved.get(em.command_key()).cloned();
        let Some(Reservation {
            id,
            device: Some(device),
            ..
        }) = reservation
        else {
            self.tell(
                &em,
                "CommandFailed",
                json!({ "type": cmd.r#type, "reason": "the command was accepted but its placement is gone; send it again" }),
            );
            return;
        };
        let member = Self::split_device(&device.0).and_then(|(peer, d)| self.member(peer).map(|m| (m, d)));
        let Some((member, member_device)) = member else {
            self.tell(
                &em,
                "DeploymentFailed",
                json!({ "deployment": id, "model": data.model, "reason": format!("device {} names no member of this cloud", device.0), "fix": "get-hardware-state lists the devices; deploy again on one of them" }),
            );
            return;
        };
        let placed = self
            .place_on(
                &member,
                &em,
                "DeployModel",
                "deploy-model",
                json!({ "model": data.model, "engine": data.engine, "memory_bytes": data.memory_bytes, "device": member_device }),
            )
            .await;
        if let Err(e) = placed {
            self.tell(
                &em,
                "DeploymentFailed",
                json!({ "deployment": id, "model": data.model, "reason": e.message, "fix": "read the peer's refusal in the reason, then deploy again", "peer": member.name }),
            );
        }
    }

    fn admit_undeploy(&self, cmd: Envelope, who: &Credential) -> Result<(), BestError> {
        require(who, "deploy")?;
        let id = cmd.data["deployment"].as_str().unwrap_or_default();
        let mut s = self.state.lock().unwrap();
        let live: Vec<String> = s
            .deployments
            .values()
            .filter(|d| who.grant.is_all() || d.holder == who.holder())
            .map(|d| d.id.clone())
            .collect();
        match s.deployments.get_mut(id) {
            None => Err(BestError::bad_request(
                "NO_SUCH_DEPLOYMENT",
                format!(
                    "no deployment {id}: the live ones are {}; get-workloads lists them",
                    crate::lifecycle::list(live.iter())
                ),
            )
            .with_details(json!({ "deployment": id, "live": live }))),
            Some(d) if !who.grant.is_all() && d.holder != who.holder() => Err(not_yours("deployment", id)),
            Some(d) if d.phase == Phase::Ending => Err(BestError::bad_request(
                "ALREADY_ENDING",
                format!("deployment {id} is already ending: its Undeployed or DeploymentFailed event follows"),
            )
            .with_details(json!({ "deployment": id }))),
            Some(d) => {
                d.phase = Phase::Ending;
                Ok(())
            }
        }
    }

    async fn undeploy(self: Arc<Self>, cmd: Envelope, em: Emitter) {
        let id = cmd.data["deployment"].as_str().unwrap_or_default().to_string();
        let Some(d) = self.state.lock().unwrap().deployments.get(&id).cloned() else {
            self.tell(&em, "Undeployed", json!({ "deployment": id, "model": null }));
            return;
        };
        let member = d.peer.as_deref().and_then(|p| self.member(p));
        let outcome = match &member {
            Some(m) => {
                self.place_on(m, &em, "UndeployModel", "undeploy-model", json!({ "deployment": id }))
                    .await
            }
            None => Err(BestError::internal(format!("deployment {id} names no member peer"))),
        };
        if let Err(e) = outcome {
            if let Some(d) = self.state.lock().unwrap().deployments.get_mut(&id) {
                d.phase = if d.endpoint.is_some() {
                    Phase::Ready
                } else {
                    Phase::Starting
                };
            }
            self.tell(
                &em,
                "CommandFailed",
                json!({ "type": cmd.r#type, "reason": format!("{}; the deployment stays", e.message) }),
            );
        }
    }

    async fn admit_run_job(&self, cmd: Envelope, who: Credential, key: String) -> Result<(), BestError> {
        require(&who, "jobs")?;
        let data: RunJobData = parse(&cmd.data)?;
        if data.device.is_some() && data.memory_bytes.is_none() {
            return Err(BestError::bad_request(
                "DEVICE_NEEDS_MEMORY",
                "device names a device but memory_bytes is missing: send memory_bytes with it, or omit device to run with none",
            )
            .with_details(json!({ "field": "memory_bytes" })));
        }
        if self.already_placed(&key).is_some() {
            return Ok(());
        }
        let (host, peers) = self.combined().await;
        let mut state = self.state.lock().unwrap();
        within_share(&state, &who, data.memory_bytes.unwrap_or(0))?;
        let placement = match data.memory_bytes {
            Some(needed) => place(
                &host,
                &state,
                &any_compute(),
                needed,
                data.device.as_deref(),
                &format!("a job from {}", data.image),
                mine(&who).as_deref(),
            )
            .map_err(|e| Self::with_peers(e, peers))?,
            None => {
                // No device wanted: the first member answering takes it, which is the same one every time.
                let first = self.up().into_iter().next().ok_or_else(|| {
                    BestError::bad_request(
                        "NO_PEER",
                        "no member peer answers: get-hardware-state lists the members and their state",
                    )
                    .with_details(json!({ "peers": peers }))
                })?;
                (DeviceId(format!("{}/", first.name)), best_peer_gpu::Compute::Cpu)
            }
        };
        state.reserved.insert(
            key,
            Reservation {
                id: new_id("job"),
                device: Some(placement.0),
                compute: Some(placement.1),
                memory_bytes: data.memory_bytes.unwrap_or(0),
                holder: who.holder(),
            },
        );
        Ok(())
    }

    async fn run_job(self: Arc<Self>, cmd: Envelope, em: Emitter) {
        let Ok(data) = parse::<RunJobData>(&cmd.data) else {
            return;
        };
        if let Some(peer) = self.already_placed(em.command_key()) {
            let mc = member_correlation(em.command_key(), &json!({}));
            self.listen_for(&peer, &mc, &em, None, None);
            return;
        }
        let reservation = self.state.lock().unwrap().reserved.get(em.command_key()).cloned();
        let Some(Reservation {
            id,
            device: Some(device),
            ..
        }) = reservation
        else {
            self.tell(
                &em,
                "CommandFailed",
                json!({ "type": cmd.r#type, "reason": "the command was accepted but its placement is gone; send it again" }),
            );
            return;
        };
        let member = Self::split_device(&device.0).and_then(|(peer, d)| self.member(peer).map(|m| (m, d)));
        let Some((member, member_device)) = member else {
            self.tell(
                &em,
                "JobFailed",
                json!({ "job": id, "image": data.image, "reason": format!("device {} names no member of this cloud", device.0), "fix": "get-hardware-state lists the devices; run it again on one of them" }),
            );
            return;
        };
        let mut body = json!({ "image": data.image, "args": data.args });
        if let Some(m) = data.memory_bytes {
            body["memory_bytes"] = json!(m);
            body["device"] = json!(member_device);
        }
        if let Err(e) = self.place_on(&member, &em, "RunJob", "run-job", body).await {
            self.tell(
                &em,
                "JobFailed",
                json!({ "job": id, "image": data.image, "reason": e.message, "fix": "read the peer's refusal in the reason, then run it again", "peer": member.name }),
            );
        }
    }

    fn admit_stop_job(&self, cmd: Envelope, who: &Credential) -> Result<(), BestError> {
        require(who, "jobs")?;
        let id = cmd.data["job"].as_str().unwrap_or_default();
        let mut s = self.state.lock().unwrap();
        let running: Vec<String> = s
            .jobs
            .values()
            .filter(|j| who.grant.is_all() || j.holder == who.holder())
            .map(|j| j.id.clone())
            .collect();
        match s.jobs.get_mut(id) {
            None => Err(BestError::bad_request(
                "NO_SUCH_JOB",
                format!(
                    "no job {id} is running: the running ones are {}; get-workloads lists them",
                    crate::lifecycle::list(running.iter())
                ),
            )
            .with_details(json!({ "job": id, "running": running }))),
            Some(j) if !who.grant.is_all() && j.holder != who.holder() => Err(not_yours("job", id)),
            Some(j) if j.phase == JobPhase::Stopping => Err(BestError::bad_request(
                "ALREADY_STOPPING",
                format!("job {id} is already stopping: its JobStopped event follows"),
            )
            .with_details(json!({ "job": id }))),
            Some(j) => {
                j.phase = JobPhase::Stopping;
                Ok(())
            }
        }
    }

    async fn stop_job(self: Arc<Self>, cmd: Envelope, em: Emitter) {
        let id = cmd.data["job"].as_str().unwrap_or_default().to_string();
        let Some(j) = self.state.lock().unwrap().jobs.get(&id).cloned() else {
            self.tell(&em, "JobStopped", json!({ "job": id }));
            return;
        };
        let member = j.peer.as_deref().and_then(|p| self.member(p));
        let outcome = match &member {
            Some(m) => self.place_on(m, &em, "StopJob", "stop-job", json!({ "job": id })).await,
            None => Err(BestError::internal(format!("job {id} names no member peer"))),
        };
        if let Err(e) = outcome {
            if let Some(j) = self.state.lock().unwrap().jobs.get_mut(&id) {
                j.phase = JobPhase::Running;
            }
            self.tell(
                &em,
                "CommandFailed",
                json!({ "type": cmd.r#type, "reason": format!("{}; the job keeps running", e.message) }),
            );
        }
    }
}

impl Upstream for Cloud {
    fn upstream_of(&self, deployment: &str, who: &Credential) -> Result<Route, BestError> {
        let state = self.state.lock().unwrap();
        let d = ready_deployment(&state, deployment, who)?;
        let member = d
            .peer
            .as_deref()
            .and_then(|p| self.member(p))
            .ok_or_else(|| BestError::internal(format!("deployment {deployment} names no member peer")))?;
        Ok(Route {
            base: format!("{}/deployments/{deployment}", member.endpoint),
            key: member.key(),
            transport: member.transport.clone(),
        })
    }
}

/// The id and correlation a member is given for a cloud command: unique to the cloud command (its sender's holder
/// and its id, the id hashed when that would be too long for an envelope), or, for a command of the cloud's own,
/// the workload it ends.
fn member_correlation(key: &str, data: &Value) -> String {
    let (sender, id) = key.split_once(':').unwrap_or(("", key));
    match (sender.is_empty(), data["deployment"].as_str().or(data["job"].as_str())) {
        (true, Some(workload)) => format!("cloud-{workload}"),
        _ => {
            let holder = holder_of_hash(sender);
            if holder.len() + 1 + id.len() > 200 {
                format!("{holder}-{}", &hash_key(id)[..32])
            } else {
                format!("{holder}-{id}")
            }
        }
    }
}

/// The device memory a member has promised to its workloads, by its own device ids, from its `get-workloads`.
fn promised_by_device(w: &Value) -> HashMap<String, u64> {
    let mut out: HashMap<String, u64> = HashMap::new();
    for list in [&w["deployments"], &w["jobs"]] {
        for x in list.as_array().into_iter().flatten() {
            if let (Some(device), Some(bytes)) = (x["device"].as_str(), x["memory_bytes"].as_u64()) {
                *out.entry(device.to_string()).or_default() += bytes;
            }
        }
    }
    out
}

fn read_saved(file: &Path) -> Option<Vec<Saved>> {
    serde_json::from_str(&std::fs::read_to_string(file).ok()?).ok()
}

/// `BEST_PEER_MEMBERS`: `name=endpoint` or `endpoint` entries, comma-separated; a name not given is the endpoint's
/// host.
pub fn parse_members(text: &str) -> Result<Vec<(String, String)>, String> {
    let mut out: Vec<(String, String)> = Vec::new();
    for entry in text.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (name, endpoint) = match entry.split_once('=') {
            Some((n, e)) if !n.contains("://") => (n.trim().to_string(), e.trim().to_string()),
            _ => (host_of(entry), entry.to_string()),
        };
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err(format!(
                "BEST_PEER_MEMBERS entry {entry:?} is not a base URL: name=https://host:7777 or https://host:7777"
            ));
        }
        if name.is_empty() || name.contains('/') {
            return Err(format!(
                "BEST_PEER_MEMBERS entry {entry:?} has no usable name: name=https://host:7777"
            ));
        }
        if out.iter().any(|(n, _)| *n == name) {
            return Err(format!(
                "BEST_PEER_MEMBERS names {name} twice: give each member its own name"
            ));
        }
        out.push((name, endpoint.trim_end_matches('/').to_string()));
    }
    if out.is_empty() {
        return Err("BEST_PEER_MEMBERS is set but names no member".into());
    }
    Ok(out)
}

fn host_of(endpoint: &str) -> String {
    let host = endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or_default();
    match host.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or_default().to_string(),
        None => host.split(':').next().unwrap_or_default().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_member_list_names_each_member_once_by_its_host_or_as_told() {
        let m = parse_members("https://10.0.0.5:7777, b=http://10.0.0.6:7777/").unwrap();
        assert_eq!(
            m,
            [
                ("10.0.0.5".to_string(), "https://10.0.0.5:7777".to_string()),
                ("b".to_string(), "http://10.0.0.6:7777".to_string())
            ]
        );
        assert!(parse_members("10.0.0.5:7777").unwrap_err().contains("base URL"));
        assert!(parse_members("a=https://x, a=https://y").unwrap_err().contains("twice"));
        assert!(parse_members(" ").unwrap_err().contains("no member"));
    }

    #[test]
    fn a_member_command_is_named_for_the_cloud_command_and_fits_an_envelope() {
        let short = member_correlation("abcdef0123456789abcdef:d1", &json!({}));
        assert_eq!(short, "abcdef0123456789-d1");
        let long = member_correlation(&format!("abcdef0123456789abcdef:{}", "x".repeat(250)), &json!({}));
        assert!(long.len() <= 64, "{long}");
        assert!(long.starts_with("abcdef0123456789-"));
        assert_eq!(
            member_correlation(":anything", &json!({ "deployment": "dep-1" })),
            "cloud-dep-1"
        );
    }
}
