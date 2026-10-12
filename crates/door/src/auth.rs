//! Sign-in as the spec defines it (SPEC.md, Agent Registration): RFC 8628 on the agent's side, unchanged, with the
//! three BEST request parameters, the three token-answer members, the credential block and the sign-in guidance
//! verbatim. Who approves is not the spec's business and not this module's either: an [`Approver`] the peer chooses
//! turns a pending registration into an admitted agent. The credential is a secret the door generates, kept hashed,
//! shown once.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::envelope::now_rfc3339;
use crate::guidance;

/// The header a credential travels in.
pub const AUTH_HEADER: &str = "X-Api-Key";
/// What a key looks like, so a leak is recognisable in a transcript.
const KEY_PREFIX: &str = "bp_";
const DEVICE_CODE_TTL: Duration = Duration::from_secs(600);
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const SESSION_LIFETIME: Duration = Duration::from_secs(8 * 3600);
/// Letters and digits a person can read back without ambiguity.
const USER_CODE_ALPHABET: &[u8] = b"BCDFGHJKLMNPQRSTVWXZ23456789";

/// An error the two OAuth endpoints answer with (RFC 6749 §5.2): never the BEST error format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthError {
    pub status: u16,
    pub error: &'static str,
    pub description: String,
}

impl OAuthError {
    pub fn invalid_request(description: impl Into<String>) -> Self {
        Self {
            status: 400,
            error: "invalid_request",
            description: description.into(),
        }
    }

    pub fn body(&self) -> Value {
        json!({ "error": self.error, "error_description": self.description })
    }
}

/// An admitted agent's credential, as the door keeps it: the hash, never the key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Credential {
    /// The connection's name, the person's label for this sign-in.
    pub name: String,
    pub hash: String,
    pub client_id: String,
    pub agent_label: Option<String>,
    pub issued_at: String,
    /// Absent for a durable credential.
    pub expires_at: Option<String>,
}

/// `durable` (default) or `session` (SPEC.md: a consumer that cannot keep a secret out of a conversation).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lifetime {
    Durable,
    Session,
}

#[derive(Clone, Debug)]
pub struct Pending {
    pub device_code: String,
    pub user_code: String,
    pub client_id: String,
    pub connection: Option<String>,
    pub agent_label: Option<String>,
    pub lifetime: Lifetime,
    created: Instant,
    last_poll: Option<Instant>,
    approved: bool,
}

/// The device authorization answer (RFC 8628 §3.2) plus BEST's `note`.
#[derive(Clone, Debug, Serialize)]
pub struct DeviceAnswer {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub verification_uri_complete: String,
    pub expires_in: u64,
    pub interval: u64,
    pub note: String,
}

/// The device request's parameters, form-encoded by the agent.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct DeviceRequest {
    pub client_id: Option<String>,
    pub connection: Option<String>,
    pub agent_label: Option<String>,
    pub credential_lifetime: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TokenRequest {
    pub grant_type: Option<String>,
    pub device_code: Option<String>,
    pub client_id: Option<String>,
}

/// The authority that admits an agent. The spec leaves the person's side of sign-in to the platform; a peer is one
/// person's machine, so the default authority is whoever controls that machine. Another approver (an identity
/// provider, any of them) is an adapter behind this trait; the door never knows which.
#[async_trait]
pub trait Approver: Send + Sync {
    /// What the verification page tells the person to do for this user code.
    fn instructions(&self, user_code: &str) -> String;
    /// Admits the registration `user_code` names when `proof` is the authority's: an error says why not.
    async fn admit(&self, user_code: &str, proof: &str) -> Result<(), OAuthError>;
}

/// Approval by whoever controls the machine: a token the peer wrote to its data directory, which only that person
/// can read, presented back through `best-peer approve <code>`.
pub struct LocalApprover {
    token: String,
}

impl LocalApprover {
    /// Reads the admin token in `dir`, writing a fresh one first when there is none.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("admin-token");
        let token = match std::fs::read_to_string(&path) {
            Ok(t) if !t.trim().is_empty() => t.trim().to_string(),
            _ => {
                let token = random_token(32);
                let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&path)?;
                f.write_all(token.as_bytes())?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
                }
                token
            }
        };
        Ok(Self { token })
    }

    pub fn from_token(token: &str) -> Self {
        Self { token: token.into() }
    }
}

#[async_trait]
impl Approver for LocalApprover {
    fn instructions(&self, user_code: &str) -> String {
        format!(
            "This peer admits agents on the word of whoever controls the machine it runs on. On that machine, run: best-peer approve {user_code}"
        )
    }

    async fn admit(&self, _user_code: &str, proof: &str) -> Result<(), OAuthError> {
        if bool::from(proof.as_bytes().ct_eq(self.token.as_bytes())) {
            Ok(())
        } else {
            Err(OAuthError {
                status: 403,
                error: "access_denied",
                description: "the proof is not this peer's admin token".into(),
            })
        }
    }
}

/// For tests: admits any code when the proof is the one it was built with.
pub struct FakeApprover {
    pub proof: String,
}

#[async_trait]
impl Approver for FakeApprover {
    fn instructions(&self, user_code: &str) -> String {
        format!("fake approver: approve {user_code}")
    }

    async fn admit(&self, _user_code: &str, proof: &str) -> Result<(), OAuthError> {
        if proof == self.proof {
            Ok(())
        } else {
            Err(OAuthError {
                status: 403,
                error: "access_denied",
                description: "wrong proof".into(),
            })
        }
    }
}

pub struct Auth {
    endpoint: String,
    approver: Box<dyn Approver>,
    credentials: Mutex<Vec<Credential>>,
    credentials_file: Mutex<Option<std::fs::File>>,
    pending: Mutex<HashMap<String, Pending>>,
}

impl Auth {
    /// Credentials kept only in memory; for tests and for a peer run without a data directory.
    pub fn ephemeral(endpoint: &str, approver: Box<dyn Approver>) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').into(),
            approver,
            credentials: Mutex::new(Vec::new()),
            credentials_file: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Credentials kept in `dir/credentials.jsonl`, hashes only.
    pub fn open(dir: &Path, endpoint: &str, approver: Box<dyn Approver>) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let auth = Self::ephemeral(endpoint, approver);
        let path = dir.join("credentials.jsonl");
        if path.exists() {
            for line in std::fs::read_to_string(&path)?.lines() {
                if let Ok(c) = serde_json::from_str::<Credential>(line) {
                    auth.credentials.lock().unwrap().push(c);
                }
            }
        }
        *auth.credentials_file.lock().unwrap() = Some(OpenOptions::new().create(true).append(true).open(&path)?);
        Ok(auth)
    }

    pub fn device_authorization_url(&self) -> String {
        format!("{}/auth/device", self.endpoint)
    }

    pub fn token_url(&self) -> String {
        format!("{}/auth/token", self.endpoint)
    }

    pub fn verification_uri(&self) -> String {
        format!("{}/auth/verify", self.endpoint)
    }

    /// The credential the key opens, if it is one this door issued and it has not expired.
    pub fn authenticate(&self, key: &str) -> Option<Credential> {
        let hash = hash_key(key);
        let now = now_rfc3339();
        self.credentials
            .lock()
            .unwrap()
            .iter()
            .find(|c| bool::from(c.hash.as_bytes().ct_eq(hash.as_bytes())))
            .filter(|c| c.expires_at.as_ref().is_none_or(|e| e.as_str() > now.as_str()))
            .cloned()
    }

    /// RFC 8628 §3.1–3.2 with BEST's parameters: a `connection` of another shape is refused with the shape.
    pub fn start_device(&self, req: DeviceRequest) -> Result<DeviceAnswer, OAuthError> {
        let client_id = match req.client_id.as_deref().map(str::trim) {
            Some(c) if !c.is_empty() => c.to_string(),
            _ => {
                return Err(OAuthError::invalid_request(
                    "client_id is required: the name of the consumer software",
                ))
            }
        };
        if let Some(c) = &req.connection {
            if !is_connection_name(c) {
                return Err(OAuthError::invalid_request(
                    "connection must be one word of lowercase letters, digits and hyphens, 1 to 64 characters, starting with a letter or digit (acme-payroll-dev)",
                ));
            }
        }
        let lifetime = match req.credential_lifetime.as_deref() {
            None | Some("durable") => Lifetime::Durable,
            Some("session") => Lifetime::Session,
            Some(other) => {
                return Err(OAuthError::invalid_request(format!(
                    "credential_lifetime must be durable or session, not {other:?}"
                )))
            }
        };
        self.expire_pending();
        let device_code = random_token(32);
        let user_code = random_user_code();
        let pending = Pending {
            device_code: device_code.clone(),
            user_code: user_code.clone(),
            client_id,
            connection: req.connection,
            agent_label: req.agent_label,
            lifetime,
            created: Instant::now(),
            last_poll: None,
            approved: false,
        };
        self.pending.lock().unwrap().insert(device_code.clone(), pending);
        Ok(DeviceAnswer {
            device_code,
            user_code: user_code.clone(),
            verification_uri: self.verification_uri(),
            verification_uri_complete: format!("{}?user_code={user_code}", self.verification_uri()),
            expires_in: DEVICE_CODE_TTL.as_secs(),
            interval: POLL_INTERVAL.as_secs(),
            note: guidance::DEVICE_TEXT.into(),
        })
    }

    /// The page the person opens: the code and what to do with it.
    pub fn verification_page(&self, user_code: Option<&str>) -> String {
        let code = user_code.unwrap_or("");
        let known = self
            .pending
            .lock()
            .unwrap()
            .values()
            .any(|p| p.user_code == code && !p.approved);
        let body = if code.is_empty() {
            "<p>Enter the code your agent showed you in the address: <code>?user_code=XXXX-XXXX</code>.</p>".to_string()
        } else if !known {
            format!(
                "<p>Code <code>{code}</code> is unknown, already used, or expired. Ask your agent to start again.</p>"
            )
        } else {
            format!(
                "<p>An agent asks to be admitted with code <code>{code}</code>.</p><p>{}</p>",
                html_escape(&self.approver.instructions(code))
            )
        };
        format!(
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>best-peer: admit an agent</title></head><body style=\"font-family:system-ui;max-width:40em;margin:3em auto\"><h1>Admit an agent</h1>{body}</body></html>"
        )
    }

    /// Marks a registration approved, on the approver's word.
    pub async fn approve(&self, user_code: &str, proof: &str) -> Result<(), OAuthError> {
        self.expire_pending();
        let exists = self
            .pending
            .lock()
            .unwrap()
            .values()
            .any(|p| p.user_code == user_code && !p.approved);
        if !exists {
            return Err(OAuthError::invalid_request(format!(
                "no pending registration has the code {user_code}"
            )));
        }
        self.approver.admit(user_code, proof).await?;
        if let Some(p) = self
            .pending
            .lock()
            .unwrap()
            .values_mut()
            .find(|p| p.user_code == user_code)
        {
            p.approved = true;
        }
        Ok(())
    }

    /// RFC 8628 §3.4–3.5: the token answer once approved, `authorization_pending` or `slow_down` until then, and
    /// `expired_token` or `invalid_grant` for a code that is not live. A `device_code` is redeemed once.
    pub fn token(&self, req: TokenRequest) -> Result<Value, OAuthError> {
        if req.grant_type.as_deref() != Some("urn:ietf:params:oauth:grant-type:device_code") {
            return Err(OAuthError {
                status: 400,
                error: "unsupported_grant_type",
                description: "grant_type must be urn:ietf:params:oauth:grant-type:device_code".into(),
            });
        }
        let Some(device_code) = req.device_code.filter(|d| !d.is_empty()) else {
            return Err(OAuthError::invalid_request("device_code is required"));
        };
        self.expire_pending();
        let mut pending = self.pending.lock().unwrap();
        let Some(p) = pending.get_mut(&device_code) else {
            return Err(OAuthError {
                status: 400,
                error: "invalid_grant",
                description: "unknown device_code".into(),
            });
        };
        if req.client_id.as_deref().is_some_and(|c| c != p.client_id) {
            return Err(OAuthError {
                status: 400,
                error: "invalid_grant",
                description: "client_id is not the one that started this registration".into(),
            });
        }
        if p.last_poll.is_some_and(|t| t.elapsed() < POLL_INTERVAL) {
            p.last_poll = Some(Instant::now());
            return Err(OAuthError {
                status: 400,
                error: "slow_down",
                description: format!("poll no faster than every {} seconds", POLL_INTERVAL.as_secs()),
            });
        }
        p.last_poll = Some(Instant::now());
        if !p.approved {
            return Err(OAuthError {
                status: 400,
                error: "authorization_pending",
                description: "the person has not approved yet".into(),
            });
        }
        let p = pending.remove(&device_code).expect("just found");
        drop(pending);
        let key = format!("{KEY_PREFIX}{}", random_token(32));
        let name = p.connection.clone().unwrap_or_else(|| host_of(&self.endpoint));
        let expires_at = match p.lifetime {
            Lifetime::Durable => None,
            Lifetime::Session => Some(rfc3339_in(SESSION_LIFETIME)),
        };
        let credential = Credential {
            name: name.clone(),
            hash: hash_key(&key),
            client_id: p.client_id,
            agent_label: p.agent_label,
            issued_at: now_rfc3339(),
            expires_at: expires_at.clone(),
        };
        self.store(&credential);
        let mut answer = json!({
            "access_token": key,
            "token_type": "apikey",
            "auth_header": AUTH_HEADER,
            "name": name,
            "tenant_id": host_of(&self.endpoint),
            "note": format!("{} This peer admitted you on the word of whoever controls it; the same person can revoke this credential on the machine.", guidance::TOKEN_TEXT),
            "endpoint": self.endpoint,
            "manifest": format!("{}/.well-known/best", self.endpoint),
            "ready_check": format!("{}/queries", self.endpoint),
            "mcp": {
                name: {
                    "command": "npx",
                    "args": ["-y", "@behavioralstate/best-mcp"],
                    "env": {
                        "BEST_ENDPOINT": self.endpoint,
                        "BEST_API_KEY": key,
                        "BEST_AUTH_TYPE": "apikey",
                        "BEST_AUTH_HEADER": AUTH_HEADER
                    }
                }
            },
            "http": {
                "header": format!("{AUTH_HEADER}: <access_token>"),
                "commands": format!("{}/commands", self.endpoint),
                "queries": format!("{}/queries", self.endpoint),
                "events": format!("{}/events", self.endpoint)
            }
        });
        if let Some(e) = expires_at {
            answer["expires_in"] = json!(SESSION_LIFETIME.as_secs());
            answer["expires_at"] = json!(e);
        }
        Ok(answer)
    }

    pub fn credentials(&self) -> Vec<Credential> {
        self.credentials.lock().unwrap().clone()
    }

    fn store(&self, c: &Credential) {
        if let Some(f) = self.credentials_file.lock().unwrap().as_mut() {
            let line = serde_json::to_string(c).unwrap();
            f.write_all(line.as_bytes())
                .and_then(|_| f.write_all(b"\n"))
                .expect("credentials are writable");
            f.flush().expect("credentials flush");
        }
        self.credentials.lock().unwrap().push(c.clone());
    }

    fn expire_pending(&self) {
        self.pending
            .lock()
            .unwrap()
            .retain(|_, p| p.created.elapsed() < DEVICE_CODE_TTL);
    }
}

/// `^[a-z0-9][a-z0-9-]{0,63}$`
pub fn is_connection_name(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 64
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

pub fn hash_key(key: &str) -> String {
    hex::encode(Sha256::digest(key.as_bytes()))
}

fn random_token(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut buf);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

fn random_user_code() -> String {
    let mut buf = [0u8; 8];
    rand::rng().fill_bytes(&mut buf);
    let chars: Vec<char> = buf
        .iter()
        .map(|b| USER_CODE_ALPHABET[(*b as usize) % USER_CODE_ALPHABET.len()] as char)
        .collect();
    format!(
        "{}-{}",
        chars[..4].iter().collect::<String>(),
        chars[4..].iter().collect::<String>()
    )
}

fn host_of(endpoint: &str) -> String {
    endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or(endpoint)
        .to_string()
}

fn rfc3339_in(d: Duration) -> String {
    (time::OffsetDateTime::now_utc() + d)
        .format(&time::format_description::well_known::Rfc3339)
        .expect("rfc3339")
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth() -> Auth {
        Auth::ephemeral("http://peer.test", Box::new(FakeApprover { proof: "ok".into() }))
    }

    fn start(auth: &Auth, connection: Option<&str>) -> DeviceAnswer {
        auth.start_device(DeviceRequest {
            client_id: Some("best-validate".into()),
            connection: connection.map(String::from),
            agent_label: Some("a test".into()),
            credential_lifetime: None,
        })
        .unwrap()
    }

    fn poll(auth: &Auth, device_code: &str) -> Result<Value, OAuthError> {
        auth.token(TokenRequest {
            grant_type: Some("urn:ietf:params:oauth:grant-type:device_code".into()),
            device_code: Some(device_code.into()),
            client_id: Some("best-validate".into()),
        })
    }

    #[test]
    fn the_device_answer_is_rfc_8628_with_a_service_generated_code_and_the_spec_words() {
        let a = start(&auth(), Some("my-peer"));
        assert!(a.device_code.len() >= 20);
        assert_ne!(a.device_code, a.user_code);
        assert_eq!(a.user_code.len(), 9);
        assert!(!a.verification_uri_complete.contains(&a.device_code));
        assert!(a
            .verification_uri_complete
            .ends_with(&format!("user_code={}", a.user_code)));
        assert_eq!(a.note, guidance::DEVICE_TEXT);
        assert_eq!(a.interval, 5);
    }

    #[test]
    fn a_connection_of_another_shape_is_refused_with_the_shape() {
        let err = auth()
            .start_device(DeviceRequest {
                client_id: Some("x".into()),
                connection: Some("Not One Word".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.error, "invalid_request");
        assert!(
            err.description.contains("lowercase letters, digits and hyphens"),
            "{}",
            err.description
        );
        assert!(is_connection_name("acme-payroll-dev"));
        assert!(!is_connection_name("-x"));
        assert!(!is_connection_name(""));
    }

    #[tokio::test]
    async fn pending_then_approved_then_a_credential_that_opens_the_door_once() {
        let auth = auth();
        let a = start(&auth, Some("my-peer"));
        let pending = poll(&auth, &a.device_code).unwrap_err();
        assert_eq!(pending.error, "authorization_pending");
        assert_eq!(poll(&auth, &a.device_code).unwrap_err().error, "slow_down");
        assert_eq!(
            auth.approve(&a.user_code, "wrong").await.unwrap_err().error,
            "access_denied"
        );
        auth.approve(&a.user_code, "ok").await.unwrap();
        // The slow_down poll counted as a poll; wait out the interval in test time by resetting.
        auth.pending.lock().unwrap().get_mut(&a.device_code).unwrap().last_poll = None;
        let answer = poll(&auth, &a.device_code).unwrap();
        let key = answer["access_token"].as_str().unwrap().to_string();
        assert!(key.starts_with("bp_"));
        assert_eq!(answer["name"], "my-peer");
        assert_eq!(answer["auth_header"], "X-Api-Key");
        assert!(answer["note"].as_str().unwrap().starts_with(guidance::TOKEN_TEXT));
        assert_eq!(answer["mcp"]["my-peer"]["env"]["BEST_API_KEY"], key);
        assert!(
            !answer["http"].to_string().contains(&key),
            "the key appears only in access_token and mcp"
        );
        assert!(answer.get("expires_in").is_none(), "durable");
        let who = auth.authenticate(&key).unwrap();
        assert_eq!(who.name, "my-peer");
        assert_eq!(who.hash, hash_key(&key));
        assert!(auth.authenticate("bp_nope").is_none());
        assert_eq!(
            poll(&auth, &a.device_code).unwrap_err().error,
            "invalid_grant",
            "redeemed once"
        );
    }

    #[tokio::test]
    async fn a_session_credential_expires_and_a_missing_connection_is_named_after_the_host() {
        let auth = auth();
        let a = auth
            .start_device(DeviceRequest {
                client_id: Some("best-validate".into()),
                credential_lifetime: Some("session".into()),
                ..Default::default()
            })
            .unwrap();
        auth.approve(&a.user_code, "ok").await.unwrap();
        let answer = poll(&auth, &a.device_code).unwrap();
        assert_eq!(answer["name"], "peer.test");
        assert_eq!(answer["expires_in"], 8 * 3600);
        let c = auth.credentials();
        assert!(c[0].expires_at.is_some());
    }

    #[test]
    fn an_unknown_code_is_invalid_grant_and_the_wrong_grant_type_is_named() {
        let auth = auth();
        assert_eq!(poll(&auth, "best-validate-unknown").unwrap_err().error, "invalid_grant");
        let err = auth
            .token(TokenRequest {
                grant_type: Some("password".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.error, "unsupported_grant_type");
    }

    #[test]
    fn the_verification_page_knows_a_live_code_and_tells_the_person_what_to_do() {
        let auth = auth();
        let a = start(&auth, None);
        let page = auth.verification_page(Some(&a.user_code));
        assert!(page.contains(&a.user_code));
        assert!(page.contains("fake approver: approve"));
        assert!(auth.verification_page(Some("ZZZZ-ZZZZ")).contains("unknown"));
    }

    #[tokio::test]
    async fn credentials_survive_a_restart_as_hashes_only() {
        let dir = tempfile::tempdir().unwrap();
        let first = Auth::open(
            dir.path(),
            "http://peer.test",
            Box::new(FakeApprover { proof: "ok".into() }),
        )
        .unwrap();
        let a = start(&first, Some("kept"));
        first.approve(&a.user_code, "ok").await.unwrap();
        let key = poll(&first, &a.device_code).unwrap()["access_token"]
            .as_str()
            .unwrap()
            .to_string();
        drop(first);
        let file = std::fs::read_to_string(dir.path().join("credentials.jsonl")).unwrap();
        assert!(!file.contains(&key), "the key is never written");
        let again = Auth::open(
            dir.path(),
            "http://peer.test",
            Box::new(FakeApprover { proof: "ok".into() }),
        )
        .unwrap();
        assert_eq!(again.authenticate(&key).unwrap().name, "kept");
    }

    #[test]
    fn the_local_approver_keeps_its_token_in_the_data_directory_and_reads_it_back() {
        let dir = tempfile::tempdir().unwrap();
        let a = LocalApprover::open(dir.path()).unwrap();
        let b = LocalApprover::open(dir.path()).unwrap();
        assert_eq!(a.token, b.token);
        assert!(a.token.len() >= 40);
        assert!(a.instructions("ABCD-EFGH").contains("best-peer approve ABCD-EFGH"));
    }
}
