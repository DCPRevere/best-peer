//! The event log: every fact the door has told, in order, kept on disk and streamed live. `GET /events` reads it,
//! `GET /events/stream` follows it, and a reconnecting client replays from `Last-Event-ID`. The log also keeps the
//! commands it accepted, per sender, so an envelope `id` is an idempotency key across restarts.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::broadcast;

use crate::envelope::Envelope;

/// An event with its position in the log; the position is the stream's order and the pagination cursor.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stored {
    pub seq: u64,
    pub envelope: Envelope,
}

/// What `GET /events` and the stream filter on (SPEC.md, Query Parameters).
#[derive(Clone, Debug, Default)]
pub struct Filter {
    pub r#type: Option<String>,
    pub correlation_id: Option<String>,
    pub source: Option<String>,
    pub from: Option<OffsetDateTime>,
    pub to: Option<OffsetDateTime>,
    pub limit: Option<usize>,
    /// The cursor from a previous page: the `seq` after which to read.
    pub after: Option<u64>,
}

/// The most events one page carries; a larger `limit` is refused, never clamped.
pub const MAX_PAGE: usize = 200;

impl Filter {
    pub fn matches(&self, s: &Stored) -> bool {
        let e = &s.envelope;
        if self.r#type.as_ref().is_some_and(|t| &e.r#type != t) {
            return false;
        }
        if self
            .correlation_id
            .as_ref()
            .is_some_and(|c| e.correlationid.as_ref() != Some(c))
        {
            return false;
        }
        if self.source.as_ref().is_some_and(|src| &e.source != src) {
            return false;
        }
        if self.from.is_some() || self.to.is_some() {
            let Ok(t) = OffsetDateTime::parse(&e.time, &Rfc3339) else {
                return false;
            };
            if self.from.is_some_and(|f| t < f) || self.to.is_some_and(|to| t > to) {
                return false;
            }
        }
        true
    }
}

/// The verdict on a command's `id` from one sender (SPEC.md, Ingestion Semantics: duplicates are ignored, a reused
/// id with a different payload is a conflict). A duplicate carries the correlation the original was given, so the
/// answer to a retry points at the same events.
#[derive(Debug, PartialEq, Eq)]
pub enum Accept {
    New,
    Duplicate { correlation: String },
    Conflict,
}

#[derive(Clone, Serialize, Deserialize)]
struct AcceptedLine {
    sender: String,
    id: String,
    hash: String,
    correlation: String,
}

pub struct EventLog {
    events: Mutex<Vec<Stored>>,
    accepted: Mutex<HashMap<(String, String), AcceptedLine>>,
    events_file: Mutex<Option<File>>,
    accepted_file: Mutex<Option<File>>,
    tx: broadcast::Sender<Stored>,
}

impl EventLog {
    /// An in-memory log that forgets everything on restart; for tests and for a peer run without a data directory.
    pub fn ephemeral() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            events: Mutex::new(Vec::new()),
            accepted: Mutex::new(HashMap::new()),
            events_file: Mutex::new(None),
            accepted_file: Mutex::new(None),
            tx,
        }
    }

    /// A log kept in `dir` (`events.jsonl`, `accepted.jsonl`), loaded now and appended to from here on. A line that
    /// cannot be read (a torn write) is skipped and logged, never fatal.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let log = Self::ephemeral();
        let events_path = dir.join("events.jsonl");
        let accepted_path = dir.join("accepted.jsonl");
        for line in read_lines(&events_path)? {
            match serde_json::from_str::<Stored>(&line) {
                Ok(s) => log.events.lock().unwrap().push(s),
                Err(e) => tracing::warn!(error = %e, "an event line could not be read and is skipped"),
            }
        }
        for line in read_lines(&accepted_path)? {
            match serde_json::from_str::<AcceptedLine>(&line) {
                Ok(a) => {
                    log.accepted.lock().unwrap().insert((a.sender.clone(), a.id.clone()), a);
                }
                Err(e) => tracing::warn!(error = %e, "an accepted-command line could not be read and is skipped"),
            }
        }
        *log.events_file.lock().unwrap() = Some(append_to(&events_path)?);
        *log.accepted_file.lock().unwrap() = Some(append_to(&accepted_path)?);
        Ok(log)
    }

    /// Appends an event and tells every subscriber, in the order of the log. The write to disk happens before anyone
    /// hears of it; a disk error is logged and the event still reaches subscribers, since a fact told cannot be
    /// untold.
    pub fn append(&self, envelope: Envelope) -> Stored {
        let mut events = self.events.lock().unwrap();
        let seq = events.last().map(|s| s.seq + 1).unwrap_or(1);
        let stored = Stored { seq, envelope };
        write_line(&self.events_file, &stored, "event");
        events.push(stored.clone());
        let _ = self.tx.send(stored.clone());
        drop(events);
        stored
    }

    /// What the log already knows of `sender`'s command `id` with payload hash `hash`: a duplicate, a conflict, or
    /// nothing. Nothing is recorded; a command is recorded by [`EventLog::accept_command`] once admitted.
    pub fn known_command(&self, sender: &str, id: &str, hash: &str) -> Option<Accept> {
        let accepted = self.accepted.lock().unwrap();
        match accepted.get(&(sender.to_string(), id.to_string())) {
            Some(a) if a.hash == hash => Some(Accept::Duplicate {
                correlation: a.correlation.clone(),
            }),
            Some(_) => Some(Accept::Conflict),
            None => None,
        }
    }

    /// Every event the log holds, oldest first, for a peer rebuilding its state from the facts it told.
    pub fn replay(&self, mut f: impl FnMut(&Envelope)) {
        let events = self.events.lock().unwrap();
        for s in events.iter() {
            f(&s.envelope);
        }
    }

    /// Records that `sender` sent a command with `id`, payload hash `hash` and correlation `correlation`, or says
    /// why it is not new. One sender's ids are theirs alone: another sender's identical id is a different command.
    pub fn accept_command(&self, sender: &str, id: &str, hash: &str, correlation: &str) -> Accept {
        let mut accepted = self.accepted.lock().unwrap();
        let key = (sender.to_string(), id.to_string());
        match accepted.get(&key) {
            Some(a) if a.hash == hash => Accept::Duplicate {
                correlation: a.correlation.clone(),
            },
            Some(_) => Accept::Conflict,
            None => {
                let line = AcceptedLine {
                    sender: sender.into(),
                    id: id.into(),
                    hash: hash.into(),
                    correlation: correlation.into(),
                };
                write_line(&self.accepted_file, &line, "accepted command");
                accepted.insert(key, line);
                Accept::New
            }
        }
    }

    /// A page of the log, oldest first, and the cursor for the next page when there is one. `limit` is what the
    /// caller asked for and already checked (1 to [`MAX_PAGE`]); the default is 50.
    pub fn list(&self, filter: &Filter) -> (Vec<Envelope>, Option<String>) {
        let limit = filter.limit.unwrap_or(50).clamp(1, MAX_PAGE);
        let events = self.events.lock().unwrap();
        let mut page: Vec<&Stored> = events
            .iter()
            .filter(|s| filter.after.is_none_or(|a| s.seq > a))
            .filter(|s| filter.matches(s))
            .take(limit + 1)
            .collect();
        let next = if page.len() > limit {
            page.truncate(limit);
            page.last().map(|s| s.seq.to_string())
        } else {
            None
        };
        (page.into_iter().map(|s| s.envelope.clone()).collect(), next)
    }

    /// Everything after the event with `id`, for a reconnecting stream. An id this log never told is not a position
    /// in it, so nothing is replayed: the client reads history with `GET /events` and follows from now.
    pub fn since_id(&self, id: &str) -> Vec<Stored> {
        let events = self.events.lock().unwrap();
        match events.iter().position(|s| s.envelope.id == id) {
            Some(i) => events[i + 1..].to_vec(),
            None => Vec::new(),
        }
    }

    /// The live feed, with the position of the last event already in the log, so a subscriber that also replays can
    /// skip what it has seen: subscribe first, then read, and the two never overlap or miss.
    pub fn subscribe(&self) -> (broadcast::Receiver<Stored>, u64) {
        let events = self.events.lock().unwrap();
        (self.tx.subscribe(), events.last().map(|s| s.seq).unwrap_or(0))
    }

    pub fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One record, one line, one write; a failure is logged, never a panic that would poison the log for everyone.
fn write_line<T: Serialize>(file: &Mutex<Option<File>>, record: &T, what: &str) {
    if let Some(f) = file.lock().unwrap().as_mut() {
        let mut line = serde_json::to_string(record).expect("a record serialises");
        line.push('\n');
        if let Err(e) = f.write_all(line.as_bytes()).and_then(|_| f.flush()) {
            tracing::error!(error = %e, "{what} could not be written to disk; it lasts until restart");
        }
    }
}

fn read_lines(path: &Path) -> std::io::Result<Vec<String>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = String::from_utf8_lossy(&std::fs::read(path)?).into_owned();
    Ok(text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(str::to_string)
        .collect())
}

fn append_to(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ev(r#type: &str, corr: &str) -> Envelope {
        Envelope::event("io.best.agents", r#type, json!({}), Some(corr.into()), None)
    }

    #[test]
    fn events_are_numbered_in_order_and_paged_with_a_cursor() {
        let log = EventLog::ephemeral();
        for i in 0..5 {
            log.append(ev("Ticked", &format!("c{i}")));
        }
        let (page, next) = log.list(&Filter {
            limit: Some(2),
            ..Default::default()
        });
        assert_eq!(page.len(), 2);
        assert_eq!(next.as_deref(), Some("2"));
        let (page2, next2) = log.list(&Filter {
            limit: Some(2),
            after: Some(2),
            ..Default::default()
        });
        assert_eq!(page2[0].correlationid.as_deref(), Some("c2"));
        assert_eq!(next2.as_deref(), Some("4"));
        let (last, none) = log.list(&Filter {
            limit: Some(2),
            after: Some(4),
            ..Default::default()
        });
        assert_eq!(last.len(), 1);
        assert!(none.is_none());
    }

    #[test]
    fn filters_narrow_by_type_correlation_and_source() {
        let log = EventLog::ephemeral();
        log.append(ev("ImagePulled", "a"));
        log.append(ev("ImagePullFailed", "a"));
        log.append(ev("ImagePulled", "b"));
        let (by_corr, _) = log.list(&Filter {
            correlation_id: Some("a".into()),
            ..Default::default()
        });
        assert_eq!(by_corr.len(), 2);
        let (by_type, _) = log.list(&Filter {
            r#type: Some("ImagePulled".into()),
            ..Default::default()
        });
        assert_eq!(by_type.len(), 2);
        let (none, _) = log.list(&Filter {
            source: Some("elsewhere".into()),
            ..Default::default()
        });
        assert!(none.is_empty());
    }

    #[test]
    fn an_id_is_an_idempotency_key_per_sender_and_a_different_payload_is_a_conflict() {
        let log = EventLog::ephemeral();
        assert_eq!(log.accept_command("alice", "x", "h1", "corr-1"), Accept::New);
        assert_eq!(
            log.accept_command("alice", "x", "h1", "corr-2"),
            Accept::Duplicate {
                correlation: "corr-1".into()
            },
            "the retry is told the original correlation"
        );
        assert_eq!(log.accept_command("alice", "x", "h2", "corr-3"), Accept::Conflict);
        assert_eq!(
            log.accept_command("bob", "x", "h1", "corr-4"),
            Accept::New,
            "another sender's id is their own"
        );
    }

    #[test]
    fn a_reconnect_replays_what_came_after_the_last_seen_id() {
        let log = EventLog::ephemeral();
        let a = log.append(ev("A", "c"));
        let _ = log.append(ev("B", "c"));
        let _ = log.append(ev("C", "c"));
        let after = log.since_id(&a.envelope.id);
        assert_eq!(
            after.iter().map(|s| s.envelope.r#type.as_str()).collect::<Vec<_>>(),
            vec!["B", "C"]
        );
        assert_eq!(
            log.since_id("never-seen").len(),
            0,
            "an id the log never told replays nothing"
        );
    }

    #[test]
    fn the_log_survives_a_restart_with_its_order_and_its_accepted_ids_and_skips_a_torn_line() {
        let dir = tempfile::tempdir().unwrap();
        let first = EventLog::open(dir.path()).unwrap();
        first.append(ev("A", "c"));
        first.append(ev("B", "c"));
        assert_eq!(first.accept_command("alice", "cmd-1", "h", "c"), Accept::New);
        drop(first);
        let path = dir.path().join("events.jsonl");
        let mut torn = std::fs::read_to_string(&path).unwrap();
        torn.push_str("{\"seq\":3,\"envelope\":{\"specversion");
        std::fs::write(&path, torn).unwrap();
        let again = EventLog::open(dir.path()).unwrap();
        let (events, _) = again.list(&Filter::default());
        assert_eq!(
            events.iter().map(|e| e.r#type.as_str()).collect::<Vec<_>>(),
            vec!["A", "B"]
        );
        assert_eq!(
            again.accept_command("alice", "cmd-1", "h", "c"),
            Accept::Duplicate {
                correlation: "c".into()
            }
        );
        let c = again.append(ev("C", "c"));
        assert_eq!(c.seq, 3);
    }

    #[tokio::test]
    async fn a_subscriber_hears_what_is_appended_after_it_subscribed_and_knows_where_the_log_was() {
        let log = EventLog::ephemeral();
        log.append(ev("Before", "c"));
        let (mut rx, last) = log.subscribe();
        assert_eq!(last, 1);
        log.append(ev("After", "c"));
        let got = rx.recv().await.unwrap();
        assert_eq!(got.envelope.r#type, "After");
        assert_eq!(got.seq, 2);
    }
}
