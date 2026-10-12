//! The event log: every fact the door has told, in order, kept on disk and streamed live. `GET /events` reads it,
//! `GET /events/stream` follows it, and a reconnecting client replays from `Last-Event-ID`. The log also keeps the
//! commands it accepted, so an envelope `id` is an idempotency key across restarts.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::broadcast;

use crate::envelope::Envelope;

/// An event with its position in the log; the position is the SSE event id's order and the pagination cursor.
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

/// The verdict on a command's `id` (SPEC.md, Ingestion Semantics: duplicates are ignored, a reused id with a
/// different payload is a conflict).
#[derive(Debug, PartialEq, Eq)]
pub enum Accept {
    New,
    Duplicate,
    Conflict,
}

#[derive(Serialize, Deserialize)]
struct AcceptedLine {
    id: String,
    hash: String,
}

pub struct EventLog {
    events: Mutex<Vec<Stored>>,
    accepted: Mutex<HashMap<String, String>>,
    events_file: Mutex<Option<File>>,
    accepted_file: Mutex<Option<File>>,
    tx: broadcast::Sender<Stored>,
}

const MAX_PAGE: usize = 200;

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

    /// A log kept in `dir` (`events.jsonl`, `accepted.jsonl`), loaded now and appended to from here on.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let log = Self::ephemeral();
        let events_path = dir.join("events.jsonl");
        let accepted_path = dir.join("accepted.jsonl");
        for line in read_lines(&events_path)? {
            if let Ok(s) = serde_json::from_str::<Stored>(&line) {
                log.events.lock().unwrap().push(s);
            }
        }
        for line in read_lines(&accepted_path)? {
            if let Ok(a) = serde_json::from_str::<AcceptedLine>(&line) {
                log.accepted.lock().unwrap().insert(a.id, a.hash);
            }
        }
        *log.events_file.lock().unwrap() = Some(append_to(&events_path)?);
        *log.accepted_file.lock().unwrap() = Some(append_to(&accepted_path)?);
        Ok(log)
    }

    /// Appends an event and tells every subscriber. The write to disk happens before anyone hears of it.
    pub fn append(&self, envelope: Envelope) -> Stored {
        let mut events = self.events.lock().unwrap();
        let seq = events.last().map(|s| s.seq + 1).unwrap_or(1);
        let stored = Stored { seq, envelope };
        if let Some(f) = self.events_file.lock().unwrap().as_mut() {
            let line = serde_json::to_string(&stored).expect("an envelope serialises");
            f.write_all(line.as_bytes())
                .and_then(|_| f.write_all(b"\n"))
                .expect("the event log is writable");
            f.flush().expect("the event log flushes");
        }
        events.push(stored.clone());
        drop(events);
        let _ = self.tx.send(stored.clone());
        stored
    }

    /// Records that a command with `id` and payload hash `hash` was accepted, or says why it was not.
    pub fn accept_command(&self, id: &str, hash: &str) -> Accept {
        let mut accepted = self.accepted.lock().unwrap();
        match accepted.get(id) {
            Some(h) if h == hash => Accept::Duplicate,
            Some(_) => Accept::Conflict,
            None => {
                if let Some(f) = self.accepted_file.lock().unwrap().as_mut() {
                    let line = serde_json::to_string(&AcceptedLine {
                        id: id.into(),
                        hash: hash.into(),
                    })
                    .unwrap();
                    f.write_all(line.as_bytes())
                        .and_then(|_| f.write_all(b"\n"))
                        .expect("the accepted log is writable");
                    f.flush().expect("the accepted log flushes");
                }
                accepted.insert(id.into(), hash.into());
                Accept::New
            }
        }
    }

    /// A page of the log, oldest first, and the cursor for the next page when there is one.
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

    pub fn subscribe(&self) -> broadcast::Receiver<Stored> {
        self.tx.subscribe()
    }

    pub fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn read_lines(path: &Path) -> std::io::Result<Vec<String>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    BufReader::new(File::open(path)?).lines().collect()
}

fn append_to(path: &PathBuf) -> std::io::Result<File> {
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
    fn an_id_is_an_idempotency_key_and_a_different_payload_is_a_conflict() {
        let log = EventLog::ephemeral();
        assert_eq!(log.accept_command("x", "h1"), Accept::New);
        assert_eq!(log.accept_command("x", "h1"), Accept::Duplicate);
        assert_eq!(log.accept_command("x", "h2"), Accept::Conflict);
        assert_eq!(log.accept_command("y", "h2"), Accept::New);
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
    fn the_log_survives_a_restart_with_its_order_and_its_accepted_ids() {
        let dir = tempfile::tempdir().unwrap();
        let first = EventLog::open(dir.path()).unwrap();
        first.append(ev("A", "c"));
        first.append(ev("B", "c"));
        assert_eq!(first.accept_command("cmd-1", "h"), Accept::New);
        drop(first);
        let again = EventLog::open(dir.path()).unwrap();
        let (events, _) = again.list(&Filter::default());
        assert_eq!(
            events.iter().map(|e| e.r#type.as_str()).collect::<Vec<_>>(),
            vec!["A", "B"]
        );
        assert_eq!(again.accept_command("cmd-1", "h"), Accept::Duplicate);
        let c = again.append(ev("C", "c"));
        assert_eq!(c.seq, 3);
    }

    #[tokio::test]
    async fn a_subscriber_hears_what_is_appended_after_it_subscribed() {
        let log = EventLog::ephemeral();
        log.append(ev("Before", "c"));
        let mut rx = log.subscribe();
        log.append(ev("After", "c"));
        assert_eq!(rx.recv().await.unwrap().envelope.r#type, "After");
    }
}
