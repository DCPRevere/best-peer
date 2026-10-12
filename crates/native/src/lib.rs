//! [`Runtime`] for engines that already run on the host, outside any container: Ollama installed as a service, a
//! vLLM started by hand. There is nothing to pull and nothing to start: a deploy's "container" is the engine's own
//! port on the host, and a stop ends the peer's claim on it, not the engine. Whether the engine answers there is the
//! `Engine` adapter's question, asked on readiness. A job needs a container engine and is refused with that fix.
//! Thin; proven on a host by hand, never unit-tested.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use best_peer_runtime::{ContainerId, Event, ImageRef, Progress, Published, Runtime, RuntimeError, Spec, Started};
use tokio::sync::broadcast;

#[derive(Default)]
pub struct NativeRuntime {
    claimed: Mutex<BTreeMap<ContainerId, Spec>>,
    tx: Option<broadcast::Sender<Event>>,
}

impl NativeRuntime {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(16);
        Self {
            claimed: Mutex::new(BTreeMap::new()),
            tx: Some(tx),
        }
    }

    fn tx(&self) -> &broadcast::Sender<Event> {
        self.tx.as_ref().expect("built with new")
    }
}

#[async_trait]
impl Runtime for NativeRuntime {
    async fn pull(&self, _image: &ImageRef, _progress: Progress) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn start(&self, spec: Spec) -> Result<Started, RuntimeError> {
        let Some(&port) = spec.ports.first() else {
            return Err(RuntimeError::Engine(
                "no container engine on this host: a job needs Docker (BEST_PEER_RUNTIME=docker)".into(),
            ));
        };
        let id = ContainerId(format!("native-{port}"));
        let mut claimed = self.claimed.lock().unwrap();
        if claimed.contains_key(&id) {
            return Err(RuntimeError::Engine(format!(
                "the engine on port {port} already serves a deployment: undeploy it first"
            )));
        }
        claimed.insert(id.clone(), spec);
        let ports = vec![Published {
            container: port,
            host: port,
        }];
        let _ = self.tx().send(Event::Started {
            id: id.clone(),
            ports: ports.clone(),
        });
        Ok(Started { id, ports })
    }

    async fn stop(&self, id: &ContainerId) -> Result<(), RuntimeError> {
        self.claimed
            .lock()
            .unwrap()
            .remove(id)
            .ok_or_else(|| RuntimeError::NoSuchContainer(id.0.clone()))?;
        let _ = self.tx().send(Event::Exited {
            id: id.clone(),
            code: 0,
        });
        Ok(())
    }

    async fn running(&self) -> Result<Vec<ContainerId>, RuntimeError> {
        Ok(self.claimed.lock().unwrap().keys().cloned().collect())
    }

    fn events(&self) -> broadcast::Receiver<Event> {
        self.tx().subscribe()
    }
}
