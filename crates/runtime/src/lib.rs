//! The container runtime abstraction best-peer owns.
//!
//! A peer runs workloads it did not write, in containers an engine it did not write manages. Everything the door
//! says about a workload (pulled, started, ready, out of memory, finished, failed) is an [`Event`] from this trait's
//! stream, so the door's lifecycle is written once against [`Runtime`] and tested against [`Fake`]. An engine
//! adapter (Docker or containerd through bollard, a microVM later) is a separate crate that implements [`Runtime`]
//! and is proven on a host by hand, never unit-tested.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use best_peer_gpu::DeviceId;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

/// An image reference as the engine understands it (`ollama/ollama:0.6`, `vllm/vllm-openai:v0.8`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageRef(pub String);

/// The engine's id for a running container.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ContainerId(pub String);

/// What to run. Devices are handed over by [`DeviceId`]; how the engine exposes them (the NVIDIA toolkit's
/// `--gpus device=<uuid>`, `/dev/kfd` and `/dev/dri` for ROCm) is the adapter's business.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spec {
    pub image: ImageRef,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub devices: Vec<DeviceId>,
    /// Container ports to publish; the adapter reports the host side in [`Event::Started`].
    pub ports: Vec<u16>,
}

/// A published port: the container's and the host's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Published {
    pub container: u16,
    pub host: u16,
}

/// Every fact the engine can tell. The door turns these into BEST events on its stream, correlated to the command
/// that caused them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Event {
    ImagePulling { image: ImageRef, progress_pct: Option<u8> },
    ImagePulled { image: ImageRef },
    Started { id: ContainerId, ports: Vec<Published> },
    Exited { id: ContainerId, code: i64 },
    OutOfMemory { id: ContainerId },
    Failed { id: ContainerId, reason: String },
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("image not found: {0}")]
    ImageNotFound(String),
    #[error("no such container: {0}")]
    NoSuchContainer(String),
    #[error("device {0} is not available to this engine")]
    DeviceUnavailable(String),
    #[error("the engine is unreachable: {0}")]
    Unreachable(String),
    #[error("the engine failed: {0}")]
    Engine(String),
}

/// What the door needs from a container engine.
#[async_trait]
pub trait Runtime: Send + Sync {
    /// Makes the image present; emits `ImagePulling` progress and `ImagePulled`.
    async fn pull(&self, image: &ImageRef) -> Result<(), RuntimeError>;
    /// Creates and starts a container; emits `Started` with the published ports.
    async fn start(&self, spec: Spec) -> Result<ContainerId, RuntimeError>;
    /// Stops and removes a container; emits `Exited`.
    async fn stop(&self, id: &ContainerId) -> Result<(), RuntimeError>;
    /// The containers this engine runs for the peer.
    async fn running(&self) -> Result<Vec<ContainerId>, RuntimeError>;
    /// Every event from now on. A slow subscriber may lag; the door rebuilds from its own stream, not from this.
    fn events(&self) -> broadcast::Receiver<Event>;
}

/// An in-memory [`Runtime`] for tests: images the test declares exist, containers that start at once, and the events
/// a real engine would emit, in order. A test can make a container fail or run out of memory.
pub struct Fake {
    images: Mutex<Vec<ImageRef>>,
    containers: Mutex<BTreeMap<ContainerId, Spec>>,
    devices: Vec<DeviceId>,
    next: Mutex<u32>,
    tx: broadcast::Sender<Event>,
}

impl Fake {
    /// A fake whose engine can see `devices` and already holds `images`.
    pub fn new(devices: Vec<DeviceId>, images: Vec<ImageRef>) -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            images: Mutex::new(images),
            containers: Mutex::new(BTreeMap::new()),
            devices,
            next: Mutex::new(0),
            tx,
        }
    }

    /// Lets a test declare an image the registry has, so a later `pull` succeeds.
    pub fn registry_has(&self, image: ImageRef) {
        self.images.lock().unwrap().push(image);
    }

    /// Drives a running container out of memory, as the engine would report it.
    pub fn out_of_memory(&self, id: &ContainerId) -> Result<(), RuntimeError> {
        self.containers
            .lock()
            .unwrap()
            .remove(id)
            .ok_or_else(|| RuntimeError::NoSuchContainer(id.0.clone()))?;
        let _ = self.tx.send(Event::OutOfMemory { id: id.clone() });
        Ok(())
    }

    /// Makes a running container fail, as the engine would report it.
    pub fn fail(&self, id: &ContainerId, reason: &str) -> Result<(), RuntimeError> {
        self.containers
            .lock()
            .unwrap()
            .remove(id)
            .ok_or_else(|| RuntimeError::NoSuchContainer(id.0.clone()))?;
        let _ = self.tx.send(Event::Failed {
            id: id.clone(),
            reason: reason.to_string(),
        });
        Ok(())
    }

    pub fn spec_of(&self, id: &ContainerId) -> Option<Spec> {
        self.containers.lock().unwrap().get(id).cloned()
    }
}

#[async_trait]
impl Runtime for Fake {
    async fn pull(&self, image: &ImageRef) -> Result<(), RuntimeError> {
        if !self.images.lock().unwrap().contains(image) {
            return Err(RuntimeError::ImageNotFound(image.0.clone()));
        }
        let _ = self.tx.send(Event::ImagePulling {
            image: image.clone(),
            progress_pct: Some(50),
        });
        let _ = self.tx.send(Event::ImagePulled { image: image.clone() });
        Ok(())
    }

    async fn start(&self, spec: Spec) -> Result<ContainerId, RuntimeError> {
        if !self.images.lock().unwrap().contains(&spec.image) {
            return Err(RuntimeError::ImageNotFound(spec.image.0.clone()));
        }
        if let Some(d) = spec.devices.iter().find(|d| !self.devices.contains(d)) {
            return Err(RuntimeError::DeviceUnavailable(d.0.clone()));
        }
        let n = {
            let mut next = self.next.lock().unwrap();
            *next += 1;
            *next
        };
        let id = ContainerId(format!("fake-{n}"));
        let ports = spec
            .ports
            .iter()
            .map(|&p| Published {
                container: p,
                host: 30000 + p,
            })
            .collect();
        self.containers.lock().unwrap().insert(id.clone(), spec);
        let _ = self.tx.send(Event::Started { id: id.clone(), ports });
        Ok(id)
    }

    async fn stop(&self, id: &ContainerId) -> Result<(), RuntimeError> {
        self.containers
            .lock()
            .unwrap()
            .remove(id)
            .ok_or_else(|| RuntimeError::NoSuchContainer(id.0.clone()))?;
        let _ = self.tx.send(Event::Exited {
            id: id.clone(),
            code: 0,
        });
        Ok(())
    }

    async fn running(&self) -> Result<Vec<ContainerId>, RuntimeError> {
        Ok(self.containers.lock().unwrap().keys().cloned().collect())
    }

    fn events(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake() -> Fake {
        Fake::new(vec!["gpu-0".into()], vec![ImageRef("ollama/ollama:0.6".into())])
    }

    fn spec(image: &str, device: &str) -> Spec {
        Spec {
            image: ImageRef(image.into()),
            args: vec![],
            env: vec![],
            devices: vec![device.into()],
            ports: vec![11434],
        }
    }

    #[tokio::test]
    async fn the_lifecycle_is_told_as_events_in_order() {
        let rt = fake();
        let mut events = rt.events();
        let image = ImageRef("ollama/ollama:0.6".into());
        rt.pull(&image).await.unwrap();
        let id = rt.start(spec("ollama/ollama:0.6", "gpu-0")).await.unwrap();
        rt.stop(&id).await.unwrap();
        assert!(matches!(events.recv().await.unwrap(), Event::ImagePulling { .. }));
        assert!(matches!(events.recv().await.unwrap(), Event::ImagePulled { .. }));
        match events.recv().await.unwrap() {
            Event::Started { id: started, ports } => {
                assert_eq!(started, id);
                assert_eq!(
                    ports,
                    vec![Published {
                        container: 11434,
                        host: 41434
                    }]
                );
            }
            other => panic!("expected Started, got {other:?}"),
        }
        assert!(matches!(events.recv().await.unwrap(), Event::Exited { code: 0, .. }));
        assert!(rt.running().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_image_the_registry_lacks_is_refused_by_name() {
        let rt = fake();
        let err = rt.pull(&ImageRef("nobody/nothing:1".into())).await.unwrap_err();
        assert!(matches!(err, RuntimeError::ImageNotFound(ref i) if i == "nobody/nothing:1"));
        rt.registry_has(ImageRef("nobody/nothing:1".into()));
        rt.pull(&ImageRef("nobody/nothing:1".into())).await.unwrap();
    }

    #[tokio::test]
    async fn a_device_the_engine_cannot_see_is_refused_by_id() {
        let rt = fake();
        let err = rt.start(spec("ollama/ollama:0.6", "gpu-9")).await.unwrap_err();
        assert!(matches!(err, RuntimeError::DeviceUnavailable(ref d) if d == "gpu-9"));
    }

    #[tokio::test]
    async fn out_of_memory_and_failure_are_events_and_end_the_container() {
        let rt = fake();
        let mut events = rt.events();
        let a = rt.start(spec("ollama/ollama:0.6", "gpu-0")).await.unwrap();
        let b = rt.start(spec("ollama/ollama:0.6", "gpu-0")).await.unwrap();
        rt.out_of_memory(&a).unwrap();
        rt.fail(&b, "exit 137").unwrap();
        let _ = events.recv().await.unwrap();
        let _ = events.recv().await.unwrap();
        assert!(matches!(events.recv().await.unwrap(), Event::OutOfMemory { id } if id == a));
        assert!(
            matches!(events.recv().await.unwrap(), Event::Failed { id, reason } if id == b && reason == "exit 137")
        );
        assert!(rt.running().await.unwrap().is_empty());
        assert!(matches!(
            rt.stop(&a).await.unwrap_err(),
            RuntimeError::NoSuchContainer(_)
        ));
    }
}
