//! The accelerator abstraction best-peer owns.
//!
//! A peer asks one question of the hardware: what devices are here, right now, and how much of each is free. The
//! answer is a [`Host`] snapshot, vendor-neutral: an NVIDIA card, an AMD card, an Apple GPU or a plain CPU all describe
//! themselves with the same words ([`Device`]), and each says which compute APIs it offers ([`Compute`]) instead of
//! which vendor made it. The door's queries, refusals and events are written against [`Gpu`] and tested against
//! [`Fake`]; a real adapter (NVML, ROCm SMI, ...) is a separate crate that implements [`Gpu`] and is proven on a host
//! by hand, never unit-tested.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// A stable, vendor-opaque identifier for one device: the NVML UUID, a PCI bus id, whatever the adapter can keep
/// stable across restarts. It is what a runtime is handed to expose the device to a container.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeviceId(pub String);

impl From<&str> for DeviceId {
    fn from(s: &str) -> Self {
        DeviceId(s.to_string())
    }
}

/// Who made the device. Informative: nothing in the door branches on it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Vendor {
    Nvidia,
    Amd,
    Intel,
    Apple,
    Other(String),
}

/// A compute API the device offers, with its version where it has one. A workload asks for one of these, never for
/// a vendor: "needs CUDA 12" or "needs ROCm 6" or "any Vulkan", and the peer matches it against the device.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "api", content = "version", rename_all = "kebab-case")]
pub enum Compute {
    Cuda(String),
    Rocm(String),
    OneApi(String),
    Metal,
    Vulkan,
    Cpu,
}

/// Device memory, in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Memory {
    pub total_bytes: u64,
    pub free_bytes: u64,
}

/// One accelerator as the peer sees it at `Host::observed_at`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Device {
    pub id: DeviceId,
    pub vendor: Vendor,
    pub name: String,
    pub memory: Memory,
    pub compute: Vec<Compute>,
    /// Degrees Celsius, where the adapter can read it.
    pub temperature_c: Option<f32>,
    /// Watts drawn now, where the adapter can read it.
    pub power_w: Option<f32>,
    /// Compute utilisation in percent, where the adapter can read it.
    pub utilisation_pct: Option<u8>,
}

/// Everything the hardware says about itself, taken at one instant.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Host {
    /// The driver the adapter talked to (its version), where there is one.
    pub driver: Option<String>,
    pub devices: Vec<Device>,
    pub observed_at: SystemTime,
}

impl Host {
    /// The devices that offer a compute API matching `wanted` and have at least `free_bytes` free, most free first.
    /// A workload's placement question, answered without naming a vendor.
    pub fn fit(&self, wanted: &Compute, free_bytes: u64) -> Vec<&Device> {
        let mut fit: Vec<&Device> = self
            .devices
            .iter()
            .filter(|d| d.memory.free_bytes >= free_bytes && d.compute.iter().any(|c| compatible(c, wanted)))
            .collect();
        fit.sort_by_key(|d| std::cmp::Reverse(d.memory.free_bytes));
        fit
    }
}

/// A device's API satisfies a wanted one when it is the same API and its version is the same or newer.
fn compatible(offered: &Compute, wanted: &Compute) -> bool {
    match (offered, wanted) {
        (Compute::Cuda(o), Compute::Cuda(w))
        | (Compute::Rocm(o), Compute::Rocm(w))
        | (Compute::OneApi(o), Compute::OneApi(w)) => version_at_least(o, w),
        (Compute::Metal, Compute::Metal) | (Compute::Vulkan, Compute::Vulkan) | (Compute::Cpu, Compute::Cpu) => true,
        _ => false,
    }
}

fn version_at_least(offered: &str, wanted: &str) -> bool {
    let parse = |v: &str| v.split('.').map(|p| p.parse::<u32>().unwrap_or(0)).collect::<Vec<_>>();
    parse(offered) >= parse(wanted)
}

#[derive(Debug, thiserror::Error)]
pub enum GpuError {
    /// The adapter's library or driver is not on this host; the peer refuses to start and says so.
    #[error("no accelerator driver on this host: {0}")]
    NoDriver(String),
    #[error("the accelerator library failed: {0}")]
    Library(String),
}

/// What the door needs from the hardware. One method, because the door only ever asks for the whole picture.
pub trait Gpu: Send + Sync {
    fn snapshot(&self) -> Result<Host, GpuError>;
}

/// An in-memory [`Gpu`] for tests: whatever devices the test says are there, with memory the test can move.
#[derive(Default)]
pub struct Fake {
    devices: Mutex<BTreeMap<DeviceId, Device>>,
    driver: Option<String>,
}

impl Fake {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_driver(mut self, driver: &str) -> Self {
        self.driver = Some(driver.to_string());
        self
    }

    pub fn with_device(self, device: Device) -> Self {
        self.devices.lock().unwrap().insert(device.id.clone(), device);
        self
    }

    /// A plain CUDA card of `total_gb` with all of it free, the common test fixture.
    pub fn cuda_card(id: &str, name: &str, total_gb: u64, cuda: &str) -> Device {
        let total = total_gb * 1024 * 1024 * 1024;
        Device {
            id: id.into(),
            vendor: Vendor::Nvidia,
            name: name.to_string(),
            memory: Memory {
                total_bytes: total,
                free_bytes: total,
            },
            compute: vec![Compute::Cuda(cuda.to_string()), Compute::Vulkan],
            temperature_c: Some(40.0),
            power_w: Some(30.0),
            utilisation_pct: Some(0),
        }
    }

    /// Moves a device's free memory, as a deploy or a release would.
    pub fn set_free(&self, id: &DeviceId, free_bytes: u64) {
        if let Some(d) = self.devices.lock().unwrap().get_mut(id) {
            d.memory.free_bytes = free_bytes.min(d.memory.total_bytes);
        }
    }
}

impl Gpu for Fake {
    fn snapshot(&self) -> Result<Host, GpuError> {
        Ok(Host {
            driver: self.driver.clone(),
            devices: self.devices.lock().unwrap().values().cloned().collect(),
            observed_at: SystemTime::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GB: u64 = 1024 * 1024 * 1024;

    fn host() -> Fake {
        Fake::new()
            .with_driver("fake 1.0")
            .with_device(Fake::cuda_card("gpu-0", "Card A", 24, "12.4"))
            .with_device(Fake::cuda_card("gpu-1", "Card B", 16, "12.4"))
            .with_device(Device {
                id: "cpu".into(),
                vendor: Vendor::Other("host".into()),
                name: "CPU".into(),
                memory: Memory {
                    total_bytes: 64 * GB,
                    free_bytes: 64 * GB,
                },
                compute: vec![Compute::Cpu],
                temperature_c: None,
                power_w: None,
                utilisation_pct: None,
            })
    }

    #[test]
    fn a_snapshot_reports_every_device_the_fake_was_given() {
        let snap = host().snapshot().unwrap();
        assert_eq!(snap.driver.as_deref(), Some("fake 1.0"));
        assert_eq!(snap.devices.len(), 3);
    }

    #[test]
    fn fit_picks_by_compute_api_and_free_memory_most_free_first() {
        let fake = host();
        fake.set_free(&"gpu-0".into(), 4 * GB);
        let snap = fake.snapshot().unwrap();
        let fit = snap.fit(&Compute::Cuda("12.0".into()), 8 * GB);
        assert_eq!(fit.iter().map(|d| d.id.0.as_str()).collect::<Vec<_>>(), vec!["gpu-1"]);
        let any_cuda = snap.fit(&Compute::Cuda("12.0".into()), GB);
        assert_eq!(
            any_cuda.iter().map(|d| d.id.0.as_str()).collect::<Vec<_>>(),
            vec!["gpu-1", "gpu-0"]
        );
    }

    #[test]
    fn a_newer_api_version_than_wanted_fits_an_older_one_does_not() {
        let snap = host().snapshot().unwrap();
        assert_eq!(snap.fit(&Compute::Cuda("13.0".into()), GB).len(), 0);
        assert_eq!(snap.fit(&Compute::Cuda("11.8".into()), GB).len(), 2);
        assert_eq!(snap.fit(&Compute::Rocm("6.0".into()), GB).len(), 0);
        assert_eq!(snap.fit(&Compute::Cpu, GB).len(), 1);
    }

    #[test]
    fn set_free_never_exceeds_total() {
        let fake = host();
        fake.set_free(&"gpu-1".into(), 1000 * GB);
        let snap = fake.snapshot().unwrap();
        let b = snap.devices.iter().find(|d| d.id.0 == "gpu-1").unwrap();
        assert_eq!(b.memory.free_bytes, b.memory.total_bytes);
    }
}
