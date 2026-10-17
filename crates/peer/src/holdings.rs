//! What a credential holds across the whole door. A credential's share (the most device memory its workloads may
//! hold together, set at the approval) is one number for the door, and the door has several surfaces that promise
//! memory: the machine's own hardware at the root, and every cloud of every workspace. Each registers here what it
//! has promised to a holder, and a share is measured against the sum, so a workload admitted in one workspace is
//! counted when another is asked for.

use std::sync::Mutex;

type Source = Box<dyn Fn(&str) -> u64 + Send + Sync>;

#[derive(Default)]
pub struct Holdings {
    sources: Mutex<Vec<Source>>,
}

impl Holdings {
    /// Adds a surface: `held` answers the device memory it has promised to a holder, admitted workloads included.
    pub fn add(&self, held: impl Fn(&str) -> u64 + Send + Sync + 'static) {
        self.sources.lock().unwrap().push(Box::new(held));
    }

    /// The device memory `holder`'s workloads hold together, on every surface of this door.
    pub fn held_by(&self, holder: &str) -> u64 {
        self.sources
            .lock()
            .unwrap()
            .iter()
            .map(|s| s(holder))
            .fold(0u64, u64::saturating_add)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_share_is_measured_against_every_surface_of_the_door() {
        let h = Holdings::default();
        assert_eq!(h.held_by("a"), 0);
        h.add(|holder| if holder == "a" { 4 } else { 0 });
        h.add(|_| 2);
        assert_eq!(h.held_by("a"), 6);
        assert_eq!(h.held_by("b"), 2);
    }
}
