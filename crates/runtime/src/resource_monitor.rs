/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use parking_lot::RwLock;
use std::fs;
use std::sync::Arc;
use sysinfo::{Pid, ProcessesToUpdate, System};

/// Monitors process resource usage and provides warnings at configurable thresholds.
///
/// The monitor tracks the Spice process resource usage (currently memory) relative to
/// available system resources and logs warnings when usage crosses specific percentage
/// thresholds (70%, 80%, 90%, 95%, 99%).
///
/// For containerized deployments, this automatically detects and uses container memory
/// limits from cgroup v1 or v2 instead of host system memory.
///
/// This is designed to be shared across the runtime and passed to components that need
/// resource monitoring during resource-intensive operations like data loading.
#[derive(Clone, Debug)]
pub struct ResourceMonitor {
    inner: Arc<RwLock<ResourceMonitorInner>>,
}

#[derive(Debug)]
struct ResourceMonitorInner {
    pid: Pid,
    total_memory: u64,
    last_warning_threshold: u8,
}

/// Attempts to read container memory limit from cgroup v2 or v1.
/// Returns None if not in a container or if the limit cannot be read.
fn get_container_memory_limit() -> Option<u64> {
    // Try cgroup v2 first (newer container runtimes)
    if let Ok(contents) = fs::read_to_string("/sys/fs/cgroup/memory.max")
        && let Ok(limit) = contents.trim().parse::<u64>()
    {
        // "max" means no limit set
        if limit != u64::MAX {
            return Some(limit);
        }
    }

    // Try cgroup v1 (Docker, older K8s)
    if let Ok(contents) = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        && let Ok(limit) = contents.trim().parse::<u64>()
    {
        // Very large values (like u64::MAX or close to it) typically mean no limit
        if limit < (1u64 << 62) {
            return Some(limit);
        }
    }

    None
}
/// Returns the total available memory in bytes.
///
/// For containerized deployments, returns the container memory limit from cgroup.
/// For bare-metal deployments, returns the system's total memory.
///
/// This function is used internally by `ResourceMonitor` and by DataFusion
/// to set default memory limits.
#[must_use]
pub fn get_total_memory() -> u64 {
    let mut system = System::new();
    system.refresh_memory();

    // Prefer container memory limit if available, otherwise use system memory
    get_container_memory_limit().unwrap_or_else(|| system.total_memory())
}

impl ResourceMonitor {
    /// Creates a new resource monitor for the current process.
    ///
    /// Automatically detects if running in a container and uses container memory
    /// limits instead of host system memory.
    #[must_use]
    pub fn new() -> Self {
        let pid = Pid::from_u32(std::process::id());
        let mut system = System::new();
        system.refresh_memory();

        // Prefer container memory limit if available, otherwise use system memory
        let total_memory = get_container_memory_limit().unwrap_or_else(|| {
            let system_memory = system.total_memory();
            tracing::debug!("Using system memory limit: {} bytes", system_memory);
            system_memory
        });

        if get_container_memory_limit().is_some() {
            tracing::debug!("Detected container memory limit: {} bytes", total_memory);
        }

        Self {
            inner: Arc::new(RwLock::new(ResourceMonitorInner {
                pid,
                total_memory,
                last_warning_threshold: 0,
            })),
        }
    }

    /// Checks current memory usage and logs warnings if thresholds are crossed.
    ///
    /// Warnings are only logged once per threshold (70%, 80%, 90%, 95%, 99%) to avoid
    /// log spam. The threshold state resets if memory usage drops below the last warning level.
    ///
    /// # Arguments
    /// * `context` - A descriptive context string (e.g., dataset name) to include in warning messages
    pub fn check_memory_usage(&self, context: &str) {
        let mut inner = self.inner.write();

        let mut system = System::new();
        system.refresh_processes(ProcessesToUpdate::Some(&[inner.pid]), true);

        let Some(process) = system.process(inner.pid) else {
            return;
        };

        let process_memory = process.memory();
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let usage_percent = ((process_memory as f64 / inner.total_memory as f64) * 100.0) as u8;

        // Reset warning threshold if usage dropped significantly
        if usage_percent < inner.last_warning_threshold.saturating_sub(5) {
            inner.last_warning_threshold = 0;
        }

        // Only warn once per threshold crossing
        if usage_percent >= 99 && inner.last_warning_threshold < 99 {
            #[allow(clippy::cast_possible_truncation)]
            tracing::warn!(
                "Memory usage at 99% ({} / {}) while loading {}",
                util::human_readable_bytes(process_memory as usize),
                util::human_readable_bytes(inner.total_memory as usize),
                context
            );
            inner.last_warning_threshold = 99;
        } else if usage_percent >= 95 && inner.last_warning_threshold < 95 {
            #[allow(clippy::cast_possible_truncation)]
            tracing::warn!(
                "Memory usage at 95% ({} / {}) while loading {}",
                util::human_readable_bytes(process_memory as usize),
                util::human_readable_bytes(inner.total_memory as usize),
                context
            );
            inner.last_warning_threshold = 95;
        } else if usage_percent >= 90 && inner.last_warning_threshold < 90 {
            #[allow(clippy::cast_possible_truncation)]
            tracing::warn!(
                "Memory usage at 90% ({} / {}) while loading {}",
                util::human_readable_bytes(process_memory as usize),
                util::human_readable_bytes(inner.total_memory as usize),
                context
            );
            inner.last_warning_threshold = 90;
        } else if usage_percent >= 80 && inner.last_warning_threshold < 80 {
            #[allow(clippy::cast_possible_truncation)]
            tracing::warn!(
                "Memory usage at 80% ({} / {}) while loading {}",
                util::human_readable_bytes(process_memory as usize),
                util::human_readable_bytes(inner.total_memory as usize),
                context
            );
            inner.last_warning_threshold = 80;
        } else if usage_percent >= 70 && inner.last_warning_threshold < 70 {
            #[allow(clippy::cast_possible_truncation)]
            tracing::warn!(
                "Memory usage at 70% ({} / {}) while loading {}",
                util::human_readable_bytes(process_memory as usize),
                util::human_readable_bytes(inner.total_memory as usize),
                context
            );
            inner.last_warning_threshold = 70;
        }
    }

    /// Returns the current memory usage percentage.
    ///
    /// Returns `None` if the process information cannot be retrieved.
    #[must_use]
    pub fn current_usage_percent(&self) -> f64 {
        let inner = self.inner.read();
        let mut system = System::new();
        system.refresh_memory();
        let process_memory = system.process(inner.pid).map(Process::memory).unwrap_or(0);
        if inner.total_memory == 0 {
            return 0.0;
        }
        #[allow(clippy::cast_precision_loss)]
        {
            (process_memory as f64 / inner.total_memory as f64) * 100.0
        }
    }
}

impl Default for ResourceMonitor {
    fn default() -> Self {
        Self::new()
    }
}
