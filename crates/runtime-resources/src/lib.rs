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

/// The effective cgroup memory limit for this process, from
/// [`telemetry::hardware::cgroup_memory_limit`] — which walks the process's
/// own cgroup path rather than reading the cgroup-root files, so a limit set
/// by `systemd-run -p MemoryMax=…`, a slice, or a Kubernetes pod cgroup binds
/// sizing exactly like a container limit does (spiceai#12179). This crate's
/// previous copy read only `/sys/fs/cgroup/memory.max`, which exists at that
/// path only inside a cgroup-namespaced container — on a bare host every
/// nested limit was invisible and budgets were sized from full host RAM.
fn get_container_memory_limit() -> Option<u64> {
    telemetry::hardware::cgroup_memory_limit()
}

/// This process's resident memory, split by what the kernel can reclaim.
///
/// The split is the whole point. A cgroup counts file-backed pages toward
/// `memory.max` but reclaims them under pressure; anonymous memory it cannot
/// reclaim, so anonymous is what gets a pod OOM-killed. On a Cayenne file-mode
/// workload roughly half of total RSS has been measured as file-backed — mapped
/// `SQLite` metastore pages and Vortex data files — so a total-only figure both
/// oversizes the pod and buries the growth that matters: the anonymous half can
/// climb a gigabyte while the total barely moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidentMemory {
    /// Total resident set size. `RssAnon + RssFile + RssShmem` on Linux, so the
    /// two halves of [`Self::split`] sum to slightly less than this whenever the
    /// process maps shared memory (usually none).
    pub total: u64,
    /// The reclaimability split, where the platform can supply it.
    ///
    /// `None` on a target with no portable source for it, and on a Linux kernel
    /// older than 4.5, which reports `VmRSS` without `RssAnon`/`RssFile`. It is
    /// an `Option` rather than a zeroed pair because a consumer cannot tell a
    /// fabricated zero from a process that genuinely holds no anonymous memory,
    /// and publishing one would put a false attribution in the time series —
    /// which is what these gauges exist to correct.
    pub split: Option<ResidentSplit>,
}

/// Resident bytes divided by what the kernel can reclaim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidentSplit {
    /// Anonymous resident bytes: heap, stacks, and other pages with no backing
    /// file. Not reclaimable, and therefore the figure to alert on and the one a
    /// heap profiler can attribute.
    pub anon: u64,
    /// File-backed resident bytes: mapped files and page cache the kernel will
    /// evict on demand. Real RSS, invisible to every heap profiler, and NOT a
    /// leak.
    pub file: u64,
}

/// Resident set size of this process in bytes, or `None` where unavailable.
///
/// The total half of [`process_resident_memory`], kept as its own entry point
/// for callers that only report the sum.
#[must_use]
pub fn process_resident_memory_bytes() -> Option<u64> {
    process_resident_memory().map(|memory| memory.total)
}

/// Parse `VmRSS`, `RssAnon`, and `RssFile` out of `/proc/self/status` contents.
///
/// Split from the read so the unit conversion and the missing-field behaviour
/// are testable on any host: the values are reported in kB and every consumer
/// wants bytes, which is exactly the kind of factor-of-1024 error that survives
/// review and then misreads a footprint by three orders of magnitude.
///
/// `RssAnon`/`RssFile` postdate `VmRSS` (Linux 4.5). On an older kernel the
/// total is still reported with no split, rather than the whole sample failing
/// and taking the total with it. Both halves are required together: one without
/// the other cannot be attributed, since neither is derivable from the total.
#[cfg(any(target_os = "linux", test))]
fn parse_proc_status_resident(status: &str) -> Option<ResidentMemory> {
    let field = |name: &str| -> Option<u64> {
        status
            .lines()
            .find(|line| line.starts_with(name))?
            .split_whitespace()
            .nth(1)?
            .parse::<u64>()
            .ok()
            .map(|kb| kb.saturating_mul(1024))
    };
    Some(ResidentMemory {
        total: field("VmRSS:")?,
        split: match (field("RssAnon:"), field("RssFile:")) {
            (Some(anon), Some(file)) => Some(ResidentSplit { anon, file }),
            _ => None,
        },
    })
}

/// This process's resident memory split into anonymous and file-backed, or
/// `None` where unavailable.
///
/// On Linux this is one read of `/proc/self/status`: `VmRSS`, `RssAnon`, and
/// `RssFile` are adjacent lines (present since Linux 4.5), so the split costs
/// only the extra parse. `/proc/self/smaps_rollup` would break it down further
/// but is materially more expensive to read, and these three separate the two
/// cases that matter.
///
/// On macOS it is one `task_info(TASK_VM_INFO)` syscall, which is also what
/// `vmmap` reports from — cheaper than constructing a `sysinfo::System` per
/// sample, and more informative.
///
/// This blocks (a filesystem read on Linux), so async callers must run it on the
/// blocking pool rather than a runtime worker.
#[must_use]
pub fn process_resident_memory() -> Option<ResidentMemory> {
    #[cfg(target_os = "linux")]
    {
        parse_proc_status_resident(&std::fs::read_to_string("/proc/self/status").ok()?)
    }
    #[cfg(target_os = "macos")]
    {
        use std::mem::size_of;

        let mut info = mach2::task_info::task_vm_info::default();
        let mut count = u32::try_from(
            size_of::<mach2::task_info::task_vm_info>() / size_of::<mach2::vm_types::natural_t>(),
        )
        .ok()?;
        // SAFETY: `task_info` writes at most `count` `natural_t`-sized words into
        // the buffer, and `count` is derived from the size of that exact struct.
        // `mach_task_self` is always a valid task port for this process.
        let status = unsafe {
            mach2::task::task_info(
                mach2::traps::mach_task_self(),
                mach2::task_info::TASK_VM_INFO,
                std::ptr::from_mut(&mut info).cast(),
                &raw mut count,
            )
        };
        if status != mach2::kern_return::KERN_SUCCESS {
            return None;
        }
        Some(ResidentMemory {
            total: info.resident_size,
            split: Some(ResidentSplit {
                // `internal` is anonymous and `external` file-backed, the same
                // split `vmmap -summary` prints.
                anon: info.internal,
                file: info.external,
            }),
        })
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        // No portable source for the split. The total is still worth having, so
        // report it with `split: None` rather than inventing an attribution.
        let mut system = System::new();
        let pid = sysinfo::Pid::from_u32(std::process::id());
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        let total = system.process(pid).map(sysinfo::Process::memory)?;
        Some(ResidentMemory { total, split: None })
    }
}

/// Returns the total available memory in bytes.
///
/// For containerized deployments, returns the container memory limit from cgroup.
/// For bare-metal deployments, returns the system's total memory.
///
/// This function is used internally by `ResourceMonitor` and by `DataFusion`
/// to set default memory limits.
#[must_use]
pub fn get_total_memory() -> u64 {
    let mut system = System::new();
    system.refresh_memory();

    // Prefer container memory limit if available, otherwise use system memory
    get_container_memory_limit().unwrap_or_else(|| system.total_memory())
}

/// Returns the host's total physical memory in bytes, IGNORING any cgroup/container
/// limit.
///
/// Use this where a downstream component sizes itself from host RAM rather than the
/// process's cgroup limit — notably `DuckDB`, whose own default `memory_limit` is ~80%
/// of host RAM. In a container (host RAM > cgroup limit) that ceiling exceeds
/// [`get_total_memory`], so the coordinated accelerator budget must project it from
/// this value, not the cgroup total.
#[must_use]
pub fn get_host_memory() -> u64 {
    let mut system = System::new();
    system.refresh_memory();
    system.total_memory()
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

        // Prefer the cgroup memory limit if one binds, otherwise host memory.
        let container_limit = get_container_memory_limit();
        let host_memory = system.total_memory();
        let total_memory = container_limit.unwrap_or_else(|| {
            tracing::debug!("Using system memory limit: {} bytes", host_memory);
            host_memory
        });

        if let Some(limit) = container_limit {
            // INFO, not debug: when a cap binds, every derived budget shrinks
            // with it, and an operator debugging an OOM (or an unexpectedly
            // small query pool) needs to see which figure sizing started from.
            tracing::info!(
                "Memory budgets sized from the cgroup memory limit: {limit} bytes (host total: {host_memory} bytes)"
            );
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
    ///
    /// # Performance
    /// This method performs blocking I/O operations (process info refresh). When calling from
    /// async contexts, wrap in `tokio::task::spawn_blocking` to avoid blocking the async runtime.
    pub fn check_memory_usage(&self, context: &str) {
        const THRESHOLDS: &[u8] = &[70, 80, 90, 95, 99];

        let mut inner = self.inner.write();

        let mut system = System::new();
        system.refresh_processes(ProcessesToUpdate::Some(&[inner.pid]), true);

        let Some(process) = system.process(inner.pid) else {
            return;
        };

        let process_memory = process.memory();
        #[expect(
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
        #[expect(clippy::cast_possible_truncation)]
        for &threshold in THRESHOLDS.iter().rev() {
            if usage_percent >= threshold && inner.last_warning_threshold < threshold {
                tracing::warn!(
                    "Memory usage at {}% ({} / {}) while loading {}",
                    threshold,
                    util::human_readable_bytes(process_memory as usize),
                    util::human_readable_bytes(inner.total_memory as usize),
                    context
                );
                inner.last_warning_threshold = threshold;
                break;
            }
        }
    }
}

impl Default for ResourceMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{ResidentMemory, parse_proc_status_resident, process_resident_memory};

    /// A real `/proc/self/status` excerpt, in the order and units the kernel
    /// writes them.
    const STATUS: &str = "\
Name:\tspiced
VmPeak:\t 6291456 kB
VmSize:\t 6291456 kB
VmRSS:\t  5940216 kB
RssAnon:\t 2451968 kB
RssFile:\t 3488248 kB
RssShmem:\t       0 kB
Threads:\t32
";

    /// The kernel reports kB and every consumer wants bytes. A missed factor of
    /// 1024 reads as a footprint three orders of magnitude off — large enough to
    /// be obvious in a dashboard, small enough to be believed in a table.
    #[test]
    fn the_split_is_parsed_in_bytes_and_sums_to_the_total() {
        let resident = parse_proc_status_resident(STATUS).expect("VmRSS is present");

        assert_eq!(resident.total, 5_940_216 * 1024);
        let split = resident
            .split
            .expect("this status carries both split lines");
        assert_eq!(split.anon, 2_451_968 * 1024);
        assert_eq!(split.file, 3_488_248 * 1024);
        // `VmRSS = RssAnon + RssFile + RssShmem`, and shmem is zero here, so the
        // two halves account for the whole total. A parse that read the wrong
        // column would still produce plausible-looking numbers but break this.
        assert_eq!(split.anon + split.file, resident.total);
    }

    /// `RssAnon`/`RssFile` arrived in Linux 4.5. Older kernels must still report
    /// a total: failing the whole sample would lose the one figure that has
    /// always been available. The split must be `None` rather than a zeroed
    /// pair — a caller publishing zeroes would claim this process holds no
    /// anonymous memory, which is the false attribution the split exists to
    /// prevent.
    #[test]
    fn a_kernel_without_the_split_reports_the_total_and_no_split() {
        let resident =
            parse_proc_status_resident("VmRSS:\t  5940216 kB\n").expect("VmRSS alone is enough");

        assert_eq!(
            resident,
            ResidentMemory {
                total: 5_940_216 * 1024,
                split: None,
            }
        );
    }

    /// One half without the other is not a split. Neither is derivable from the
    /// total (`VmRSS` also counts shmem), so a partial read has to decline.
    #[test]
    fn one_half_of_the_split_is_not_a_split() {
        for status in [
            "VmRSS:\t  5940216 kB\nRssAnon:\t 2451968 kB\n",
            "VmRSS:\t  5940216 kB\nRssFile:\t 3488248 kB\n",
        ] {
            let resident = parse_proc_status_resident(status).expect("VmRSS is present");
            assert_eq!(resident.total, 5_940_216 * 1024);
            assert!(
                resident.split.is_none(),
                "a half-populated split must not be published: {status:?}"
            );
        }
    }

    /// Without `VmRSS` there is no sample to report. Returning zero would enter
    /// the time series as a process that suddenly uses no memory.
    #[test]
    fn a_status_without_vmrss_reports_nothing() {
        assert!(parse_proc_status_resident("Name:\tspiced\nThreads:\t32\n").is_none());
        assert!(parse_proc_status_resident("").is_none());
    }

    /// Exercises the real platform path — the procfs read on Linux, the
    /// `task_info` syscall on macOS. CI runs Linux, so without this the `unsafe`
    /// mach block would be compiled but never executed anywhere.
    #[test]
    fn the_live_reading_is_plausible() {
        let resident = process_resident_memory().expect("this process is resident");

        assert!(resident.total > 0, "a running process has resident memory");

        // The split only exists on the platforms that implement it; elsewhere it
        // is `None` rather than a fabricated attribution, so asserting it
        // unconditionally would fail on every other target. Linux additionally
        // reports `None` when the kernel's `/proc/self/status` carries no
        // `RssAnon`/`RssFile` pair, which is the same contract, so require the
        // pair only where the syscall cannot omit it.
        #[cfg(target_os = "macos")]
        let split = Some(
            resident
                .split
                .expect("`task_info(TASK_VM_INFO)` always supplies the split"),
        );
        #[cfg(target_os = "linux")]
        let split = resident.split;

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        if let Some(split) = split {
            assert!(
                split.anon > 0,
                "a running process has anonymous memory (heap and stacks)"
            );
        }
    }
}
