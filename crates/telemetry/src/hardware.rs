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

//! Hardware detection for anonymous telemetry.
//!
//! Detects the number of vCPUs, GPUs, and available memory on the host machine,
//! including support for containerized environments (Docker, Kubernetes).
//!
//! ## Container Support
//!
//! For containerized deployments, this module automatically detects and respects
//! container resource limits from cgroup v1 and v2:
//!
//! - **CPU limits**: Reads from cgroup v2 `cpu.max` or cgroup v1 `cpu.cfs_quota_us`
//! - **Memory limits**: Reads from cgroup v2 `memory.max` or cgroup v1 `memory.limit_in_bytes`
//!
//! ### Cgroup Path Resolution
//!
//! Cgroup mountpoints are resolved via `/proc/self/mountinfo` with `/sys/fs/cgroup`
//! as the fallback. The process's cgroup path is read from `/proc/self/cgroup`.
//!
//! ## GPU Detection
//!
//! Supports detection of:
//! - NVIDIA GPUs via `/proc/driver/nvidia/gpus/`, `/dev/nvidia*`, and
//!   `NVIDIA_VISIBLE_DEVICES` environment variable
//! - Apple Metal GPUs on macOS (architecture-based detection)
//!
//! ## Performance
//!
//! Hardware detection is designed to be fast and non-blocking:
//! - Minimizes filesystem reads

use sysinfo::System;
use util::human_readable_bytes;

/// Hardware information for telemetry reporting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardwareInfo {
    /// Number of logical CPUs (vCPUs) available.
    pub vcpu_count: usize,
    /// Number of GPUs detected.
    pub gpu_count: usize,
    /// Total memory available in bytes.
    pub total_memory_bytes: u64,
}

impl Default for HardwareInfo {
    fn default() -> Self {
        Self {
            vcpu_count: 1,
            gpu_count: 0,
            total_memory_bytes: 0,
        }
    }
}

impl HardwareInfo {
    /// Detects hardware information for the current system.
    ///
    /// For containerized deployments, this attempts to detect container resource limits
    /// from cgroup v1/v2 before falling back to host system values.
    ///
    /// This function performs blocking I/O operations (filesystem reads) and should
    /// only be called from synchronous contexts. For async contexts, use
    /// [`detect_async`](Self::detect_async) instead.
    #[must_use]
    pub fn detect() -> Self {
        let vcpu_count = detect_vcpu_count();
        let gpu_count = detect_gpu_count();
        let total_memory_bytes = detect_total_memory();

        Self {
            vcpu_count,
            gpu_count,
            total_memory_bytes,
        }
    }

    /// Async version of [`detect`](Self::detect) that runs hardware detection
    /// in a blocking thread pool.
    ///
    /// This should be used when calling from async contexts to avoid blocking
    /// the async runtime. The actual detection is offloaded to a blocking thread
    /// via `tokio::task::spawn_blocking`.
    ///
    /// # Errors
    ///
    /// Returns an error if the blocking task fails to execute (e.g., runtime shutdown).
    pub async fn detect_async() -> Result<Self, tokio::task::JoinError> {
        tokio::task::spawn_blocking(Self::detect).await
    }

    /// Logs the detected hardware information at debug level.
    pub fn log_debug(&self) {
        #[expect(clippy::cast_possible_truncation)]
        let memory_human = human_readable_bytes(self.total_memory_bytes as usize);
        tracing::debug!(
            vcpu_count = self.vcpu_count,
            gpu_count = self.gpu_count,
            total_memory_bytes = self.total_memory_bytes,
            total_memory_human = %memory_human,
            "Detected hardware resources"
        );
    }
}

impl std::fmt::Display for HardwareInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[expect(clippy::cast_possible_truncation)]
        let memory_human = human_readable_bytes(self.total_memory_bytes as usize);
        write!(
            f,
            "vCPUs: {}, GPUs: {}, Memory: {}",
            self.vcpu_count, self.gpu_count, memory_human
        )
    }
}

// =============================================================================
// CPU Detection
// =============================================================================

/// Detects the number of vCPUs available to the process.
///
/// For containers, respects cgroup CPU limits. Falls back to host CPU count
/// if container limits are not set or cannot be read.
fn detect_vcpu_count() -> usize {
    // First try container CPU limits (cgroup v2, then v1)
    if let Some(container_cpus) = get_container_cpu_limit() {
        return container_cpus;
    }

    // Fall back to sysinfo which returns logical CPUs
    get_system_cpu_count()
}

/// Gets the CPU count from the system using sysinfo.
fn get_system_cpu_count() -> usize {
    let mut system = System::new();
    system.refresh_cpu_list(sysinfo::CpuRefreshKind::nothing());
    let cpu_count = system.cpus().len();

    if cpu_count > 0 {
        cpu_count
    } else {
        // Absolute fallback - every system has at least 1 CPU
        tracing::warn!(
            "sysinfo returned 0 CPUs, falling back to 1. This may indicate a detection problem."
        );
        1
    }
}

/// Attempts to read container CPU limit from cgroup v2 or v1.
/// Returns None if not in a container or if the limit cannot be read.
///
/// The effective CPU count is the minimum of:
/// - CPU quota limit (from `cpu.max` or `cpu.cfs_quota_us`)
/// - cpuset effective count (from `cpuset.cpus.effective` or `cpuset.cpus`)
fn get_container_cpu_limit() -> Option<usize> {
    let quota_limit = get_cpu_quota_limit();
    let cpuset_limit = get_cpuset_effective_count();

    match (quota_limit, cpuset_limit) {
        (Some(quota), Some(cpuset)) => Some(quota.min(cpuset)),
        (Some(quota), None) => Some(quota),
        (None, Some(cpuset)) => Some(cpuset),
        (None, None) => None,
    }
}

/// Gets CPU quota limit from cgroup v2 or v1.
fn get_cpu_quota_limit() -> Option<usize> {
    // Try cgroup v2 first (newer container runtimes like containerd, newer Docker)
    if let Some(cpus) = get_cgroup_v2_cpu_limit() {
        return Some(cpus);
    }

    // Try cgroup v1 (older Docker, older K8s)
    get_cgroup_v1_cpu_limit()
}

/// Gets the effective CPU count from cpuset controller.
/// cpuset can further restrict which CPUs are available beyond quota limits.
fn get_cpuset_effective_count() -> Option<usize> {
    // Try cgroup v2 cpuset.cpus.effective first
    if let Some(count) = get_cgroup_v2_cpuset_effective() {
        return Some(count);
    }

    // Try cgroup v1 cpuset.cpus
    get_cgroup_v1_cpuset()
}

/// Gets effective CPU count from cgroup v2 `cpuset.cpus.effective`.
fn get_cgroup_v2_cpuset_effective() -> Option<usize> {
    let cgroup_path = get_process_cgroup_v2_path()?;
    let mountpoint = get_cgroup2_mountpoint().unwrap_or_else(|| "/sys/fs/cgroup".to_string());

    let path = build_cgroup_file_path(&mountpoint, &cgroup_path, "cpuset.cpus.effective");
    let contents = std::fs::read_to_string(&path).ok()?;
    parse_cpuset_cpus(&contents)
}

/// Gets CPU count from cgroup v1 `cpuset.cpus`.
fn get_cgroup_v1_cpuset() -> Option<usize> {
    let cpuset_path = get_process_cgroup_v1_path("cpuset")?;
    let mountpoint =
        get_cgroup_v1_mountpoint("cpuset").unwrap_or_else(|| "/sys/fs/cgroup/cpuset".to_string());

    let path = build_cgroup_file_path(&mountpoint, &cpuset_path, "cpuset.cpus");
    let contents = std::fs::read_to_string(&path).ok()?;
    parse_cpuset_cpus(&contents)
}

/// Parses cpuset.cpus or cpuset.cpus.effective content.
/// Format: comma-separated list of CPU ranges, e.g., "0-3,5,7-9"
fn parse_cpuset_cpus(contents: &str) -> Option<usize> {
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut count = 0;
    for part in trimmed.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some((start, end)) = part.split_once('-') {
            // Range like "0-3"
            let start: usize = start.trim().parse().ok()?;
            let end: usize = end.trim().parse().ok()?;
            if end >= start {
                count += end - start + 1;
            }
        } else {
            // Single CPU like "5"
            let _cpu: usize = part.parse().ok()?;
            count += 1;
        }
    }

    if count > 0 { Some(count) } else { None }
}

/// Builds a cgroup file path, handling root path "/" cleanly.
fn build_cgroup_file_path(mountpoint: &str, cgroup_path: &str, filename: &str) -> String {
    if cgroup_path == "/" || cgroup_path.is_empty() {
        format!("{mountpoint}/{filename}")
    } else {
        format!("{mountpoint}{cgroup_path}/{filename}")
    }
}

// =============================================================================
// Cgroup Path Detection
// =============================================================================

/// Gets the process's cgroup v2 path from `/proc/self/cgroup`.
/// Returns the path (possibly "/") or None if not on cgroup v2.
fn get_process_cgroup_v2_path() -> Option<String> {
    let contents = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    parse_proc_cgroup_v2_path(&contents)
}

/// Gets the process's cgroup v1 path for a specific controller.
fn get_process_cgroup_v1_path(controller: &str) -> Option<String> {
    let contents = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    parse_proc_cgroup_v1_path(&contents, controller)
}

/// Parses `/proc/self/cgroup` to extract the cgroup v2 path.
///
/// Expected format for cgroup v2: `0::<path>`
/// Example: `0::/user.slice/user-1000.slice/user@1000.service/app.slice/run.service`
fn parse_proc_cgroup_v2_path(contents: &str) -> Option<String> {
    for line in contents.lines() {
        // cgroup v2 unified hierarchy has format "0::<path>"
        if let Some(path) = line.strip_prefix("0::") {
            let trimmed = path.trim();
            // Return the path even if it's "/" - we handle that in build_cgroup_file_path
            if trimmed.is_empty() {
                return Some("/".to_string());
            }
            return Some(trimmed.to_string());
        }
    }
    None
}

/// Parses `/proc/self/cgroup` to extract a cgroup v1 controller path.
///
/// Expected format: `<id>:<controllers>:<path>`
/// Example: `7:cpuset:/kubepods/besteffort/pod12345`
fn parse_proc_cgroup_v1_path(contents: &str, controller: &str) -> Option<String> {
    for line in contents.lines() {
        let parts: Vec<&str> = line.splitn(3, ':').collect();
        if parts.len() < 3 {
            continue;
        }

        let controllers = parts[1];
        let path = parts[2].trim();

        // Check if this line contains our controller
        // Controllers can be comma-separated (e.g., "cpu,cpuacct")
        if controllers.split(',').any(|c| c == controller) {
            if path.is_empty() {
                return Some("/".to_string());
            }
            return Some(path.to_string());
        }
    }
    None
}

/// Gets the cgroup2 mountpoint from `/proc/self/mountinfo`.
fn get_cgroup2_mountpoint() -> Option<String> {
    let contents = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    parse_mountinfo_cgroup2(&contents)
}

/// Gets the cgroup v1 mountpoint for a specific controller from `/proc/self/mountinfo`.
fn get_cgroup_v1_mountpoint(controller: &str) -> Option<String> {
    let contents = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    parse_mountinfo_cgroup_v1(&contents, controller)
}

/// Parses `/proc/self/mountinfo` to find the cgroup2 mountpoint.
///
/// Format: `<id> <parent> <major>:<minor> <root> <mount_point> <options> <optional>... - <fstype> <source> <super_options>`
fn parse_mountinfo_cgroup2(contents: &str) -> Option<String> {
    for line in contents.lines() {
        // Split by " - " to separate mount options from filesystem info
        let parts: Vec<&str> = line.splitn(2, " - ").collect();
        if parts.len() < 2 {
            continue;
        }

        let fs_info = parts[1];
        let fs_parts: Vec<&str> = fs_info.split_whitespace().collect();
        if fs_parts.is_empty() {
            continue;
        }

        // Check if filesystem type is cgroup2
        if fs_parts[0] == "cgroup2" {
            // Mount point is the 5th field (index 4) in the first part
            let mount_parts: Vec<&str> = parts[0].split_whitespace().collect();
            if mount_parts.len() >= 5 {
                return Some(mount_parts[4].to_string());
            }
        }
    }
    None
}

/// Parses `/proc/self/mountinfo` to find a cgroup v1 controller mountpoint.
fn parse_mountinfo_cgroup_v1(contents: &str, controller: &str) -> Option<String> {
    for line in contents.lines() {
        let parts: Vec<&str> = line.splitn(2, " - ").collect();
        if parts.len() < 2 {
            continue;
        }

        let fs_info = parts[1];
        let fs_parts: Vec<&str> = fs_info.split_whitespace().collect();
        if fs_parts.is_empty() {
            continue;
        }

        // Check if filesystem type is cgroup (v1)
        if fs_parts[0] == "cgroup" {
            // Check super options for our controller
            // Super options are typically the 3rd field in fs_info
            if fs_parts.len() >= 3 {
                let super_options = fs_parts[2];
                if super_options.split(',').any(|opt| opt == controller) {
                    let mount_parts: Vec<&str> = parts[0].split_whitespace().collect();
                    if mount_parts.len() >= 5 {
                        return Some(mount_parts[4].to_string());
                    }
                }
            }
        }
    }
    None
}

/// Reads CPU limit from cgroup v2.
/// cgroup v2 uses `cpu.max` file with format: `$MAX $PERIOD`
fn get_cgroup_v2_cpu_limit() -> Option<usize> {
    let cgroup_path = get_process_cgroup_v2_path()?;
    let mountpoint = get_cgroup2_mountpoint().unwrap_or_else(|| "/sys/fs/cgroup".to_string());

    let path = build_cgroup_file_path(&mountpoint, &cgroup_path, "cpu.max");
    let contents = std::fs::read_to_string(&path).ok()?;
    parse_cgroup_v2_cpu_max(&contents)
}

/// Parses cgroup v2 cpu.max content.
/// Format: "$MAX $PERIOD" where MAX can be "max" (unlimited) or a number.
fn parse_cgroup_v2_cpu_max(contents: &str) -> Option<usize> {
    let parts: Vec<&str> = contents.split_whitespace().collect();

    if parts.len() < 2 {
        return None;
    }

    // "max" means no limit
    if parts[0] == "max" {
        return None;
    }

    let quota: u64 = parts[0].parse().ok()?;
    let period: u64 = parts[1].parse().ok()?;

    if period == 0 {
        return None;
    }

    // Calculate number of CPUs: quota / period, rounded up
    #[expect(clippy::cast_possible_truncation)]
    let cpus = quota.div_ceil(period) as usize;
    Some(cpus.max(1))
}

/// Reads CPU limit from cgroup v1.
/// cgroup v1 uses separate files for quota and period.
fn get_cgroup_v1_cpu_limit() -> Option<usize> {
    let cpu_path = get_process_cgroup_v1_path("cpu")?;
    let mountpoint =
        get_cgroup_v1_mountpoint("cpu").unwrap_or_else(|| "/sys/fs/cgroup/cpu".to_string());

    let quota_path = build_cgroup_file_path(&mountpoint, &cpu_path, "cpu.cfs_quota_us");
    let period_path = build_cgroup_file_path(&mountpoint, &cpu_path, "cpu.cfs_period_us");

    let quota_str = std::fs::read_to_string(&quota_path).ok()?;
    let period_str = std::fs::read_to_string(&period_path).ok()?;

    parse_cgroup_v1_cpu_quota(&quota_str, &period_str)
}

/// Parses cgroup v1 CPU quota and period.
/// Quota of -1 means unlimited.
fn parse_cgroup_v1_cpu_quota(quota_str: &str, period_str: &str) -> Option<usize> {
    let quota: i64 = quota_str.trim().parse().ok()?;
    let period: u64 = period_str.trim().parse().ok()?;

    // quota of -1 means no limit
    if quota < 0 || period == 0 {
        return None;
    }

    #[expect(clippy::cast_sign_loss)]
    let quota_u64 = quota as u64;

    // Calculate number of CPUs: quota / period, rounded up
    #[expect(clippy::cast_possible_truncation)]
    let cpus = quota_u64.div_ceil(period) as usize;
    Some(cpus.max(1))
}

// =============================================================================
// Memory Detection
// =============================================================================

/// Detects the total memory available in bytes.
///
/// For containerized deployments, returns the container memory limit from cgroup.
/// For bare-metal deployments, returns the system's total memory.
fn detect_total_memory() -> u64 {
    // Prefer container memory limit if available
    if let Some(container_memory) = get_container_memory_limit() {
        return container_memory;
    }

    // Fall back to system memory
    get_system_total_memory()
}

/// Gets the total system memory using sysinfo.
fn get_system_total_memory() -> u64 {
    let mut system = System::new();
    system.refresh_memory();
    system.total_memory()
}

/// Attempts to read container memory limit from cgroup v2 or v1.
/// Returns None if not in a container or if the limit cannot be read.
fn get_container_memory_limit() -> Option<u64> {
    // Try cgroup v2 first (newer container runtimes)
    if let Some(limit) = get_cgroup_v2_memory_limit() {
        return Some(limit);
    }

    // Try cgroup v1 (Docker, older K8s)
    get_cgroup_v1_memory_limit()
}

/// Reads memory limit from cgroup v2.
fn get_cgroup_v2_memory_limit() -> Option<u64> {
    let cgroup_path = get_process_cgroup_v2_path()?;
    let mountpoint = get_cgroup2_mountpoint().unwrap_or_else(|| "/sys/fs/cgroup".to_string());

    let path = build_cgroup_file_path(&mountpoint, &cgroup_path, "memory.max");
    let contents = std::fs::read_to_string(&path).ok()?;
    parse_cgroup_v2_memory_max(&contents)
}

/// Parses cgroup v2 memory.max content.
/// "max" means unlimited.
fn parse_cgroup_v2_memory_max(contents: &str) -> Option<u64> {
    let trimmed = contents.trim();

    // "max" means no limit
    if trimmed == "max" {
        return None;
    }

    let limit: u64 = trimmed.parse().ok()?;

    // Very large values typically mean no limit
    if limit >= u64::MAX - 1 {
        return None;
    }

    Some(limit)
}

/// Reads memory limit from cgroup v1.
fn get_cgroup_v1_memory_limit() -> Option<u64> {
    let mem_path = get_process_cgroup_v1_path("memory")?;
    let mountpoint =
        get_cgroup_v1_mountpoint("memory").unwrap_or_else(|| "/sys/fs/cgroup/memory".to_string());

    let path = build_cgroup_file_path(&mountpoint, &mem_path, "memory.limit_in_bytes");
    let contents = std::fs::read_to_string(&path).ok()?;
    parse_cgroup_v1_memory_limit(&contents)
}

/// Parses cgroup v1 memory limit.
fn parse_cgroup_v1_memory_limit(contents: &str) -> Option<u64> {
    let limit: u64 = contents.trim().parse().ok()?;

    // Very large values (like u64::MAX or close to it) typically mean no limit
    // Use 2^62 as threshold - anything above is considered "no limit"
    if limit >= (1u64 << 62) {
        return None;
    }

    Some(limit)
}

// =============================================================================
// GPU Detection
// =============================================================================

/// Detects the number of GPUs available on the system.
///
/// Currently supports:
/// - NVIDIA GPUs via filesystem inspection (Linux)
/// - Apple Metal GPUs on macOS
///
/// For containers, this looks at the host's GPU configuration that has been
/// passed through to the container (e.g., via nvidia-container-runtime).
fn detect_gpu_count() -> usize {
    // Try NVIDIA GPU detection first (works on Linux, including containers)
    if let Some(count) = detect_nvidia_gpus() {
        return count;
    }

    // Try Apple Metal on macOS
    #[cfg(target_os = "macos")]
    if let Some(count) = detect_metal_gpus() {
        return count;
    }

    0
}

/// Detects NVIDIA GPUs using multiple methods for robustness.
///
/// This approach works both on bare metal and in containers with NVIDIA runtime.
fn detect_nvidia_gpus() -> Option<usize> {
    // Method 1: Check /proc/driver/nvidia/gpus/ (Linux with NVIDIA driver)
    // This is the most reliable method when available
    if let Some(count) = detect_nvidia_via_proc() {
        return Some(count);
    }

    // Method 2: Check for NVIDIA device files (/dev/nvidia0, etc.)
    if let Some(count) = detect_nvidia_via_dev() {
        return Some(count);
    }

    // Method 3: Check NVIDIA_VISIBLE_DEVICES environment variable
    // This is commonly set in containerized environments
    detect_nvidia_via_env()
}

/// Detects NVIDIA GPUs via /proc/driver/nvidia/gpus/ directory.
fn detect_nvidia_via_proc() -> Option<usize> {
    let entries = std::fs::read_dir("/proc/driver/nvidia/gpus").ok()?;
    let count = entries.filter_map(Result::ok).count();

    if count > 0 { Some(count) } else { None }
}

/// Detects NVIDIA GPUs via /dev/nvidia* device files.
fn detect_nvidia_via_dev() -> Option<usize> {
    // Support up to 16 GPUs - sufficient for most deployments
    const MAX_GPUS: usize = 16;

    let mut count = 0;
    for i in 0..MAX_GPUS {
        let device_path = format!("/dev/nvidia{i}");
        if std::path::Path::new(&device_path).exists() {
            count += 1;
        } else {
            // Devices are numbered sequentially, stop at first missing
            break;
        }
    }

    if count > 0 { Some(count) } else { None }
}

/// Detects NVIDIA GPUs via `NVIDIA_VISIBLE_DEVICES` environment variable.
fn detect_nvidia_via_env() -> Option<usize> {
    let visible_devices = std::env::var("NVIDIA_VISIBLE_DEVICES").ok()?;
    parse_nvidia_visible_devices(&visible_devices)
}

/// Parses the `NVIDIA_VISIBLE_DEVICES` environment variable.
fn parse_nvidia_visible_devices(value: &str) -> Option<usize> {
    let trimmed = value.trim();

    if trimmed.is_empty() {
        return Some(0);
    }

    // "all" means all GPUs are visible - can't determine count
    if trimmed.eq_ignore_ascii_case("all") {
        return None;
    }

    // "none" or "void" means no GPUs visible
    if trimmed.eq_ignore_ascii_case("none") || trimmed.eq_ignore_ascii_case("void") {
        return Some(0);
    }

    // Count comma-separated device IDs (e.g., "0,1,2" or "GPU-uuid1,GPU-uuid2")
    let count = trimmed.split(',').filter(|s| !s.is_empty()).count();
    Some(count)
}

/// Detects Apple Metal GPUs on macOS.
///
/// Returns `Some(1)` for known macOS architectures (`aarch64`, `x86_64`) since all
/// modern Macs have at least one Metal-capable GPU. Returns `None` for unknown
/// architectures as a safety measure.
#[cfg(target_os = "macos")]
#[expect(
    clippy::unnecessary_wraps,
    reason = "Intentional: None for unknown archs"
)]
fn detect_metal_gpus() -> Option<usize> {
    // Apple Silicon (aarch64) always has 1 integrated GPU (Apple GPU)
    // This is a reliable detection since all Apple Silicon Macs have Metal support
    // For Intel Macs (x86_64), return 1 as they typically have at least an integrated GPU
    // Note: Some Intel Macs have both integrated and discrete GPUs, but we report 1
    // as the minimum guaranteed count
    #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
    {
        Some(1)
    }

    // Unknown architecture - shouldn't happen on macOS but handle gracefully
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        None
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // HardwareInfo Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_hardware_info_detect_returns_valid_values() {
        const MIN_MEMORY: u64 = 1024 * 1024;
        let info = HardwareInfo::detect();

        // vCPU count must be at least 1 - every system has at least one CPU
        assert!(
            info.vcpu_count >= 1,
            "vCPU count should be at least 1, got {vcpu_count}",
            vcpu_count = info.vcpu_count
        );

        // Memory should be at least 1MB - sanity check for modern systems
        assert!(
            info.total_memory_bytes >= MIN_MEMORY,
            "Total memory should be at least 1MB, got {total_memory_bytes} bytes",
            total_memory_bytes = info.total_memory_bytes
        );

        // GPU count can be 0 - just verify it doesn't panic and is reasonable
        assert!(
            info.gpu_count <= 64,
            "GPU count seems unreasonably high: {gpu_count}",
            gpu_count = info.gpu_count
        );
    }

    #[test]
    fn test_hardware_info_display() {
        let info = HardwareInfo {
            vcpu_count: 4,
            gpu_count: 1,
            total_memory_bytes: 8 * 1024 * 1024 * 1024, // 8 GiB
        };
        let display = format!("{info}");
        assert!(display.contains("vCPUs: 4"), "Display missing vCPU count");
        assert!(display.contains("GPUs: 1"), "Display missing GPU count");
        assert!(display.contains("GiB"), "Display missing memory unit");
    }

    #[test]
    fn test_hardware_info_equality() {
        let info1 = HardwareInfo {
            vcpu_count: 4,
            gpu_count: 1,
            total_memory_bytes: 8 * 1024 * 1024 * 1024,
        };
        let info2 = info1.clone();
        assert_eq!(info1, info2);
    }

    // -------------------------------------------------------------------------
    // CPU Detection Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_detect_vcpu_count_returns_positive() {
        let count = detect_vcpu_count();
        assert!(count >= 1, "vCPU count should be at least 1, got {count}");
    }

    #[test]
    fn test_get_system_cpu_count_returns_positive() {
        let count = get_system_cpu_count();
        assert!(
            count >= 1,
            "System CPU count should be at least 1, got {count}"
        );
    }

    #[test]
    fn test_parse_cgroup_v2_cpu_max_limited() {
        // 2 CPUs: 200000 / 100000 = 2
        assert_eq!(parse_cgroup_v2_cpu_max("200000 100000\n"), Some(2));

        // 4 CPUs
        assert_eq!(parse_cgroup_v2_cpu_max("400000 100000"), Some(4));

        // 1 CPU (partial)
        assert_eq!(parse_cgroup_v2_cpu_max("50000 100000"), Some(1));

        // Rounds up: 150000 / 100000 = 1.5 -> 2
        assert_eq!(parse_cgroup_v2_cpu_max("150000 100000"), Some(2));
    }

    #[test]
    fn test_parse_cgroup_v2_cpu_max_unlimited() {
        // "max" means no limit
        assert_eq!(parse_cgroup_v2_cpu_max("max 100000\n"), None);
        assert_eq!(parse_cgroup_v2_cpu_max("max 100000"), None);
    }

    #[test]
    fn test_parse_cgroup_v2_cpu_max_invalid() {
        assert_eq!(parse_cgroup_v2_cpu_max(""), None);
        assert_eq!(parse_cgroup_v2_cpu_max("invalid"), None);
        assert_eq!(parse_cgroup_v2_cpu_max("100000"), None); // Missing period
        assert_eq!(parse_cgroup_v2_cpu_max("100000 0"), None); // Zero period
    }

    #[test]
    fn test_parse_cgroup_v1_cpu_quota_limited() {
        // 2 CPUs
        assert_eq!(parse_cgroup_v1_cpu_quota("200000\n", "100000\n"), Some(2));

        // 4 CPUs
        assert_eq!(parse_cgroup_v1_cpu_quota("400000", "100000"), Some(4));
    }

    #[test]
    fn test_parse_cgroup_v1_cpu_quota_unlimited() {
        // -1 means no limit
        assert_eq!(parse_cgroup_v1_cpu_quota("-1\n", "100000\n"), None);
    }

    #[test]
    fn test_parse_cgroup_v1_cpu_quota_invalid() {
        assert_eq!(parse_cgroup_v1_cpu_quota("", "100000"), None);
        assert_eq!(parse_cgroup_v1_cpu_quota("100000", ""), None);
        assert_eq!(parse_cgroup_v1_cpu_quota("invalid", "100000"), None);
        assert_eq!(parse_cgroup_v1_cpu_quota("100000", "0"), None); // Zero period
    }

    #[test]
    fn test_parse_cpuset_cpus_single() {
        // Single CPU
        assert_eq!(parse_cpuset_cpus("0\n"), Some(1));
        assert_eq!(parse_cpuset_cpus("5"), Some(1));
    }

    #[test]
    fn test_parse_cpuset_cpus_range() {
        // CPU range
        assert_eq!(parse_cpuset_cpus("0-3\n"), Some(4));
        assert_eq!(parse_cpuset_cpus("0-7"), Some(8));
        assert_eq!(parse_cpuset_cpus("2-5"), Some(4));
    }

    #[test]
    fn test_parse_cpuset_cpus_mixed() {
        // Mixed single CPUs and ranges
        assert_eq!(parse_cpuset_cpus("0-3,5,7-9\n"), Some(8)); // 0,1,2,3,5,7,8,9
        assert_eq!(parse_cpuset_cpus("0,2,4,6"), Some(4));
        assert_eq!(parse_cpuset_cpus("0-1,4-5"), Some(4));
    }

    #[test]
    fn test_parse_cpuset_cpus_invalid() {
        assert_eq!(parse_cpuset_cpus(""), None);
        assert_eq!(parse_cpuset_cpus("   "), None);
        assert_eq!(parse_cpuset_cpus("invalid"), None);
    }

    #[test]
    fn test_parse_proc_cgroup_v2_path_user_slice() {
        // Typical user session on systemd-managed Linux
        let contents = "0::/user.slice/user-1000.slice/user@1000.service/app.slice/run-p363181-i363182.service\n";
        assert_eq!(
            parse_proc_cgroup_v2_path(contents),
            Some("/user.slice/user-1000.slice/user@1000.service/app.slice/run-p363181-i363182.service".to_string())
        );
    }

    #[test]
    fn test_parse_proc_cgroup_v2_path_container() {
        // Docker container with cgroup v2
        let contents = "0::/docker/abc123def456\n";
        assert_eq!(
            parse_proc_cgroup_v2_path(contents),
            Some("/docker/abc123def456".to_string())
        );

        // Kubernetes pod
        let contents = "0::/kubepods/besteffort/pod12345/container-abc\n";
        assert_eq!(
            parse_proc_cgroup_v2_path(contents),
            Some("/kubepods/besteffort/pod12345/container-abc".to_string())
        );
    }

    #[test]
    fn test_parse_proc_cgroup_v2_path_root() {
        // Root cgroup should return "/" (we now handle this in build_cgroup_file_path)
        let contents = "0::/\n";
        assert_eq!(parse_proc_cgroup_v2_path(contents), Some("/".to_string()));

        // Empty path should return "/"
        let contents = "0::\n";
        assert_eq!(parse_proc_cgroup_v2_path(contents), Some("/".to_string()));
    }

    #[test]
    fn test_parse_proc_cgroup_v2_path_cgroup_v1_only() {
        // cgroup v1 only (no 0:: line)
        let contents =
            "12:blkio:/docker/abc123\n11:memory:/docker/abc123\n10:cpu,cpuacct:/docker/abc123\n";
        assert_eq!(parse_proc_cgroup_v2_path(contents), None);
    }

    #[test]
    fn test_parse_proc_cgroup_v2_path_hybrid() {
        // Hybrid cgroup v1/v2 setup (some distros)
        let contents = "12:blkio:/docker/abc123\n0::/system.slice/docker.service\n";
        assert_eq!(
            parse_proc_cgroup_v2_path(contents),
            Some("/system.slice/docker.service".to_string())
        );
    }

    #[test]
    fn test_parse_proc_cgroup_v1_path() {
        // Typical cgroup v1 content
        let contents = "12:blkio:/docker/abc123\n11:memory:/docker/abc123\n10:cpu,cpuacct:/docker/abc123\n7:cpuset:/kubepods/pod12345\n";

        assert_eq!(
            parse_proc_cgroup_v1_path(contents, "cpuset"),
            Some("/kubepods/pod12345".to_string())
        );
        assert_eq!(
            parse_proc_cgroup_v1_path(contents, "memory"),
            Some("/docker/abc123".to_string())
        );
        assert_eq!(
            parse_proc_cgroup_v1_path(contents, "cpu"),
            Some("/docker/abc123".to_string())
        );
        assert_eq!(
            parse_proc_cgroup_v1_path(contents, "cpuacct"),
            Some("/docker/abc123".to_string())
        );
        assert_eq!(parse_proc_cgroup_v1_path(contents, "nonexistent"), None);
    }

    #[test]
    fn test_parse_proc_cgroup_v1_path_root() {
        let contents = "7:cpuset:/\n";
        assert_eq!(
            parse_proc_cgroup_v1_path(contents, "cpuset"),
            Some("/".to_string())
        );
    }

    #[test]
    fn test_parse_mountinfo_cgroup2() {
        // Typical cgroup2 mount
        let contents = "29 28 0:25 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:9 - cgroup2 cgroup2 rw,nsdelegate,memory_recursiveprot\n";
        assert_eq!(
            parse_mountinfo_cgroup2(contents),
            Some("/sys/fs/cgroup".to_string())
        );

        // Custom mountpoint
        let contents = "29 28 0:25 / /custom/cgroup2 rw shared:9 - cgroup2 cgroup2 rw\n";
        assert_eq!(
            parse_mountinfo_cgroup2(contents),
            Some("/custom/cgroup2".to_string())
        );

        // No cgroup2 mount
        let contents = "29 28 0:25 / /sys/fs/cgroup/cpuset rw - cgroup cgroup rw,cpuset\n";
        assert_eq!(parse_mountinfo_cgroup2(contents), None);
    }

    #[test]
    fn test_parse_mountinfo_cgroup_v1() {
        let contents = "30 29 0:26 / /sys/fs/cgroup/cpuset rw,nosuid,nodev,noexec,relatime shared:10 - cgroup cgroup rw,cpuset\n\
                        31 29 0:27 / /sys/fs/cgroup/cpu,cpuacct rw,nosuid,nodev,noexec,relatime shared:11 - cgroup cgroup rw,cpu,cpuacct\n\
                        32 29 0:28 / /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime shared:12 - cgroup cgroup rw,memory\n";

        assert_eq!(
            parse_mountinfo_cgroup_v1(contents, "cpuset"),
            Some("/sys/fs/cgroup/cpuset".to_string())
        );
        assert_eq!(
            parse_mountinfo_cgroup_v1(contents, "memory"),
            Some("/sys/fs/cgroup/memory".to_string())
        );
        assert_eq!(
            parse_mountinfo_cgroup_v1(contents, "cpu"),
            Some("/sys/fs/cgroup/cpu,cpuacct".to_string())
        );
        assert_eq!(parse_mountinfo_cgroup_v1(contents, "nonexistent"), None);
    }

    #[test]
    fn test_build_cgroup_file_path() {
        // Normal path
        assert_eq!(
            build_cgroup_file_path("/sys/fs/cgroup", "/docker/abc123", "cpu.max"),
            "/sys/fs/cgroup/docker/abc123/cpu.max"
        );

        // Root path
        assert_eq!(
            build_cgroup_file_path("/sys/fs/cgroup", "/", "cpu.max"),
            "/sys/fs/cgroup/cpu.max"
        );

        // Empty path (treated as root)
        assert_eq!(
            build_cgroup_file_path("/sys/fs/cgroup", "", "cpu.max"),
            "/sys/fs/cgroup/cpu.max"
        );
    }

    // -------------------------------------------------------------------------
    // Memory Detection Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_detect_total_memory_returns_positive() {
        let memory = detect_total_memory();
        assert!(
            memory >= 1024 * 1024,
            "Memory should be at least 1MB, got {memory} bytes"
        );
    }

    #[test]
    fn test_get_system_total_memory_returns_positive() {
        let memory = get_system_total_memory();
        assert!(
            memory >= 1024 * 1024,
            "System memory should be at least 1MB, got {memory} bytes"
        );
    }

    #[test]
    fn test_parse_cgroup_v2_memory_max_limited() {
        // 1 GiB limit
        assert_eq!(
            parse_cgroup_v2_memory_max("1073741824\n"),
            Some(1_073_741_824)
        );

        // 512 MiB limit
        assert_eq!(parse_cgroup_v2_memory_max("536870912"), Some(536_870_912));
    }

    #[test]
    fn test_parse_cgroup_v2_memory_max_unlimited() {
        assert_eq!(parse_cgroup_v2_memory_max("max\n"), None);
        assert_eq!(parse_cgroup_v2_memory_max("max"), None);
        // Near-max values are treated as unlimited
        assert_eq!(parse_cgroup_v2_memory_max(&format!("{}", u64::MAX)), None);
    }

    #[test]
    fn test_parse_cgroup_v2_memory_max_invalid() {
        assert_eq!(parse_cgroup_v2_memory_max(""), None);
        assert_eq!(parse_cgroup_v2_memory_max("invalid"), None);
    }

    #[test]
    fn test_parse_cgroup_v1_memory_limit_limited() {
        // 1 GiB limit
        assert_eq!(
            parse_cgroup_v1_memory_limit("1073741824\n"),
            Some(1_073_741_824)
        );
    }

    #[test]
    fn test_parse_cgroup_v1_memory_limit_unlimited() {
        // Very large values are treated as unlimited
        let large_limit = (1u64 << 62).to_string();
        assert_eq!(parse_cgroup_v1_memory_limit(&large_limit), None);
    }

    // -------------------------------------------------------------------------
    // GPU Detection Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_detect_gpu_count_no_panic() {
        // Just ensure GPU detection doesn't panic on any platform
        let _ = detect_gpu_count();
    }

    #[test]
    fn test_parse_nvidia_visible_devices_all() {
        // "all" means we can't determine count
        assert_eq!(parse_nvidia_visible_devices("all"), None);
        assert_eq!(parse_nvidia_visible_devices("ALL"), None);
        assert_eq!(parse_nvidia_visible_devices("All"), None);
    }

    #[test]
    fn test_parse_nvidia_visible_devices_none() {
        assert_eq!(parse_nvidia_visible_devices("none"), Some(0));
        assert_eq!(parse_nvidia_visible_devices("NONE"), Some(0));
        assert_eq!(parse_nvidia_visible_devices("void"), Some(0));
        assert_eq!(parse_nvidia_visible_devices(""), Some(0));
    }

    #[test]
    fn test_parse_nvidia_visible_devices_specific() {
        // Single GPU
        assert_eq!(parse_nvidia_visible_devices("0"), Some(1));
        assert_eq!(parse_nvidia_visible_devices("GPU-12345"), Some(1));

        // Multiple GPUs
        assert_eq!(parse_nvidia_visible_devices("0,1"), Some(2));
        assert_eq!(parse_nvidia_visible_devices("0,1,2"), Some(3));
        assert_eq!(parse_nvidia_visible_devices("0,1,2,3"), Some(4));

        // With whitespace
        assert_eq!(parse_nvidia_visible_devices("  0,1  "), Some(2));
    }

    #[test]
    fn test_parse_nvidia_visible_devices_uuids() {
        // GPU UUIDs (common in enterprise deployments)
        assert_eq!(
            parse_nvidia_visible_devices("GPU-a1b2c3d4,GPU-e5f6g7h8"),
            Some(2)
        );
    }

    // -------------------------------------------------------------------------
    // Integration Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_hardware_info_detect_is_consistent() {
        // Multiple calls should return consistent results
        let info1 = HardwareInfo::detect();
        let info2 = HardwareInfo::detect();

        // CPU and memory should be identical
        assert_eq!(
            info1.vcpu_count, info2.vcpu_count,
            "vCPU count should be consistent"
        );
        assert_eq!(
            info1.total_memory_bytes, info2.total_memory_bytes,
            "Memory should be consistent"
        );
        // GPU count should also be consistent
        assert_eq!(
            info1.gpu_count, info2.gpu_count,
            "GPU count should be consistent"
        );
    }

    #[test]
    fn test_hardware_detection_performance() {
        // Hardware detection should complete quickly (< 500ms)
        // Using 500ms threshold for robustness across different environments
        // (containerized environments with slow I/O may take longer)
        let start = std::time::Instant::now();
        let _info = HardwareInfo::detect();
        let elapsed = start.elapsed();

        assert!(
            elapsed.as_millis() < 500,
            "Hardware detection took too long: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_hardware_info_detect_async() {
        let result = HardwareInfo::detect_async().await;
        assert!(result.is_ok(), "detect_async should not fail");
        let info = result.expect("detect_async returned error");
        assert!(info.vcpu_count >= 1, "vCPU count should be at least 1");
    }
}
