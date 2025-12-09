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
//! - **CPU limits**: Reads from `/sys/fs/cgroup/cpu.max` (v2) or
//!   `/sys/fs/cgroup/cpu/cpu.cfs_quota_us` (v1)
//! - **Memory limits**: Reads from `/sys/fs/cgroup/memory.max` (v2) or
//!   `/sys/fs/cgroup/memory/memory.limit_in_bytes` (v1)
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
//! - Uses lazy initialization patterns
//! - Minimizes filesystem reads
//! - Caches results where appropriate

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

impl HardwareInfo {
    /// Detects hardware information for the current system.
    ///
    /// For containerized deployments, this attempts to detect container resource limits
    /// from cgroup v1/v2 before falling back to host system values.
    ///
    /// This function is designed to be fast and should not significantly impact
    /// runtime startup time.
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
        1
    }
}

/// Attempts to read container CPU limit from cgroup v2 or v1.
/// Returns None if not in a container or if the limit cannot be read.
fn get_container_cpu_limit() -> Option<usize> {
    // Try cgroup v2 first (newer container runtimes like containerd, newer Docker)
    if let Some(cpus) = get_cgroup_v2_cpu_limit() {
        return Some(cpus);
    }

    // Try cgroup v1 (older Docker, older K8s)
    get_cgroup_v1_cpu_limit()
}

/// Reads CPU limit from cgroup v2.
/// cgroup v2 uses cpu.max file with format: "$MAX $PERIOD"
fn get_cgroup_v2_cpu_limit() -> Option<usize> {
    let contents = std::fs::read_to_string("/sys/fs/cgroup/cpu.max").ok()?;
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
    let quota_str = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").ok()?;
    let period_str = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us").ok()?;

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
    let contents = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
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
    let contents = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes").ok()?;
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
        // Hardware detection should complete quickly (< 100ms)
        let start = std::time::Instant::now();
        let _info = HardwareInfo::detect();
        let elapsed = start.elapsed();

        assert!(
            elapsed.as_millis() < 100,
            "Hardware detection took too long: {elapsed:?}"
        );
    }
}
