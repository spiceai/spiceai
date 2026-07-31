/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! cgroup v1/v2 readers for the CPU entitlement, plus the cgroup path
//! resolution other `/sys/fs/cgroup` consumers in the runtime share.
//!
//! Everything that interprets a file's *contents* is a pure function over a
//! `&str`, so the whole matrix is unit-testable on any platform with no live
//! cgroup. Only the handful of functions that touch `/proc` and `/sys` are
//! Linux-gated, and they return `None` everywhere else.

/// cgroup v1's default `cpu.shares`, and the value a container runtime leaves in
/// place when the operator sets no CPU share at all.
pub const DEFAULT_SHARES: u64 = 1024;

/// cgroup v2's default `cpu.weight` — the kernel default, systemd's default
/// `CPUWeight`, and what a plain `docker run` leaves alone.
pub const DEFAULT_WEIGHT: u64 = 100;

/// The kubelet's `cpu.shares` floor. A pod that declares no CPU request
/// (best-effort, or a memory-only request) gets exactly this.
pub const FLOOR_SHARES: u64 = 2;

/// libcontainer's shares-to-weight numerator/denominator:
/// `weight = 1 + ((shares - 2) * WEIGHT_SPAN) / SHARES_SPAN`.
const WEIGHT_SPAN: u64 = 9999;
const SHARES_SPAN: u64 = 262_142;

/// A CPU share reading, normalized to cgroup v1 `cpu.shares` units.
///
/// The two flags record what the raw value means, which the ladder needs and the
/// share count alone cannot express:
///
/// - `is_default` — the raw value was the *unset* default for the cgroup version
///   it came from. A default share carries no information about an entitlement:
///   it is what an unconstrained process under systemd or a plain `docker run`
///   reports, so it must never drive a warning outside Kubernetes.
/// - `is_floor` — the raw value was the kubelet's no-request floor. A pod with
///   no `requests.cpu` must keep sizing for the node it can burst onto, not for
///   the 2 shares the kubelet wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuShares {
    /// Shares in cgroup v1 units (a v2 `cpu.weight` is inverted into these).
    pub shares: u64,
    /// The raw value was the cgroup version's unset default.
    pub is_default: bool,
    /// The raw value was the kubelet's no-CPU-request floor.
    pub is_floor: bool,
}

impl CpuShares {
    /// The entitlement these shares imply, in millicores.
    ///
    /// Inverts the kubelet's `shares = millicpu * 1024 / 1000`, then snaps to
    /// the nearest 100m. The snap exists because the cgroup v2 `cpu.weight`
    /// round-trip is lossy by a fraction of a percent in both directions:
    /// without it a `requests.cpu: 4` pod reads back as 4008m (rounding *up* to
    /// five cores) and a `requests.cpu: 1` pod as 987m. Kubernetes CPU requests
    /// are written in round numbers, so snapping recovers the operator's actual
    /// value. Below 100m the snap is skipped — it would round a tiny request to
    /// zero, and every sub-core entitlement resolves to the same one-core floor
    /// anyway.
    #[must_use]
    pub const fn millicores(self) -> u64 {
        let raw = self.shares.saturating_mul(1000) / 1024;
        if raw < 100 {
            raw
        } else {
            (raw + 50) / 100 * 100
        }
    }

    /// Whether the implied entitlement says anything about what was requested.
    #[must_use]
    pub const fn is_meaningful(self) -> bool {
        !self.is_floor
    }
}

/// Parse cgroup v2 `cpu.max` — `"<quota> <period>"`, where a quota of `max`
/// means no limit — into an entitlement in millicores.
///
/// Returns `None` when there is no quota, or when the contents are malformed.
#[must_use]
pub fn parse_cpu_max(contents: &str) -> Option<u64> {
    let mut parts = contents.split_whitespace();
    let quota = parts.next()?;
    if quota == "max" {
        return None;
    }
    let quota: u64 = quota.parse().ok()?;
    // The period is optional in the file format; the kernel default is 100000µs.
    let period: u64 = parts.next().map_or(Ok(100_000), str::parse).ok()?;
    quota_to_millicores(quota, period)
}

/// Parse the cgroup v1 pair `cpu.cfs_quota_us` / `cpu.cfs_period_us` into an
/// entitlement in millicores. A quota of `-1` means no limit.
#[must_use]
pub fn parse_cfs_quota(quota: &str, period: &str) -> Option<u64> {
    let quota: i64 = quota.trim().parse().ok()?;
    if quota <= 0 {
        return None;
    }
    let period: u64 = period.trim().parse().ok()?;
    quota_to_millicores(u64::try_from(quota).ok()?, period)
}

fn quota_to_millicores(quota: u64, period: u64) -> Option<u64> {
    if period == 0 {
        return None;
    }
    Some(quota.saturating_mul(1000) / period)
}

/// Parse cgroup v1 `cpu.shares`.
#[must_use]
pub fn parse_shares(contents: &str) -> Option<CpuShares> {
    let shares: u64 = contents.trim().parse().ok()?;
    Some(CpuShares {
        shares,
        is_default: shares == DEFAULT_SHARES,
        is_floor: shares <= FLOOR_SHARES,
    })
}

/// Parse cgroup v2 `cpu.weight`, inverting it back to v1 shares.
///
/// libcontainer maps shares onto the v2 weight range with
/// `weight = 1 + ((shares - 2) * 9999) / 262142`, truncating. Each weight
/// therefore names a *range* of shares; [`weight_to_shares`] returns that
/// range's midpoint. `weight == 1` is the bottom bucket, which the kubelet
/// writes both for a pod with no CPU request and for any request under ~27m —
/// indistinguishable, so it is reported as the floor.
#[must_use]
pub fn parse_weight(contents: &str) -> Option<CpuShares> {
    let weight: u64 = contents.trim().parse().ok()?;
    Some(CpuShares {
        shares: weight_to_shares(weight),
        is_default: weight == DEFAULT_WEIGHT,
        is_floor: weight <= 1,
    })
}

/// Invert libcontainer's `weight = 1 + ((shares - 2) * 9999) / 262142` to the
/// midpoint of the share range that maps to `weight`.
#[must_use]
pub fn weight_to_shares(weight: u64) -> u64 {
    // The forward map truncates, so `weight` covers shares in
    // `[inv(weight), inv(weight + 1))`. Take that interval's midpoint —
    // `(2 * (weight - 1) + 1) / 2 * SHARES_SPAN / WEIGHT_SPAN` — evaluated in
    // integers by doubling the denominator, and rounded to nearest.
    let numerator = (2 * weight.saturating_sub(1) + 1).saturating_mul(SHARES_SPAN);
    FLOOR_SHARES + (numerator + WEIGHT_SPAN) / (2 * WEIGHT_SPAN)
}

/// The path to `filename` in this process's cgroup v2 hierarchy, or `None` when
/// the system is not running cgroup v2 (or is not Linux).
#[must_use]
pub fn v2_file_path(filename: &str) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let cgroup_path = parse_proc_cgroup_v2_path(&read_to_string("/proc/self/cgroup")?)?;
        let mountpoint = read_to_string("/proc/self/mountinfo")
            .and_then(|s| parse_mountinfo_cgroup2(&s))
            .unwrap_or_else(|| "/sys/fs/cgroup".to_string());
        Some(cgroup_file_path(&mountpoint, &cgroup_path, filename))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = filename;
        None
    }
}

/// The path to `filename` under this process's cgroup v1 `controller`
/// hierarchy, or `None` when that controller is not mounted (or not Linux).
#[must_use]
pub fn v1_file_path(controller: &str, filename: &str) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let cgroup_path =
            parse_proc_cgroup_v1_path(&read_to_string("/proc/self/cgroup")?, controller)?;
        let mountpoint = read_to_string("/proc/self/mountinfo")
            .and_then(|s| parse_mountinfo_cgroup_v1(&s, controller))
            .unwrap_or_else(|| format!("/sys/fs/cgroup/{controller}"));
        Some(cgroup_file_path(&mountpoint, &cgroup_path, filename))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (controller, filename);
        None
    }
}

/// Read a file whose entire contents are a single unsigned integer.
#[must_use]
pub fn read_u64_file(path: &str) -> Option<u64> {
    read_to_string(path)?.trim().parse().ok()
}

fn read_to_string(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// Join a cgroup mountpoint, the process's path within the hierarchy, and a
/// filename.
#[must_use]
pub fn cgroup_file_path(mountpoint: &str, cgroup_path: &str, filename: &str) -> String {
    if cgroup_path == "/" || cgroup_path.is_empty() {
        format!("{mountpoint}/{filename}")
    } else {
        format!("{mountpoint}{cgroup_path}/{filename}")
    }
}

/// The process's cgroup v2 path from `/proc/self/cgroup` (the `0::` line).
#[must_use]
pub fn parse_proc_cgroup_v2_path(contents: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        line.strip_prefix("0::").map(|path| {
            let trimmed = path.trim();
            if trimmed.is_empty() {
                "/".to_string()
            } else {
                trimmed.to_string()
            }
        })
    })
}

/// The process's cgroup v1 path for `controller` from `/proc/self/cgroup`.
#[must_use]
pub fn parse_proc_cgroup_v1_path(contents: &str, controller: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let mut parts = line.splitn(3, ':');
        let _hierarchy = parts.next()?;
        let controllers = parts.next()?;
        let path = parts.next()?.trim();
        controllers.split(',').any(|c| c == controller).then(|| {
            if path.is_empty() {
                "/".to_string()
            } else {
                path.to_string()
            }
        })
    })
}

/// The cgroup v2 mountpoint from `/proc/self/mountinfo`.
#[must_use]
pub fn parse_mountinfo_cgroup2(contents: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let (mount, fs) = line.split_once(" - ")?;
        (fs.split_whitespace().next()? == "cgroup2")
            .then(|| mount.split_whitespace().nth(4).map(ToString::to_string))?
    })
}

/// The cgroup v1 mountpoint for `controller` from `/proc/self/mountinfo`.
#[must_use]
pub fn parse_mountinfo_cgroup_v1(contents: &str, controller: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let (mount, fs) = line.split_once(" - ")?;
        let mut fs_parts = fs.split_whitespace();
        if fs_parts.next()? != "cgroup" {
            return None;
        }
        let _source = fs_parts.next()?;
        let super_options = fs_parts.next()?;
        super_options
            .split(',')
            .any(|opt| opt == controller)
            .then(|| mount.split_whitespace().nth(4).map(ToString::to_string))?
    })
}

/// Whether this process is running under a Kubernetes-managed cgroup.
///
/// Only under Kubernetes is a CPU share known to have been derived from a
/// declared `requests.cpu`; everywhere else it is a relative scheduling weight
/// with an ambiguous default.
///
/// `KUBERNETES_SERVICE_HOST` is the primary signal — the kubelet injects it into
/// every pod by default. The `kubepods` cgroup path is a fallback for a pod that
/// has it suppressed (`enableServiceLinks` and a custom API service can both
/// remove it); it only helps when the container is *not* in its own cgroup
/// namespace, since a namespaced `/proc/self/cgroup` reads `0::/` and carries no
/// slice name. A pod that suppresses the variable *and* runs namespaced falls
/// through to affinity — the prior behaviour, and the escape hatch is
/// `SPICE_CPU_CORES`.
#[must_use]
pub fn in_kubernetes() -> bool {
    if std::env::var_os("KUBERNETES_SERVICE_HOST").is_some() {
        return true;
    }
    read_to_string("/proc/self/cgroup").is_some_and(|c| proc_cgroup_is_kubepods(&c))
}

/// Whether `/proc/self/cgroup` contents name a kubelet-managed slice.
#[must_use]
pub fn proc_cgroup_is_kubepods(contents: &str) -> bool {
    contents.contains("kubepods")
}

/// Whether this process is running inside a container of any flavour.
///
/// Used only to decide whether a non-default CPU share is worth warning about:
/// on bare metal a share is just systemd's scheduling weight and says nothing
/// about an entitlement. Best-effort by design — a missed container means one
/// fewer advisory warning, never a different budget.
#[must_use]
pub fn in_container() -> bool {
    if in_kubernetes() || std::path::Path::new("/.dockerenv").exists() {
        return true;
    }
    read_to_string("/proc/self/cgroup").is_some_and(|c| proc_cgroup_is_container(&c))
}

/// Whether `/proc/self/cgroup` contents name a container-runtime slice.
#[must_use]
pub fn proc_cgroup_is_container(contents: &str) -> bool {
    ["docker", "containerd", "kubepods", "lxc", "libpod", "crio"]
        .iter()
        .any(|marker| contents.contains(marker))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// libcontainer's forward map, so the inverse is tested against the real
    /// thing rather than against itself.
    fn shares_to_weight(shares: u64) -> u64 {
        1 + ((shares - FLOOR_SHARES) * WEIGHT_SPAN) / SHARES_SPAN
    }

    /// The kubelet's `requests.cpu` -> `cpu.shares` map.
    fn millicores_to_shares(millicores: u64) -> u64 {
        (millicores * 1024 / 1000).max(FLOOR_SHARES)
    }

    #[test]
    fn cpu_max_v2() {
        assert_eq!(parse_cpu_max("400000 100000"), Some(4000));
        assert_eq!(parse_cpu_max("350000 100000\n"), Some(3500));
        assert_eq!(parse_cpu_max("50000 100000"), Some(500));
        // A bare quota uses the kernel's default 100000µs period.
        assert_eq!(parse_cpu_max("200000"), Some(2000));
        // `max` = no quota.
        assert_eq!(parse_cpu_max("max 100000"), None);
        assert_eq!(parse_cpu_max("max"), None);
        // Malformed.
        assert_eq!(parse_cpu_max(""), None);
        assert_eq!(parse_cpu_max("garbage 100000"), None);
        assert_eq!(parse_cpu_max("400000 garbage"), None);
        assert_eq!(parse_cpu_max("400000 0"), None);
    }

    #[test]
    fn cfs_quota_v1() {
        assert_eq!(parse_cfs_quota("400000", "100000"), Some(4000));
        assert_eq!(parse_cfs_quota(" 350000 \n", " 100000 \n"), Some(3500));
        // `-1` = no quota.
        assert_eq!(parse_cfs_quota("-1", "100000"), None);
        assert_eq!(parse_cfs_quota("0", "100000"), None);
        // Malformed.
        assert_eq!(parse_cfs_quota("garbage", "100000"), None);
        assert_eq!(parse_cfs_quota("400000", "garbage"), None);
        assert_eq!(parse_cfs_quota("400000", "0"), None);
    }

    #[test]
    fn shares_v1() {
        let one_core = parse_shares("1024").expect("parses");
        assert_eq!(one_core.millicores(), 1000);
        assert!(one_core.is_default);
        assert!(one_core.is_meaningful());

        let four_cores = parse_shares("4096").expect("parses");
        assert_eq!(four_cores.millicores(), 4000);
        assert!(!four_cores.is_default);

        // The kubelet floor: a pod that declared no CPU request.
        assert!(!parse_shares("2").expect("parses").is_meaningful());

        assert_eq!(parse_shares("garbage"), None);
    }

    #[test]
    fn weight_v2_maps_kubernetes_requests_back_to_cores() {
        // `requests.cpu: 1` -> kubelet shares 1024 -> weight 39.
        assert_eq!(shares_to_weight(1024), 39);
        let one_core = parse_weight("39").expect("parses");
        assert_eq!(one_core.millicores().div_ceil(1000), 1);
        assert!(!one_core.is_default);
        assert!(one_core.is_meaningful());

        // `requests.cpu: 4` -> kubelet shares 4096 -> weight 157.
        assert_eq!(shares_to_weight(4096), 157);
        assert_eq!(parse_weight("157").expect("parses").millicores(), 4000);

        // `requests.cpu: 3500m` -> kubelet shares 3584 -> weight 137.
        assert_eq!(shares_to_weight(3584), 137);
        assert_eq!(parse_weight("137").expect("parses").millicores(), 3500);

        // The kernel/systemd/docker default carries no entitlement information.
        assert!(parse_weight("100").expect("parses").is_default);

        // Weight 1 is the bottom bucket: no CPU request, or one so small it is
        // indistinguishable from none.
        assert!(!parse_weight("1").expect("parses").is_meaningful());
        assert_eq!(shares_to_weight(FLOOR_SHARES), 1);

        assert_eq!(parse_weight("garbage"), None);
    }

    /// The v2 weight round-trip is lossy; the property that must hold is that
    /// the recovered entitlement is within the quantization error of the input,
    /// so a whole-core request comes back as that whole core count.
    #[test]
    fn weight_inversion_round_trips_across_the_millicore_range() {
        for millicores in (100..=64_000).step_by(10) {
            let shares = millicores_to_shares(millicores);
            let weight = shares_to_weight(shares);
            let recovered = CpuShares {
                shares: weight_to_shares(weight),
                is_default: false,
                is_floor: false,
            }
            .millicores();
            // One weight step spans ~26 shares (~26 millicores); allow that plus
            // the 100m snap.
            let tolerance = 100 + millicores / 200;
            assert!(
                recovered.abs_diff(millicores) <= tolerance,
                "{millicores}m -> shares {shares} -> weight {weight} -> {recovered}m (tolerance {tolerance})"
            );
        }
    }

    /// Every whole-core Kubernetes request must survive the v2 round-trip
    /// exactly — this is the case that decides thread-pool sizing in the field.
    #[test]
    fn whole_core_requests_survive_the_v2_round_trip_exactly() {
        for cores in 1..=384_u64 {
            let millicores = cores * 1000;
            let weight = shares_to_weight(millicores_to_shares(millicores));
            let recovered = CpuShares {
                shares: weight_to_shares(weight),
                is_default: false,
                is_floor: false,
            }
            .millicores();
            assert_eq!(
                recovered, millicores,
                "requests.cpu: {cores} (weight {weight})"
            );
        }
    }

    #[test]
    fn proc_and_mountinfo_parsers() {
        assert_eq!(
            parse_proc_cgroup_v2_path("0::/kubepods.slice/pod123\n"),
            Some("/kubepods.slice/pod123".to_string())
        );
        assert_eq!(parse_proc_cgroup_v2_path("0::\n"), Some("/".to_string()));
        assert_eq!(
            parse_proc_cgroup_v1_path("7:cpu,cpuacct:/x\n8:memory:/mem\n", "memory"),
            Some("/mem".to_string())
        );
        assert_eq!(
            parse_proc_cgroup_v1_path("7:cpu,cpuacct:/x\n", "cpuacct"),
            Some("/x".to_string())
        );
        assert_eq!(
            parse_mountinfo_cgroup2("29 28 0:25 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n"),
            Some("/sys/fs/cgroup".to_string())
        );
        assert_eq!(
            parse_mountinfo_cgroup_v1(
                "30 28 0:26 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
                "memory",
            ),
            Some("/sys/fs/cgroup/memory".to_string())
        );
        assert_eq!(
            cgroup_file_path("/sys/fs/cgroup", "/", "memory.current"),
            "/sys/fs/cgroup/memory.current"
        );
        assert_eq!(
            cgroup_file_path("/sys/fs/cgroup", "/kubepods/pod1", "cpu.max"),
            "/sys/fs/cgroup/kubepods/pod1/cpu.max"
        );
    }

    #[test]
    fn container_and_kubernetes_markers() {
        assert!(proc_cgroup_is_kubepods("0::/kubepods.slice/pod123\n"));
        assert!(!proc_cgroup_is_kubepods(
            "0::/system.slice/spiced.service\n"
        ));
        assert!(proc_cgroup_is_container("0::/docker/abc123\n"));
        assert!(proc_cgroup_is_container("0::/kubepods.slice/pod123\n"));
        assert!(!proc_cgroup_is_container(
            "0::/system.slice/spiced.service\n"
        ));
    }
}
