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

//! cgroup v1/v2 readers for the CPU *quota* and *share*, plus the cgroup path
//! resolution they need.
//!
//! Only the quota ever reaches sizing. A cgroup CPU *share* (v1 `cpu.shares`, v2
//! `cpu.weight`) is read for reporting alone and must never enter the detection
//! ladder: under Kubernetes the kubelet derives it from `requests.cpu`, but a
//! request is a scheduling floor, not a ceiling — a burstable pod is entitled to
//! every idle core on its node, and sizing from the request would take that
//! away. Only a quota (`limits.cpu`) is a real ceiling. The share is exposed so
//! that an operator can see the request and the limit next to the budget the
//! runtime actually chose.
//!
//! Everything that interprets a file's *contents* is a pure function over a
//! `&str`, so it is unit-testable on any platform with no live cgroup. Only the
//! handful of functions that touch `/proc` and `/sys` are Linux-gated, and they
//! return `None` everywhere else.

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

/// cgroup v1 shares that correspond to one CPU.
const SHARES_PER_CORE: u64 = 1024;

/// The share Kubernetes assigns a container with no `requests.cpu` at all. Read
/// back as "no request expressed" rather than as a request of ~2 millicores.
const SHARES_NO_REQUEST: u64 = 2;

/// Parse cgroup v1 `cpu.shares` into the `requests.cpu` it was derived from, in
/// millicores.
///
/// Reporting only — see the module docs. `None` when the value is the
/// no-request floor or the contents are malformed.
///
/// The mapping is not injective: outside Kubernetes the default share is
/// [`SHARES_PER_CORE`], which is indistinguishable from an explicit request of
/// one CPU. Read the result as "the CPU share this cgroup carries", not as proof
/// that an operator wrote `requests.cpu`.
#[must_use]
pub fn parse_cpu_shares(contents: &str) -> Option<u64> {
    let shares: u64 = contents.trim().parse().ok()?;
    if shares <= SHARES_NO_REQUEST {
        return None;
    }
    Some(shares.saturating_mul(1000) / SHARES_PER_CORE)
}

/// Parse cgroup v2 `cpu.weight` into the `requests.cpu` it was derived from, in
/// millicores.
///
/// Reporting only — see the module docs. `None` when the weight is the
/// no-request floor or the contents are malformed.
///
/// Inverts the kubelet's share-to-weight conversion,
/// `weight = 1 + ((shares - 2) * 9999) / 262142`. Container runtimes outside
/// Kubernetes do not all use that formula, so on a plain Docker host the
/// recovered value is approximate.
#[must_use]
pub fn parse_cpu_weight(contents: &str) -> Option<u64> {
    let weight: u64 = contents.trim().parse().ok()?;
    if weight <= 1 {
        return None;
    }
    let shares = SHARES_NO_REQUEST.saturating_add(
        weight
            .saturating_sub(1)
            .saturating_mul(262_142)
            .saturating_div(9_999),
    );
    Some(shares.saturating_mul(1000) / SHARES_PER_CORE)
}

/// This process's cgroup v2 mountpoint and path within the hierarchy, or `None`
/// when the system is not running cgroup v2 (or is not Linux).
#[must_use]
pub fn v2_mount_and_path() -> Option<(String, String)> {
    #[cfg(target_os = "linux")]
    {
        let cgroup_path = parse_proc_cgroup_v2_path(&read_to_string("/proc/self/cgroup")?)?;
        let mountpoint = read_to_string("/proc/self/mountinfo")
            .and_then(|s| parse_mountinfo_cgroup2(&s))
            .unwrap_or_else(|| "/sys/fs/cgroup".to_string());
        Some((mountpoint, cgroup_path))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// This process's cgroup v1 mountpoint and path for `controller`, or `None` when
/// that controller is not mounted (or is not Linux).
#[must_use]
pub fn v1_mount_and_path(controller: &str) -> Option<(String, String)> {
    #[cfg(target_os = "linux")]
    {
        let cgroup_path =
            parse_proc_cgroup_v1_path(&read_to_string("/proc/self/cgroup")?, controller)?;
        let mountpoint = read_to_string("/proc/self/mountinfo")
            .and_then(|s| parse_mountinfo_cgroup_v1(&s, controller))
            .unwrap_or_else(|| format!("/sys/fs/cgroup/{controller}"));
        Some((mountpoint, cgroup_path))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = controller;
        None
    }
}

/// The smallest value `read_level` yields anywhere along `cgroup_path`, walking
/// leaf to root.
///
/// A quota must be read along the whole path, not just at the leaf: the kernel
/// enforces the smallest limit anywhere above the process, so a quota on an
/// ancestor — a systemd slice above the service, the Kubernetes *pod* cgroup
/// above the container — binds exactly as tightly as one on the leaf. Reading
/// only the leaf reports such a process as unlimited and sizes it for the whole
/// node.
///
/// `read_level` receives the mountpoint and the path relative to it, so the walk
/// can be driven over a temporary directory in tests.
pub fn min_along_cgroup_path<T: Ord>(
    mountpoint: &str,
    cgroup_path: &str,
    read_level: impl Fn(&str, &str) -> Option<T>,
) -> Option<T> {
    let mut best: Option<T> = None;
    let mut rel = cgroup_path.trim_end_matches('/').to_string();
    loop {
        let value = read_level(mountpoint, if rel.is_empty() { "/" } else { &rel });
        best = match (best, value) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, b) => b,
        };
        if rel.is_empty() {
            return best;
        }
        match rel.rfind('/') {
            Some(0) | None => rel.clear(),
            Some(cut) => rel.truncate(cut),
        }
    }
}

/// Linux-only: the cgroup paths are the sole callers, and they compile to
/// `None` elsewhere.
#[cfg(target_os = "linux")]
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A quota anywhere above the process binds it, so the walk must visit every
    /// level and take the smallest — reading only the leaf reported a limited
    /// process as unlimited and sized it for the whole node.
    #[test]
    fn a_quota_is_found_anywhere_along_the_path() {
        let levels = |pairs: &'static [(&'static str, u64)]| {
            move |_mount: &str, rel: &str| {
                pairs
                    .iter()
                    .find(|(path, _)| *path == rel)
                    .map(|(_, value)| *value)
            }
        };
        let walk = |pairs| {
            min_along_cgroup_path(
                "/sys/fs/cgroup",
                "/kubepods/pod123/container",
                levels(pairs),
            )
        };

        // Only the Kubernetes pod cgroup carries the quota.
        assert_eq!(walk(&[("/kubepods/pod123", 4000)]), Some(4000));
        // The tightest limit anywhere wins, whichever level it sits on.
        assert_eq!(
            walk(&[
                ("/kubepods/pod123/container", 8000),
                ("/kubepods/pod123", 4000),
            ]),
            Some(4000)
        );
        assert_eq!(
            walk(&[
                ("/kubepods/pod123/container", 2000),
                ("/kubepods/pod123", 4000),
            ]),
            Some(2000)
        );
        // The root is part of the walk.
        assert_eq!(walk(&[("/", 1000)]), Some(1000));
        // Nothing anywhere means unlimited.
        assert_eq!(walk(&[]), None);
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
}
