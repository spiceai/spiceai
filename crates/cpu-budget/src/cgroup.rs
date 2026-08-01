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

//! cgroup v1/v2 readers for the CPU *quota*, plus the cgroup path resolution
//! they need.
//!
//! Only a quota is read. A cgroup CPU *share* (v1 `cpu.shares`, v2 `cpu.weight`)
//! is deliberately ignored: under Kubernetes the kubelet derives it from
//! `requests.cpu`, but a request is a scheduling floor, not a ceiling — a
//! burstable pod is entitled to every idle core on its node, and sizing from the
//! request would take that away. Only a quota (`limits.cpu`) is a real ceiling.
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
