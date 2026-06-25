/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::{fmt::Display, path::Path};

use crate::component::dataset::acceleration::StorageProfile;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolvedAccelerationStorage {
    LocalSsd,
    Ebs,
    Tmpfs,
    Unknown,
}

impl Display for ResolvedAccelerationStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalSsd => write!(f, "local_ssd"),
            Self::Ebs => write!(f, "ebs"),
            Self::Tmpfs => write!(f, "tmpfs"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

#[must_use]
pub(crate) fn resolve_acceleration_storage(
    configured_storage: StorageProfile,
    path: &Path,
) -> ResolvedAccelerationStorage {
    match configured_storage {
        StorageProfile::Auto => detect_path_storage(path),
        StorageProfile::LocalSsd => ResolvedAccelerationStorage::LocalSsd,
        StorageProfile::Ebs => ResolvedAccelerationStorage::Ebs,
        StorageProfile::Tmpfs => ResolvedAccelerationStorage::Tmpfs,
    }
}

/// Resolve the storage profile from a path, off the async runtime when the
/// profile is `Auto` (which performs blocking `/proc` and `/sys` reads on
/// Linux). Explicit profiles short-circuit synchronously.
pub(crate) async fn resolve_acceleration_storage_async(
    configured_storage: StorageProfile,
    path: &str,
) -> ResolvedAccelerationStorage {
    if configured_storage != StorageProfile::Auto {
        return resolve_acceleration_storage(configured_storage, Path::new(path));
    }

    let path = std::path::PathBuf::from(path);
    match tokio::task::spawn_blocking(move || {
        resolve_acceleration_storage(configured_storage, &path)
    })
    .await
    {
        Ok(storage) => storage,
        Err(err) => {
            tracing::debug!("Failed to detect acceleration storage profile: {err}");
            ResolvedAccelerationStorage::Unknown
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn detect_path_storage(_path: &Path) -> ResolvedAccelerationStorage {
    ResolvedAccelerationStorage::Unknown
}

#[cfg(target_os = "linux")]
fn detect_path_storage(path: &Path) -> ResolvedAccelerationStorage {
    use std::{collections::HashSet, fs, path::PathBuf};

    let detection_path = normalize_detection_path(path);
    let Ok(mountinfo) = fs::read_to_string("/proc/self/mountinfo") else {
        return ResolvedAccelerationStorage::Unknown;
    };

    let Some(mount) = find_longest_matching_mount(&detection_path, &mountinfo) else {
        return ResolvedAccelerationStorage::Unknown;
    };

    if is_in_memory_fstype(&mount.fstype) {
        return ResolvedAccelerationStorage::Tmpfs;
    }

    let dev_block_path = PathBuf::from("/sys/dev/block").join(&mount.major_minor);
    let mut visited = HashSet::new();
    let devices = collect_block_devices(&dev_block_path, &mut visited);

    classify_block_devices(&devices)
}

#[cfg(target_os = "linux")]
fn is_in_memory_fstype(fstype: &str) -> bool {
    matches!(fstype, "tmpfs" | "ramfs")
}

#[cfg(target_os = "linux")]
fn normalize_detection_path(path: &Path) -> std::path::PathBuf {
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
    };

    let existing_path = if absolute_path.exists() {
        absolute_path.as_path()
    } else {
        absolute_path
            .ancestors()
            .find(|ancestor| ancestor.exists())
            .unwrap_or(absolute_path.as_path())
    };

    existing_path
        .canonicalize()
        .unwrap_or_else(|_| absolute_path.clone())
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MountInfo {
    major_minor: String,
    mount_point: std::path::PathBuf,
    fstype: String,
}

#[cfg(target_os = "linux")]
fn find_longest_matching_mount(path: &Path, mountinfo: &str) -> Option<MountInfo> {
    mountinfo
        .lines()
        .filter_map(parse_mountinfo_line)
        .filter(|mount| path.starts_with(&mount.mount_point))
        .max_by_key(|mount| mount.mount_point.components().count())
}

#[cfg(target_os = "linux")]
fn parse_mountinfo_line(line: &str) -> Option<MountInfo> {
    // mountinfo format:
    // mount_id parent_id major:minor root mount_point mount_options optional - fstype source super_options
    let (before_sep, after_sep) = line.split_once(" - ")?;
    let before_fields: Vec<&str> = before_sep.split(' ').collect();
    let major_minor = before_fields.get(2)?;
    let mount_point = before_fields.get(4)?;

    let after_fields: Vec<&str> = after_sep.split(' ').collect();
    let fstype = after_fields.first()?;

    Some(MountInfo {
        major_minor: (*major_minor).to_string(),
        mount_point: std::path::PathBuf::from(unescape_mountinfo_path(mount_point)),
        fstype: (*fstype).to_string(),
    })
}

#[cfg(target_os = "linux")]
fn unescape_mountinfo_path(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut output = String::with_capacity(path.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'\\'
            && index + 3 < bytes.len()
            && bytes[index + 1].is_ascii_digit()
            && bytes[index + 2].is_ascii_digit()
            && bytes[index + 3].is_ascii_digit()
        {
            let value = (bytes[index + 1] - b'0') * 64
                + (bytes[index + 2] - b'0') * 8
                + (bytes[index + 3] - b'0');
            output.push(char::from(value));
            index += 4;
        } else {
            output.push(char::from(bytes[index]));
            index += 1;
        }
    }

    output
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockDevice {
    name: String,
    model: Option<String>,
    vendor: Option<String>,
    rotational: Option<bool>,
}

#[cfg(target_os = "linux")]
fn collect_block_devices(
    dev_block_path: &Path,
    visited: &mut std::collections::HashSet<std::path::PathBuf>,
) -> Vec<BlockDevice> {
    let canonical_path = dev_block_path
        .canonicalize()
        .unwrap_or_else(|_| dev_block_path.to_path_buf());
    if !visited.insert(canonical_path.clone()) {
        return Vec::new();
    }

    let mut devices = block_device_name_from_sys_path(&canonical_path)
        .map(|name| vec![read_block_device(&name)])
        .unwrap_or_default();

    if let Ok(entries) = std::fs::read_dir(dev_block_path.join("slaves")) {
        for entry in entries.filter_map(Result::ok) {
            devices.extend(collect_block_devices(&entry.path(), visited));
        }
    }

    devices
}

#[cfg(target_os = "linux")]
fn block_device_name_from_sys_path(path: &Path) -> Option<String> {
    let mut block_component_seen = false;
    for component in path.components() {
        let component = component.as_os_str().to_string_lossy();
        if block_component_seen {
            return Some(component.into_owned());
        }
        if component == "block" {
            block_component_seen = true;
        }
    }

    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

#[cfg(target_os = "linux")]
fn read_block_device(name: &str) -> BlockDevice {
    let sys_block_path = std::path::PathBuf::from("/sys/block").join(name);
    BlockDevice {
        name: name.to_ascii_lowercase(),
        model: read_trimmed_lowercase(&sys_block_path.join("device/model")),
        vendor: read_trimmed_lowercase(&sys_block_path.join("device/vendor")),
        rotational: read_trimmed_lowercase(&sys_block_path.join("queue/rotational")).and_then(
            |rotational| match rotational.as_str() {
                "0" => Some(false),
                "1" => Some(true),
                _ => None,
            },
        ),
    }
}

#[cfg(target_os = "linux")]
fn read_trimmed_lowercase(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|contents| contents.trim().to_ascii_lowercase())
        .filter(|contents| !contents.is_empty())
}

#[cfg(target_os = "linux")]
fn classify_block_devices(devices: &[BlockDevice]) -> ResolvedAccelerationStorage {
    let mut non_rotational_storage_found = false;

    for device in devices {
        let description = format!(
            "{} {} {}",
            device.name,
            device.model.as_deref().unwrap_or_default(),
            device.vendor.as_deref().unwrap_or_default()
        );

        if description.contains("amazon elastic block store")
            || description.contains("amazon ebs")
            || description.contains("elastic block store")
        {
            return ResolvedAccelerationStorage::Ebs;
        }

        // Azure Managed Disks (Premium SSD, Standard SSD, Standard HDD, Ultra
        // Disk) attach to Linux VMs as Hyper-V virtual SCSI disks reporting
        // vendor "Msft" and model "Virtual Disk". Treat them like EBS for
        // pool sizing because they share the same network-attached latency
        // characteristics.
        if (description.contains("msft") && description.contains("virtual disk"))
            || description.contains("microsoft virtual disk")
            || description.contains("azure managed disk")
        {
            return ResolvedAccelerationStorage::Ebs;
        }

        if description.contains("amazon ec2 nvme instance storage") {
            return ResolvedAccelerationStorage::LocalSsd;
        }

        if device.name.starts_with("nvme") || device.rotational == Some(false) {
            non_rotational_storage_found = true;
        }
    }

    if non_rotational_storage_found {
        ResolvedAccelerationStorage::LocalSsd
    } else {
        ResolvedAccelerationStorage::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn unescapes_mountinfo_paths() {
        assert_eq!(
            unescape_mountinfo_path("/mnt/local\\040ssd"),
            "/mnt/local ssd"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn finds_longest_matching_mount() {
        let mountinfo = "1 0 8:1 / / rw - ext4 /dev/sda1 rw\n2 1 259:0 / /mnt/local\\040ssd rw - ext4 /dev/nvme0n1 rw";
        let mount = find_longest_matching_mount(Path::new("/mnt/local ssd/table.db"), mountinfo)
            .expect("mount should resolve");
        assert_eq!(mount.major_minor, "259:0");
        assert_eq!(mount.fstype, "ext4");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn detects_tmpfs_fstype() {
        let mountinfo =
            "1 0 8:1 / / rw - ext4 /dev/sda1 rw\n2 1 0:42 / /mnt/ram rw - tmpfs tmpfs rw,size=8G";
        let mount = find_longest_matching_mount(Path::new("/mnt/ram/cache.db"), mountinfo)
            .expect("mount should resolve");
        assert_eq!(mount.fstype, "tmpfs");
        assert!(is_in_memory_fstype(&mount.fstype));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn classifies_amazon_ebs_before_nvme() {
        let devices = vec![BlockDevice {
            name: "nvme1n1".to_string(),
            model: Some("amazon elastic block store".to_string()),
            vendor: Some("amazon ec2".to_string()),
            rotational: Some(false),
        }];

        assert_eq!(
            classify_block_devices(&devices),
            ResolvedAccelerationStorage::Ebs
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn classifies_instance_store_as_local_ssd() {
        let devices = vec![BlockDevice {
            name: "nvme0n1".to_string(),
            model: Some("amazon ec2 nvme instance storage".to_string()),
            vendor: Some("amazon ec2".to_string()),
            rotational: Some(false),
        }];

        assert_eq!(
            classify_block_devices(&devices),
            ResolvedAccelerationStorage::LocalSsd
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn classifies_azure_managed_disk_as_ebs() {
        let devices = vec![BlockDevice {
            name: "sda".to_string(),
            model: Some("virtual disk".to_string()),
            vendor: Some("msft".to_string()),
            rotational: Some(false),
        }];

        assert_eq!(
            classify_block_devices(&devices),
            ResolvedAccelerationStorage::Ebs
        );
    }

    #[test]
    fn honors_configured_storage_override() {
        assert_eq!(
            resolve_acceleration_storage(StorageProfile::Ebs, Path::new("/does/not/matter")),
            ResolvedAccelerationStorage::Ebs
        );
        assert_eq!(
            resolve_acceleration_storage(StorageProfile::LocalSsd, Path::new("/does/not/matter")),
            ResolvedAccelerationStorage::LocalSsd
        );
        assert_eq!(
            resolve_acceleration_storage(StorageProfile::Tmpfs, Path::new("/does/not/matter")),
            ResolvedAccelerationStorage::Tmpfs
        );
    }
}
