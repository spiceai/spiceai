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

//! Free disk space for restoring a pinned snapshot: a restore that runs out of space
//! midway leaves nothing usable, so it does not start without room for the whole
//! snapshot.

use std::path::{Path, PathBuf};

use crate::layout::AccelerationLayout;

/// Room kept free beyond the snapshot itself, at least this much.
const MIN_MARGIN_BYTES: u64 = 64 * 1024 * 1024;

/// A disk with less free space than a restore needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Shortfall {
    pub path: PathBuf,
    pub needed: u64,
    pub free: u64,
}

/// The disk space a restore of a `size`-byte snapshot needs beyond the snapshot.
fn margin(size: u64) -> u64 {
    (size / 10).max(MIN_MARGIN_BYTES)
}

/// A disk, and its free space in bytes.
#[derive(Debug, Clone, Copy)]
struct Disk {
    device: u64,
    free: u64,
}

/// The first disk that has too little free space to restore a `size`-byte snapshot to
/// `layout`. Each disk the restore writes to needs room for the whole snapshot, since
/// how a snapshot splits between a directory layout's directories is not known before
/// it is restored. A directory layout restores from an archive downloaded to
/// `temp_dir` first, so the archive needs room too, on the same disk or its own. A
/// disk whose free space cannot be read is not counted short.
pub(super) fn shortfall(
    layout: &AccelerationLayout,
    size: u64,
    temp_dir: &Path,
) -> Option<Shortfall> {
    shortfall_on(layout, size, temp_dir, |path| {
        Some(Disk {
            device: device(path)?,
            free: fs4::available_space(path).ok()?,
        })
    })
}

/// [`shortfall`], with `disk` reading the disk an existing path is on.
fn shortfall_on(
    layout: &AccelerationLayout,
    size: u64,
    temp_dir: &Path,
    disk: impl Fn(&Path) -> Option<Disk>,
) -> Option<Shortfall> {
    let (targets, archive): (Vec<&Path>, bool) = match layout {
        AccelerationLayout::None => return None,
        AccelerationLayout::File { path } => (vec![path.as_path()], false),
        AccelerationLayout::Directories { dirs } => {
            (dirs.iter().map(|(dir, _)| dir.as_path()).collect(), true)
        }
    };
    let locate = |path: &Path| {
        let path = existing_ancestor(path)?;
        let found = disk(&path)?;
        Some((path, found))
    };
    let mut needs: Vec<(PathBuf, Disk, u64)> = Vec::new();
    for (path, disk) in targets.into_iter().filter_map(&locate) {
        if !needs
            .iter()
            .any(|(_, known, _)| known.device == disk.device)
        {
            needs.push((path, disk, size.saturating_add(margin(size))));
        }
    }
    if archive && let Some((path, disk)) = locate(temp_dir) {
        match needs
            .iter_mut()
            .find(|(_, known, _)| known.device == disk.device)
        {
            Some((_, _, needed)) => *needed = needed.saturating_add(size),
            None => needs.push((path, disk, size.saturating_add(margin(size)))),
        }
    }
    needs
        .into_iter()
        .find(|(_, disk, needed)| disk.free < *needed)
        .map(|(path, disk, needed)| Shortfall {
            path,
            needed,
            free: disk.free,
        })
}

/// `path`, or its nearest ancestor that exists.
fn existing_ancestor(path: &Path) -> Option<PathBuf> {
    path.ancestors()
        .find(|ancestor| ancestor.exists())
        .map(Path::to_path_buf)
}

#[cfg(unix)]
fn device(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|metadata| metadata.dev())
}

#[cfg(not(unix))]
fn device(_path: &Path) -> Option<u64> {
    Some(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_larger_than_the_free_space_is_short() {
        let dir = tempfile::tempdir().expect("tempdir");
        let free = fs4::available_space(dir.path()).expect("free space");
        let layout = AccelerationLayout::File {
            path: dir.path().join("pinned").join("orders.db"),
        };

        let short = shortfall(&layout, free, dir.path()).expect("short of space");
        assert_eq!(short.path, dir.path());
        assert_eq!(short.needed, free + margin(free));
        assert!(short.free <= free + MIN_MARGIN_BYTES, "free space was read");

        assert_eq!(shortfall(&layout, 1024, dir.path()), None);
    }

    #[test]
    fn an_archive_on_the_same_disk_needs_room_too() {
        let dir = tempfile::tempdir().expect("tempdir");
        let free = fs4::available_space(dir.path()).expect("free space");
        let size = free / 2;
        let file = AccelerationLayout::File {
            path: dir.path().join("orders.db"),
        };
        let directories = AccelerationLayout::Directories {
            dirs: vec![(dir.path().join("cayenne"), "data".to_string())],
        };

        assert_eq!(shortfall(&file, size, dir.path()), None);
        let short = shortfall(&directories, size, dir.path()).expect("archive and copy");
        assert_eq!(short.needed, 2 * size + margin(size));
    }

    #[test]
    fn every_disk_a_directory_layout_restores_to_needs_room_for_the_snapshot() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let dir = tempfile::tempdir().expect("tempdir");
        let metadata = dir.path().join("metadata");
        let data = dir.path().join("data");
        std::fs::create_dir_all(&metadata).expect("metadata dir");
        std::fs::create_dir_all(&data).expect("data dir");
        let layout = AccelerationLayout::cayenne(metadata, data.clone());
        // The data directory is on a disk of its own with 1 GiB free; the metadata
        // directory and the archive share a disk with 1 TiB free.
        let disk = |path: &Path| {
            Some(if path == data {
                Disk {
                    device: 2,
                    free: GIB,
                }
            } else {
                Disk {
                    device: 1,
                    free: 1024 * GIB,
                }
            })
        };

        let short =
            shortfall_on(&layout, 2 * GIB, dir.path(), disk).expect("the data disk is short");
        assert_eq!(short.path, data);
        assert_eq!(short.needed, 2 * GIB + margin(2 * GIB));
        assert_eq!(short.free, GIB);
        assert_eq!(shortfall_on(&layout, GIB / 2, dir.path(), disk), None);
    }
}
