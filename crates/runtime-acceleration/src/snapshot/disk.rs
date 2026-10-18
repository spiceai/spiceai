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

/// The first disk that has too little free space to restore a `size`-byte snapshot to
/// `layout`. A directory layout restores from an archive downloaded to `temp_dir`
/// first, so the archive needs room too, on the same disk or its own. A disk whose
/// free space cannot be read is not counted short.
pub(super) fn shortfall(layout: &AccelerationLayout, size: u64, temp_dir: &Path) -> Option<Shortfall> {
    let (target, archive) = match layout {
        AccelerationLayout::None => return None,
        AccelerationLayout::File { path } => (path.as_path(), false),
        AccelerationLayout::Directories { dirs } => (dirs.first()?.0.as_path(), true),
    };
    let target = existing_ancestor(target)?;
    let mut needs = vec![(target, size.saturating_add(margin(size)))];
    if archive {
        let temp = existing_ancestor(temp_dir)?;
        match needs.iter_mut().find(|(path, _)| same_disk(path, &temp)) {
            Some((_, needed)) => *needed = needed.saturating_add(size),
            None => needs.push((temp, size.saturating_add(margin(size)))),
        }
    }
    needs.into_iter().find_map(|(path, needed)| {
        let free = fs4::available_space(&path).ok()?;
        (free < needed).then_some(Shortfall { path, needed, free })
    })
}

/// `path`, or its nearest ancestor that exists.
fn existing_ancestor(path: &Path) -> Option<PathBuf> {
    path.ancestors()
        .find(|ancestor| ancestor.exists())
        .map(Path::to_path_buf)
}

#[cfg(unix)]
fn same_disk(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(a), std::fs::metadata(b)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev(),
        _ => true,
    }
}

#[cfg(not(unix))]
fn same_disk(_a: &Path, _b: &Path) -> bool {
    true
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
}
