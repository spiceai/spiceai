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

//! An exclusive lock on a cache directory, so two caches (two processes, or
//! two opens in one process) never share one directory's files and
//! accounting.
//!
//! [`File::try_lock`] on `<dir>/lock`, taken without waiting. The lock is
//! released when the [`DirLock`] is dropped or the process exits; a lock file
//! left behind by a crash does not block the next open. The directory is
//! made mode `0700` and the lock file `0600`.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use snafu::ResultExt;

use crate::Result;
use crate::block_store::slab::{owner_dir, owner_open};
use crate::error::{IoSnafu, LockedSnafu};

#[derive(Debug)]
pub(crate) struct DirLock {
    _file: File,
}

impl DirLock {
    /// Creates `dir` if needed and locks it. Blocking: run it off the async
    /// runtime.
    pub(crate) fn acquire(dir: &Path) -> Result<Self> {
        owner_dir(dir).context(IoSnafu {
            action: "create",
            path: dir.to_path_buf(),
        })?;
        let path: PathBuf = dir.join("lock");
        let file = owner_open(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false),
            &path,
        )
        .context(IoSnafu {
            action: "open",
            path: path.clone(),
        })?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => LockedSnafu {
                path: dir.to_path_buf(),
            }
            .fail(),
            Err(TryLockError::Error(e)) => Err(e).context(IoSnafu {
                action: "lock",
                path,
            }),
        }
    }
}
