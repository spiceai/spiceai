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

//! Directories a Spicepod package is installed into, held open while it is
//! written.
//!
//! On Unix an [`InstallDir`] is a descriptor for a directory opened without
//! following a link, and every child is created, opened and renamed relative to
//! it. A directory checked once therefore stays the directory that is written
//! to: swapping it, or any directory above it, for a symbolic link afterwards
//! cannot send a write anywhere else. Elsewhere the same operations go through
//! paths, after checking each one for a link.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

/// What is at a name inside an [`InstallDir`].
pub(super) enum Entry {
    Missing,
    /// A symbolic link, which is never followed.
    Link,
    Directory(InstallDir),
    /// A file, or anything else that is not a directory.
    Other,
}

/// A directory that installation writes into.
pub(super) struct InstallDir {
    /// The path the directory was reached by, for messages only. On Unix it
    /// may no longer name this directory; the descriptor is what is used.
    path: PathBuf,
    #[cfg(unix)]
    fd: std::os::fd::OwnedFd,
}

impl InstallDir {
    /// Open the project directory, `path`. A link in `path` is followed: the
    /// project is where the user chose to run, and everything beneath it is
    /// reached from it without following links.
    pub(super) fn open(path: &Path) -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use rustix::fs::{Mode, OFlags};
            let fd = rustix::fs::open(
                path,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            )?;
            Ok(Self {
                path: path.to_path_buf(),
                fd,
            })
        }
        #[cfg(not(unix))]
        {
            if !std::fs::metadata(path)?.is_dir() {
                return Err(std::io::Error::from(std::io::ErrorKind::NotADirectory));
            }
            Ok(Self {
                path: path.to_path_buf(),
            })
        }
    }

    /// The path this directory was reached by, for messages.
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    /// The path of `name` inside this directory, for messages.
    pub(super) fn child_path(&self, name: &OsStr) -> PathBuf {
        self.path.join(name)
    }

    pub(super) fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            path: self.path.clone(),
            #[cfg(unix)]
            fd: self.fd.try_clone()?,
        })
    }

    /// What is at `name`, opening it when it is a directory. A link is
    /// reported, not followed.
    pub(super) fn entry(&self, name: &OsStr) -> std::io::Result<Entry> {
        #[cfg(unix)]
        {
            use rustix::fs::{Mode, OFlags};
            use rustix::io::Errno;
            match rustix::fs::openat(
                &self.fd,
                name,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(fd) => Ok(Entry::Directory(Self {
                    path: self.child_path(name),
                    fd,
                })),
                Err(Errno::NOENT) => Ok(Entry::Missing),
                // `O_NOFOLLOW` fails a link with `ELOOP`, and `O_DIRECTORY`
                // fails anything else that is not a directory with `ENOTDIR`;
                // which one a link gets differs between systems.
                Err(Errno::LOOP | Errno::NOTDIR) => {
                    match rustix::fs::statat(&self.fd, name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)
                    {
                        Ok(stat)
                            if rustix::fs::FileType::from_raw_mode(stat.st_mode)
                                == rustix::fs::FileType::Symlink =>
                        {
                            Ok(Entry::Link)
                        }
                        Ok(_) => Ok(Entry::Other),
                        Err(Errno::NOENT) => Ok(Entry::Missing),
                        Err(error) => Err(error.into()),
                    }
                }
                Err(error) => Err(error.into()),
            }
        }
        #[cfg(not(unix))]
        {
            let path = self.child_path(name);
            match std::fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_symlink() => Ok(Entry::Link),
                Ok(metadata) if metadata.is_dir() => Ok(Entry::Directory(Self { path })),
                Ok(_) => Ok(Entry::Other),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Entry::Missing),
                Err(error) => Err(error),
            }
        }
    }

    /// Create the directory `name`, readable only by its owner when `private`.
    /// Returns `false` when something is already at `name`.
    pub(super) fn create_dir(&self, name: &OsStr, private: bool) -> std::io::Result<bool> {
        #[cfg(unix)]
        {
            let mode = if private {
                rustix::fs::Mode::RWXU
            } else {
                rustix::fs::Mode::RWXU | rustix::fs::Mode::RWXG | rustix::fs::Mode::RWXO
            };
            match rustix::fs::mkdirat(&self.fd, name, mode) {
                Ok(()) => Ok(true),
                Err(rustix::io::Errno::EXIST) => Ok(false),
                Err(error) => Err(error.into()),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = private;
            match std::fs::create_dir(self.child_path(name)) {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
                Err(error) => Err(error),
            }
        }
    }

    /// Create (or truncate) the file `name` for writing, never through a link.
    pub(super) fn create_file(&self, name: &OsStr) -> std::io::Result<std::fs::File> {
        #[cfg(unix)]
        {
            use rustix::fs::{Mode, OFlags};
            let fd = rustix::fs::openat(
                &self.fd,
                name,
                OFlags::WRONLY
                    | OFlags::CREATE
                    | OFlags::TRUNC
                    | OFlags::NOFOLLOW
                    | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP | Mode::ROTH | Mode::WOTH,
            )?;
            Ok(std::fs::File::from(fd))
        }
        #[cfg(not(unix))]
        {
            std::fs::File::create(self.child_path(name))
        }
    }

    /// Move the entry `name` here to `to_name` in `to`. An entry already at
    /// `to_name` is replaced, and a link there is replaced, not followed.
    pub(super) fn rename(
        &self,
        name: &OsStr,
        to: &InstallDir,
        to_name: &OsStr,
    ) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            rustix::fs::renameat(&self.fd, name, &to.fd, to_name).map_err(Into::into)
        }
        #[cfg(not(unix))]
        {
            std::fs::rename(self.child_path(name), to.child_path(to_name))
        }
    }

    /// Remove the entry `name`, and everything in it when it is a directory,
    /// relative to this directory. A link anywhere in it is removed, never
    /// followed. A missing entry is not an error.
    pub(super) fn remove_tree(&self, name: &OsStr) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            use rustix::fs::AtFlags;
            match self.entry(name)? {
                Entry::Missing => Ok(()),
                Entry::Directory(dir) => {
                    for (child, is_dir) in dir.list()? {
                        if is_dir {
                            dir.remove_tree(&child)?;
                        } else {
                            rustix::fs::unlinkat(&dir.fd, child.as_os_str(), AtFlags::empty())?;
                        }
                    }
                    rustix::fs::unlinkat(&self.fd, name, AtFlags::REMOVEDIR).map_err(Into::into)
                }
                Entry::Link | Entry::Other => {
                    rustix::fs::unlinkat(&self.fd, name, AtFlags::empty()).map_err(Into::into)
                }
            }
        }
        #[cfg(not(unix))]
        {
            let path = self.child_path(name);
            match std::fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(&path),
                Ok(_) => std::fs::remove_file(&path),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
        }
    }

    /// The names in this directory, each with whether it is a directory (a
    /// link to one is not).
    pub(super) fn list(&self) -> std::io::Result<Vec<(OsString, bool)>> {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt as _;
            let mut names = Vec::new();
            for entry in rustix::fs::Dir::read_from(&self.fd)? {
                let entry = entry?;
                let name = OsStr::from_bytes(entry.file_name().to_bytes());
                if name == "." || name == ".." {
                    continue;
                }
                let is_dir = match entry.file_type() {
                    rustix::fs::FileType::Directory => true,
                    rustix::fs::FileType::Unknown => matches!(
                        rustix::fs::statat(&self.fd, name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW),
                        Ok(stat) if rustix::fs::FileType::from_raw_mode(stat.st_mode)
                            == rustix::fs::FileType::Directory
                    ),
                    _ => false,
                };
                names.push((name.to_os_string(), is_dir));
            }
            Ok(names)
        }
        #[cfg(not(unix))]
        {
            let mut names = Vec::new();
            for entry in std::fs::read_dir(&self.path)? {
                let entry = entry?;
                names.push((entry.file_name(), entry.file_type()?.is_dir()));
            }
            Ok(names)
        }
    }
}
