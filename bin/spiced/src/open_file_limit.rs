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

//! Raises the soft open file limit (`RLIMIT_NOFILE`) to the hard limit at startup.
//!
//! Containers usually start a process with a soft limit of 1024 under a hard limit
//! of 524288 or more. Every client connection, acceleration file, and pipe shares
//! the soft limit, so a few hundred keep-alive connections are enough to make
//! acceleration files fail to open. An unprivileged process may raise its own soft
//! limit up to the hard limit, which is what the Go runtime does for every Go
//! program since Go 1.19.

use std::fmt;

/// What raising the limit did. Reported after the startup banner, since the raise
/// itself happens before anything may log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenFileLimit {
    /// The soft limit was raised.
    Raised { from: u64, to: u64 },
    /// The soft limit was already as high as this process may set it.
    Unchanged { limit: u64 },
    /// The soft limit could not be raised, and stays at `limit`.
    NotRaised {
        limit: u64,
        target: u64,
        cause: String,
    },
    /// The limit could not be read at all.
    Unknown { cause: String },
}

/// Raise the soft open file limit as far as this process may, and report what happened.
///
/// `None` on a platform with no open file limit to raise.
#[must_use]
pub fn raise() -> Option<OpenFileLimit> {
    imp::raise()
}

impl OpenFileLimit {
    /// Log the outcome. INFO when the limit is known, WARN when it could not be raised
    /// or read, since then connections and acceleration files compete for a low budget.
    pub fn log(&self) {
        match self {
            Self::Raised { .. } | Self::Unchanged { .. } => tracing::info!("{self}"),
            Self::NotRaised { .. } | Self::Unknown { .. } => tracing::warn!("{self}"),
        }
    }
}

impl fmt::Display for OpenFileLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Raised { from, to } => {
                write!(f, "Raised the open file limit from {from} to {to}")
            }
            Self::Unchanged { limit } => write!(f, "Open file limit is {limit}"),
            Self::NotRaised {
                limit,
                target,
                cause,
            } => write!(
                f,
                "Failed to raise the open file limit from {limit} to {target}, so client connections and acceleration files together are limited to {limit} open files, and queries or refreshes beyond that fail with 'Too many open files'. Raise the limit where spiced is started (`ulimit -n`, or the container's `nofile` limit). Cause: {cause}"
            ),
            Self::Unknown { cause } => write!(
                f,
                "Failed to read the open file limit, so it was not raised and client connections and acceleration files may fail with 'Too many open files' under load. Cause: {cause}"
            ),
        }
    }
}

/// The soft limit to request: the hard limit, capped by the most the platform lets
/// one process set (`platform_max`).
///
/// macOS reports an unlimited hard limit but rejects a soft limit above
/// `kern.maxfilesperproc` with `EINVAL`. Linux rejects a hard limit above
/// `fs.nr_open` when it is set, so a hard limit read back is always settable there.
fn target_soft_limit(hard: u64, platform_max: Option<u64>) -> u64 {
    platform_max.map_or(hard, |max| hard.min(max))
}

#[cfg(unix)]
mod imp {
    use super::{OpenFileLimit, target_soft_limit};
    use nix::sys::resource::{Resource, getrlimit, setrlimit};

    pub(super) fn raise() -> Option<OpenFileLimit> {
        let (soft, hard) = match getrlimit(Resource::RLIMIT_NOFILE) {
            Ok(limits) => limits,
            Err(err) => {
                return Some(OpenFileLimit::Unknown {
                    cause: err.to_string(),
                });
            }
        };
        let target = target_soft_limit(hard, platform_max());
        if soft >= target {
            return Some(OpenFileLimit::Unchanged { limit: soft });
        }
        Some(match setrlimit(Resource::RLIMIT_NOFILE, target, hard) {
            Ok(()) => OpenFileLimit::Raised {
                from: soft,
                to: target,
            },
            Err(err) => OpenFileLimit::NotRaised {
                limit: soft,
                target,
                cause: err.to_string(),
            },
        })
    }

    #[cfg(target_os = "macos")]
    fn platform_max() -> Option<u64> {
        let mut value: libc::c_int = 0;
        let mut size = std::mem::size_of::<libc::c_int>();
        // SAFETY: the name is a NUL-terminated literal, `value` is a `c_int` that
        // outlives the call, and `size` holds its size in bytes, as the
        // `kern.maxfilesperproc` sysctl expects.
        let rc = unsafe {
            libc::sysctlbyname(
                c"kern.maxfilesperproc".as_ptr(),
                (&raw mut value).cast(),
                &raw mut size,
                std::ptr::null_mut(),
                0,
            )
        };
        if rc == 0 {
            u64::try_from(value).ok()
        } else {
            // Without the sysctl, fall back to `OPEN_MAX`, the ceiling macOS
            // documents for `setrlimit(RLIMIT_NOFILE)`.
            Some(10240)
        }
    }

    #[cfg(not(target_os = "macos"))]
    fn platform_max() -> Option<u64> {
        None
    }
}

#[cfg(not(unix))]
mod imp {
    use super::OpenFileLimit;

    // Windows has no per-process open file limit to raise: handles are bounded by
    // memory, not by an rlimit.
    pub(super) fn raise() -> Option<OpenFileLimit> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_is_the_hard_limit_without_a_platform_cap() {
        assert_eq!(target_soft_limit(524_288, None), 524_288);
        assert_eq!(target_soft_limit(u64::MAX, None), u64::MAX);
    }

    #[test]
    fn target_is_capped_by_the_platform_maximum() {
        // macOS: unlimited hard limit, `kern.maxfilesperproc` caps the soft limit.
        assert_eq!(target_soft_limit(u64::MAX, Some(245_760)), 245_760);
        // A hard limit below the platform cap is the target as is.
        assert_eq!(target_soft_limit(4096, Some(245_760)), 4096);
    }

    #[test]
    fn messages() {
        assert_eq!(
            OpenFileLimit::Raised {
                from: 1024,
                to: 524_288
            }
            .to_string(),
            "Raised the open file limit from 1024 to 524288"
        );
        assert_eq!(
            OpenFileLimit::Unchanged { limit: 1_048_576 }.to_string(),
            "Open file limit is 1048576"
        );
        assert_eq!(
            OpenFileLimit::NotRaised {
                limit: 1024,
                target: 524_288,
                cause: "EPERM: Operation not permitted".to_string(),
            }
            .to_string(),
            "Failed to raise the open file limit from 1024 to 524288, so client connections and acceleration files together are limited to 1024 open files, and queries or refreshes beyond that fail with 'Too many open files'. Raise the limit where spiced is started (`ulimit -n`, or the container's `nofile` limit). Cause: EPERM: Operation not permitted"
        );
    }
}
