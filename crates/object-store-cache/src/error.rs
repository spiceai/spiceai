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

use std::path::PathBuf;

use snafu::Snafu;

/// Errors of the node cache and its block stores. Opening a cache can fail
/// with any of them; on the read path the cache treats every one as a miss and
/// reads the object store instead.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to {action} cache file {}: {source}", path.display()))]
    Io {
        action: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("A blocking cache task failed: {source}"))]
    Join { source: tokio::task::JoinError },

    #[snafu(display(
        "The block key is {len} bytes, more than the {max} bytes an entry header holds"
    ))]
    KeyTooLong { len: usize, max: usize },

    #[snafu(display("The block is {len} bytes, more than the {max} bytes one entry holds"))]
    BlockTooLarge { len: usize, max: usize },

    #[snafu(display(
        "Failed to create a slab file in cache directory {}: every slab number is used. Remove the directory's slab files to reset the cache",
        path.display()
    ))]
    NoSlabId { path: PathBuf },

    #[snafu(display(
        "Failed to open the node cache in {}: another node cache, in this or another process, has it open. Give each cache its own directory",
        path.display()
    ))]
    Locked { path: PathBuf },

    #[snafu(display("Invalid node cache configuration: {reason}"))]
    InvalidConfig { reason: String },

    #[snafu(display(
        "Failed to prefetch from store namespace '{namespace}': no store is wrapped under that namespace. Wrap the store with NodeCache::wrap first"
    ))]
    UnknownStore { namespace: String },

    #[snafu(display("The node cache must be opened inside a Tokio runtime: {source}"))]
    NoRuntime {
        source: tokio::runtime::TryCurrentError,
    },

    #[snafu(display(
        "Failed to open the node cache: it needs Unix file APIs, which this platform does not have. Run without the node cache on this platform"
    ))]
    UnsupportedPlatform,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
