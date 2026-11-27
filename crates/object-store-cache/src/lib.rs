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

//! A node-local disk cache of immutable object-store byte blocks.
//!
//! One [`NodeCache`] per process holds the blocks of every object store it
//! [wraps](NodeCache::wrap), in packed slab files under one budget. The
//! wrapper serves reads of immutable paths in fixed-size blocks keyed by the
//! caller's namespace for the store, the path, the object size, the block
//! size and the block index, and passes everything else to the wrapped
//! store. A cache error never fails a read the store can serve.
//!
//! The capacity is the budget, less when the volume would otherwise keep
//! less than its reserve free (15 % of the volume, at least 4 GiB); space is
//! reclaimed by whole slab weighed by a read-frequency sketch; removals and
//! object sizes survive a restart; a caller evicts objects it no longer
//! references with [`NodeCache::evict_unreferenced`], and the wrapper evicts
//! durably what its own writes, renames and deletes change; and one cache at
//! a time may open a directory. The cache's directory and files are readable by
//! their owner only.
//!
//! The cache needs Unix file APIs; on other platforms [`NodeCache::open`]
//! returns [`Error::UnsupportedPlatform`].

mod block_store;
#[cfg(unix)]
mod dir_lock;
mod error;
mod frequency;
mod lru;
mod node_cache;
mod store;
mod volume;

pub use error::{Error, Result};
pub use node_cache::{NodeCache, NodeCacheConfig, NodeCacheStats, Prefetched};
pub use store::Cacheable;
pub use volume::{Space, Statvfs, VolumeSpace};

/// Internals the crate's own integration tests reach: the block-store
/// contract for fault injection, the slab store, the entry size, and the
/// tuning knobs. Not part of the supported API.
#[doc(hidden)]
pub mod testing {
    pub use crate::block_store::codec::entry_len;
    #[cfg(unix)]
    pub use crate::block_store::slab::SlabStore;
    pub use crate::block_store::{
        BlockHeat, BlockKey, BlockStore, Digest, ObjectInfo, ObjectKey, Reclaimed, StoreCounters,
        StoredMeta,
    };
    pub use crate::node_cache::{Tuning, open_with, with_block_store};
    pub use crate::volume::Reserve;
}
