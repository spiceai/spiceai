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

//! The objects a store holds blocks of, with a count of resident blocks and
//! the object's metadata (e-tag, modification time) per object. Built from
//! the slab indexes (or entry headers) at open and kept up to date as entries
//! come and go, so the node cache can find an object's size and metadata
//! after a restart and list a store's objects for eviction by reference.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use super::{ObjectInfo, ObjectKey, StoredMeta};

#[derive(Debug)]
struct Resident {
    key: Arc<ObjectKey>,
    blocks: u64,
    meta: StoredMeta,
}

/// Store URL → path → the sizes held (normally one). Paths are ordered, so
/// the objects under a prefix are found without walking the store's others.
#[derive(Debug, Default)]
pub(crate) struct Objects {
    stores: HashMap<Arc<str>, BTreeMap<Arc<str>, Vec<Resident>>>,
}

impl Objects {
    /// Counts `n` more resident blocks of the object, records `meta` when it
    /// says anything, and returns the object's shared key.
    pub(crate) fn acquire(
        &mut self,
        store: &str,
        path: &str,
        object_size: u64,
        block_size: u64,
        n: u64,
        meta: &StoredMeta,
    ) -> Arc<ObjectKey> {
        let store_key = match self.stores.get_key_value(store) {
            Some((k, _)) => Arc::clone(k),
            None => Arc::from(store),
        };
        let paths = self.stores.entry(Arc::clone(&store_key)).or_default();
        let path_key = match paths.get_key_value(path) {
            Some((k, _)) => Arc::clone(k),
            None => Arc::from(path),
        };
        let sizes = paths.entry(Arc::clone(&path_key)).or_default();
        if let Some(r) = sizes
            .iter_mut()
            .find(|r| r.key.object_size == object_size && r.key.block_size == block_size)
        {
            r.blocks += n;
            if !meta.is_empty() && r.meta != *meta {
                r.meta = meta.clone();
            }
            return Arc::clone(&r.key);
        }
        let key = Arc::new(ObjectKey {
            store: store_key,
            path: path_key,
            object_size,
            block_size,
        });
        sizes.push(Resident {
            key: Arc::clone(&key),
            blocks: n,
            meta: meta.clone(),
        });
        key
    }

    /// Counts one resident block of the object less.
    pub(crate) fn release(&mut self, key: &ObjectKey) {
        let Some(paths) = self.stores.get_mut(&*key.store) else {
            return;
        };
        let Some(sizes) = paths.get_mut(&*key.path) else {
            return;
        };
        if let Some(pos) = sizes.iter().position(|r| {
            r.key.object_size == key.object_size && r.key.block_size == key.block_size
        }) {
            sizes[pos].blocks = sizes[pos].blocks.saturating_sub(1);
            if sizes[pos].blocks == 0 {
                sizes.swap_remove(pos);
            }
        }
        if sizes.is_empty() {
            paths.remove(&*key.path);
        }
        if paths.is_empty() {
            self.stores.remove(&*key.store);
        }
    }

    /// The paths of `store` with resident blocks at `prefix` or under it on a
    /// segment boundary (every path for an empty prefix).
    pub(crate) fn paths_under(&self, store: &str, prefix: &str) -> Vec<Arc<str>> {
        let Some(paths) = self.stores.get(store) else {
            return Vec::new();
        };
        let prefix = prefix.trim_end_matches('/');
        if prefix.is_empty() {
            return paths.keys().cloned().collect();
        }
        let under = format!("{prefix}/");
        paths
            .get_key_value(prefix)
            .map(|(path, _)| Arc::clone(path))
            .into_iter()
            .chain(
                paths
                    .range::<str, _>((Bound::Included(under.as_str()), Bound::Unbounded))
                    .take_while(|(path, _)| path.starts_with(&under))
                    .map(|(path, _)| Arc::clone(path)),
            )
            .collect()
    }

    pub(crate) fn of_path(&self, store: &str, path: &str) -> Vec<ObjectKey> {
        self.stores
            .get(store)
            .and_then(|paths| paths.get(path))
            .map(|sizes| sizes.iter().map(|r| (*r.key).clone()).collect())
            .unwrap_or_default()
    }

    fn find(&self, key: &ObjectKey) -> Option<&Resident> {
        self.stores
            .get(&*key.store)?
            .get(&*key.path)?
            .iter()
            .find(|r| r.key.object_size == key.object_size && r.key.block_size == key.block_size)
    }

    /// The metadata recorded for the object, or `None` when it holds no
    /// blocks.
    pub(crate) fn meta_of(&self, key: &ObjectKey) -> Option<StoredMeta> {
        self.find(key).map(|r| r.meta.clone())
    }

    /// The object's resident blocks.
    pub(crate) fn resident_blocks(&self, key: &ObjectKey) -> u64 {
        self.find(key).map_or(0, |r| r.blocks)
    }

    pub(crate) fn info_of(&self, store: &str, path: &str, block_size: u64) -> Option<ObjectInfo> {
        let sizes = self.stores.get(store)?.get(path)?;
        let mut held = sizes.iter().filter(|r| r.key.block_size == block_size);
        let first = held.next()?;
        // Two sizes of one path: the object was rewritten, which the
        // `Cacheable` contract excludes. Report none, so the caller asks the
        // store.
        held.next().is_none().then(|| ObjectInfo {
            size: first.key.object_size,
            meta: first.meta.clone(),
        })
    }
}
