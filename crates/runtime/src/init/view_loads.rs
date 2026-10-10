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

//! Tracks startup view registrations that are still pending, so a Spicepod
//! change that replaces or removes a view can stop its startup registration.
//!
//! Startup view tasks wait on their dependencies, which can take as long as a
//! failing dataset keeps retrying. Without a way to stop them, a hot reload that
//! changes or removes such a view would see the old task register the replaced
//! definition after the reload's own removal pass.

use std::{collections::HashMap, sync::Arc};

use datafusion::common::{ResolvedTableReference, TableReference};
use tokio_util::sync::CancellationToken;

use crate::datafusion::resolve_table_reference;

#[derive(Default)]
pub(crate) struct ViewLoads {
    pending: parking_lot::Mutex<HashMap<ResolvedTableReference, (u64, CancellationToken)>>,
    next_id: std::sync::atomic::AtomicU64,
}

/// A pending startup registration of a view, until it is dropped.
pub(crate) struct ViewLoad {
    loads: Arc<ViewLoads>,
    name: ResolvedTableReference,
    id: u64,
    token: CancellationToken,
}

impl ViewLoads {
    /// Registers a pending startup registration of `name`, superseding any
    /// earlier one.
    pub(crate) fn begin(self: &Arc<Self>, name: &TableReference) -> ViewLoad {
        let name = resolve_table_reference(name.clone());
        let token = CancellationToken::new();
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some((_, old)) = self
            .pending
            .lock()
            .insert(name.clone(), (id, token.clone()))
        {
            old.cancel();
        }
        ViewLoad {
            loads: Arc::clone(self),
            name,
            id,
            token,
        }
    }

    /// Cancels the pending startup registration of `name`, if any. Returns
    /// whether one was pending.
    pub(crate) fn supersede(&self, name: &TableReference) -> bool {
        let name = resolve_table_reference(name.clone());
        match self.pending.lock().remove(&name) {
            Some((_, token)) => {
                token.cancel();
                true
            }
            None => false,
        }
    }
}

impl ViewLoad {
    pub(crate) fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

impl Drop for ViewLoad {
    fn drop(&mut self) {
        let mut pending = self.loads.pending.lock();
        if pending
            .get(&self.name)
            .is_some_and(|(id, _)| *id == self.id)
        {
            pending.remove(&self.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supersede_cancels_only_the_named_view() {
        let loads = Arc::new(ViewLoads::default());
        let a = loads.begin(&TableReference::bare("a"));
        let b = loads.begin(&TableReference::bare("b"));
        assert!(loads.supersede(&TableReference::parse_str("public.a")));
        assert!(a.token().is_cancelled());
        assert!(!b.token().is_cancelled());
        assert!(!loads.supersede(&TableReference::bare("a")));
    }

    #[test]
    fn a_dropped_load_leaves_nothing_to_supersede() {
        let loads = Arc::new(ViewLoads::default());
        drop(loads.begin(&TableReference::bare("a")));
        assert!(!loads.supersede(&TableReference::bare("a")));
    }

    #[test]
    fn a_newer_load_supersedes_the_older_and_survives_its_drop() {
        let loads = Arc::new(ViewLoads::default());
        let old = loads.begin(&TableReference::bare("a"));
        let new = loads.begin(&TableReference::bare("a"));
        assert!(old.token().is_cancelled());
        drop(old);
        assert!(loads.supersede(&TableReference::bare("a")));
        assert!(new.token().is_cancelled());
    }
}
