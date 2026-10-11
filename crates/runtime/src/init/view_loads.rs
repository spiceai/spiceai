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

//! Tracks the startup registrations of views, so a Spicepod change that
//! replaces or removes a view stops its startup registration.
//!
//! A startup view registration waits for the datasets and views it reads from,
//! which takes as long as a failing dataset keeps retrying. A Spicepod change
//! applied meanwhile registers or removes the views it touches itself
//! (`Runtime::apply_view_diff`), so the startup registration of such a view must
//! not run afterwards: it would register the definition the change replaced.
//!
//! [`ViewLoads::supersede`] covers both orders. A registration already pending
//! is cancelled, and `supersede` returns only once it has ended, so it cannot
//! register after the caller replaces or removes the view. A registration that
//! has not begun yet — the startup load reads its app before it initializes the
//! accelerators, and a change can land in between — never begins:
//! [`ViewLoads::begin`] refuses a view that has been superseded.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion::common::{ResolvedTableReference, TableReference};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::datafusion::resolve_table_reference;

#[derive(Default)]
pub(crate) struct ViewLoads {
    state: parking_lot::Mutex<State>,
}

#[derive(Default)]
struct State {
    /// The startup registration of each view that has begun and not ended.
    pending: HashMap<ResolvedTableReference, Pending>,
    /// Views a Spicepod change has taken over. Never cleared: only startup
    /// registrations `begin`, and one that begins after a change took its view
    /// over would register the definition the change replaced.
    superseded: HashSet<ResolvedTableReference>,
    next_id: u64,
}

struct Pending {
    id: u64,
    /// Cancelled to stop the registration.
    cancel: CancellationToken,
    /// Cancelled once the registration has ended, however it ended.
    ended: CancellationToken,
}

/// A pending startup registration of a view, until it is dropped.
pub(crate) struct ViewLoad {
    loads: Arc<ViewLoads>,
    name: ResolvedTableReference,
    id: u64,
    cancel: CancellationToken,
    /// Signals [`ViewLoads::supersede`] when this load is dropped.
    _ended: DropGuard,
}

impl ViewLoads {
    /// Begins the startup registration of `name`, or returns `None` when it
    /// must not run: a Spicepod change has already taken the view over, or a
    /// registration of it is already pending.
    pub(crate) fn begin(self: &Arc<Self>, name: &TableReference) -> Option<ViewLoad> {
        let name = resolve_table_reference(name.clone());
        let mut state = self.state.lock();
        if state.superseded.contains(&name) || state.pending.contains_key(&name) {
            return None;
        }
        let id = state.next_id;
        state.next_id += 1;
        let cancel = CancellationToken::new();
        let ended = CancellationToken::new();
        state.pending.insert(
            name.clone(),
            Pending {
                id,
                cancel: cancel.clone(),
                ended: ended.clone(),
            },
        );
        Some(ViewLoad {
            loads: Arc::clone(self),
            name,
            id,
            cancel,
            _ended: ended.drop_guard(),
        })
    }

    /// Stops the startup registration of `name` for good: a pending one is
    /// cancelled and this returns once it has ended, and one that has not begun
    /// never will. Returns whether one was pending.
    pub(crate) async fn supersede(&self, name: &TableReference) -> bool {
        let name = resolve_table_reference(name.clone());
        let removed = {
            let mut state = self.state.lock();
            let removed = state.pending.remove(&name);
            state.superseded.insert(name);
            removed
        };
        let Some(pending) = removed else {
            return false;
        };
        pending.cancel.cancel();
        pending.ended.cancelled().await;
        true
    }
}

impl ViewLoad {
    /// Cancelled when a Spicepod change supersedes this registration. The
    /// registration must then stop without registering the view and drop this
    /// load: [`ViewLoads::supersede`] waits for that.
    pub(crate) fn token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

impl Drop for ViewLoad {
    fn drop(&mut self) {
        let mut state = self.loads.state.lock();
        if state
            .pending
            .get(&self.name)
            .is_some_and(|pending| pending.id == self.id)
        {
            state.pending.remove(&self.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn name(table: &str) -> TableReference {
        TableReference::parse_str(table)
    }

    #[tokio::test]
    async fn supersede_cancels_only_the_named_view_and_waits_for_it_to_end() {
        let loads = Arc::new(ViewLoads::default());
        let a = loads.begin(&name("a")).expect("nothing has superseded `a`");
        let b = loads.begin(&name("b")).expect("nothing has superseded `b`");
        let token = a.token();
        let ended = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ended_task = Arc::clone(&ended);
        tokio::spawn(async move {
            token.cancelled().await;
            // The registration takes a while to notice and stop.
            tokio::time::sleep(Duration::from_millis(50)).await;
            ended_task.store(true, std::sync::atomic::Ordering::SeqCst);
            drop(a);
        });

        // Matched by resolved name: `public.a` is `a`.
        assert!(loads.supersede(&name("public.a")).await);
        assert!(
            ended.load(std::sync::atomic::Ordering::SeqCst),
            "supersede must return only after the superseded registration has ended"
        );
        assert!(!b.token().is_cancelled());
        assert!(!loads.supersede(&name("a")).await);
    }

    #[tokio::test]
    async fn a_view_superseded_before_its_registration_begins_never_begins() {
        let loads = Arc::new(ViewLoads::default());
        assert!(
            !loads.supersede(&name("spice.public.a")).await,
            "nothing was pending"
        );
        assert!(
            loads.begin(&name("a")).is_none(),
            "a Spicepod change took `a` over, so the startup load must not register its old definition"
        );
        assert!(loads.begin(&name("b")).is_some());
    }

    #[tokio::test]
    async fn one_registration_per_view_at_a_time() {
        let loads = Arc::new(ViewLoads::default());
        let first = loads.begin(&name("a")).expect("first registration begins");
        assert!(
            loads.begin(&name("public.a")).is_none(),
            "a second registration of a pending view must not begin"
        );
        drop(first);
        let second = loads
            .begin(&name("a"))
            .expect("once the first has ended, another may begin");
        let token = second.token();
        tokio::spawn(async move {
            second.token().cancelled().await;
            drop(second);
        });
        tokio::time::timeout(Duration::from_secs(5), loads.supersede(&name("a")))
            .await
            .expect("supersede must return once the registration it cancelled ends");
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn a_dropped_registration_leaves_nothing_to_supersede() {
        let loads = Arc::new(ViewLoads::default());
        drop(loads.begin(&name("a")));
        assert!(!loads.supersede(&name("a")).await);
    }
}
