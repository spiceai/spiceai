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

//! Task-local "this task already holds [`super::table::CayenneTableProvider::write_lock`]".
//!
//! `tokio::sync::Mutex` is not reentrant. Autocommit `UPDATE` holds that lock
//! as an [`Arc`]`<`[`OwnedMutexGuard`]`>` across rematerialize + delete + insert
//! so the read is judged by the same capture the delete is (the #13828
//! pattern). Nested deletion and data sinks skip a second acquire via
//! [`is_held`] / [`acquire_or_share`].
//!
//! The hold is task-local and does **not** propagate into `tokio::spawn`. A
//! spawned commit must carry a clone of [`held_guard`] (see
//! `PreparedResolvedAppend`), or it will re-acquire and deadlock; dropping
//! only the caller's guard on cancel would also release the lock mid-publish.

use std::sync::Arc;

use tokio::sync::OwnedMutexGuard;

tokio::task_local! {
    static WRITE_LOCK_HELD: Arc<OwnedMutexGuard<()>>;
}

/// `true` when the current task is inside [`scope`].
#[must_use]
pub(crate) fn is_held() -> bool {
    WRITE_LOCK_HELD.try_with(|_| ()).is_ok()
}

/// The outer `write_lock` hold, when the current task is inside [`scope`].
///
/// `None` in a `tokio::spawn`ed task: task-locals do not cross that boundary.
#[must_use]
pub(crate) fn held_guard() -> Option<Arc<OwnedMutexGuard<()>>> {
    WRITE_LOCK_HELD.try_with(Arc::clone).ok()
}

/// Run `fut` with [`is_held`] true and [`held_guard`] equal to `guard`.
///
/// The caller must already hold `write_lock`; `guard` is that owned hold.
pub(crate) async fn scope<F, T>(guard: Arc<OwnedMutexGuard<()>>, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    WRITE_LOCK_HELD.scope(guard, fut).await
}

/// The table `write_lock`, as an owned guard: the outer hold when this task
/// already has it, otherwise a new acquire.
///
/// When the outer hold is set this returns [`dummy_owned_guard`]. Exclusive
/// access is [`held_guard`], which a spawned commit must clone onto the
/// prepared append so cancel cannot drop the real lock mid-publish.
pub(crate) async fn acquire_or_share(lock: &Arc<tokio::sync::Mutex<()>>) -> OwnedMutexGuard<()> {
    if is_held() {
        dummy_owned_guard()
    } else {
        Arc::clone(lock).lock_owned().await
    }
}

/// An owned guard on a private mutex, for write paths that require
/// `OwnedMutexGuard` only so a spawned commit can outlive the caller.
///
/// Use only when [`is_held`] is true. Exclusive access is the outer
/// `write_lock` hold, not this guard.
pub(crate) fn dummy_owned_guard() -> OwnedMutexGuard<()> {
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let Ok(guard) = lock.try_lock_owned() else {
        unreachable!("a mutex created on this line cannot already be held");
    };
    guard
}
