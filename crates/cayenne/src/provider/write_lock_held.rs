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
//! across rematerialize + delete + insert so the read is judged by the same
//! capture the delete is (the #13828 pattern). The deletion sink and the data
//! sink each take the same lock; this flag lets them skip the acquire.

tokio::task_local! {
    static WRITE_LOCK_HELD: ();
}

/// `true` when the current task is inside [`scope`].
#[must_use]
pub(crate) fn is_held() -> bool {
    WRITE_LOCK_HELD.try_with(|()| ()).is_ok()
}

/// Run `fut` with [`is_held`] true. The caller must already hold `write_lock`.
pub(crate) async fn scope<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    WRITE_LOCK_HELD.scope((), fut).await
}

/// An owned guard on a private mutex, for write paths that require
/// `OwnedMutexGuard` only so a spawned commit can outlive the caller.
///
/// Use only when [`is_held`] is true. Exclusive access is the outer
/// `write_lock` hold, not this guard; that hold is owned by a task that waits
/// for the write, and so for any commit the write hands this guard to.
pub(crate) fn dummy_owned_guard() -> tokio::sync::OwnedMutexGuard<()> {
    let lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
    let Ok(guard) = lock.try_lock_owned() else {
        unreachable!("a mutex created on this line cannot already be held");
    };
    guard
}
