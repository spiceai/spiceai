/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Deletes the shared rate-control state of origins no instance uses.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use object_store::{ObjectStore, path::Path};
use object_store_occ::cleanup;

/// How long an origin's rate-control file stays after its last write. Every
/// instance that sends requests to an origin rewrites the origin's file each
/// lease window, so a file this old belongs to an origin no instance uses.
pub const UNUSED_ORIGIN_RETENTION: Duration = Duration::from_hours(7 * 24);

/// Deletes the rate-control files under `prefix` that were last written at least
/// [`UNUSED_ORIGIN_RETENTION`] before `now`, and returns how many it deleted. An
/// empty `prefix` would name the whole store, so it deletes nothing.
///
/// An instance that uses the origin again creates its file again at its next
/// lease refresh, so deleting a file another instance has just rewritten costs
/// at most one lease window of that origin's shared accounting.
///
/// # Errors
///
/// Returns the store's error when listing or deleting fails. Files deleted
/// before the failure stay deleted.
pub async fn delete_unused_state(
    store: &dyn ObjectStore,
    prefix: &str,
    now: SystemTime,
) -> object_store::Result<usize> {
    let retention_ms = millis(cleanup::age(UNUSED_ORIGIN_RETENTION));
    let now_ms = now.duration_since(UNIX_EPOCH).map_or(0, millis);
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        return Ok(0);
    }
    let prefix = Path::from(prefix);
    let mut objects = store.list(Some(&prefix));
    let mut deleted = 0;
    while let Some(object) = objects.try_next().await? {
        let is_state_file = object
            .location
            .extension()
            .is_some_and(|extension| extension == "json");
        let last_written_ms = u64::try_from(object.last_modified.timestamp_millis()).unwrap_or(0);
        if is_state_file && now_ms >= last_written_ms.saturating_add(retention_ms) {
            cleanup::delete_if_present(store, &object.location).await?;
            deleted += 1;
        }
    }
    Ok(deleted)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};

    async fn stored_paths(store: &InMemory) -> Vec<String> {
        let mut paths: Vec<String> = store
            .list(None)
            .map_ok(|object| object.location.to_string())
            .try_collect()
            .await
            .expect("list");
        paths.sort();
        paths
    }

    async fn seed(store: &InMemory, paths: &[&str]) {
        for path in paths {
            store
                .put(&Path::from(*path), PutPayload::from_static(b"{}"))
                .await
                .expect("put");
        }
    }

    /// A file goes once no instance has written it for seven days, measured from
    /// its last write; a newer file and objects that are not state files stay.
    #[tokio::test]
    async fn a_file_unwritten_for_seven_days_is_deleted() {
        let store = InMemory::new();
        seed(
            &store,
            &[
                "rate-control/app/api.example.com_443-1.json",
                "rate-control/other-app/api.example.com_443-1.json",
                "rate-control/app/notes.txt",
            ],
        )
        .await;
        let written_at = SystemTime::now();

        let just_under = written_at + UNUSED_ORIGIN_RETENTION - Duration::from_secs(1);
        assert_eq!(
            delete_unused_state(&store, "rate-control", just_under)
                .await
                .expect("cleanup succeeds"),
            0
        );
        assert_eq!(stored_paths(&store).await.len(), 3);

        // The written timestamp has millisecond resolution, so allow for it.
        let past = written_at + UNUSED_ORIGIN_RETENTION + Duration::from_millis(1);
        assert_eq!(
            delete_unused_state(&store, "/rate-control/", past)
                .await
                .expect("cleanup succeeds"),
            2
        );
        assert_eq!(
            stored_paths(&store).await,
            vec!["rate-control/app/notes.txt".to_string()]
        );
    }

    /// Two instances that clean the same location at once both succeed.
    #[tokio::test]
    async fn two_instances_cleaning_at_once_both_succeed() {
        let store = InMemory::new();
        let paths: Vec<String> = (0..50)
            .map(|origin| format!("rate-control/app/origin-{origin}.json"))
            .collect();
        seed(
            &store,
            &paths.iter().map(String::as_str).collect::<Vec<_>>(),
        )
        .await;
        let later = SystemTime::now() + UNUSED_ORIGIN_RETENTION + Duration::from_secs(1);

        let (first, second) = tokio::join!(
            delete_unused_state(&store, "rate-control", later),
            delete_unused_state(&store, "rate-control", later)
        );
        let deleted = first.expect("the first cleanup succeeds")
            + second.expect("the second cleanup succeeds");
        assert!(
            deleted >= 50,
            "every file is deleted by one cleanup or the other, deleted {deleted}"
        );
        assert_eq!(stored_paths(&store).await, Vec::<String>::new());
    }
}
