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

//! Age-based cleanup of shared state that no instance needs any more.
//!
//! Every instance that uses a state location runs the cleanup of the state it
//! knows, about once an hour. The rules depend only on the age of what they
//! delete, and deleting an object that is already gone counts as done, so two
//! instances that clean the same location at once do no harm and need no lease.

use std::time::Duration;

use object_store::{ObjectStore, ObjectStoreExt, path::Path};

/// The mean time between two cleanup passes of one instance.
pub const PASS_INTERVAL: Duration = Duration::from_hours(1);

/// The wait before an instance's next cleanup pass: a random time between half
/// and one and a half [`PASS_INTERVAL`]s, so instances that start together do
/// not clean together.
#[must_use]
pub fn next_pass_delay() -> Duration {
    let interval = age(PASS_INTERVAL);
    let interval_ms = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX);
    let half = interval_ms / 2;
    Duration::from_millis(half.saturating_add(rand::random_range(0..=interval_ms)))
}

/// A cleanup rule's age. Always `age` in a release build.
///
/// A debug build divides it by `SPICE_TEST_STATE_AGE_DIVISOR` when that is set,
/// so an end-to-end test can watch every rule fire within minutes.
#[must_use]
pub fn age(age: Duration) -> Duration {
    #[cfg(debug_assertions)]
    if let Some(divisor) = *TEST_AGE_DIVISOR {
        return age / divisor;
    }
    age
}

/// `age` for a log line: whole days from two days up, else whole hours, minutes
/// or seconds, such as `14d`, `24h`, `90m` or `84s`.
#[must_use]
pub fn describe(age: Duration) -> String {
    const MINUTE: u64 = 60;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;
    let secs = age.as_secs();
    if secs >= 2 * DAY && secs % DAY == 0 {
        format!("{}d", secs / DAY)
    } else if secs >= HOUR && secs % HOUR == 0 {
        format!("{}h", secs / HOUR)
    } else if secs >= MINUTE && secs % MINUTE == 0 {
        format!("{}m", secs / MINUTE)
    } else {
        format!("{secs}s")
    }
}

#[cfg(debug_assertions)]
static TEST_AGE_DIVISOR: std::sync::LazyLock<Option<u32>> = std::sync::LazyLock::new(|| {
    std::env::var("SPICE_TEST_STATE_AGE_DIVISOR")
        .ok()
        .and_then(|divisor| divisor.parse::<u32>().ok())
        .filter(|divisor| *divisor > 0)
});

/// Deletes `path`, counting an object that is already gone as deleted.
///
/// # Errors
///
/// Returns the store's error for any failure other than the object being gone.
pub async fn delete_if_present(store: &dyn ObjectStore, path: &Path) -> object_store::Result<()> {
    match store.delete(path).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(source) => Err(source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{PutPayload, local::LocalFileSystem};

    /// A store that reports deleting a missing object as an error, as a local
    /// directory does, still counts it as done.
    #[tokio::test]
    async fn deleting_an_object_that_is_already_gone_counts_as_done() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalFileSystem::new_with_prefix(dir.path()).expect("local store");
        let path = Path::from("state/object.json");
        store
            .put(&path, PutPayload::from_static(b"{}"))
            .await
            .expect("put the object");

        delete_if_present(&store, &path)
            .await
            .expect("delete the object");
        assert!(matches!(
            store.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(matches!(
            store.delete(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        delete_if_present(&store, &path)
            .await
            .expect("deleting it again counts as done");
    }
}
