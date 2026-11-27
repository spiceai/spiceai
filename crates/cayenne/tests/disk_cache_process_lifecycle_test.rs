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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// A runtime that shuts down before any object-native catalog is created
/// disables its opener permanently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_prevents_a_pending_disk_cache_opener_from_running() {
    crate::common::require_process_per_test("it registers the process disk cache opener");
    let calls = Arc::new(AtomicUsize::new(0));
    let opener_calls = Arc::clone(&calls);
    assert!(cayenne::disk_cache::register_opener(move || async move {
        opener_calls.fetch_add(1, Ordering::SeqCst);
    }));

    cayenne::disk_cache::close_process_disk_cache(Duration::ZERO).await;
    assert!(
        cayenne::disk_cache::process_disk_cache_or_open()
            .await
            .is_none()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
