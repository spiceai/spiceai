/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use super::*;
use crate::accelerated::caching::CacheWriteSender;

#[tokio::test]
async fn unregister_waits_for_parent_selection_and_preserves_other_generations() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (parent, accelerator, _) = crate::accelerated::drain_tests::table().await;
        let synchronized = SynchronizedTable::from(
            &parent,
            Arc::clone(&accelerator),
            TableReference::bare("child"),
        );
        let retired = InFlightRevalidations::default();
        let replacement = InFlightRevalidations::default();
        let sibling = InFlightRevalidations::default();
        let registry = parent.synchronized_children();
        let (writer, _receiver) = tokio::sync::mpsc::channel(1);
        for claims in [&retired, &replacement, &sibling] {
            registry.write().await.push(SynchronizedCacheTarget {
                accelerator: Arc::clone(&accelerator),
                writer: CacheWriteSender::Batched(writer.clone()),
                in_flight: Arc::clone(claims),
            });
        }
        // Parent fanout retains its registry reader while selecting child work.
        let selected = registry.read().await;
        assert!(Arc::ptr_eq(&selected[0].in_flight, &retired));
        let mut unregister = Box::pin(synchronized.unregister_cache_child(&retired));
        assert!(futures::poll!(unregister.as_mut()).is_pending());
        assert_eq!(selected.len(), 3);
        drop(selected);
        unregister.await;
        {
            let remaining = registry.read().await;
            assert_eq!(remaining.len(), 2);
            assert!(Arc::ptr_eq(&remaining[0].in_flight, &replacement));
            assert!(Arc::ptr_eq(&remaining[1].in_flight, &sibling));
        }
        synchronized.unregister_cache_child(&retired).await;
        assert_eq!(
            registry.read().await.len(),
            2,
            "repeated removal is harmless"
        );
    })
    .await
    .expect("unregister must finish after the parent reader releases");
}
