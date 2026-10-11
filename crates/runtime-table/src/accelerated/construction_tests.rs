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

use super::{
    Builder, caching::SynchronizedCacheTarget, refresh::Refresh,
    synchronized_table::SynchronizedTable,
};
use crate::federated::FederatedTable;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::{common::TableReference, datasource::TableProvider};
use runtime_component::dataset::acceleration::RefreshMode;
use std::{sync::Arc, task::Poll, time::Duration};
use tokio::runtime::Handle;

fn memory() -> Arc<dyn TableProvider> {
    Arc::new(
        data_components::arrow::write::MemTable::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![vec![]],
        )
        .expect("Arrow table"),
    )
}

fn builder(name: &str, accelerator: Arc<dyn TableProvider>) -> Builder {
    Builder::new(
        runtime_status::RuntimeStatus::new(),
        TableReference::bare(name),
        Arc::new(FederatedTable::new_unchecked(memory())),
        "test".to_string(),
        accelerator,
        Refresh::new(RefreshMode::Caching),
        Handle::current(),
    )
}

#[tokio::test]
async fn cancelled_child_construction_does_not_retain_a_fanout_target() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let parent = builder("parent", memory()).build().await.expect("parent");
        let registry = parent.synchronized_children();
        let mut completed = 0;
        let mut cancelled = 0;
        // Vary the cooperative budget to suspend construction at its actual awaits.
        for debit in 0..128 {
            let accelerator = memory();
            let weak = Arc::downgrade(&accelerator);
            let mut builder = builder("child", accelerator);
            builder
                .synchronize_with(&parent)
                .await
                .expect("compatible parent");
            let mut build = Box::pin(builder.build());
            tokio::task::yield_now().await;
            for _ in 0..debit {
                tokio::task::consume_budget().await;
            }
            match futures::poll!(build.as_mut()) {
                Poll::Ready(result) => {
                    drop(build);
                    let child = result.expect("child construction");
                    assert_eq!(registry.read().await.len(), 1);
                    child.drain_changes().await.expect("child drain");
                    drop(child);
                    completed += 1;
                }
                Poll::Pending => {
                    drop(build);
                    cancelled += 1;
                }
            }
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if registry.read().await.is_empty() && weak.upgrade().is_none() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("construction/drain must release the registry target and accelerator");
        }
        assert!(completed > 0);
        assert!(cancelled > 0);
        parent.drain_changes().await.expect("parent drain");
    })
    .await
    .expect("bounded construction cancellation coverage");
}

#[tokio::test]
async fn prepared_child_is_not_published_after_parent_closes() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let parent = builder("parent", memory()).build().await.expect("parent");
        let child = builder("child", memory())
            .build()
            .await
            .expect("child owner");
        let synchronized = SynchronizedTable::from(
            &parent,
            child.get_accelerator(),
            TableReference::bare("child"),
        );
        let prepared = synchronized
            .prepare_cache_child(SynchronizedCacheTarget {
                accelerator: child.get_accelerator(),
                writer: child.batch_write_tx.clone().expect("cache writer"),
                in_flight: Arc::clone(&child.in_flight_revalidations),
            })
            .await
            .expect("child snapshot");
        let drain = parent.begin_changes_drain();
        prepared
            .publish()
            .expect_err("a draining parent refuses new children");
        assert!(parent.synchronized_children().read().await.is_empty());
        drain.wait().await.expect("parent drain");
        child.drain_changes().await.expect("child drain");
    })
    .await
    .expect("closed parent must not retain child initialization");
}
