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

use super::pinned::{NotServed, PinState};
use super::{Builder, ZeroResultsAction, refresh};
use crate::federated::FederatedTable;
use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{common::TableReference, datasource::TableProvider, prelude::SessionContext};
use std::sync::Arc;
use tokio::runtime::Handle;

/// A serving reader with `on_zero_results: use_source` falls back to its source when its
/// snapshot has no matching rows, except while it serves a snapshot it served as the pin.
#[tokio::test]
async fn a_table_serving_the_pin_never_falls_back_to_the_source() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("rows");
    let source = Arc::new(
        data_components::arrow::write::MemTable::try_new(Arc::clone(&schema), vec![vec![rows]])
            .expect("source"),
    );
    let accelerator: Arc<dyn TableProvider> = Arc::new(
        data_components::arrow::write::MemTable::try_new(schema, vec![vec![]])
            .expect("Arrow accelerator"),
    );
    let mut builder = Builder::new(
        runtime_status::RuntimeStatus::new(),
        TableReference::bare("orders"),
        Arc::new(FederatedTable::new_unchecked(source)),
        "arrow".into(),
        accelerator,
        refresh::Refresh::new(super::RefreshMode::Disabled),
        Handle::current(),
    );
    builder
        .zero_results_action(ZeroResultsAction::UseSource)
        .initial_load_complete(true);
    let table = Arc::new(builder.build().await.expect("table"));
    let ctx = SessionContext::new();
    ctx.register_table("orders", Arc::clone(&table).into_table())
        .expect("register");
    let rows = || async {
        ctx.sql("SELECT id FROM orders")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("query")
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>()
    };

    assert_eq!(rows().await, 3, "no rows here: the source answers");

    let _ = table.pin_status().set(PinState {
        pinned: Some(41),
        serves: Some(41),
        not_served: None,
    });
    assert_eq!(
        rows().await,
        0,
        "the pinned snapshot's rows are all it serves"
    );

    let _ = table.pin_status().set(PinState {
        pinned: Some(40),
        serves: Some(41),
        not_served: Some(NotServed::LoadFailed {
            reason: "the directory is read-only".to_string(),
        }),
    });
    assert_eq!(
        rows().await,
        0,
        "the pin moved to a snapshot it cannot load: it keeps serving only the snapshot it served as the pin"
    );

    let _ = table.pin_status().set(PinState {
        pinned: Some(41),
        serves: Some(44),
        not_served: None,
    });
    assert_eq!(
        rows().await,
        3,
        "not serving the pin: the source answers again"
    );
}
