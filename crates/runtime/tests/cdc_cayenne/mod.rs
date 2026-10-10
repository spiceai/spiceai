/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Change-data-capture envelopes applied through `RefreshTask::start_changes_stream`
//! into a Cayenne accelerator, end to end.

use std::sync::Arc;

use accelerator_cayenne::CayenneAccelerator;
use data_accelerator_api::DataAccelerator;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use runtime::accelerated::refresh_task::RefreshTaskBuilder;
use runtime::federated::FederatedTable;
use runtime::status::RuntimeStatus;
use runtime_acceleration::change_sink::ChangeSinkContext;
use tokio::runtime::Handle;

mod debezium;
mod heartbeat;
mod inline;

/// Build the refresh task with the change sink a Cayenne dataset gets in
/// production, so the apply goes through Cayenne's deferred-durability path
/// rather than the generic provider sink.
pub(super) async fn make_refresh_task(
    accelerator: Arc<dyn TableProvider>,
    table_name: &str,
) -> runtime::accelerated::refresh_task::RefreshTask {
    let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
    let binding =
        ChangeSinkContext::new(TableReference::bare(table_name), Arc::clone(&accelerator));
    let write_lock = Arc::clone(&binding.write_lock);
    let sink = CayenneAccelerator::new()
        .change_sink(binding, &Handle::current(), 2)
        .await
        .expect("bind Cayenne change sink")
        .expect("Cayenne provides a change sink");
    RefreshTaskBuilder::new(
        RuntimeStatus::new(),
        TableReference::bare(table_name),
        federated,
        None,
        accelerator,
        Handle::current(),
        write_lock,
    )
    .with_change_sink(Some(sink))
    .build()
}
