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

use std::{any::Any, sync::Arc};

use crate::{
    Runtime,
    component::dataset::{DatasetSpec, builder::DatasetBuilder},
    dataconnector::reconnecting::{ConnectorBuilder, ReconnectingConnector},
    datafusion::Table,
};
use async_trait::async_trait;
use data_connector_api::{ConnectorContext, DataConnector, DataConnectorResult};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::TableReference,
    datasource::{MemTable, TableProvider},
};
use runtime_table::federated::FederatedTable;

#[derive(Debug)]
struct Source {
    table: Arc<dyn TableProvider>,
}
#[async_trait]
impl DataConnector for Source {
    fn as_any(&self) -> &dyn Any {
        self
    }
    async fn read_provider(
        &self,
        _ctx: &dyn ConnectorContext,
        _ds: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Arc::clone(&self.table))
    }
    async fn metadata_provider(
        &self,
        _ds: &DatasetSpec,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        Some(Ok(Arc::clone(&self.table)))
    }
}
#[tokio::test]
async fn delayed_connector_construction_keeps_metadata_registration_pending() {
    let runtime = Arc::new(Runtime::builder().build().await);
    let mut builder = DatasetBuilder::try_new("delayed:any".into(), "orders")
        .expect("valid metadata-enabled dataset");
    builder.has_metadata_table = true;
    let ds = Arc::new(
        builder
            .with_app(Arc::new(
                app::AppBuilder::new("metadata_registration").build(),
            ))
            .with_runtime(Arc::clone(&runtime))
            .build()
            .expect("valid metadata-enabled dataset"),
    );
    let table: Arc<dyn TableProvider> = Arc::new(
        MemTable::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![vec![]],
        )
        .expect("valid in-memory source table"),
    );
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let source: Arc<dyn DataConnector> = Arc::new(Source {
        table: Arc::clone(&table),
    });
    let build: ConnectorBuilder = Arc::new({
        let gate = Arc::clone(&gate);
        let source = Arc::clone(&source);
        move || {
            let gate = Arc::clone(&gate);
            let source = Arc::clone(&source);
            Box::pin(async move {
                let permit = gate
                    .acquire()
                    .await
                    .expect("construction gate remains open");
                permit.forget();
                Ok(source)
            })
        }
    });
    let wrapper = Arc::new(ReconnectingConnector::new("delayed", build));
    let metadata = TableReference::partial("metadata", "orders");
    let df = runtime.datafusion();
    let registration = tokio::spawn({
        let df = Arc::clone(&df);
        let ds = Arc::clone(&ds);
        let table = Arc::clone(&table);
        let wrapper = Arc::clone(&wrapper);
        async move {
            df.register_table(
                ds,
                Table::Federated {
                    data_connector: wrapper,
                    federated_read_table: FederatedTable::new_unchecked(table),
                    generation: crate::datafusion::FederatedGeneration::Drain,
                },
            )
            .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let registered_before_connector = registration.is_finished();
    gate.add_permits(1);
    registration
        .await
        .expect("registration task completes")
        .expect("table registration succeeds");
    // Explicitly complete construction if metadata registration never requested it.
    let context = crate::dataconnector::parameters::RuntimeConnectorContext::for_dataset(&ds);
    wrapper
        .read_provider(&context, &ds)
        .await
        .expect("connector construction succeeds");
    assert!(
        !registered_before_connector,
        "metadata-enabled registration must await connector construction"
    );
    assert!(
        df.get_table(&ds.name).await.is_some(),
        "the main table must register"
    );
    assert!(
        df.get_table(&metadata).await.is_some(),
        "the requested metadata table must register"
    );
}
