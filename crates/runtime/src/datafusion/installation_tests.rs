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

use super::Table;
use crate::{
    Runtime,
    component::{
        access::AccessMode,
        dataset::{
            acceleration::{Acceleration, RefreshMode},
            builder::DatasetBuilder,
        },
    },
    federated::FederatedTable,
};
use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use async_trait::async_trait;
use data_connector_api::{
    ConnectorComponent, ConnectorContext, DataConnector, DataConnectorError, DataConnectorResult,
};
use datafusion::datasource::TableProvider;
use runtime_component::dataset::{DatasetSpec, TimeFormat};
use std::{any::Any, sync::Arc, time::Duration};
use tokio::sync::{Notify, Semaphore};

#[derive(Debug)]
struct MetadataGate {
    provider: Arc<dyn TableProvider>,
    entered: Notify,
    release: Semaphore,
    fail: bool,
}

#[async_trait]
impl DataConnector for MetadataGate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _: &dyn ConnectorContext,
        _: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Arc::clone(&self.provider))
    }

    async fn read_write_provider(
        &self,
        _: &dyn ConnectorContext,
        _: &DatasetSpec,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        Some(Ok(Arc::clone(&self.provider)))
    }

    async fn metadata_provider(
        &self,
        dataset: &DatasetSpec,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        self.entered.notify_one();
        self.release
            .acquire()
            .await
            .expect("release metadata preparation")
            .forget();
        self.fail.then(|| {
            Err(DataConnectorError::InternalWithSource {
                dataconnector: "installation-test".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: "controlled metadata failure".into(),
            })
        })
    }
}

#[derive(Clone, Copy)]
enum Outcome {
    CancelMetadata,
    FailMetadata,
    CancelBookkeeping,
    Complete,
}

async fn installation(outcome: Outcome) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let runtime = Arc::new(Runtime::builder().build().await);
        let df = runtime.datafusion();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vec![7]))])
            .expect("source row");
        let provider: Arc<dyn TableProvider> = Arc::new(
            data_components::arrow::write::MemTable::try_new(schema, vec![vec![batch]])
                .expect("Arrow source"),
        );
        let source = Arc::new(MetadataGate {
            provider: Arc::clone(&provider), entered: Notify::new(), release: Semaphore::new(0),
            fail: matches!(outcome, Outcome::FailMetadata),
        });
        let mut builder = DatasetBuilder::try_new("test:memory".into(), "installation_test")
            .expect("dataset")
            .with_app(Arc::new(app::AppBuilder::new("installation-test").build()))
            .with_runtime(Arc::clone(&runtime));
        builder.acceleration = Some(Acceleration { refresh_mode: Some(RefreshMode::Append), ..Acceleration::default() });
        builder.time_column = Some("id".into());
        builder.time_format = Some(TimeFormat::UnixSeconds);
        builder.access = AccessMode::ReadWrite;
        let dataset = Arc::new(builder.build().expect("dataset configuration"));
        let name = dataset.name.clone();
        let table = Table::Accelerated {
            source: Arc::clone(&source) as Arc<dyn DataConnector>,
            federated_read_table: FederatedTable::new_unchecked(provider),
            accelerated_table: None, secrets: runtime.secrets(),
            bootstrap_status: runtime_acceleration::BootstrapStatus::none().into(),
            initial_partition_filters: None,
        };
        let mut registration = Box::pin(df.register_table(dataset, table));
        tokio::select! {
            () = source.entered.notified() => {}
            result = registration.as_mut() => panic!("registration ended before metadata: {:?}", result.err()),
        }
        assert!(!df.table_exists(&name), "metadata preparation cannot expose the catalog entry");
        assert!(!df.is_writable(&name), "metadata preparation cannot expose write access");
        match outcome {
            Outcome::CancelMetadata => drop(registration),
            Outcome::FailMetadata => {
                source.release.add_permits(1);
                assert!(registration.await.is_err());
            }
            Outcome::CancelBookkeeping => {
                let bookkeeping = df.accelerated_tables.write().await;
                source.release.add_permits(1);
                assert!(futures::poll!(registration.as_mut()).is_pending());
                assert!(!df.table_exists(&name));
                drop(registration);
                drop(bookkeeping);
            }
            Outcome::Complete => {
                source.release.add_permits(1);
                registration.await.expect("complete registration");
                assert!(df.table_exists(&name));
                assert!(df.is_writable(&name));
                assert!(df.is_accelerated(&name).await);
                let provider = df.get_table(&name).await.expect("installed table");
                let table = spice_table::find_layer::<crate::accelerated::AcceleratedTable>(
                    provider.as_ref(), spice_table::LayerWalk::Read,
                ).expect("accelerated layer");
                let permit = table.change_sink().expect("bound sink").reserve().await.expect("live owner");
                drop(permit);
            }
        }
        if !matches!(outcome, Outcome::Complete) {
            assert!(!df.table_exists(&name));
            assert!(!df.is_writable(&name));
            assert!(!df.is_accelerated(&name).await);
        }
        df.remove_table(&name).await.expect("drain and remove the generation");
        assert!(!df.table_exists(&name));
        assert!(!df.is_writable(&name));
    }).await.expect("installation must complete or cancel without hanging");
}

#[tokio::test]
async fn change_sink_installation_cancelled_metadata_does_not_publish() {
    installation(Outcome::CancelMetadata).await;
}

#[tokio::test]
async fn change_sink_installation_failed_metadata_does_not_publish() {
    installation(Outcome::FailMetadata).await;
}

#[tokio::test]
async fn change_sink_installation_cancelled_bookkeeping_does_not_publish() {
    installation(Outcome::CancelBookkeeping).await;
}

#[tokio::test]
async fn change_sink_installation_publishes_a_live_owner() {
    installation(Outcome::Complete).await;
}
