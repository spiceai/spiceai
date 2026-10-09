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

use super::{DatasetPlacement, Table};
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
use datafusion::{
    common::{DataFusionError, TableReference},
    datasource::TableProvider,
};
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
        Some(if self.fail {
            Err(DataConnectorError::InternalWithSource {
                dataconnector: "installation-test".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: "controlled metadata failure".into(),
            })
        } else {
            Ok(Arc::clone(&self.provider))
        })
    }
}

#[derive(Debug)]
struct RefuseInstallation;

impl DatasetPlacement for RefuseInstallation {
    fn install(&self, _: &TableReference, _: Arc<dyn TableProvider>) -> super::Result<()> {
        Err(super::Error::UnableToRegisterTableToDataFusion {
            source: DataFusionError::Execution("controlled catalog refusal".into()),
        })
    }
}

#[derive(Clone, Copy)]
enum Outcome {
    CancelMetadata,
    FailMetadata,
    CancelBookkeeping,
    FailCatalog,
    RestoreMetadata,
    FailMetadataPublication,
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
        let metadata_name = TableReference::partial("metadata", name.to_string());
        let previous_metadata: Option<Arc<dyn TableProvider>> = if matches!(outcome, Outcome::RestoreMetadata) {
            let previous: Arc<dyn TableProvider> = Arc::new(
                data_components::arrow::write::MemTable::try_new(provider.schema(), vec![vec![]]).expect("prior metadata"),
            );
            df.ctx.register_table(metadata_name.clone(), Arc::clone(&previous)).expect("install prior metadata");
            Some(previous)
        } else { None };
        if matches!(outcome, Outcome::FailCatalog | Outcome::RestoreMetadata) {
            df.set_dataset_placement(&name, Arc::new(RefuseInstallation));
        }
        if matches!(outcome, Outcome::FailMetadataPublication) {
            df.ctx.catalog("spice").expect("catalog").deregister_schema("metadata", false).expect("remove empty metadata schema");
        }
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
        assert_eq!(df.table_exists(&metadata_name), previous_metadata.is_some());
        match outcome {
            Outcome::CancelMetadata => drop(registration),
            Outcome::FailMetadata | Outcome::FailCatalog | Outcome::RestoreMetadata | Outcome::FailMetadataPublication => {
                source.release.add_permits(1);
                registration.await.expect_err("failed preparation fails registration");
            }
            Outcome::CancelBookkeeping => {
                let bookkeeping = df.accelerated_tables.write().await;
                source.release.add_permits(1);
                assert!(futures::poll!(registration.as_mut()).is_pending());
                assert!(!df.table_exists(&name));
                assert!(!df.table_exists(&metadata_name));
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
        let actual_metadata = df.get_table(&metadata_name).await;
        let expected_metadata = if matches!(outcome, Outcome::Complete) { Some(&source.provider) } else { previous_metadata.as_ref() };
        match (actual_metadata, expected_metadata) {
            (Some(actual), Some(expected)) => assert!(Arc::ptr_eq(&actual, expected)),
            (None, None) => {},
            _ => panic!("metadata publication must match the installation outcome"),
        }
        df.remove_table(&name).await.expect("drain and remove the generation");
        if !matches!(outcome, Outcome::FailMetadataPublication) {
            df.ctx.deregister_table(metadata_name).expect("remove fixture metadata");
        }
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
async fn change_sink_installation_failed_catalog_removes_metadata() {
    installation(Outcome::FailCatalog).await;
}

#[tokio::test]
async fn change_sink_installation_failed_catalog_restores_metadata() {
    installation(Outcome::RestoreMetadata).await;
}

#[tokio::test]
async fn change_sink_installation_metadata_publication_failure_does_not_publish() {
    installation(Outcome::FailMetadataPublication).await;
}

#[tokio::test]
async fn change_sink_installation_publishes_a_live_owner() {
    installation(Outcome::Complete).await;
}

#[cfg(not(windows))]
#[tokio::test]
async fn change_sink_initialization_refusal_keeps_installed_generation_live() {
    use crate::component::dataset::acceleration::Mode;

    tokio::time::timeout(Duration::from_secs(10), async {
        let runtime = Arc::new(Runtime::builder().build().await);
        let df = runtime.datafusion();
        let app = Arc::new(app::AppBuilder::new("initialization-test").build());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let provider: Arc<dyn TableProvider> = Arc::new(
            data_components::arrow::write::MemTable::try_new(schema, vec![vec![]])
                .expect("Arrow source"),
        );
        let source = Arc::new(MetadataGate {
            provider: Arc::clone(&provider),
            entered: Notify::new(),
            release: Semaphore::new(1),
            fail: false,
        });
        let build = |acceleration| {
            let mut builder = DatasetBuilder::try_new("test:memory".into(), "initialization_test")
                .expect("dataset")
                .with_app(Arc::clone(&app))
                .with_runtime(Arc::clone(&runtime));
            builder.acceleration = Some(acceleration);
            builder.time_column = Some("id".into());
            builder.time_format = Some(TimeFormat::UnixSeconds);
            builder.access = AccessMode::ReadWrite;
            Arc::new(builder.build().expect("dataset configuration"))
        };
        let dataset = build(Acceleration {
            refresh_mode: Some(RefreshMode::Append),
            ..Acceleration::default()
        });
        let name = dataset.name.clone();
        df.register_table(
            dataset,
            Table::Accelerated {
                source: source as Arc<dyn DataConnector>,
                federated_read_table: FederatedTable::new_unchecked(provider),
                accelerated_table: None,
                secrets: runtime.secrets(),
                bootstrap_status: runtime_acceleration::BootstrapStatus::none().into(),
                initial_partition_filters: None,
            },
        )
        .await
        .expect("install the current generation");
        let installed = df.get_table(&name).await.expect("registered table");
        let table = spice_table::find_layer::<crate::accelerated::AcceleratedTable>(
            installed.as_ref(),
            spice_table::LayerWalk::Read,
        )
        .expect("accelerated layer");
        let sink = table.change_sink().expect("bound sink");
        drop(
            sink.reserve()
                .await
                .expect("live generation before refusal"),
        );
        let root = tempfile::tempdir().expect("fixture directory");
        let storage = root.path().join("storage");
        for InitializationCase {
            engine,
            accelerator,
            params,
            expected_error,
        } in invalid_initializations(&storage)
        {
            let replacement = build(Acceleration {
                engine,
                mode: Mode::File,
                params: params
                    .into_iter()
                    .map(|(key, value)| (key.into(), value))
                    .collect(),
                ..Acceleration::default()
            });
            let error = df
                .initialize_accelerator(replacement, accelerator)
                .await
                .err()
                .expect("invalid initialization must refuse");
            assert!(error.to_string().contains(expected_error), "{error}");
            assert!(!storage.exists(), "validation must not create storage");
            let current = df
                .get_table(&name)
                .await
                .expect("registered table survives");
            assert!(Arc::ptr_eq(&installed, &current));
            drop(
                sink.reserve()
                    .await
                    .expect("refusal cannot close the installed sink"),
            );
        }
        df.remove_table(&name)
            .await
            .expect("refusal cannot fence later removal");
        df.ctx
            .deregister_table(TableReference::partial("metadata", name.to_string()))
            .expect("remove fixture metadata");
    })
    .await
    .expect("initialization validation must finish");
}

#[cfg(not(windows))]
struct InitializationCase {
    engine: crate::component::dataset::acceleration::Engine,
    accelerator: Arc<dyn data_accelerator_api::DataAccelerator>,
    params: Vec<(&'static str, String)>,
    expected_error: &'static str,
}

#[cfg(not(windows))]
fn invalid_initializations(storage: &std::path::Path) -> [InitializationCase; 4] {
    use crate::component::dataset::acceleration::Engine;

    let invalid_file = storage
        .join("invalid.extension")
        .to_string_lossy()
        .into_owned();
    [
        InitializationCase {
            engine: Engine::Cayenne,
            accelerator: Arc::new(accelerator_cayenne::CayenneAccelerator::new()),
            params: vec![
                ("cayenne_file_path", storage.to_string_lossy().into_owned()),
                (
                    "cayenne_metadata_dir",
                    storage
                        .join("initialization_test/catalog")
                        .to_string_lossy()
                        .into_owned(),
                ),
            ],
            expected_error: "contains the Cayenne metastore directory",
        },
        InitializationCase {
            engine: Engine::DuckDB,
            accelerator: Arc::new(accelerator_duckdb::DuckDBAccelerator::new()),
            params: vec![("duckdb_file", invalid_file.clone())],
            expected_error: "extension",
        },
        InitializationCase {
            engine: Engine::Sqlite,
            accelerator: Arc::new(accelerator_sqlite::SqliteAccelerator::new()),
            params: vec![("sqlite_file", invalid_file.clone())],
            expected_error: "extension",
        },
        InitializationCase {
            engine: Engine::Turso,
            accelerator: Arc::new(accelerator_turso::TursoAccelerator::new()),
            params: vec![("turso_file", invalid_file)],
            expected_error: "extension",
        },
    ]
}
