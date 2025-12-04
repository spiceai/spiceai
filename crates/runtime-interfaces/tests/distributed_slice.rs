/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::any::Any;
use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::datasource::empty::EmptyTable;
use runtime_interfaces::ParameterSpec;
use runtime_interfaces::acceleration::AccelerationSource;
use runtime_interfaces::dataaccelerator::{
    DATA_ACCELERATOR_REGISTRATIONS, DataAccelerator, Engine,
};
use runtime_interfaces::dataconnector::{
    ConnectorParams, DATA_CONNECTOR_REGISTRATIONS, DataConnector, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime_interfaces::datasets::DatasetInfo;
use runtime_interfaces::{register_data_accelerator, register_data_connector};

#[derive(Default)]
struct DummyConnectorFactory;

impl DummyConnectorFactory {
    fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self)
    }
}

#[async_trait]
impl DataConnectorFactory for DummyConnectorFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        _params: ConnectorParams,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(async { Ok(Arc::new(DummyConnector) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        "dummy"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

struct DummyConnector;

impl std::fmt::Debug for DummyConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DummyConnector").finish()
    }
}

#[async_trait]
impl DataConnector for DummyConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _dataset: &dyn DatasetInfo,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Arc::new(EmptyTable::new(Arc::new(Schema::empty()))))
    }
}

register_data_connector!("dummy", DummyConnectorFactory);

struct DummyAccelerator;

impl DummyAccelerator {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DataAccelerator for DummyAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn create_external_table(
        &self,
        _cmd: datafusion::logical_expr::CreateExternalTable,
        _source: Option<&dyn AccelerationSource>,
        _partition_by: Vec<runtime_table_partition::expression::PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Arc::new(EmptyTable::new(Arc::new(Schema::empty()))))
    }

    fn name(&self) -> &'static str {
        "dummy"
    }

    fn prefix(&self) -> &'static str {
        "dummy"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

register_data_accelerator!(Engine::Arrow, DummyAccelerator);

#[test]
fn collects_connectors_and_accelerators() {
    // Accessing the distributed slices ensures linkme collected registrations.
    assert!(
        DATA_CONNECTOR_REGISTRATIONS
            .iter()
            .map(|r| r.name)
            .any(|name| name == "dummy"),
        "dummy connector should be registered"
    );

    assert!(
        DATA_ACCELERATOR_REGISTRATIONS
            .iter()
            .map(|r| r.engine)
            .any(|engine| engine == Engine::Arrow),
        "dummy accelerator should be registered"
    );
}
