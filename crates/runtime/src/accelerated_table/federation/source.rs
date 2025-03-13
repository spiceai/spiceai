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

use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{datasource::TableType, logical_expr::TableSource};
use datafusion_federation::{
    table_reference::MultiPartTableReference, FederatedTableSource, FederationProvider,
};

use super::AcceleratedTableFederationProvider;

pub struct AcceleratedTableFederatedTableSource {
    provider: Arc<AcceleratedTableFederationProvider>,
    schema: SchemaRef,
    remote_table_name: Option<MultiPartTableReference>,
}

impl AcceleratedTableFederatedTableSource {
    pub fn new_with_schema(
        provider: Arc<AcceleratedTableFederationProvider>,
        schema: SchemaRef,
        remote_table_name: Option<MultiPartTableReference>,
    ) -> Self {
        Self {
            provider,
            schema,
            remote_table_name,
        }
    }
}

impl FederatedTableSource for AcceleratedTableFederatedTableSource {
    fn remote_table_name(&self) -> Option<MultiPartTableReference> {
        self.remote_table_name.clone()
    }

    fn federation_provider(&self) -> Arc<dyn FederationProvider> {
        Arc::clone(&self.provider) as Arc<dyn FederationProvider>
    }
}

impl TableSource for AcceleratedTableFederatedTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
}
