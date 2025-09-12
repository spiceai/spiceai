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

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::{any::Any, sync::Weak};

use arrow::datatypes::{Schema, SchemaRef};

use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::iceberg::provider::IcebergCatalogProvider;
use datafusion::common::GetExt;
use datafusion::execution::SessionState;
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::sql::TableReference;
use datafusion::{
    catalog::Session,
    common::{Statistics, not_impl_err},
    datasource::{
        file_format::{FileFormat, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileSinkConfig, FileSource},
    },
    error::Result,
    physical_expr::LexRequirement,
    physical_plan::{ExecutionPlan, PhysicalExpr},
};
use datafusion_datasource::file_format::FileFormatFactory;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use iceberg_datafusion::physical_plan::{IcebergCommitExec, IcebergWriteExec};
use object_store::{ObjectMeta, ObjectStore};
use parking_lot::RwLock;
use url::{Host, Url};

/// New line delimited JSON `FileFormat` implementation.
#[derive(Debug, Default, Clone)]
pub struct IcebergFileFormat {
    // The DataFusion catalog to retrieve the Iceberg catalog from.
    state: Weak<RwLock<SessionState>>,

    options: HashMap<String, String>,
}

impl GetExt for IcebergFileFormat {
    fn get_ext(&self) -> String {
        "iceberg".to_string()
    }
}

#[async_trait]
impl FileFormatFactory for IcebergFileFormat {
    /// Initialize a [FileFormat] and configure based on session and command level options
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(IcebergFileFormat {
            state: self.state.clone(),
            options: format_options.clone(),
        }))
    }

    /// Initialize a [FileFormat] with all options set to default values
    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(self.clone())
    }

    /// Returns the table source as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl IcebergFileFormat {
    #[must_use]
    pub fn new(state: Weak<RwLock<SessionState>>) -> Self {
        Self {
            state,
            options: HashMap::default(),
        }
    }
}

#[async_trait]
impl FileFormat for IcebergFileFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "parquet".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        Ok("parquet".to_string())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        not_impl_err!("not yet implemented")
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        _conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("not yet implemented")
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_url: &Url = conf.table_paths[0].as_ref();
        let Host::Domain(table_ref) = input_url.host().expect("should have host") else {
            panic!("only domain hosts are supported for iceberg table refs");
        };
        let table_ref = TableReference::parse_str(table_ref);

        let catalog_name = table_ref.catalog().expect("should have catalog");
        let schema_name = table_ref.schema().expect("should have schema");
        let table_name = table_ref.table();
        let incoming_schema = input.schema();

        let state = self.state.upgrade().expect("state should not be dropped");

        let catalog = {
            let session_state = state.read();

            let catalog = session_state
                .catalog_list()
                .catalog(catalog_name)
                .expect("should get catalog");
            drop(session_state);
            catalog
        };

        let iceberg_catalog_provider = catalog
            .as_ref()
            .as_any()
            .downcast_ref::<IcebergCatalogProvider>()
            .expect("should be iceberg catalog provider");
        let iceberg_catalog = Arc::clone(&iceberg_catalog_provider.client);

        let schema_ns = NamespaceIdent::new(schema_name.to_string());
        if !iceberg_catalog
            .namespace_exists(&schema_ns)
            .await
            .expect("failed to check namespace")
        {
            tracing::debug!("creating namespace: {:?}", schema_ns);
            let _ = iceberg_catalog
                .create_namespace(&schema_ns, HashMap::default())
                .await
                .expect("failed to create namespace");
        }

        let location = self.options.get("format.location").map(|v| v.to_string());
        println!("location: {location:?}");

        let table_ident = TableIdent::new(schema_ns.clone(), table_name.to_string());
        let table = match iceberg_catalog.load_table(&table_ident).await {
            Ok(table) => table,
            Err(e) => {
                tracing::debug!("creating table: {e:?}");
                // Assume the table doesn't exist - this needs to be revised before merging to trunk
                let incoming_schema = assign_field_ids(&incoming_schema);
                let iceberg_schema = arrow_schema_to_schema(&incoming_schema)
                    .expect("failed to convert arrow schema to iceberg schema");
                let table_creation = TableCreation::builder()
                    .schema(iceberg_schema)
                    .name(table_name.to_string())
                    .location_opt(location)
                    .build();
                iceberg_catalog
                    .create_table(&schema_ns, table_creation)
                    .await
                    .expect("failed to create table")
            }
        };

        iceberg_catalog_provider
            .refresh()
            .await
            .expect("failed to refresh catalog");

        let write_plan = Arc::new(IcebergWriteExec::new(
            table.clone(),
            input,
            Arc::clone(&incoming_schema),
        ));

        // Merge the outputs of write_plan into one so we can commit all files together
        let coalesce_partitions = Arc::new(CoalescePartitionsExec::new(write_plan));

        Ok(Arc::new(IcebergCommitExec::new(
            table,
            iceberg_catalog,
            coalesce_partitions,
            incoming_schema,
        )))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        unimplemented!("file_source is not implemented for IcebergFileFormat")
    }
}

fn assign_field_ids(schema: &Schema) -> Schema {
    let mut fields = vec![];
    for (i, field) in schema.fields.iter().enumerate() {
        let field = Arc::unwrap_or_clone(Arc::clone(field));
        fields.push(field.with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            format!("{i}"),
        )])));
    }
    Schema::new(fields)
}
