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

use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use std::{any::Any, sync::Weak};

use arrow::datatypes::SchemaRef;

use async_trait::async_trait;
use data_components::iceberg::provider::IcebergCatalogProvider;
use datafusion::execution::SessionState;
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
use object_store::{ObjectMeta, ObjectStore};
use snafu::prelude::*;
use url::{Host, Url};

/// New line delimited JSON `FileFormat` implementation.
#[derive(Debug, Default)]
pub struct IcebergFileFormat {
    // The DataFusion catalog to retrieve the Iceberg catalog from.
    state: Weak<RwLock<SessionState>>,
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
        let session_state = state.read().expect("should get session state");

        let catalog = session_state
            .catalog_list()
            .catalog(catalog_name)
            .expect("should get catalog");
        let iceberg_catalog_provider = catalog
            .as_ref()
            .as_any()
            .downcast_ref::<IcebergCatalogProvider>()
            .expect("should be iceberg catalog provider");
        let iceberg_catalog = Arc::clone(&iceberg_catalog_provider.client);

        let write_plan = Arc::new(IcebergWriteExec::new(
            self.table.clone(),
            input,
            self.schema.clone(),
        ));

        // Merge the outputs of write_plan into one so we can commit all files together
        let coalesce_partitions = Arc::new(CoalescePartitionsExec::new(write_plan));

        Ok(Arc::new(IcebergCommitExec::new(
            self.table.clone(),
            catalog,
            coalesce_partitions,
            self.schema.clone(),
        )))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        unimplemented!("file_source is not implemented for IcebergFileFormat")
    }
}
