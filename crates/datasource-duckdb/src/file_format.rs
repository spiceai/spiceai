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

use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{GetExt, Statistics},
    datasource::{
        file_format::{
            FileFormat, FileFormatFactory, FilePushdownSupport,
            file_compression_type::FileCompressionType,
        },
        physical_plan::{FileScanConfig, FileSinkConfig, FileSource},
    },
    error::DataFusionError,
    physical_expr::LexRequirement,
    physical_plan::{ExecutionPlan, PhysicalExpr},
    prelude::Expr,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::source::DuckDBSource;

static EXTENSION: &str = "duckdb";

#[derive(Debug)]
pub struct DuckDBFormatFactory {}

impl FileFormatFactory for DuckDBFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>, DataFusionError> {
        Ok(self.default())
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(DuckDBFormat::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for DuckDBFormatFactory {
    fn get_ext(&self) -> String {
        EXTENSION.to_string()
    }
}

#[derive(Debug, Default)]
pub struct DuckDBFormat {}

#[async_trait]
impl FileFormat for DuckDBFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String, DataFusionError> {
        Ok(self.get_ext())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<SchemaRef, DataFusionError> {
        todo!()
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics, DataFusionError> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        _conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        todo!()
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        todo!()
    }

    fn supports_filters_pushdown(
        &self,
        _file_schema: &Schema,
        _table_schema: &Schema,
        _filters: &[&Expr],
    ) -> Result<FilePushdownSupport, DataFusionError> {
        Ok(FilePushdownSupport::Supported)
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(DuckDBSource::default())
    }
}
