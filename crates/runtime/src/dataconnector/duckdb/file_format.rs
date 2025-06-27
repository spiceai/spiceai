/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-20.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Statistics;
use datafusion::datasource::file_format::FileFormat;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::FileScanConfig;
use object_store::{ObjectMeta, ObjectStore};

/// A `FileFormat` for DuckDB files.
#[derive(Debug, Clone)]
pub struct DuckDBFileFormat {
    // Add any configuration options for your DuckDB file format here.
    // For example, if you need to specify a table name within the DuckDB file.
    table_name: String,
}

impl DuckDBFileFormat {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }
}

#[async_trait]
impl FileFormat for DuckDBFileFormat {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    /// Returns the extension for this FileFormat, e.g. "file.csv" -> csv
    fn get_ext(&self) -> String {
        todo!()
    }

    /// Returns the extension for this FileFormat when compressed, e.g. "file.csv.gz" -> csv
    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        todo!()
    }

    /// Infer the common schema of the provided objects. The objects will usually
    /// be analysed up to a given number of records or files (as specified in the
    /// format config) then give the estimated common schema. This might fail if
    /// the files have schemas that cannot be merged.
    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        todo!()
    }

    /// Infer the statistics for the provided object. The cost and accuracy of the
    /// estimated statistics might vary greatly between file formats.
    ///
    /// `table_schema` is the (combined) schema of the overall table
    /// and may be a superset of the schema contained in this file.
    ///
    /// TODO: should the file source return statistics for only columns referred to in the table schema?
    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        todo!()
    }

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    /// Take a list of files and the configuration to convert it to the
    /// appropriate writer executor according to this file format.
    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    /// Check if the specified file format has support for pushing down the provided filters within
    /// the given schemas. Added initially to support the Parquet file format's ability to do this.
    fn supports_filters_pushdown(
        &self,
        _file_schema: &Schema,
        _table_schema: &Schema,
        _filters: &[&Expr],
    ) -> Result<FilePushdownSupport> {
        Ok(FilePushdownSupport::NoSupport)
    }

    /// Return the related FileSource such as `CsvSource`, `JsonSource`, etc.
    fn file_source(&self) -> Arc<dyn FileSource> {
        todo!()
    }
}
