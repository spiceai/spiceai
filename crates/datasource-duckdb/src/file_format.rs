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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics},
    datasource::{
        file_format::{
            FileFormat, FileFormatFactory, FilePushdownSupport,
            file_compression_type::FileCompressionType,
        },
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource},
    },
    error::DataFusionError,
    physical_expr::LexRequirement,
    physical_plan::{ExecutionPlan, PhysicalExpr},
    prelude::Expr,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::{EXTENSION, source::DuckDBSource};

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
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef, DataFusionError> {
        let Some(object) = objects.first() else {
            return Err(DataFusionError::Execution(
                "No DuckDB files provided for schema inference".to_string(),
            ));
        };

        let file_path = object.location.to_string();
        tracing::debug!("Inferring schema from DuckDB file: {:?}", object.location);

        let conn = duckdb::Connection::open(&file_path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to open DuckDB connection: {e}"))
        })?;

        let mut stmt = conn
            .prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
            .map_err(|e| DataFusionError::Execution(format!("Failed to query all tables: {e}")))?;

        let mut rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| DataFusionError::Execution(format!("Failed to map query results: {e}")))?;

        let table_name = rows
            .next()
            .ok_or_else(|| {
                DataFusionError::Execution("No tables found in DuckDB database".to_string())
            })?
            .map_err(|e| DataFusionError::Execution(format!("Failed to get table name: {e}")))?;

        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info('{table_name}')"))
            .map_err(|e| DataFusionError::Execution(format!("Failed to query table info: {e}")))?;

        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                ))
            })
            .map_err(|e| DataFusionError::Execution(format!("Failed to map query results: {e}")))?;

        let mut fields = Vec::new();
        for row_result in rows {
            let (name, duckdb_type, not_null) = row_result
                .map_err(|e| DataFusionError::Execution(format!("Failed to get row: {e}")))?;
            let data_type = match duckdb_type.as_str() {
                "BOOLEAN" => DataType::Boolean,
                "TINYINT" => DataType::Int8,
                "SMALLINT" => DataType::Int16,
                "INTEGER" => DataType::Int32,
                "BIGINT" => DataType::Int64,
                "UTINYINT" => DataType::UInt8,
                "USMALLINT" => DataType::UInt16,
                "UINTEGER" => DataType::UInt32,
                "UBIGINT" => DataType::UInt64,
                "FLOAT" => DataType::Float32,
                "DOUBLE" => DataType::Float64,
                "VARCHAR" => DataType::Utf8,
                "BLOB" => DataType::Binary,
                "DATE" => DataType::Date32,
                "TIME" => DataType::Time64(TimeUnit::Microsecond),
                "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, None),
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Unsupported DuckDB type: {duckdb_type}"
                    )));
                }
            };
            fields.push(Field::new(name, data_type, !not_null));
        }

        Ok(Arc::new(Schema::new(fields)))
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
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let conf_builder = FileScanConfigBuilder::from(conf);

        let file_source = Arc::new(DuckDBSource::default());

        let data_source = conf_builder.with_source(file_source).build();

        Ok(DataSourceExec::from_data_source(data_source))
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    use datafusion::prelude::SessionContext;
    use object_store::path::Path;

    #[tokio::test]
    async fn test_infer_schema() {
        let file_path = PathBuf::from("infer_schema.duckdb");

        let conn = duckdb::Connection::open(&file_path).expect("Failed to open DuckDB connection");
        conn.execute_batch(
            "CREATE TABLE mytable (
                id INTEGER NOT NULL,
                name VARCHAR,
                is_active BOOLEAN,
                value DOUBLE
            );",
        )
        .expect("Failed to create table");

        let format = DuckDBFormat::default();
        let session_ctx = SessionContext::new();
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let object_meta = ObjectMeta {
            location: Path::from(file_path.display().to_string()),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        };

        let inferred_schema = format
            .infer_schema(&session_ctx.state(), &object_store, &[object_meta])
            .await
            .expect("Failed to infer schema");

        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("is_active", DataType::Boolean, true),
            Field::new("value", DataType::Float64, true),
        ]));

        assert_eq!(inferred_schema, expected_schema);

        std::fs::remove_file(file_path).expect("failed to clean up");
    }
}
