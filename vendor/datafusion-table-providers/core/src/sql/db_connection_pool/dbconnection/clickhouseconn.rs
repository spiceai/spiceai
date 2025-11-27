use std::io::Cursor;
use std::{any::Any, sync::Arc};

use arrow::array::RecordBatch;
use arrow_ipc::reader::{StreamDecoder, StreamReader};
use async_trait::async_trait;
use clickhouse::{Client, Row};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::{execution::SendableRecordBatchStream, sql::TableReference};
use regex::Regex;
use serde::Deserialize;
use snafu::ResultExt;

use super::{AsyncDbConnection, DbConnection, Error, SyncDbConnection};

impl DbConnection<Client, ()> for Client {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_sync(&self) -> Option<&dyn SyncDbConnection<Client, ()>> {
        None
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Client, ()>> {
        Some(self)
    }
}

#[async_trait]
impl AsyncDbConnection<Client, ()> for Client {
    fn new(conn: Client) -> Self
    where
        Self: Sized,
    {
        conn
    }

    async fn tables(&self, schema: &str) -> Result<Vec<String>, Error> {
        #[derive(Row, Deserialize)]
        struct Row {
            name: String,
        }

        let tables: Vec<Row> = self
            .query("SELECT name FROM system.tables WHERE database = ?")
            .bind(schema)
            .fetch_all()
            .await
            .boxed()
            .context(super::UnableToGetTablesSnafu)?;

        Ok(tables.into_iter().map(|x| x.name).collect())
    }

    async fn schemas(&self) -> Result<Vec<String>, Error> {
        #[derive(Row, Deserialize)]
        struct Row {
            name: String,
        }
        let tables: Vec<Row> = self
            .query("SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')")
            .fetch_all()
            .await
            .boxed()
            .context(super::UnableToGetSchemasSnafu)?;

        Ok(tables.into_iter().map(|x| x.name).collect())
    }

    /// Get the schema for a table reference.
    ///
    /// # Arguments
    ///
    /// * `table_reference` - The table reference.
    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, Error> {
        #[derive(Row, Deserialize)]
        struct CatalogRow {
            db: String,
        }

        let database = match table_reference.schema() {
            Some(db) => db.to_string(),
            None => {
                let row: CatalogRow = self
                    .query("SELECT currentDatabase() AS db")
                    .fetch_one()
                    .await
                    .boxed()
                    .context(super::UnableToGetSchemaSnafu)?;
                row.db
            }
        };

        #[derive(Row, Deserialize)]
        struct TableInfoRow {
            engine: String,
            as_select: String,
        }

        let table_info: TableInfoRow = self
            .query("SELECT engine, as_select FROM system.tables WHERE database = ? AND name = ?")
            .bind(&database)
            .bind(table_reference.table())
            .fetch_one()
            .await
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let is_view = matches!(
            table_info.engine.to_uppercase().as_str(),
            "VIEW" | "MATERIALIZEDVIEW"
        );

        let statement = if is_view {
            let view_query = table_info.as_select;
            format!(
                "SELECT * FROM ({}) LIMIT 0",
                replace_clickhouse_ddl_parameters(&view_query)
            )
        } else {
            let table_ref = TableReference::partial(database, table_reference.table());
            format!("SELECT * FROM {} LIMIT 0", table_ref.to_quoted_string())
        };

        let mut bytes = self
            .query(&statement)
            .fetch_bytes("ArrowStream")
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let reader = bytes
            .collect()
            .await
            .boxed()
            .and_then(|bytes| StreamReader::try_new(Cursor::new(bytes), None).boxed())
            .context(super::UnableToGetSchemaSnafu)?;

        return Ok(reader.schema());
    }

    /// Query the database with the given SQL statement and parameters, returning a `Result` of `SendableRecordBatchStream`.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement.
    /// * `params` - The parameters for the SQL statement.
    /// * `projected_schema` - The Projected schema for the query.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    async fn query_arrow(
        &self,
        sql: &str,
        _params: &[()],
        projected_schema: Option<SchemaRef>,
    ) -> super::Result<SendableRecordBatchStream> {
        let query = self.query(sql);

        let mut bytes_stream = query
            .fetch_bytes("ArrowStream")
            .boxed()
            .context(super::UnableToQueryArrowSnafu)?;

        let mut first_batch: Option<RecordBatch> = None;
        let mut decoder = StreamDecoder::new();

        // fetch till first set of records
        while let Some(buf) = bytes_stream.next().await? {
            if let Some(batch) = decoder.decode(&mut buf.into())? {
                first_batch = Some(batch);
                break;
            }
        }

        if let Some(first_batch) = first_batch {
            let schema = first_batch.schema();
            let stream = async_stream::stream! {
                yield Ok(first_batch);
                while let Some(buf) = bytes_stream
                    .next()
                    .await
                    .map_err(|er| arrow::error::ArrowError::ExternalError(Box::new(er)))?
                {
                    if let Some(batch) = decoder.decode(&mut buf.into())? {
                        yield Ok(batch);
                    }
                }
            };
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        } else if let Some(schema) = projected_schema {
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                EmptyRecordBatchStream::new(schema),
            )))
        } else {
            let schema: Arc<Schema> = Schema::empty().into();
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                EmptyRecordBatchStream::new(schema),
            )))
        }
    }

    /// Execute the given SQL statement with parameters, returning the number of affected rows.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement.
    /// * `params` - The parameters for the SQL statement.
    async fn execute(&self, sql: &str, params: &[()]) -> super::Result<u64> {
        let mut query = self.query(sql);

        for param in params {
            query = query.bind(param);
        }

        query
            .execute()
            .await
            .boxed()
            .context(super::UnableToQueryArrowSnafu)?;

        Ok(0)
    }
}

pub fn replace_clickhouse_ddl_parameters(ddl_query: &str) -> String {
    // Regex to find parameters in the format {parameter_name:DataType}
    let param_pattern = Regex::new(r"\{(\w+?):(\w+?)\}").unwrap();

    let modified_query = param_pattern.replace_all(ddl_query, |caps: &regex::Captures| {
        // match against the datatype
        let data_type = caps.get(2).map_or("", |m| m.as_str());
        match data_type.to_lowercase().as_str() {
            "string" => "''".to_string(),
            "uint8" | "uint16" | "uint32" | "uint64" | "int8" | "int16" | "int32" | "int64" => {
                "0".to_string()
            }
            "float32" | "float64" => "0.0".to_string(),
            "date" => "'2000-01-01'".to_string(),
            "datetime" => "'2000-01-01 00:00:00'".to_string(),
            "bool" => "false".to_string(),
            _ => "''".to_string(),
        }
    });

    modified_query.into_owned()
}
