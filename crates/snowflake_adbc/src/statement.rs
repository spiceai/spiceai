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

use crate::client::SnowflakeClient;
use crate::error::{Error, Result};
use crate::types::QueryResponseData;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use futures::stream::{BoxStream, Stream, StreamExt};
use std::io::Cursor;
use std::sync::Arc;

/// A statement for query execution using Snowflake's Arrow protocol.
///
/// Statements execute queries and return results as Arrow IPC batches over HTTP.
#[derive(Debug)]
pub struct Statement {
    client: Arc<SnowflakeClient>,
    sql_query: Option<String>,
    result_data: Option<QueryResponseData>,
    schema: Option<Arc<Schema>>,
}

impl Statement {
    pub(crate) fn new(client: Arc<SnowflakeClient>) -> Self {
        Self {
            client,
            sql_query: None,
            result_data: None,
            schema: None,
        }
    }

    /// Set the SQL query to execute.
    pub fn set_sql_query(&mut self, sql: &str) {
        self.sql_query = Some(sql.to_string());
    }

    /// Execute the statement and request Arrow-formatted results.
    pub async fn execute(&mut self) -> Result<()> {
        let sql = self.sql_query.as_ref().ok_or_else(|| Error::InvalidState {
            message: "No SQL query set".to_string(),
        })?;

        // Execute query with Arrow format
        let response = self.client.execute_query_arrow(sql).await?;
        
        // If we got inline Arrow data (rowSetBase64), decode the schema from it
        if let Some(ref row_set_base64) = response.data.row_set_base64 {
            let arrow_data = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                row_set_base64,
            )
            .map_err(|e| Error::InvalidState {
                message: format!("Failed to decode base64 Arrow data: {}", e),
            })?;

            let cursor = Cursor::new(arrow_data);
            let reader = StreamReader::try_new(cursor, None)?;
            self.schema = Some(Arc::new(reader.schema().as_ref().clone()));
        } else if let Some(ref chunks) = response.data.chunks {
            // If no inline data but we have chunks, download first chunk to get schema
            if let Some(first_chunk) = chunks.first() {
                let arrow_bytes = self.client.download_arrow_chunk(first_chunk).await?;
                let cursor = Cursor::new(arrow_bytes.to_vec());
                let reader = StreamReader::try_new(cursor, None)?;
                self.schema = Some(Arc::new(reader.schema().as_ref().clone()));
            }
        }

        if self.schema.is_none() {
            return Err(Error::InvalidState {
                message: format!(
                    "No schema available from Snowflake response for query. Query result format was '{}'. This may indicate the session parameter GO_QUERY_RESULT_FORMAT is not set to 'arrow'.",
                    response.data.query_result_format
                ),
            });
        }

        self.result_data = Some(response.data);
        Ok(())
    }

    /// Get the result schema.
    pub fn schema(&self) -> Option<Arc<Schema>> {
        self.schema.clone()
    }

    /// Get a stream of Arrow record batches.
    /// 
    /// Downloads Arrow IPC chunks from Snowflake and streams the decoded batches.
    pub async fn into_record_batch_stream(
        self,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let result_data = self.result_data.ok_or_else(|| Error::InvalidState {
            message: "Statement has not been executed".to_string(),
        })?;

        let client = self.client.clone();
        let mut all_batches = Vec::new();

        // First, handle inline data if present
        if let Some(row_set_base64) = result_data.row_set_base64 {
            let arrow_data = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                &row_set_base64,
            )
            .map_err(|e| Error::InvalidState {
                message: format!("Failed to decode base64 Arrow data: {}", e),
            })?;

            let cursor = Cursor::new(arrow_data);
            let mut reader = StreamReader::try_new(cursor, None)?;

            while let Some(batch_result) = reader.next() {
                all_batches.push(batch_result?);
            }
        }

        // Then download and decode Arrow chunks
        if let Some(chunks) = result_data.chunks {
            for chunk in &chunks {
                let arrow_bytes = client.download_arrow_chunk(chunk).await?;
                
                let cursor = Cursor::new(arrow_bytes.to_vec());
                let mut reader = StreamReader::try_new(cursor, None)?;

                while let Some(batch_result) = reader.next() {
                    all_batches.push(batch_result?);
                }
            }
        }

        Ok(futures::stream::iter(all_batches.into_iter().map(Ok)).boxed())
    }

    /// Get the total number of rows (if available without fetching all data).
    pub fn row_count(&self) -> Option<i64> {
        self.result_data.as_ref().map(|d| d.total)
    }
}
