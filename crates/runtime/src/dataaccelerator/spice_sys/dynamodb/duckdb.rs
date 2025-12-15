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

use super::{DYNAMODB_STREAMS_TABLE_NAME, DynamoDBCheckpointMetadata, DynamoDBSys, Error, Result};
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use std::sync::Arc;

impl DynamoDBSys {
    pub(super) fn upsert_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
        metadata: &DynamoDBCheckpointMetadata,
    ) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {DYNAMODB_STREAMS_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                checkpoint_data TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"
        );
        duckdb_conn
            .execute(&create_table, [])
            .map_err(Error::external)?;

        let upsert = format!(
            "INSERT INTO {DYNAMODB_STREAMS_TABLE_NAME} (dataset_name, checkpoint_data, created_at, updated_at)
             VALUES (?, ?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET
                checkpoint_data = excluded.checkpoint_data,
                updated_at = now()"
        );

        duckdb_conn
            .execute(&upsert, [&self.dataset_name, &metadata.checkpoint_data])
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) fn get_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
    ) -> Option<DynamoDBCheckpointMetadata> {
        let mut db_conn = Arc::clone(pool).connect_sync().ok()?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .ok()?
            .get_underlying_conn_mut();

        let query = format!(
            "SELECT checkpoint_data FROM {DYNAMODB_STREAMS_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).ok()?;
        let mut rows = stmt.query([&self.dataset_name]).ok()?;

        if let Some(row) = rows.next().ok()? {
            let checkpoint_data: String = row.get(0).ok()?;

            Some(DynamoDBCheckpointMetadata { checkpoint_data })
        } else {
            None
        }
    }
}
