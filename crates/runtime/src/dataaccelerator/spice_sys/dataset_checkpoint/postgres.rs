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

use std::time::SystemTime;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;

use super::{CHECKPOINT_TABLE_NAME, DatasetCheckpoint, Error, Result, SCHEMA_MIGRATION_01_STMT};

impl DatasetCheckpoint {
    pub(super) async fn init_postgres(pool: &PostgresConnectionPool) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        );
        conn.conn
            .execute(&create_table, &[])
            .await
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn exists_postgres(&self, pool: &PostgresConnectionPool) -> Result<bool> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;
        let query =
            format!("SELECT 1 FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = $1 LIMIT 1");
        let stmt = conn.conn.prepare(&query).await.map_err(Error::external)?;
        let row = conn
            .conn
            .query_opt(&stmt, &[&self.dataset_name])
            .await
            .map_err(Error::external)?;
        Ok(row.is_some())
    }

    pub(super) async fn last_checkpoint_time_postgres(
        &self,
        pool: &PostgresConnectionPool,
    ) -> Result<Option<SystemTime>> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;

        let query = format!(
            "SELECT updated_at FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1"
        );
        let stmt = conn.conn.prepare(&query).await.map_err(Error::external)?;
        let rows = conn
            .conn
            .query(&stmt, &[&self.dataset_name])
            .await
            .map_err(Error::external)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };

        let checkpoint_time: Option<SystemTime> = row.get(0);
        Ok(checkpoint_time)
    }

    pub(super) async fn checkpoint_postgres(
        &self,
        pool: &PostgresConnectionPool,
        schema: &SchemaRef,
        refresh_sql: Option<&str>,
    ) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;
        let schema_json = Self::serialize_schema(schema)?;

        let upsert = format!(
            "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, updated_at, schema_json, refresh_sql)
             VALUES ($1, CURRENT_TIMESTAMP, $2, $3)
             ON CONFLICT (dataset_name) DO UPDATE
             SET updated_at = CURRENT_TIMESTAMP, schema_json = $2, refresh_sql = $3"
        );
        conn.conn
            .execute(&upsert, &[&self.dataset_name, &schema_json, &refresh_sql])
            .await
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn migrate_postgres(pool: &PostgresConnectionPool) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;
        conn.conn
            .execute(SCHEMA_MIGRATION_01_STMT, &[])
            .await
            .map_err(Error::external)?;
        conn.conn
            .execute(super::REFRESH_SQL_MIGRATION_STMT, &[])
            .await
            .map_err(Error::external)?;
        Ok(())
    }

    pub(super) async fn get_schema_postgres(
        &self,
        pool: &PostgresConnectionPool,
    ) -> Result<Option<SchemaRef>> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;
        let query =
            format!("SELECT schema_json FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = $1");
        let row = conn
            .conn
            .query_opt(&query, &[&self.dataset_name])
            .await
            .map_err(Error::external)?;

        match row {
            Some(row) => {
                let schema_json: Option<String> = row.get(0);
                match schema_json {
                    Some(json) => Ok(Some(Self::deserialize_schema(&json)?)),
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    pub(super) async fn get_refresh_sql_postgres(
        &self,
        pool: &PostgresConnectionPool,
    ) -> Result<Option<String>> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;
        let query =
            format!("SELECT refresh_sql FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = $1");
        let row = conn
            .conn
            .query_opt(&query, &[&self.dataset_name])
            .await
            .map_err(Error::external)?;

        match row {
            Some(row) => Ok(row.get(0)),
            None => Ok(None),
        }
    }

    pub(super) async fn delete_postgres(&self, pool: &PostgresConnectionPool) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;

        let delete = format!("DELETE FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = $1");
        conn.conn
            .execute(&delete, &[&self.dataset_name])
            .await
            .map_err(Error::external)?;

        Ok(())
    }
}
