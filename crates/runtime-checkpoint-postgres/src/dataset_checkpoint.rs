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

//! The dataset schema / refresh-SQL checkpoint table.

use std::{sync::Arc, time::SystemTime};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use runtime_acceleration::dataset_checkpoint::{
    DatasetCheckpointer, deserialize_schema, serialize_schema,
};
use runtime_checkpoint_api::CheckpointError;

use crate::store_error;

const CHECKPOINT_TABLE_NAME: &str = "spice_sys_dataset_checkpoint";
const SCHEMA_MIGRATION_01_STMT: &str =
    "ALTER TABLE spice_sys_dataset_checkpoint ADD COLUMN IF NOT EXISTS schema_json TEXT";
const REFRESH_SQL_MIGRATION_STMT: &str =
    "ALTER TABLE spice_sys_dataset_checkpoint ADD COLUMN IF NOT EXISTS refresh_sql TEXT";

/// Dataset schema/refresh-SQL checkpoint backed by a `PostgreSQL` accelerator.
pub struct PostgresDatasetCheckpointer {
    pool: Arc<PostgresConnectionPool>,
    dataset_name: String,
}

impl PostgresDatasetCheckpointer {
    /// Opens the checkpoint table for `dataset_name`, creating and migrating it if
    /// needed.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::Store`] when the table cannot be created or migrated.
    pub async fn try_new(
        pool: Arc<PostgresConnectionPool>,
        dataset_name: String,
    ) -> Result<Self, CheckpointError> {
        Self::init_postgres(&pool).await?;
        Self::migrate_postgres(&pool).await?;
        Ok(Self::new(pool, dataset_name))
    }

    /// The checkpointer over an already-initialized table.
    ///
    /// [`Self::try_new`] is the constructor callers want; this one exists for the
    /// migration tests, which have to observe a pre-migration table.
    fn new(pool: Arc<PostgresConnectionPool>, dataset_name: String) -> Self {
        Self { pool, dataset_name }
    }

    async fn init_postgres(pool: &PostgresConnectionPool) -> Result<(), CheckpointError> {
        let conn = pool.connect_direct().await.map_err(store_error)?;

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
            .map_err(store_error)?;

        Ok(())
    }

    async fn exists_inner(&self) -> Result<bool, CheckpointError> {
        let pool = &self.pool;
        let conn = pool.connect_direct().await.map_err(store_error)?;
        let query =
            format!("SELECT 1 FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = $1 LIMIT 1");
        let stmt = conn.conn.prepare(&query).await.map_err(store_error)?;
        let row = conn
            .conn
            .query_opt(&stmt, &[&self.dataset_name])
            .await
            .map_err(store_error)?;
        Ok(row.is_some())
    }

    async fn last_checkpoint_time_inner(&self) -> Result<Option<SystemTime>, CheckpointError> {
        let pool = &self.pool;
        let conn = pool.connect_direct().await.map_err(store_error)?;

        let query = format!(
            "SELECT updated_at FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1"
        );
        let stmt = conn.conn.prepare(&query).await.map_err(store_error)?;
        let rows = conn
            .conn
            .query(&stmt, &[&self.dataset_name])
            .await
            .map_err(store_error)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };

        let checkpoint_time: Option<SystemTime> = row.get(0);
        Ok(checkpoint_time)
    }

    async fn checkpoint_inner(
        &self,
        schema: &SchemaRef,
        refresh_sql: Option<&str>,
    ) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let conn = pool.connect_direct().await.map_err(store_error)?;
        let schema_json = serialize_schema(schema).map_err(store_error)?;

        let upsert = format!(
            "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, updated_at, schema_json, refresh_sql)
             VALUES ($1, CURRENT_TIMESTAMP, $2, $3)
             ON CONFLICT (dataset_name) DO UPDATE
             SET updated_at = CURRENT_TIMESTAMP, schema_json = $2, refresh_sql = $3"
        );
        conn.conn
            .execute(&upsert, &[&self.dataset_name, &schema_json, &refresh_sql])
            .await
            .map_err(store_error)?;

        Ok(())
    }

    async fn migrate_postgres(pool: &PostgresConnectionPool) -> Result<(), CheckpointError> {
        let conn = pool.connect_direct().await.map_err(store_error)?;
        conn.conn
            .execute(SCHEMA_MIGRATION_01_STMT, &[])
            .await
            .map_err(store_error)?;
        conn.conn
            .execute(REFRESH_SQL_MIGRATION_STMT, &[])
            .await
            .map_err(store_error)?;
        Ok(())
    }

    async fn get_schema_inner(&self) -> Result<Option<SchemaRef>, CheckpointError> {
        let pool = &self.pool;
        let conn = pool.connect_direct().await.map_err(store_error)?;
        let query =
            format!("SELECT schema_json FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = $1");
        let row = conn
            .conn
            .query_opt(&query, &[&self.dataset_name])
            .await
            .map_err(store_error)?;

        match row {
            Some(row) => {
                let schema_json: Option<String> = row.get(0);
                match schema_json {
                    Some(json) => Ok(Some(deserialize_schema(&json).map_err(store_error)?)),
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    async fn get_refresh_sql_inner(&self) -> Result<Option<String>, CheckpointError> {
        let pool = &self.pool;
        let conn = pool.connect_direct().await.map_err(store_error)?;
        let query =
            format!("SELECT refresh_sql FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = $1");
        let row = conn
            .conn
            .query_opt(&query, &[&self.dataset_name])
            .await
            .map_err(store_error)?;

        match row {
            Some(row) => Ok(row.get(0)),
            None => Ok(None),
        }
    }

    async fn delete_inner(&self) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let conn = pool.connect_direct().await.map_err(store_error)?;

        let delete = format!("DELETE FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = $1");
        conn.conn
            .execute(&delete, &[&self.dataset_name])
            .await
            .map_err(store_error)?;

        Ok(())
    }
}

#[async_trait]
impl DatasetCheckpointer for PostgresDatasetCheckpointer {
    async fn exists(&self) -> bool {
        self.exists_inner().await.unwrap_or(false)
    }

    async fn checkpoint(
        &self,
        schema: &SchemaRef,
        refresh_sql: Option<&str>,
    ) -> runtime_acceleration::dataset_checkpoint::Result<()> {
        self.checkpoint_inner(schema, refresh_sql)
            .await
            .map_err(Into::into)
    }

    async fn get_schema(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<SchemaRef>> {
        self.get_schema_inner().await.map_err(Into::into)
    }

    async fn last_checkpoint_time(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<SystemTime>> {
        self.last_checkpoint_time_inner().await.map_err(Into::into)
    }

    async fn get_refresh_sql(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<String>> {
        self.get_refresh_sql_inner().await.map_err(Into::into)
    }

    async fn delete(&self) -> runtime_acceleration::dataset_checkpoint::Result<()> {
        self.delete_inner().await.map_err(Into::into)
    }
}
