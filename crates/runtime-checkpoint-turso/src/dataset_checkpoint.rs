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
use chrono::{DateTime, NaiveDateTime, Utc};
use data_components::turso::TursoConnectionPool;
use runtime_acceleration::dataset_checkpoint::{
    DatasetCheckpointer, deserialize_schema, serialize_schema,
};
use runtime_checkpoint_api::CheckpointError;

use crate::store_error;

const CHECKPOINT_TABLE_NAME: &str = "spice_sys_dataset_checkpoint";

/// Dataset schema/refresh-SQL checkpoint backed by a `Turso` accelerator.
pub struct TursoDatasetCheckpointer {
    pool: Arc<TursoConnectionPool>,
    dataset_name: String,
}

impl TursoDatasetCheckpointer {
    /// Opens the checkpoint table for `dataset_name`, creating and migrating it if
    /// needed.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::Store`] when the table cannot be created or migrated.
    pub async fn try_new(
        pool: Arc<TursoConnectionPool>,
        dataset_name: String,
    ) -> Result<Self, CheckpointError> {
        Self::init_turso(&pool).await?;
        Self::migrate_turso(&pool).await?;
        Ok(Self::new(pool, dataset_name))
    }

    /// The checkpointer over an already-initialized table.
    ///
    /// [`Self::try_new`] is the constructor callers want; this one exists for the
    /// migration tests, which have to observe a pre-migration table.
    fn new(pool: Arc<TursoConnectionPool>, dataset_name: String) -> Self {
        Self { pool, dataset_name }
    }

    async fn init_turso(pool: &Arc<TursoConnectionPool>) -> Result<(), CheckpointError> {
        let _schema_guard = pool.acquire_schema_write_lock().await;
        let conn = pool.connect().await.map_err(store_error)?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                schema_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        );
        conn.execute(&create_table, ()).await.map_err(store_error)?;

        Ok(())
    }

    async fn migrate_turso(pool: &Arc<TursoConnectionPool>) -> Result<(), CheckpointError> {
        let _schema_guard = pool.acquire_schema_write_lock().await;
        let conn = pool.connect().await.map_err(store_error)?;

        // Check if schema_json column exists
        let query = format!("PRAGMA table_info({CHECKPOINT_TABLE_NAME})");
        let mut rows = conn.query(&query, ()).await.map_err(store_error)?;

        let mut columns = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_error)? {
            if let Ok(name) = row.get::<String>(1) {
                columns.push(name);
            }
        }

        if !columns.contains(&"schema_json".to_string()) {
            conn.execute(
                &format!("ALTER TABLE {CHECKPOINT_TABLE_NAME} ADD COLUMN schema_json TEXT"),
                (),
            )
            .await
            .map_err(store_error)?;
        }

        if !columns.contains(&"refresh_sql".to_string()) {
            conn.execute(
                &format!("ALTER TABLE {CHECKPOINT_TABLE_NAME} ADD COLUMN refresh_sql TEXT"),
                (),
            )
            .await
            .map_err(store_error)?;
        }

        Ok(())
    }

    async fn exists_inner(&self) -> Result<bool, CheckpointError> {
        let pool = &self.pool;
        let conn = pool.connect().await.map_err(store_error)?;

        let query = format!("SELECT 1 FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1");
        let mut rows = conn
            .query(&query, turso::params![self.dataset_name.clone()])
            .await
            .map_err(store_error)?;

        let exists = rows.next().await.map_err(store_error)?.is_some();
        Ok(exists)
    }

    async fn last_checkpoint_time_inner(&self) -> Result<Option<SystemTime>, CheckpointError> {
        let pool = &self.pool;
        let conn = pool.connect().await.map_err(store_error)?;

        let query = format!(
            "SELECT updated_at FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1"
        );
        let mut rows = conn
            .query(&query, turso::params![self.dataset_name.clone()])
            .await
            .map_err(store_error)?;

        if let Some(row) = rows.next().await.map_err(store_error)? {
            let timestamp_str: String = row.get(0).map_err(store_error)?;
            // SQLite CURRENT_TIMESTAMP returns 'YYYY-MM-DD HH:MM:SS' format
            // Parse using strptime format instead of RFC3339
            let checkpoint_time =
                NaiveDateTime::parse_from_str(&timestamp_str, "%Y-%m-%d %H:%M:%S")
                    .map(|naive_dt| DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc))
                    .or_else(|_| {
                        // Fallback to RFC3339 for backwards compatibility or if format differs
                        DateTime::parse_from_rfc3339(&timestamp_str)
                            .map(|dt| dt.with_timezone(&Utc))
                    })
                    .map_err(store_error)?;
            Ok(Some(checkpoint_time.into()))
        } else {
            Ok(None)
        }
    }

    async fn checkpoint_inner(
        &self,
        schema: &SchemaRef,
        refresh_sql: Option<&str>,
    ) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let _schema_guard = pool.acquire_schema_read_lock().await;
        let conn = pool.connect().await.map_err(store_error)?;
        let schema_json = serialize_schema(schema).map_err(store_error)?;
        let refresh_sql_owned = refresh_sql.map(ToString::to_string);

        let upsert = format!(
            "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, schema_json, refresh_sql, updated_at)
             VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE
             SET schema_json = ?2, refresh_sql = ?3, updated_at = CURRENT_TIMESTAMP"
        );
        conn.execute(
            &upsert,
            turso::params![self.dataset_name.clone(), schema_json, refresh_sql_owned],
        )
        .await
        .map_err(store_error)?;

        Ok(())
    }

    async fn set_schema_inner(&self, schema: &SchemaRef) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let _schema_guard = pool.acquire_schema_read_lock().await;
        let conn = pool.connect().await.map_err(store_error)?;
        let schema_json = serialize_schema(schema).map_err(store_error)?;

        // No upsert: an absent row must stay absent rather than gain a fresh
        // `updated_at`, and `refresh_sql`/`created_at`/`updated_at` are untouched.
        let update =
            format!("UPDATE {CHECKPOINT_TABLE_NAME} SET schema_json = ?2 WHERE dataset_name = ?1");
        conn.execute(
            &update,
            turso::params![self.dataset_name.clone(), schema_json],
        )
        .await
        .map_err(store_error)?;

        Ok(())
    }

    async fn get_schema_inner(&self) -> Result<Option<SchemaRef>, CheckpointError> {
        let pool = &self.pool;
        let conn = pool.connect().await.map_err(store_error)?;

        let query =
            format!("SELECT schema_json FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?");
        let mut rows = conn
            .query(&query, turso::params![self.dataset_name.clone()])
            .await
            .map_err(store_error)?;

        if let Some(row) = rows.next().await.map_err(store_error)? {
            let schema_json: String = row.get(0).map_err(store_error)?;
            Ok(Some(deserialize_schema(&schema_json).map_err(store_error)?))
        } else {
            Ok(None)
        }
    }

    async fn get_refresh_sql_inner(&self) -> Result<Option<String>, CheckpointError> {
        let pool = &self.pool;
        let conn = pool.connect().await.map_err(store_error)?;

        let query =
            format!("SELECT refresh_sql FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?");
        let mut rows = conn
            .query(&query, turso::params![self.dataset_name.clone()])
            .await
            .map_err(store_error)?;

        if let Some(row) = rows.next().await.map_err(store_error)? {
            let refresh_sql: Option<String> = row.get(0).map_err(store_error)?;
            Ok(refresh_sql)
        } else {
            Ok(None)
        }
    }

    async fn delete_inner(&self) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let _schema_guard = pool.acquire_schema_read_lock().await;
        let conn = pool.connect().await.map_err(store_error)?;

        let delete = format!("DELETE FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?");
        conn.execute(&delete, turso::params![self.dataset_name.clone()])
            .await
            .map_err(store_error)?;

        Ok(())
    }
}

#[async_trait]
impl DatasetCheckpointer for TursoDatasetCheckpointer {
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

    async fn set_schema(
        &self,
        schema: &SchemaRef,
    ) -> runtime_acceleration::dataset_checkpoint::Result<()> {
        self.set_schema_inner(schema).await.map_err(Into::into)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    async fn create_in_memory_turso_checkpoint()
    -> (TursoDatasetCheckpointer, Arc<TursoConnectionPool>) {
        let pool = Arc::new(
            TursoConnectionPool::new(":memory:")
                .await
                .expect("to build an in-memory Turso pool"),
        );
        let checkpoint =
            TursoDatasetCheckpointer::try_new(Arc::clone(&pool), "test_dataset".to_string())
                .await
                .expect("to initialize the Turso checkpoint table");
        (checkpoint, pool)
    }

    /// A schema repair must correct the recorded schema without telling the refresh
    /// scheduler the data was just refreshed. Regression test for #13817.
    #[tokio::test]
    async fn set_schema_rewrites_the_schema_without_touching_the_freshness_clock() {
        let (checkpoint, pool) = create_in_memory_turso_checkpoint().await;

        let original = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        // What a repair writes back: the same columns, `name` no longer nullable.
        let repaired = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        checkpoint
            .checkpoint(&original, Some("SELECT 1"))
            .await
            .expect("seed checkpoint");

        backdate_checkpoint_by_seven_days(&pool).await;

        let before = checkpoint
            .last_checkpoint_time()
            .await
            .expect("read checkpoint time")
            .expect("checkpoint time present");

        checkpoint
            .set_schema(&repaired)
            .await
            .expect("schema-only write");

        // Read back through a fresh checkpointer over the same store: acceptance is what
        // the row holds, not what the call returned.
        let reader = TursoDatasetCheckpointer::new(Arc::clone(&pool), "test_dataset".to_string());

        let after = reader
            .last_checkpoint_time()
            .await
            .expect("read checkpoint time")
            .expect("checkpoint time present");
        assert_eq!(
            after, before,
            "a schema-only write must leave the freshness clock alone"
        );

        assert_eq!(
            reader
                .get_schema()
                .await
                .expect("read schema")
                .expect("schema present"),
            repaired,
            "the repaired schema must be the one stored"
        );

        assert_eq!(
            reader.get_refresh_sql().await.expect("read refresh sql"),
            Some("SELECT 1".to_string()),
            "a schema-only write must preserve the stored refresh SQL"
        );
    }

    /// A dataset with no checkpoint must not gain one — a row created here would carry a
    /// fresh `updated_at`, which is the deferral the schema-only write exists to avoid.
    #[tokio::test]
    async fn set_schema_leaves_an_absent_checkpoint_absent() {
        let (checkpoint, _pool) = create_in_memory_turso_checkpoint().await;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        checkpoint
            .set_schema(&schema)
            .await
            .expect("schema-only write on an absent checkpoint");

        assert!(
            !checkpoint.exists().await,
            "a schema-only write must not create a checkpoint row"
        );
        assert!(
            checkpoint
                .last_checkpoint_time()
                .await
                .expect("read checkpoint time")
                .is_none(),
            "an absent checkpoint must not gain a freshness timestamp"
        );
    }

    /// Backdates the checkpoint's recorded refresh by seven days, as a dataset
    /// bootstrapping from a legacy snapshot would be.
    async fn backdate_checkpoint_by_seven_days(pool: &Arc<TursoConnectionPool>) {
        let conn = pool.connect().await.expect("turso connection");
        conn.execute(
            &format!("UPDATE {CHECKPOINT_TABLE_NAME} SET updated_at = datetime('now', '-7 days')"),
            (),
        )
        .await
        .expect("backdate updated_at");
    }
}
