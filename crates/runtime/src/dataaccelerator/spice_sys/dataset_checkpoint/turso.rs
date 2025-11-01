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

use std::{sync::Arc, time::SystemTime};

use super::{CHECKPOINT_TABLE_NAME, DatasetCheckpoint, Error, Result};
use crate::dataaccelerator::turso::TursoConnectionPool;
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::arrow::datatypes::SchemaRef;

impl DatasetCheckpoint {
    pub(super) async fn init_turso(pool: &Arc<TursoConnectionPool>) -> Result<()> {
        let conn = pool.connect().await.map_err(Error::external)?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                schema_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        );
        conn.execute(&create_table, ())
            .await
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn migrate_turso(pool: &Arc<TursoConnectionPool>) -> Result<()> {
        let conn = pool.connect().await.map_err(Error::external)?;

        // Check if schema_json column exists
        let query = format!("PRAGMA table_info({CHECKPOINT_TABLE_NAME})");
        let mut rows = conn.query(&query, ()).await.map_err(Error::external)?;

        let mut columns = Vec::new();
        while let Some(row) = rows.next().await.map_err(Error::external)? {
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
            .map_err(Error::external)?;
        }

        Ok(())
    }

    pub(super) async fn exists_turso(&self, pool: &Arc<TursoConnectionPool>) -> Result<bool> {
        let conn = pool.connect().await.map_err(Error::external)?;

        let query = format!("SELECT 1 FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1");
        let mut rows = conn
            .query(&query, turso::params![self.dataset_name.clone()])
            .await
            .map_err(Error::external)?;

        let exists = rows.next().await.map_err(Error::external)?.is_some();
        Ok(exists)
    }

    pub(super) async fn last_checkpoint_time_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
    ) -> Result<Option<SystemTime>> {
        let conn = pool.connect().await.map_err(Error::external)?;

        let query = format!(
            "SELECT updated_at FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1"
        );
        let mut rows = conn
            .query(&query, turso::params![self.dataset_name.clone()])
            .await
            .map_err(Error::external)?;

        if let Some(row) = rows.next().await.map_err(Error::external)? {
            let timestamp_str: String = row.get(0).map_err(Error::external)?;
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
                    .map_err(Error::external)?;
            Ok(Some(checkpoint_time.into()))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn checkpoint_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
        schema: &SchemaRef,
    ) -> Result<()> {
        let conn = pool.connect().await.map_err(Error::external)?;
        let schema_json = Self::serialize_schema(schema)?;

        let upsert = format!(
            "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, schema_json, updated_at)
             VALUES (?1, ?2, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE 
             SET schema_json = ?2, updated_at = CURRENT_TIMESTAMP"
        );
        conn.execute(
            &upsert,
            turso::params![self.dataset_name.clone(), schema_json],
        )
        .await
        .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn get_schema_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
    ) -> Result<Option<SchemaRef>> {
        let conn = pool.connect().await.map_err(Error::external)?;

        let query =
            format!("SELECT schema_json FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?");
        let mut rows = conn
            .query(&query, turso::params![self.dataset_name.clone()])
            .await
            .map_err(Error::external)?;

        if let Some(row) = rows.next().await.map_err(Error::external)? {
            let schema_json: String = row.get(0).map_err(Error::external)?;
            Ok(Some(Self::deserialize_schema(&schema_json)?))
        } else {
            Ok(None)
        }
    }
}
