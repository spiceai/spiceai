/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The `MySQL` binlog resume-position table.

use std::sync::Arc;

use async_trait::async_trait;
use data_components::turso::TursoConnectionPool;
use runtime_checkpoint_api::{
    CheckpointError,
    mysql_binlog::{MySqlBinlogCheckpoint, MySqlBinlogStore, position_from_i64, position_to_i64},
};

use crate::store_error;

const MYSQL_BINLOG_TABLE_NAME: &str = "spice_sys_mysql_binlog";

fn migrate_columns() -> [String; 2] {
    [
        format!("ALTER TABLE {MYSQL_BINLOG_TABLE_NAME} ADD COLUMN gtid_executed TEXT"),
        format!("ALTER TABLE {MYSQL_BINLOG_TABLE_NAME} ADD COLUMN cursor_type TEXT"),
    ]
}

/// `MySQL` binlog position store backed by a `Turso` accelerator.
pub struct TursoMySqlBinlogStore {
    pool: Arc<TursoConnectionPool>,
    dataset_name: String,
}

impl TursoMySqlBinlogStore {
    #[must_use]
    pub fn new(pool: Arc<TursoConnectionPool>, dataset_name: String) -> Self {
        Self { pool, dataset_name }
    }
}

#[async_trait]
impl MySqlBinlogStore for TursoMySqlBinlogStore {
    async fn upsert(&self, checkpoint: &MySqlBinlogCheckpoint) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let dataset_name = self.dataset_name.clone();
        let binlog_file = checkpoint.binlog_file.clone();
        let binlog_pos = position_to_i64(checkpoint.binlog_pos);
        let schema_json = checkpoint.schema_json.clone();
        let gtid_executed = checkpoint.gtid_executed.clone();
        let cursor_type = checkpoint.cursor_type.clone();

        let conn = pool.connect().await.map_err(store_error)?;

        {
            let _schema_guard = pool.acquire_schema_write_lock().await;
            let create_table = format!(
                "CREATE TABLE IF NOT EXISTS {MYSQL_BINLOG_TABLE_NAME} (
                    dataset_name TEXT PRIMARY KEY,
                    binlog_file TEXT NOT NULL,
                    binlog_pos BIGINT NOT NULL,
                    schema_json TEXT,
                    gtid_executed TEXT,
                    cursor_type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"
            );
            conn.execute(&create_table, ()).await.map_err(store_error)?;
            // Migrate tables created before these columns existed. No
            // `IF NOT EXISTS` for ADD COLUMN in SQLite/Turso — a duplicate
            // column (already migrated) errors and is ignored.
            for migration in migrate_columns() {
                let _ = conn.execute(&migration, ()).await;
            }
        }

        let _schema_guard = pool.acquire_schema_read_lock().await;
        let upsert = format!(
            "INSERT INTO {MYSQL_BINLOG_TABLE_NAME}
             (dataset_name, binlog_file, binlog_pos, schema_json, gtid_executed, cursor_type, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                binlog_file = ?2,
                binlog_pos = ?3,
                schema_json = ?4,
                gtid_executed = ?5,
                cursor_type = ?6,
                updated_at = CURRENT_TIMESTAMP"
        );

        conn.execute(
            &upsert,
            turso::params![
                dataset_name,
                binlog_file,
                binlog_pos,
                schema_json,
                gtid_executed,
                cursor_type
            ],
        )
        .await
        .map_err(store_error)?;

        Ok(())
    }

    async fn get(&self) -> Option<MySqlBinlogCheckpoint> {
        let pool = &self.pool;
        let dataset_name = self.dataset_name.clone();
        let conn = pool.connect().await.ok()?;
        // Ensure the added columns exist so the SELECT doesn't fail on a table
        // created before they were added. Idempotent; ignored otherwise.
        {
            let _schema_guard = pool.acquire_schema_write_lock().await;
            for migration in migrate_columns() {
                let _ = conn.execute(&migration, ()).await;
            }
        }
        let query = format!(
            "SELECT binlog_file, binlog_pos, schema_json, gtid_executed, cursor_type, strftime('%s', updated_at) FROM {MYSQL_BINLOG_TABLE_NAME} WHERE dataset_name = ?"
        );

        let mut rows = conn
            .query(&query, turso::params![dataset_name])
            .await
            .ok()?;
        let row = rows.next().await.ok()??;

        let binlog_file = row.get::<String>(0).ok()?;
        let binlog_pos = row.get::<i64>(1).ok()?;
        let schema_json: Option<String> = row.get::<String>(2).ok();
        let gtid_executed: Option<String> = row.get::<String>(3).ok();
        let cursor_type: Option<String> = row.get::<String>(4).ok();
        let updated_at_epoch: Option<i64> = row.get::<i64>(5).ok();
        let updated_at = updated_at_epoch.and_then(|epoch| {
            u64::try_from(epoch)
                .ok()
                .and_then(|e| std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(e)))
        });

        Some(MySqlBinlogCheckpoint {
            binlog_file,
            binlog_pos: position_from_i64(binlog_pos),
            schema_json,
            gtid_executed,
            cursor_type,
            updated_at,
        })
    }

    async fn delete(&self) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let dataset_name = self.dataset_name.clone();
        let _schema_guard = pool.acquire_schema_read_lock().await;
        let conn = pool.connect().await.map_err(store_error)?;
        let delete = format!("DELETE FROM {MYSQL_BINLOG_TABLE_NAME} WHERE dataset_name = ?1");
        conn.execute(&delete, turso::params![dataset_name])
            .await
            .map_err(store_error)?;

        Ok(())
    }
}
