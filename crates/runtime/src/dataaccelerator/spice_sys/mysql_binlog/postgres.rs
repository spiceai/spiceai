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

use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;

use super::{Error, MYSQL_BINLOG_TABLE_NAME, MySqlBinlogCheckpoint, MySqlBinlogSys, Result};

/// Idempotent migrations adding the columns that postdate the initial schema.
/// Postgres supports `IF NOT EXISTS`, so each is a clean no-op when present.
fn migrate_columns() -> [String; 2] {
    [
        format!(
            "ALTER TABLE {MYSQL_BINLOG_TABLE_NAME} ADD COLUMN IF NOT EXISTS gtid_executed TEXT"
        ),
        format!("ALTER TABLE {MYSQL_BINLOG_TABLE_NAME} ADD COLUMN IF NOT EXISTS cursor_type TEXT"),
    ]
}

impl MySqlBinlogSys {
    pub(super) async fn upsert_postgres(
        &self,
        pool: &PostgresConnectionPool,
        checkpoint: &MySqlBinlogCheckpoint,
    ) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;

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
        conn.conn
            .execute(&create_table, &[])
            .await
            .map_err(Error::external)?;
        // Migrate tables created before these columns existed. Idempotent.
        for migration in migrate_columns() {
            conn.conn
                .execute(&migration, &[])
                .await
                .map_err(Error::external)?;
        }

        let upsert = format!(
            "INSERT INTO {MYSQL_BINLOG_TABLE_NAME}
             (dataset_name, binlog_file, binlog_pos, schema_json, gtid_executed, cursor_type, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                binlog_file = EXCLUDED.binlog_file,
                binlog_pos = EXCLUDED.binlog_pos,
                schema_json = EXCLUDED.schema_json,
                gtid_executed = EXCLUDED.gtid_executed,
                cursor_type = EXCLUDED.cursor_type,
                updated_at = CURRENT_TIMESTAMP"
        );

        conn.conn
            .execute(
                &upsert,
                &[
                    &self.dataset_name,
                    &checkpoint.binlog_file,
                    &Self::position_to_i64(checkpoint.binlog_pos),
                    &checkpoint.schema_json,
                    &checkpoint.gtid_executed,
                    &checkpoint.cursor_type,
                ],
            )
            .await
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn get_postgres(
        &self,
        pool: &PostgresConnectionPool,
    ) -> Option<MySqlBinlogCheckpoint> {
        let conn = pool.connect_direct().await.ok()?;
        // Ensure the added columns exist so the SELECT doesn't fail on a table
        // created before they were added. Idempotent; ignored otherwise.
        for migration in migrate_columns() {
            let _ = conn.conn.execute(&migration, &[]).await;
        }
        let query = format!(
            "SELECT binlog_file, binlog_pos, schema_json, gtid_executed, cursor_type, EXTRACT(EPOCH FROM updated_at) FROM {MYSQL_BINLOG_TABLE_NAME} WHERE dataset_name = $1"
        );
        let stmt = conn.conn.prepare(&query).await.ok()?;
        let row = conn
            .conn
            .query_opt(&stmt, &[&self.dataset_name])
            .await
            .ok()??;

        let binlog_file: String = row.get(0);
        let binlog_pos: i64 = row.get(1);
        let schema_json: Option<String> = row.get(2);
        let gtid_executed: Option<String> = row.get(3);
        let cursor_type: Option<String> = row.get(4);
        let updated_at_epoch: Option<f64> = row.get(5);
        let updated_at = updated_at_epoch.and_then(|epoch| {
            std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs_f64(epoch))
        });

        Some(MySqlBinlogCheckpoint {
            binlog_file,
            binlog_pos: Self::position_from_i64(binlog_pos),
            schema_json,
            gtid_executed,
            cursor_type,
            updated_at,
        })
    }

    pub(super) async fn delete_postgres(&self, pool: &PostgresConnectionPool) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;
        let delete = format!("DELETE FROM {MYSQL_BINLOG_TABLE_NAME} WHERE dataset_name = $1");
        conn.conn
            .execute(&delete, &[&self.dataset_name])
            .await
            .map_err(Error::external)?;

        Ok(())
    }
}
