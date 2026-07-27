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

use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool,
};

use super::{Error, MYSQL_BINLOG_TABLE_NAME, MySqlBinlogCheckpoint, MySqlBinlogSys, Result};

/// Idempotent migration adding the columns that postdate the initial schema.
/// `SQLite`'s `ALTER TABLE ADD COLUMN` has no `IF NOT EXISTS`, so a duplicate
/// column (already migrated) or missing table errors — both are ignored.
fn migrate_columns(conn: &rusqlite::Connection) {
    for column in ["gtid_executed TEXT", "cursor_type TEXT"] {
        let _ = conn.execute(
            &format!("ALTER TABLE {MYSQL_BINLOG_TABLE_NAME} ADD COLUMN {column}"),
            [],
        );
    }
}

impl MySqlBinlogSys {
    pub(super) async fn upsert_sqlite(
        &self,
        pool: &SqliteConnectionPool,
        checkpoint: &MySqlBinlogCheckpoint,
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let binlog_file = checkpoint.binlog_file.clone();
        let binlog_pos = Self::position_to_i64(checkpoint.binlog_pos);
        let schema_json = checkpoint.schema_json.clone();
        let gtid_executed = checkpoint.gtid_executed.clone();
        let cursor_type = checkpoint.cursor_type.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        conn.conn
            .call(
                move |conn: &mut rusqlite::Connection| -> Result<(), rusqlite::Error> {
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
                    conn.execute(&create_table, [])?;
                    // Migrate tables created before these columns existed.
                    migrate_columns(conn);

                    let upsert = format!(
                        "INSERT INTO {MYSQL_BINLOG_TABLE_NAME}
                         (dataset_name, binlog_file, binlog_pos, schema_json, gtid_executed, cursor_type, updated_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, CURRENT_TIMESTAMP)
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
                        rusqlite::params![
                            dataset_name,
                            binlog_file,
                            binlog_pos,
                            schema_json,
                            gtid_executed,
                            cursor_type
                        ],
                    )?;

                    Ok::<(), rusqlite::Error>(())
                },
            )
            .await
            .map_err(Error::external)
    }

    pub(super) async fn get_sqlite(
        &self,
        pool: &SqliteConnectionPool,
    ) -> Option<MySqlBinlogCheckpoint> {
        let dataset_name = self.dataset_name.clone();

        let conn_sync = pool.connect_sync();
        let conn = conn_sync.as_any().downcast_ref::<SqliteConnection>()?;

        conn.conn
            .call(move |conn: &mut rusqlite::Connection| -> Result<MySqlBinlogCheckpoint, rusqlite::Error> {
                // Ensure the added columns exist so the SELECT doesn't fail on a
                // table created before they were added (ignored otherwise).
                migrate_columns(conn);
                let query = format!(
                    "SELECT binlog_file, binlog_pos, schema_json, gtid_executed, cursor_type, strftime('%s', updated_at) FROM {MYSQL_BINLOG_TABLE_NAME} WHERE dataset_name = ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([dataset_name])?;

                if let Some(row) = rows.next()? {
                    let binlog_file: String = row.get(0)?;
                    let binlog_pos: i64 = row.get(1)?;
                    let schema_json: Option<String> = row.get(2).ok();
                    let gtid_executed: Option<String> = row.get(3).ok();
                    let cursor_type: Option<String> = row.get(4).ok();
                    let updated_at_epoch: Option<i64> = row.get(5).ok();
                    let updated_at = updated_at_epoch.and_then(|epoch| {
                        u64::try_from(epoch).ok().and_then(|e| {
                            std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(e))
                        })
                    });

                    Ok(MySqlBinlogCheckpoint {
                        binlog_file,
                        binlog_pos: MySqlBinlogSys::position_from_i64(binlog_pos),
                        schema_json,
                        gtid_executed,
                        cursor_type,
                        updated_at,
                    })
                } else {
                    Err(rusqlite::Error::QueryReturnedNoRows)
                }
            })
            .await
            .ok()
    }

    pub(super) async fn delete_sqlite(&self, pool: &SqliteConnectionPool) -> Result<()> {
        let dataset_name = self.dataset_name.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        conn.conn
            .call(
                move |conn: &mut rusqlite::Connection| -> Result<(), rusqlite::Error> {
                    let delete =
                        format!("DELETE FROM {MYSQL_BINLOG_TABLE_NAME} WHERE dataset_name = ?1");
                    conn.execute(&delete, [dataset_name])?;
                    Ok::<(), rusqlite::Error>(())
                },
            )
            .await
            .map_err(Error::external)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        builder::RuntimeBuilder,
        component::dataset::{
            Dataset,
            acceleration::{Acceleration, Engine, Mode},
            builder::DatasetBuilder,
        },
        dataaccelerator::spice_sys::OpenOption,
    };
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn create_test_dataset(ds_name: &str) -> (Dataset, TempDir) {
        let app = app::AppBuilder::new("test").build();
        let runtime = RuntimeBuilder::new().build().await;

        let mut dataset = DatasetBuilder::try_new("spice.ai".to_string(), ds_name)
            .expect("to create dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(runtime))
            .build()
            .expect("to create dataset");

        let temp_dir = TempDir::new().expect("to create temp dir");
        let db_path = temp_dir
            .path()
            .join(format!("mysql_binlog_sqlite_test_{ds_name}.db"));

        dataset.acceleration = Some(Acceleration {
            engine: Engine::Sqlite,
            mode: Mode::File,
            params: [(
                "sqlite_file".to_string(),
                db_path.to_string_lossy().to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        (dataset, temp_dir)
    }

    fn create_test_checkpoint() -> MySqlBinlogCheckpoint {
        MySqlBinlogCheckpoint {
            binlog_file: "binlog.000042".to_string(),
            binlog_pos: 1_234_567,
            schema_json: Some(r#"{"fields":[]}"#.to_string()),
            gtid_executed: Some("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5".to_string()),
            cursor_type: Some("gtid".to_string()),
            updated_at: None,
        }
    }

    #[tokio::test]
    async fn test_sqlite_roundtrip() {
        let (dataset, _temp_dir) = create_test_dataset("test_mysql_binlog_sqlite_roundtrip").await;
        let sys = MySqlBinlogSys::try_new(&dataset, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MySqlBinlogSys");

        let checkpoint = create_test_checkpoint();
        sys.upsert(&checkpoint).await.expect("to upsert checkpoint");

        let retrieved = sys.get().await.expect("to retrieve checkpoint");
        assert_eq!(retrieved.binlog_file, checkpoint.binlog_file);
        assert_eq!(retrieved.binlog_pos, checkpoint.binlog_pos);
        assert_eq!(retrieved.schema_json, checkpoint.schema_json);
        assert_eq!(retrieved.gtid_executed, checkpoint.gtid_executed);
        assert_eq!(retrieved.cursor_type, checkpoint.cursor_type);
    }

    /// F7 correctness landmine: a GTID checkpoint with an *empty* executed set
    /// (`gtid_mode = ON`, zero transactions yet) must round-trip as `gtid`, not
    /// silently reclassify as file+offset because the empty string collapsed to
    /// `NULL`. The explicit `cursor_type` column is what guarantees this.
    #[tokio::test]
    async fn test_sqlite_empty_gtid_set_stays_gtid() {
        let (dataset, _temp_dir) = create_test_dataset("test_mysql_binlog_sqlite_empty_gtid").await;
        let sys = MySqlBinlogSys::try_new(&dataset, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MySqlBinlogSys");

        let checkpoint = MySqlBinlogCheckpoint {
            binlog_file: "binlog.000001".to_string(),
            binlog_pos: 4,
            schema_json: None,
            gtid_executed: Some(String::new()),
            cursor_type: Some("gtid".to_string()),
            updated_at: None,
        };
        sys.upsert(&checkpoint).await.expect("to upsert checkpoint");

        let retrieved = sys.get().await.expect("to retrieve checkpoint");
        assert_eq!(
            retrieved.cursor_type.as_deref(),
            Some("gtid"),
            "an empty GTID set must still classify as gtid"
        );
    }

    #[tokio::test]
    async fn test_sqlite_checkpoint_overwrite() {
        let (dataset, _temp_dir) = create_test_dataset("test_mysql_binlog_sqlite_overwrite").await;
        let sys = MySqlBinlogSys::try_new(&dataset, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MySqlBinlogSys");

        let mut checkpoint = create_test_checkpoint();
        sys.upsert(&checkpoint).await.expect("to upsert checkpoint");

        checkpoint.binlog_file = "binlog.000043".to_string();
        checkpoint.binlog_pos = 4;
        sys.upsert(&checkpoint)
            .await
            .expect("to overwrite checkpoint");

        let retrieved = sys.get().await.expect("to retrieve checkpoint");
        assert_eq!(retrieved.binlog_file, "binlog.000043");
        assert_eq!(retrieved.binlog_pos, 4);
    }

    #[tokio::test]
    async fn test_sqlite_get_nonexistent() {
        let (dataset, _temp_dir) =
            create_test_dataset("test_mysql_binlog_sqlite_get_nonexistent").await;
        let sys = MySqlBinlogSys::try_new(&dataset, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MySqlBinlogSys");

        assert!(sys.get().await.is_none());
    }

    #[tokio::test]
    async fn test_sqlite_delete() {
        let (dataset, _temp_dir) = create_test_dataset("test_mysql_binlog_sqlite_delete").await;
        let sys = MySqlBinlogSys::try_new(&dataset, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MySqlBinlogSys");

        sys.upsert(&create_test_checkpoint())
            .await
            .expect("to upsert checkpoint");
        assert!(sys.get().await.is_some());

        sys.delete().await.expect("to delete");
        assert!(sys.get().await.is_none());
    }

    #[tokio::test]
    async fn test_sqlite_null_schema_json() {
        let (dataset, _temp_dir) =
            create_test_dataset("test_mysql_binlog_sqlite_null_schema").await;
        let sys = MySqlBinlogSys::try_new(&dataset, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MySqlBinlogSys");

        let checkpoint = MySqlBinlogCheckpoint {
            binlog_file: "binlog.000001".to_string(),
            binlog_pos: 4,
            schema_json: None,
            gtid_executed: None,
            cursor_type: None,
            updated_at: None,
        };
        sys.upsert(&checkpoint).await.expect("to upsert checkpoint");

        let retrieved = sys.get().await.expect("to retrieve checkpoint");
        assert_eq!(retrieved.binlog_file, checkpoint.binlog_file);
        assert!(retrieved.schema_json.is_none());
        assert!(retrieved.gtid_executed.is_none());
        assert!(retrieved.cursor_type.is_none());
    }
}
