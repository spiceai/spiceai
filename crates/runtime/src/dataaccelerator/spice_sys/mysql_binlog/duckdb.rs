/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::sync::Arc;

use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;

use super::{Error, MYSQL_BINLOG_TABLE_NAME, MySqlBinlogCheckpoint, MySqlBinlogSys, Result};

impl MySqlBinlogSys {
    pub(super) fn upsert_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
        checkpoint: &MySqlBinlogCheckpoint,
    ) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {MYSQL_BINLOG_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                binlog_file TEXT NOT NULL,
                binlog_pos BIGINT NOT NULL,
                schema_json TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"
        );
        duckdb_conn
            .execute(&create_table, [])
            .map_err(Error::external)?;

        let upsert = format!(
            "INSERT INTO {MYSQL_BINLOG_TABLE_NAME}
                (dataset_name, binlog_file, binlog_pos, schema_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET
                binlog_file = excluded.binlog_file,
                binlog_pos = excluded.binlog_pos,
                schema_json = excluded.schema_json,
                updated_at = now()"
        );

        duckdb_conn
            .execute(
                &upsert,
                duckdb::params![
                    self.dataset_name,
                    checkpoint.binlog_file,
                    Self::position_to_i64(checkpoint.binlog_pos),
                    checkpoint.schema_json,
                ],
            )
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) fn get_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
    ) -> Option<MySqlBinlogCheckpoint> {
        use std::time::{Duration, UNIX_EPOCH};

        let mut db_conn = Arc::clone(pool).connect_sync().ok()?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .ok()?
            .get_underlying_conn_mut();

        let query = format!(
            "SELECT binlog_file, binlog_pos, schema_json, CAST(epoch(updated_at) AS DOUBLE) FROM {MYSQL_BINLOG_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).ok()?;
        let mut rows = stmt.query([&self.dataset_name]).ok()?;

        if let Some(row) = rows.next().ok()? {
            let binlog_file: String = row.get(0).ok()?;
            let binlog_pos: i64 = row.get(1).ok()?;
            let schema_json: Option<String> = row.get(2).ok();
            let updated_at_epoch: Option<f64> = row.get(3).ok();
            let updated_at = updated_at_epoch
                .and_then(|epoch| UNIX_EPOCH.checked_add(Duration::from_secs_f64(epoch)));

            Some(MySqlBinlogCheckpoint {
                binlog_file,
                binlog_pos: Self::position_from_i64(binlog_pos),
                schema_json,
                updated_at,
            })
        } else {
            None
        }
    }

    pub(super) fn delete_duckdb(&self, pool: &Arc<DuckDbConnectionPool>) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        let delete = format!("DELETE FROM {MYSQL_BINLOG_TABLE_NAME} WHERE dataset_name = ?");
        duckdb_conn
            .execute(&delete, [&self.dataset_name])
            .map_err(Error::external)?;

        Ok(())
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
        let db_path = temp_dir.path().join("mysql_binlog_duckdb_test.db");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::DuckDB,
            mode: Mode::File,
            params: [(
                "duckdb_file".to_string(),
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
            updated_at: None,
        }
    }

    #[tokio::test]
    async fn test_duckdb_roundtrip() {
        let (ds, _tmp) = create_test_dataset("test_mysql_binlog_duckdb_roundtrip").await;
        let sys = MySqlBinlogSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MySqlBinlogSys");

        let checkpoint = create_test_checkpoint();
        sys.upsert(&checkpoint).await.expect("to upsert checkpoint");

        let retrieved = sys.get().await.expect("to retrieve checkpoint");
        assert_eq!(retrieved.binlog_file, checkpoint.binlog_file);
        assert_eq!(retrieved.binlog_pos, checkpoint.binlog_pos);
        assert_eq!(retrieved.schema_json, checkpoint.schema_json);
    }

    #[tokio::test]
    async fn test_duckdb_checkpoint_overwrite() {
        let (ds, _tmp) = create_test_dataset("test_mysql_binlog_duckdb_overwrite").await;
        let sys = MySqlBinlogSys::try_new(&ds, OpenOption::CreateIfNotExists)
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
    async fn test_duckdb_get_nonexistent() {
        let (ds, _tmp) = create_test_dataset("test_mysql_binlog_duckdb_get_nonexistent").await;
        let sys = MySqlBinlogSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MySqlBinlogSys");

        assert!(sys.get().await.is_none());
    }

    #[tokio::test]
    async fn test_duckdb_delete() {
        let (ds, _tmp) = create_test_dataset("test_mysql_binlog_duckdb_delete").await;
        let sys = MySqlBinlogSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MySqlBinlogSys");

        sys.upsert(&create_test_checkpoint())
            .await
            .expect("to upsert checkpoint");
        assert!(sys.get().await.is_some());

        sys.delete().await.expect("to delete");
        assert!(sys.get().await.is_none());
    }
}
