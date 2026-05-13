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

use super::{Error, MONGODB_TABLE_NAME, MongoCheckpointMetadata, MongoSys, Result};

impl MongoSys {
    pub(super) fn upsert_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
        metadata: &MongoCheckpointMetadata,
    ) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {MONGODB_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                resume_token_json TEXT NOT NULL,
                cluster_time_ts BIGINT,
                schema_json TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"
        );
        duckdb_conn
            .execute(&create_table, [])
            .map_err(Error::external)?;

        let upsert = format!(
            "INSERT INTO {MONGODB_TABLE_NAME}
                (dataset_name, resume_token_json, cluster_time_ts, schema_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET
                resume_token_json = excluded.resume_token_json,
                cluster_time_ts = excluded.cluster_time_ts,
                schema_json = excluded.schema_json,
                updated_at = now()"
        );

        duckdb_conn
            .execute(
                &upsert,
                duckdb::params![
                    self.dataset_name,
                    metadata.resume_token_json,
                    metadata.cluster_time_ts,
                    metadata.schema_json,
                ],
            )
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) fn get_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
    ) -> Option<MongoCheckpointMetadata> {
        use std::time::{Duration, UNIX_EPOCH};

        let mut db_conn = Arc::clone(pool).connect_sync().ok()?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .ok()?
            .get_underlying_conn_mut();

        let query = format!(
            "SELECT resume_token_json, cluster_time_ts, schema_json, epoch(updated_at) FROM {MONGODB_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).ok()?;
        let mut rows = stmt.query([&self.dataset_name]).ok()?;

        if let Some(row) = rows.next().ok()? {
            let resume_token_json: String = row.get(0).ok()?;
            let cluster_time_ts: Option<i64> = row.get(1).ok();
            let schema_json: Option<String> = row.get(2).ok();
            let updated_at_epoch: Option<f64> = row.get(3).ok();
            let updated_at = updated_at_epoch
                .and_then(|epoch| UNIX_EPOCH.checked_add(Duration::from_secs_f64(epoch)));

            Some(MongoCheckpointMetadata {
                resume_token_json,
                cluster_time_ts,
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

        let delete = format!("DELETE FROM {MONGODB_TABLE_NAME} WHERE dataset_name = ?");
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
        let db_path = temp_dir.path().join("mongodb_duckdb_test.db");

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

    fn create_test_metadata() -> MongoCheckpointMetadata {
        MongoCheckpointMetadata {
            resume_token_json: r#"{"_data":"82650000000000000001"}"#.to_string(),
            cluster_time_ts: Some(1_700_000_000),
            schema_json: Some(r#"{"fields":[]}"#.to_string()),
            updated_at: None,
        }
    }

    #[tokio::test]
    async fn test_duckdb_roundtrip() {
        let (ds, _tmp) = create_test_dataset("test_mongodb_duckdb_roundtrip").await;
        let mongo_sys = MongoSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MongoSys");

        let metadata = create_test_metadata();
        mongo_sys
            .upsert(&metadata)
            .await
            .expect("to upsert metadata");

        let retrieved = mongo_sys.get().await.expect("to retrieve metadata");
        assert_eq!(retrieved.resume_token_json, metadata.resume_token_json);
        assert_eq!(retrieved.cluster_time_ts, metadata.cluster_time_ts);
        assert_eq!(retrieved.schema_json, metadata.schema_json);
    }

    #[tokio::test]
    async fn test_duckdb_metadata_overwrite() {
        let (ds, _tmp) = create_test_dataset("test_mongodb_duckdb_overwrite").await;
        let mongo_sys = MongoSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MongoSys");
        let mut metadata = create_test_metadata();

        mongo_sys
            .upsert(&metadata)
            .await
            .expect("to upsert metadata");

        metadata.resume_token_json = r#"{"_data":"82650000000000000002"}"#.to_string();
        mongo_sys
            .upsert(&metadata)
            .await
            .expect("to overwrite metadata");

        let retrieved = mongo_sys.get().await.expect("to retrieve metadata");
        assert_eq!(retrieved.resume_token_json, metadata.resume_token_json);
    }

    #[tokio::test]
    async fn test_duckdb_get_nonexistent() {
        let (ds, _tmp) = create_test_dataset("test_mongodb_duckdb_get_nonexistent").await;
        let mongo_sys = MongoSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MongoSys");

        assert!(mongo_sys.get().await.is_none());
    }

    #[tokio::test]
    async fn test_duckdb_delete() {
        let (ds, _tmp) = create_test_dataset("test_mongodb_duckdb_delete").await;
        let mongo_sys = MongoSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MongoSys");

        mongo_sys
            .upsert(&create_test_metadata())
            .await
            .expect("to upsert metadata");
        assert!(mongo_sys.get().await.is_some());

        mongo_sys.delete().await.expect("to delete");
        assert!(mongo_sys.get().await.is_none());
    }
}
