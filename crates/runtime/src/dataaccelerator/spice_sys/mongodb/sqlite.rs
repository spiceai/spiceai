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

use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool,
};

use super::{Error, MONGODB_TABLE_NAME, MongoCheckpointMetadata, MongoSys, Result};

impl MongoSys {
    pub(super) async fn upsert_sqlite(
        &self,
        pool: &SqliteConnectionPool,
        metadata: &MongoCheckpointMetadata,
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let resume_token_json = metadata.resume_token_json.clone();
        let cluster_time_ts = metadata.cluster_time_ts;
        let schema_json = metadata.schema_json.clone();

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
                        "CREATE TABLE IF NOT EXISTS {MONGODB_TABLE_NAME} (
                            dataset_name TEXT PRIMARY KEY,
                            resume_token_json TEXT NOT NULL,
                            cluster_time_ts INTEGER,
                            schema_json TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )"
                    );
                    conn.execute(&create_table, [])?;

                    let upsert = format!(
                        "INSERT INTO {MONGODB_TABLE_NAME}
                         (dataset_name, resume_token_json, cluster_time_ts, schema_json, updated_at)
                         VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP)
                         ON CONFLICT (dataset_name) DO UPDATE SET
                            resume_token_json = ?2,
                            cluster_time_ts = ?3,
                            schema_json = ?4,
                            updated_at = CURRENT_TIMESTAMP"
                    );

                    conn.execute(
                        &upsert,
                        rusqlite::params![
                            dataset_name,
                            resume_token_json,
                            cluster_time_ts,
                            schema_json,
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
    ) -> Option<MongoCheckpointMetadata> {
        let dataset_name = self.dataset_name.clone();

        let conn_sync = pool.connect_sync();
        let conn = conn_sync.as_any().downcast_ref::<SqliteConnection>()?;

        conn.conn
            .call(move |conn: &mut rusqlite::Connection| -> Result<MongoCheckpointMetadata, rusqlite::Error> {
                let query = format!(
                    "SELECT resume_token_json, cluster_time_ts, schema_json, strftime('%s', updated_at) FROM {MONGODB_TABLE_NAME} WHERE dataset_name = ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([dataset_name])?;

                if let Some(row) = rows.next()? {
                    let resume_token_json: String = row.get(0)?;
                    let cluster_time_ts: Option<i64> = row.get(1).ok();
                    let schema_json: Option<String> = row.get(2).ok();
                    let updated_at_epoch: Option<i64> = row.get(3).ok();
                    let updated_at = updated_at_epoch.and_then(|epoch| {
                        u64::try_from(epoch).ok().and_then(|e| {
                            std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(e))
                        })
                    });

                    Ok(MongoCheckpointMetadata {
                        resume_token_json,
                        cluster_time_ts,
                        schema_json,
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
                    let delete = format!(
                        "DELETE FROM {MONGODB_TABLE_NAME} WHERE dataset_name = ?1"
                    );
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
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    async fn create_test_dataset(ds_name: &str) -> Dataset {
        let app = app::AppBuilder::new("test").build();
        let runtime = RuntimeBuilder::new().build().await;

        let mut dataset = DatasetBuilder::try_new("spice.ai".to_string(), ds_name)
            .expect("to create dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(runtime))
            .build()
            .expect("to create dataset");

        let db_file = format!(".spice/data/mongodb_sqlite_test_{ds_name}.db");
        dataset.acceleration = Some(Acceleration {
            engine: Engine::Sqlite,
            mode: Mode::File,
            params: [("sqlite_file".to_string(), db_file)].into_iter().collect(),
            ..Default::default()
        });

        dataset
    }

    fn create_test_metadata() -> MongoCheckpointMetadata {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        MongoCheckpointMetadata {
            resume_token_json: r#"{"_data":"82650000000000000001"}"#.to_string(),
            cluster_time_ts: Some(1_700_000_000),
            schema_json: Some(MongoSys::serialize_schema(&schema).expect("schema serializes")),
            updated_at: None,
        }
    }

    #[tokio::test]
    async fn test_sqlite_roundtrip() {
        let ds = create_test_dataset("test_mongodb_sqlite_roundtrip").await;
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
    async fn test_sqlite_metadata_overwrite() {
        let ds = create_test_dataset("test_mongodb_sqlite_overwrite").await;
        let mongo_sys = MongoSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MongoSys");

        let mut metadata = create_test_metadata();
        mongo_sys
            .upsert(&metadata)
            .await
            .expect("to upsert metadata");

        metadata.resume_token_json = r#"{"_data":"82650000000000000002"}"#.to_string();
        metadata.cluster_time_ts = Some(1_700_000_500);
        mongo_sys
            .upsert(&metadata)
            .await
            .expect("to overwrite metadata");

        let retrieved = mongo_sys.get().await.expect("to retrieve metadata");
        assert_eq!(retrieved.resume_token_json, metadata.resume_token_json);
        assert_eq!(retrieved.cluster_time_ts, metadata.cluster_time_ts);
    }

    #[tokio::test]
    async fn test_sqlite_get_nonexistent() {
        let ds = create_test_dataset("test_mongodb_sqlite_get_nonexistent").await;
        let mongo_sys = MongoSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MongoSys");

        assert!(mongo_sys.get().await.is_none());
    }

    #[tokio::test]
    async fn test_sqlite_delete() {
        let ds = create_test_dataset("test_mongodb_sqlite_delete").await;
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

    #[tokio::test]
    async fn test_sqlite_null_optional_columns() {
        let ds = create_test_dataset("test_mongodb_sqlite_null_optional").await;
        let mongo_sys = MongoSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create MongoSys");

        let metadata = MongoCheckpointMetadata {
            resume_token_json: r#"{"_data":"abc"}"#.to_string(),
            cluster_time_ts: None,
            schema_json: None,
            updated_at: None,
        };
        mongo_sys
            .upsert(&metadata)
            .await
            .expect("to upsert metadata");

        let retrieved = mongo_sys.get().await.expect("to retrieve metadata");
        assert_eq!(retrieved.resume_token_json, metadata.resume_token_json);
        assert!(retrieved.cluster_time_ts.is_none());
        assert!(retrieved.schema_json.is_none());
    }
}
