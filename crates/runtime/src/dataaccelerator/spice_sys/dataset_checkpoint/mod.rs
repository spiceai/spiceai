/*
Copyright 2024 The Spice.ai OSS Authors

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

//! CREATE TABLE `spice_sys_dataset_checkpoint` (
//!     `dataset_name` TEXT PRIMARY KEY,
//!     `created_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP`,
//!     `updated_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP` ON UPDATE `CURRENT_TIMESTAMP`,
//! );

use super::{acceleration_connection, AccelerationConnection, Result};
use crate::component::dataset::Dataset;
#[cfg(feature = "duckdb")]
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
#[cfg(feature = "postgres")]
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use std::sync::Arc;
#[cfg(feature = "sqlite")]
use tokio_rusqlite::Connection;

const CHECKPOINT_TABLE_NAME: &str = "spice_sys_dataset_checkpoint";

pub struct DatasetCheckpoint {
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl DatasetCheckpoint {
    pub async fn try_new(dataset: &Dataset) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, false).await?,
        })
    }

    pub async fn try_new_create_if_not_exists(dataset: &Dataset) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, true).await?,
        })
    }

    #[allow(unreachable_patterns)]
    pub async fn exists(&self) -> bool {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.exists_duckdb(pool).ok().unwrap_or(false),
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(pool) => {
                self.exists_postgres(pool).await.ok().unwrap_or(false)
            }
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => {
                self.exists_sqlite(conn).await.ok().unwrap_or(false)
            }
            _ => unreachable!(),
        }
    }

    #[allow(unreachable_patterns)]
    pub async fn checkpoint(&self) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.checkpoint_duckdb(pool),
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(pool) => self.checkpoint_postgres(pool).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.checkpoint_sqlite(conn).await,
            _ => unreachable!(),
        }
    }

    #[cfg(feature = "duckdb")]
    fn exists_duckdb(&self, pool: &Arc<DuckDbConnectionPool>) -> Result<bool> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(|e| e.to_string())?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(|e| e.to_string())?
            .get_underlying_conn_mut();

        let query = format!("SELECT 1 FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1");
        let mut stmt = duckdb_conn.prepare(&query).map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query([&self.dataset_name])
            .map_err(|e| e.to_string())?;

        Ok(rows.next().map_err(|e| e.to_string())?.is_some())
    }

    #[cfg(feature = "postgres")]
    async fn exists_postgres(&self, pool: &PostgresConnectionPool) -> Result<bool> {
        let conn = pool.connect_direct().await.map_err(|e| e.to_string())?;
        let query =
            format!("SELECT 1 FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = $1 LIMIT 1");
        let stmt = conn.conn.prepare(&query).await.map_err(|e| e.to_string())?;
        let row = conn
            .conn
            .query_opt(&stmt, &[&self.dataset_name])
            .await
            .map_err(|e| e.to_string())?;
        Ok(row.is_some())
    }

    #[cfg(feature = "sqlite")]
    async fn exists_sqlite(&self, conn: &Connection) -> Result<bool> {
        let dataset_name = self.dataset_name.clone();
        conn.call(move |conn| {
            let query =
                format!("SELECT 1 FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1");
            let mut stmt = conn.prepare(&query)?;
            let mut rows = stmt.query([dataset_name])?;
            Ok(rows.next()?.is_some())
        })
        .await
        .map_err(|e| e.to_string().into())
    }

    #[cfg(feature = "duckdb")]
    fn checkpoint_duckdb(&self, pool: &Arc<DuckDbConnectionPool>) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(|e| e.to_string())?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(|e| e.to_string())?
            .get_underlying_conn_mut();

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"
        );
        duckdb_conn
            .execute(&create_table, [])
            .map_err(|e| e.to_string())?;

        let upsert = format!(
            "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, created_at, updated_at)
             VALUES (?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET updated_at = now()"
        );
        duckdb_conn
            .execute(&upsert, [&self.dataset_name])
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    #[cfg(feature = "postgres")]
    async fn checkpoint_postgres(&self, pool: &PostgresConnectionPool) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(|e| e.to_string())?;

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
            .map_err(|e| e.to_string())?;

        let upsert = format!(
            "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, updated_at)
             VALUES ($1, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET updated_at = CURRENT_TIMESTAMP"
        );
        conn.conn
            .execute(&upsert, &[&self.dataset_name])
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    #[cfg(feature = "sqlite")]
    async fn checkpoint_sqlite(&self, conn: &Connection) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        conn.call(move |conn| {
            let create_table = format!(
                "CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE_NAME} (
                    dataset_name TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"
            );
            conn.execute(&create_table, [])?;

            let upsert = format!(
                "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, updated_at)
                 VALUES (?1, CURRENT_TIMESTAMP)
                 ON CONFLICT (dataset_name) DO UPDATE SET updated_at = CURRENT_TIMESTAMP"
            );
            conn.execute(&upsert, [dataset_name])?;

            Ok(())
        })
        .await
        .map_err(|e| e.to_string().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[cfg(feature = "duckdb")]
    fn create_in_memory_duckdb_checkpoint() -> DatasetCheckpoint {
        let pool = Arc::new(
            DuckDbConnectionPool::new_memory().expect("Failed to create in-memory DuckDB database"),
        );
        DatasetCheckpoint {
            dataset_name: "test_dataset".to_string(),
            acceleration_connection: AccelerationConnection::DuckDB(pool),
        }
    }

    #[cfg(feature = "sqlite")]
    async fn create_in_memory_sqlite_checkpoint() -> DatasetCheckpoint {
        let conn = Connection::open_in_memory()
            .await
            .expect("Failed to open in-memory SQLite database");
        DatasetCheckpoint {
            dataset_name: "test_dataset".to_string(),
            acceleration_connection: AccelerationConnection::SQLite(conn),
        }
    }

    #[tokio::test]
    #[cfg(feature = "duckdb")]
    async fn test_duckdb_checkpoint_exists() {
        let checkpoint = create_in_memory_duckdb_checkpoint();

        // Initially, the checkpoint should not exist
        assert!(!checkpoint.exists().await);

        // Create the checkpoint
        checkpoint
            .checkpoint()
            .await
            .expect("Failed to create checkpoint");

        // Now the checkpoint should exist
        assert!(checkpoint.exists().await);
    }

    #[tokio::test]
    #[cfg(feature = "duckdb")]
    async fn test_duckdb_checkpoint_update() {
        let checkpoint = create_in_memory_duckdb_checkpoint();

        // Create the initial checkpoint
        checkpoint
            .checkpoint()
            .await
            .expect("Failed to create initial checkpoint");

        // Sleep for a short time to ensure the timestamp changes
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Update the checkpoint
        checkpoint
            .checkpoint()
            .await
            .expect("Failed to update checkpoint");

        // Verify that the updated_at timestamp has changed
        if let AccelerationConnection::DuckDB(pool) = &checkpoint.acceleration_connection {
            let mut db_conn = Arc::clone(pool)
                .connect_sync()
                .expect("Failed to connect to DuckDB");
            let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
                .expect("Failed to get DuckDB connection")
                .get_underlying_conn_mut();

            let query = format!(
                "SELECT created_at, updated_at FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?",
            );
            let mut stmt = duckdb_conn
                .prepare(&query)
                .expect("Failed to prepare SQL statement");
            let mut rows = stmt
                .query([&checkpoint.dataset_name])
                .expect("Failed to execute query");

            if let Some(row) = rows.next().expect("Failed to fetch row") {
                let created_at: duckdb::types::Value =
                    row.get(0).expect("Failed to get created_at");
                let duckdb::types::Value::Timestamp(_, created_at) = created_at else {
                    panic!("created_at is not a timestamp");
                };
                let updated_at: duckdb::types::Value =
                    row.get(1).expect("Failed to get updated_at");
                let duckdb::types::Value::Timestamp(_, updated_at) = updated_at else {
                    panic!("updated_at is not a timestamp");
                };
                assert_ne!(
                    created_at, updated_at,
                    "created_at and updated_at should be different"
                );
            } else {
                panic!("No checkpoint found");
            }
        } else {
            panic!("Unexpected acceleration connection type");
        }
    }

    #[tokio::test]
    #[cfg(feature = "sqlite")]
    async fn test_sqlite_checkpoint_exists() {
        let checkpoint = create_in_memory_sqlite_checkpoint().await;

        // Initially, the checkpoint should not exist
        assert!(!checkpoint.exists().await);

        // Create the checkpoint
        checkpoint
            .checkpoint()
            .await
            .expect("Failed to create checkpoint");

        // Now the checkpoint should exist
        assert!(checkpoint.exists().await);
    }

    #[tokio::test]
    #[cfg(feature = "sqlite")]
    async fn test_sqlite_checkpoint_update() {
        let checkpoint = create_in_memory_sqlite_checkpoint().await;

        // Create the initial checkpoint
        checkpoint
            .checkpoint()
            .await
            .expect("Failed to create initial checkpoint");

        // Sleep for a short time to ensure the timestamp changes
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Update the checkpoint
        checkpoint
            .checkpoint()
            .await
            .expect("Failed to update checkpoint");

        // Verify that the updated_at timestamp has changed
        let AccelerationConnection::SQLite(conn) = &checkpoint.acceleration_connection else {
            panic!("Unexpected acceleration connection type");
        };
        let result = conn
            .call(move |conn| {
                let query = format!(
                    "SELECT created_at, updated_at FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?",
                );
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([&checkpoint.dataset_name])?;

                if let Some(row) = rows.next()? {
                    let created_at: String = row.get(0)?;
                    let updated_at: String = row.get(1)?;
                    Ok((created_at, updated_at))
                } else {
                    Err(tokio_rusqlite::Error::Other(
                        "No checkpoint found".into(),
                    ))
                }
            })
            .await
            .expect("Failed to fetch checkpoint data");

        let (created_at, updated_at) = result;
        assert_ne!(
            created_at, updated_at,
            "created_at and updated_at should be different"
        );
    }
}
