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

use anyhow::{Error, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion_table_providers::sql::db_connection_pool::{
    sqlitepool::SqliteConnectionPool, DbConnectionPool, JoinPushDown,
};

pub(crate) async fn query_local_db(
    db_file_path: &str,
    query: &str,
) -> Result<SendableRecordBatchStream> {
    let conn_pool = SqliteConnectionPool::new(
        db_file_path,
        datafusion_table_providers::sql::db_connection_pool::Mode::File,
        JoinPushDown::Disallow,
        vec![],
        std::time::Duration::from_millis(5000),
    )
    .await
    .map_err(|e| Error::msg(format!("Failed to create SQLite connection pool: {e}")))?;

    let conn_dyn = conn_pool
        .connect()
        .await
        .map_err(|e| Error::msg(format!("Failed to connect to SQLite: {e}")))?;

    let conn = conn_dyn
        .as_async()
        .ok_or_else(|| Error::msg("Failed to obtain async connection"))?;

    conn.query_arrow(query, &[], None)
        .await
        .map_err(|e| Error::msg(format!("Failed to execute query: {e}")))
}
