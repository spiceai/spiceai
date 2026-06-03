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

use super::{CachingEngineSys, Error, Result};
use datafusion_table_providers::duckdb::DuckDB;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use std::sync::Arc;

impl CachingEngineSys {
    pub(super) fn update_fetched_at_duckdb(&self, pool: &Arc<DuckDbConnectionPool>) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        let tx = duckdb_conn.transaction().map_err(Error::external)?;

        let has_table = table_exists(&tx, &self.dataset_name)?;
        let mut internal_tables = list_internal_tables(&tx, &self.dataset_name)?;

        // Determine the actual table name (internal or direct)
        let table_name = match (internal_tables.pop(), has_table) {
            (Some((internal_name, _)), _) => internal_name.to_string(),
            (None, true) => self.dataset_name.clone(),
            (None, false) => {
                // No table exists yet
                tracing::warn!("No table found for dataset: {}", self.dataset_name);
                return Ok(());
            }
        };

        // Update fetched_at for the table
        let update_query = format!(
            "UPDATE \"{table_name}\" SET _fetched_at = (now() AT TIME ZONE 'UTC')::TIMESTAMP_NS"
        );
        tx.execute(&update_query, []).map_err(Error::external)?;

        tx.commit().map_err(Error::external)?;
        Ok(())
    }
}

fn table_exists(tx: &spiceai_duckdb::Transaction<'_>, table_name: &str) -> Result<bool> {
    let mut stmt = tx
        .prepare("SELECT 1 FROM duckdb_tables() WHERE table_name = ?")
        .map_err(Error::external)?;
    let mut rows = stmt.query([table_name]).map_err(Error::external)?;
    Ok(rows.next().map_err(Error::external)?.is_some())
}

fn list_internal_tables(
    tx: &spiceai_duckdb::Transaction<'_>,
    table_name: &str,
) -> Result<Vec<(String, u64)>> {
    let pattern = format!("__data_{table_name}%");
    let mut stmt = tx
        .prepare("SELECT table_name FROM duckdb_tables() WHERE table_name LIKE ?")
        .map_err(Error::external)?;
    let mut rows = stmt.query([pattern]).map_err(Error::external)?;

    let mut table_names = Vec::new();
    while let Some(row) = rows.next().map_err(Error::external)? {
        let internal_table_name: String = row.get(0).map_err(Error::external)?;
        let Some(inner_name) = internal_table_name.strip_prefix("__data_") else {
            continue;
        };
        let Some((inner_table_name, timestamp)) = inner_name.rsplit_once('_') else {
            continue;
        };
        if inner_table_name != table_name {
            continue;
        }
        let timestamp = timestamp.parse::<u64>().map_err(Error::external)?;
        table_names.push((internal_table_name, timestamp));
    }

    table_names.sort_by(|left, right| left.1.cmp(&right.1));
    Ok(table_names)
}
