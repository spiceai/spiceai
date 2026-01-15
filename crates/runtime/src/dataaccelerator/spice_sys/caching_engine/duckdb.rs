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
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use std::sync::Arc;

impl CachingEngineSys {
    pub(super) fn update_fetched_at_duckdb(&self, pool: &Arc<DuckDbConnectionPool>) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        // Find all tables that start with __data_ prefix
        let mut stmt = duckdb_conn
            .prepare("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_name LIKE '__data_%'")
            .map_err(Error::external)?;

        let table_names: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .map_err(Error::external)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::external)?;

        for table_name in table_names {
            let update_query = format!(
                "UPDATE \"{}\" SET fetched_at = (now() AT TIME ZONE 'UTC')::TIMESTAMP_NS",
                table_name
            );
            duckdb_conn
                .execute(&update_query, [])
                .map_err(Error::external)?;
        }

        Ok(())
    }
}
