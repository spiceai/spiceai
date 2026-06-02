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
use datafusion_table_providers::duckdb::{DuckDB, RelationName, TableDefinition};
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use std::sync::Arc;

impl CachingEngineSys {
    pub(super) fn update_fetched_at_duckdb(&self, pool: &Arc<DuckDbConnectionPool>) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        let tx = duckdb_conn.transaction().map_err(Error::external)?;

        // Create a TableDefinition from the dataset name to find internal tables
        let table_definition = TableDefinition::new(
            RelationName::new(&self.dataset_name),
            Arc::new(arrow::datatypes::Schema::empty()), // Schema not needed for listing tables
        );

        let has_table = table_definition.has_table(&tx).map_err(Error::external)?;
        let mut internal_tables = table_definition
            .list_internal_tables(&tx)
            .map_err(Error::external)?;

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
            "UPDATE \"{table_name}\" SET fetched_at = (now() AT TIME ZONE 'UTC')::TIMESTAMP_NS"
        );
        tx.execute(&update_query, []).map_err(Error::external)?;

        tx.commit().map_err(Error::external)?;
        Ok(())
    }
}
