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
use arrow::datatypes::SchemaRef;
use arrow_sql_gen::statement::{CreateTableBuilder, IndexBuilder};
use datafusion::common::{Constraint, Constraints};
use db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::Transaction;
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

use super::DuckDB;
use crate::util::indexes::{self, IndexType};

/// Responsible for creating a `DuckDB` table along with any constraints and indexes
pub(crate) struct TableCreator {
    table_name: String,
    schema: SchemaRef,
    pool: Arc<DuckDbConnectionPool>,
    constraints: Option<Constraints>,
    indexes: HashMap<String, IndexType>,
    created: bool,
}

impl TableCreator {
    pub fn new(table_name: String, schema: SchemaRef, pool: Arc<DuckDbConnectionPool>) -> Self {
        Self {
            table_name,
            schema,
            pool,
            constraints: None,
            indexes: HashMap::new(),
            created: false,
        }
    }

    pub fn constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = Some(constraints);
        self
    }

    pub fn indexes(mut self, indexes: HashMap<String, IndexType>) -> Self {
        self.indexes = indexes;
        self
    }

    fn indexes_vec(&self) -> Vec<(Vec<&str>, IndexType)> {
        self.indexes
            .iter()
            .map(|(key, ty)| (indexes::index_columns(key), *ty))
            .collect()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn create(mut self) -> super::Result<DuckDB> {
        assert!(!self.created, "Table already created");
        let mut primary_keys: Vec<String> = Vec::new();
        if let Some(constraints) = &self.constraints {
            for constraint in constraints.clone() {
                if let Constraint::PrimaryKey(cols) = constraint.clone() {
                    cols.iter()
                        .map(|col| self.schema.field(*col).name())
                        .for_each(|col| {
                            primary_keys.push(col.to_string());
                        });
                }
            }
        };

        let mut db_conn = Arc::clone(&self.pool)
            .connect_sync()
            .context(super::DbConnectionSnafu)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)?;

        let tx = duckdb_conn
            .conn
            .transaction()
            .context(super::UnableToBeginTransactionSnafu)?;

        self.create_table(&tx, primary_keys)?;

        for index in self.indexes_vec() {
            self.create_index(&tx, index.0, index.1 == IndexType::Unique)?;
        }

        tx.commit()
            .context(super::UnableToCommitDuckDBTransactionSnafu)?;

        let constraints = self.constraints.clone().unwrap_or(Constraints::empty());

        let mut duckdb =
            DuckDB::existing_table(self.table_name.clone(), Arc::clone(&self.pool), constraints);

        self.created = true;

        duckdb.table_creator = Some(self);

        Ok(duckdb)
    }

    /// Creates a copy of the `DuckDB` table with the same schema and constraints
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn create_empty_clone(&self) -> super::Result<DuckDB> {
        assert!(self.created, "Table must be created before cloning");

        let new_table_name = format!(
            "{}_spice_{}",
            self.table_name,
            &Uuid::new_v4().to_string()[..8]
        );
        tracing::debug!(
            "Creating empty table {} from {}",
            new_table_name,
            self.table_name,
        );

        let new_table_creator = TableCreator {
            table_name: new_table_name.clone(),
            schema: Arc::clone(&self.schema),
            pool: Arc::clone(&self.pool),
            constraints: self.constraints.clone(),
            indexes: self.indexes.clone(),
            created: false,
        };

        new_table_creator.create()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn delete_table(self, tx: &Transaction<'_>) -> super::Result<()> {
        assert!(self.created, "Table must be created before deleting");
        for index in self.indexes_vec() {
            self.drop_index(tx, index.0)?;
        }
        self.drop_table(tx)?;

        Ok(())
    }

    /// Consumes the current table and replaces `table_to_replace` with the current table's contents.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn replace_table(
        mut self,
        tx: &Transaction<'_>,
        table_to_replace: &TableCreator,
    ) -> super::Result<()> {
        assert!(
            self.created,
            "Table must be created before replacing another table"
        );

        // Drop indexes and table for the table we want to replace
        for index in table_to_replace.indexes_vec() {
            table_to_replace.drop_index(tx, index.0)?;
        }
        // Drop the old table with the name we want to claim
        table_to_replace.drop_table(tx)?;

        // DuckDB doesn't support renaming tables with existing indexes, so first drop them, rename the table and recreate them.
        for index in self.indexes_vec() {
            self.drop_index(tx, index.0)?;
        }
        // Rename our table to the target table name
        self.rename_table(tx, table_to_replace.table_name.as_str())?;
        // Update our table name to the target table name so the indexes are created correctly
        self.table_name.clone_from(&table_to_replace.table_name);
        // Recreate the indexes, now for our newly renamed table.
        for index in self.indexes_vec() {
            self.create_index(tx, index.0, index.1 == IndexType::Unique)?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn create_table(
        &self,
        transaction: &Transaction<'_>,
        primary_keys: Vec<String>,
    ) -> super::Result<()> {
        let create_table_statement =
            CreateTableBuilder::new(Arc::clone(&self.schema), &self.table_name)
                .primary_keys(primary_keys);
        let sql = create_table_statement.build_postgres();
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToCreateDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn drop_table(&self, transaction: &Transaction<'_>) -> super::Result<()> {
        let sql = format!(r#"DROP TABLE IF EXISTS "{}""#, self.table_name);
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToDropDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn rename_table(
        &self,
        transaction: &Transaction<'_>,
        new_table_name: &str,
    ) -> super::Result<()> {
        let sql = format!(
            r#"ALTER TABLE "{}" RENAME TO "{new_table_name}""#,
            self.table_name
        );
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToRenameDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn create_index(
        &self,
        transaction: &Transaction<'_>,
        columns: Vec<&str>,
        unique: bool,
    ) -> super::Result<()> {
        let mut index_builder = IndexBuilder::new(&self.table_name, columns);
        if unique {
            index_builder = index_builder.unique();
        }
        let sql = index_builder.build_postgres();
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToCreateIndexOnDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn drop_index(&self, transaction: &Transaction<'_>, columns: Vec<&str>) -> super::Result<()> {
        let index_name = IndexBuilder::new(&self.table_name, columns).index_name();

        let sql = format!(r#"DROP INDEX IF EXISTS "{index_name}""#);
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToDropIndexOnDuckDBTableSnafu)?;

        Ok(())
    }
}
