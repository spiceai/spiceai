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

use async_trait::async_trait;
use datafusion::{
    catalog::Session, datasource::TableProvider, logical_expr::Expr, physical_plan::ExecutionPlan,
};
use datafusion_table_providers::{
    sql::sql_provider_datafusion::expr::Engine,
    sqlite::{write::SqliteTableWriter, Sqlite},
    util,
};
use rusqlite::Transaction;
use std::sync::Arc;

use crate::delete::{DeletionExec, DeletionSink, DeletionTableProvider};

#[async_trait]
impl DeletionTableProvider for SqliteTableWriter {
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(SqliteDeletionSink::new(self.sqlite(), filters)),
            &self.schema(),
        )))
    }
}

struct SqliteDeletionSink {
    sqlite: Arc<Sqlite>,
    filters: Vec<Expr>,
}

impl SqliteDeletionSink {
    fn new(sqlite: Arc<Sqlite>, filters: &[Expr]) -> Self {
        Self {
            sqlite,
            filters: filters.to_vec(),
        }
    }
}

#[async_trait]
impl DeletionSink for SqliteDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut db_conn = self.sqlite.connect().await?;
        let sqlite_conn = Sqlite::sqlite_conn(&mut db_conn)?;
        let sqlite = Arc::clone(&self.sqlite);
        let sql = util::filters_to_sql(&self.filters, Some(Engine::SQLite))?;

        let count: u64 = sqlite_conn
            .conn
            .call(move |conn| {
                let tx = conn.transaction()?;

                let count = delete_from(sqlite.table_name(), &tx, &sql)?;

                tx.commit()?;

                Ok(count)
            })
            .await?;

        Ok(count)
    }
}

fn delete_from(
    table_name: &str,
    transaction: &Transaction<'_>,
    where_clause: &str,
) -> rusqlite::Result<u64> {
    transaction.execute(
        format!(r#"DELETE FROM "{table_name}" WHERE {where_clause}"#).as_str(),
        [],
    )?;
    let count: u64 = transaction.query_row("SELECT changes()", [], |row| row.get(0))?;

    Ok(count)
}
