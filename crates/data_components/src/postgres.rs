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
    sql::TableReference,
};
use std::sync::Arc;
use tokio_postgres::Transaction;

use crate::{
    Read, ReadWrite,
    delete::{DeletionExec, DeletionSink, DeletionTableProvider},
};

use datafusion_table_providers::{
    postgres::{Postgres, PostgresTableFactory, write::PostgresTableWriter},
    sql::sql_provider_datafusion::expr,
};

#[async_trait]
impl Read for PostgresTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        self.table_provider(table_reference).await
    }
}

#[async_trait]
impl ReadWrite for PostgresTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        self.read_write_table_provider(table_reference).await
    }
}

#[async_trait]
impl DeletionTableProvider for PostgresTableWriter {
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(PostgresDeletionSink::new(self.postgres(), filters)),
            &self.schema(),
        )))
    }
}

struct PostgresDeletionSink {
    postgres: Arc<Postgres>,
    filters: Vec<Expr>,
}

impl PostgresDeletionSink {
    fn new(postgres: Arc<Postgres>, filters: &[Expr]) -> Self {
        Self {
            postgres,
            filters: filters.to_vec(),
        }
    }
}

#[expect(clippy::cast_sign_loss)]
async fn delete_from(
    table_name: &str,
    transaction: &Transaction<'_>,
    where_clause: &str,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let row = transaction
        .query_one(
            format!(
                r#"WITH deleted AS (DELETE FROM "{table_name}" WHERE {where_clause} RETURNING *) SELECT COUNT(*) FROM deleted"#,
            )
            .as_str(),
            &[],
        )
        .await?;

    let deleted: i64 = row.get(0);

    Ok(deleted as u64)
}

#[async_trait]
impl DeletionSink for PostgresDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut db_conn = self.postgres.connect().await?;
        let postgres_conn = Postgres::postgres_conn(&mut db_conn)?;
        let tx = postgres_conn.conn.transaction().await?;
        // When filters is empty, return 0 to prevent accidental full table deletion.
        // This is intentional - callers must provide explicit filters for deletion.
        let count = if self.filters.is_empty() {
            0
        } else {
            let sql_filters: Result<Vec<String>, _> = self
                .filters
                .iter()
                .map(|f| expr::to_sql_with_engine(f, None))
                .collect();
            let sql_where = sql_filters
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
                .join(" AND ");
            delete_from(self.postgres.table_name(), &tx, &sql_where).await?
        };
        tx.commit().await?;

        Ok(count)
    }
}
