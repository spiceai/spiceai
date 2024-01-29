use snafu::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::dataupdate::DataUpdate;
use arrow::record_batch::RecordBatch;
use datafusion::{
    datasource::MemTable,
    execution::context::SessionContext,
    physical_plan::collect,
    sql::{parser::DFParser, sqlparser::dialect::AnsiDialect, TableReference},
};

use super::{DataBackend, Result, UnableToAddDataSnafu, UnableToParseSqlSnafu};

pub struct MemTableBackend {
    ctx: Arc<SessionContext>,
    name: String,
}

impl MemTableBackend {
    #[must_use]
    pub fn new(ctx: Arc<SessionContext>, name: &str) -> Self {
        MemTableBackend {
            ctx,
            name: name.replace('.', "_").to_string(),
        }
    }
}

impl DataBackend for MemTableBackend {
    fn add_data(
        &self,
        data_update: DataUpdate,
    ) -> Pin<Box<(dyn Future<Output = Result<()>> + Send + '_)>> {
        Box::pin(async move {
            if data_update.data.is_empty() {
                tracing::trace!(
                    "No data to add for log sequence number {log_sequence_number:?}",
                    log_sequence_number = data_update.log_sequence_number
                );
                return Ok(());
            }

            let table_exists = self
                .ctx
                .table_exist(TableReference::from(self.name.clone()))
                .unwrap_or(false);

            if !table_exists {
                tracing::trace!(
                    "Creating table for log sequence number {log_sequence_number:?}",
                    log_sequence_number = data_update.log_sequence_number
                );
                let schema = data_update.data[0].schema();
                let table = MemTable::try_new(schema, vec![data_update.data])
                    .context(UnableToAddDataSnafu)?;

                self.ctx
                    .register_table(TableReference::from(self.name.clone()), Arc::new(table))
                    .context(UnableToAddDataSnafu)?;

                tracing::trace!(
                    "Created table for log sequence number {log_sequence_number:?}",
                    log_sequence_number = data_update.log_sequence_number
                );
                return Ok(());
            }

            let log_sequence_number = data_update.log_sequence_number.unwrap_or_default();

            let table_insert = MemTableInsert {
                log_sequence_number,
                name: self.name.clone(),
                ctx: self.ctx.clone(),
            };

            table_insert.insert(data_update.data).await?;
            Ok(())
        })
    }
}

struct MemTableInsert {
    log_sequence_number: u64,
    name: String,
    ctx: Arc<SessionContext>,
}

impl MemTableInsert {
    fn temp_table_name(name: &str, log_sequence_number: u64) -> String {
        format!("{name}_{log_sequence_number}")
    }

    async fn insert(&self, data: Vec<RecordBatch>) -> Result<()> {
        let schema = data[0].schema();
        let table = MemTable::try_new(schema, vec![data]).context(UnableToAddDataSnafu)?;

        // There is probably a better way to do this than registering a temp table
        let temp_table_name = MemTableInsert::temp_table_name(&self.name, self.log_sequence_number);
        self.ctx
            .register_table(
                TableReference::from(temp_table_name.clone()),
                Arc::new(table),
            )
            .context(UnableToAddDataSnafu)?;

        let sql_stmt = format!(
            r#"INSERT INTO "{name}" SELECT * FROM "{temp_table_name}""#,
            name = self.name,
        );
        tracing::trace!("Inserting data with SQL: {sql_stmt}");
        let statements = DFParser::parse_sql_with_dialect(&sql_stmt, &AnsiDialect {})
            .context(UnableToParseSqlSnafu)?;
        for statement in statements {
            let plan = self
                .ctx
                .state()
                .statement_to_plan(statement)
                .await
                .context(UnableToAddDataSnafu)?;
            let df = self
                .ctx
                .execute_logical_plan(plan)
                .await
                .context(UnableToAddDataSnafu)?;
            let physical_plan = df
                .create_physical_plan()
                .await
                .context(UnableToAddDataSnafu)?;
            let task_ctx = self.ctx.task_ctx();
            collect(physical_plan, task_ctx.clone())
                .await
                .context(UnableToAddDataSnafu)?;
        }

        Ok(())
    }
}

impl Drop for MemTableInsert {
    fn drop(&mut self) {
        let temp_table_name = TableReference::from(MemTableInsert::temp_table_name(
            self.name.as_str(),
            self.log_sequence_number,
        ));
        let deregister_result = self.ctx.deregister_table(temp_table_name);
        if let Err(e) = deregister_result {
            tracing::error!("Error dropping temp table: {e:?}");
        }
    }
}
