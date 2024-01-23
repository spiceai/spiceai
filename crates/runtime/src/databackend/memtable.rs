use snafu::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::{
    datasource::MemTable,
    execution::context::SessionContext,
    physical_plan::collect,
    sql::{parser::DFParser, sqlparser::dialect::AnsiDialect, TableReference},
};

pub struct MemTableBackend {
    ctx: Arc<SessionContext>,
    name: String,
    table_created: bool,
}

impl MemTableBackend {
    #[must_use]
    pub fn new(ctx: Arc<SessionContext>, name: &str) -> Self {
        MemTableBackend {
            ctx,
            name: name.to_string(),
            table_created: false,
        }
    }
}

impl super::DataBackend for MemTableBackend {
    fn add_data(
        &mut self,
        log_sequence_number: u64,
        data: Vec<RecordBatch>,
    ) -> Pin<Box<(dyn Future<Output = super::Result<()>> + Send + '_)>> {
        Box::pin(async move {
            if data.is_empty() {
                tracing::trace!("No data to add for log sequence number {log_sequence_number}");
                return Ok(());
            }

            if !self.table_created {
                tracing::trace!("Creating table for log sequence number {log_sequence_number}");
                let schema = data[0].schema();
                let table =
                    MemTable::try_new(schema, vec![data]).context(super::UnableToAddDataSnafu)?;

                self.ctx
                    .register_table(TableReference::from(self.name.clone()), Arc::new(table))
                    .context(super::UnableToAddDataSnafu)?;

                tracing::trace!("Created table for log sequence number {log_sequence_number}");
                self.table_created = true;
                return Ok(());
            }

            let table_insert = MemTableInsert {
                log_sequence_number,
                name: self.name.clone(),
                ctx: self.ctx.clone(),
            };

            table_insert.insert(data).await?;
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

    async fn insert(&self, data: Vec<RecordBatch>) -> super::Result<()> {
        let schema = data[0].schema();
        let table = MemTable::try_new(schema, vec![data]).context(super::UnableToAddDataSnafu)?;

        // There is probably a better way to do this than registering a temp table
        let temp_table_name = MemTableInsert::temp_table_name(&self.name, self.log_sequence_number);
        self.ctx
            .register_table(
                TableReference::from(temp_table_name.clone()),
                Arc::new(table),
            )
            .context(super::UnableToAddDataSnafu)?;

        let sql_stmt = format!(
            r#"INSERT INTO "{name}" SELECT * FROM "{temp_table_name}""#,
            name = self.name,
        );
        tracing::trace!("Inserting data with SQL: {sql_stmt}");
        let statements = DFParser::parse_sql_with_dialect(&sql_stmt, &AnsiDialect {})
            .context(super::UnableToParseSqlSnafu)?;
        for statement in statements {
            let plan = self
                .ctx
                .state()
                .statement_to_plan(statement)
                .await
                .context(super::UnableToAddDataSnafu)?;
            let df = self
                .ctx
                .execute_logical_plan(plan)
                .await
                .context(super::UnableToAddDataSnafu)?;
            let physical_plan = df
                .create_physical_plan()
                .await
                .context(super::UnableToAddDataSnafu)?;
            let task_ctx = self.ctx.task_ctx();
            collect(physical_plan, task_ctx.clone())
                .await
                .context(super::UnableToAddDataSnafu)?;
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
