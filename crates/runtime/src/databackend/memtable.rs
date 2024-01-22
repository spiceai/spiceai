use snafu::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::{datasource::MemTable, execution::context::SessionContext, sql::TableReference};

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
                let schema = data[0].schema();
                let table =
                    MemTable::try_new(schema, vec![data]).context(super::UnableToAddDataSnafu)?;

                self.ctx
                    .register_table(TableReference::from(self.name.clone()), Arc::new(table))
                    .context(super::UnableToAddDataSnafu)?;

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

        let temp_table_name = MemTableInsert::temp_table_name(&self.name, self.log_sequence_number);
        // Register the arrow records as a temporary table
        self.ctx
            .register_table(
                TableReference::from(temp_table_name.clone()),
                Arc::new(table),
            )
            .context(super::UnableToAddDataSnafu)?;

        // Insert the data into the main table
        self.ctx
            .sql(&format!(
                "INSERT INTO {name} SELECT * FROM {temp_table_name}",
                name = self.name,
                temp_table_name = temp_table_name
            ))
            .await
            .context(super::UnableToAddDataSnafu)?;
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
