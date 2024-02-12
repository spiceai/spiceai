use snafu::prelude::*;
use std::{mem, sync::Arc};

use crate::dataupdate::{DataUpdate, UpdateType};
use arrow::record_batch::RecordBatch;
use datafusion::{
    datasource::MemTable,
    error::DataFusionError,
    execution::context::SessionContext,
    physical_plan::collect,
    sql::{
        parser::DFParser,
        sqlparser::{self, dialect::AnsiDialect},
        TableReference,
    },
};

use super::{BackendAsyncResult, DataBackend};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to add data"))]
    UnableToAddData { source: DataFusionError },

    #[snafu(display("Unable to execute SQL statement"))]
    UnableToExecuteSql { source: DataFusionError },

    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct MemTableBackend {
    ctx: Arc<SessionContext>,
    name: String,
}

impl MemTableBackend {
    #[must_use]
    pub fn new(ctx: Arc<SessionContext>, name: &str) -> Self {
        MemTableBackend {
            ctx,
            name: name.to_owned(),
        }
    }

    async fn sql(ctx: Arc<SessionContext>, sql: &str) -> Result<()> {
        let statements = DFParser::parse_sql_with_dialect(sql, &AnsiDialect {})
            .context(UnableToParseSqlSnafu)?;
        for statement in statements {
            let plan = ctx
                .state()
                .statement_to_plan(statement)
                .await
                .context(UnableToExecuteSqlSnafu)?;
            let df = ctx
                .execute_logical_plan(plan)
                .await
                .context(UnableToExecuteSqlSnafu)?;
            let physical_plan = df
                .create_physical_plan()
                .await
                .context(UnableToExecuteSqlSnafu)?;
            collect(physical_plan, ctx.task_ctx())
                .await
                .context(UnableToExecuteSqlSnafu)?;
        }
        Ok(())
    }
}

impl DataBackend for MemTableBackend {
    fn add_data(&self, data_update: DataUpdate) -> BackendAsyncResult {
        Box::pin(async move {
            if data_update.data.is_empty() {
                tracing::trace!("No data to add");
                return Ok(());
            }

            let mut table_update = MemTableUpdate {
                name: self.name.clone(),
                data: data_update.data,
                update_type: data_update.update_type,
                ctx: self.ctx.clone(),
            };

            table_update.update().await?;
            Ok(())
        })
    }
}

struct MemTableUpdate {
    name: String,
    data: Vec<RecordBatch>,
    update_type: UpdateType,
    ctx: Arc<SessionContext>,
}

fn temp_table_name(name: &str) -> String {
    format!("{name}_update")
}

impl MemTableUpdate {
    async fn update(&mut self) -> Result<()> {
        let schema = self.data[0].schema();
        let data = mem::take(&mut self.data);
        let table = MemTable::try_new(schema, vec![data]).context(UnableToAddDataSnafu)?;

        // There is probably a better way to do this than registering a temp table
        let temp_table_name = temp_table_name(&self.name);
        self.ctx
            .register_table(
                TableReference::bare(temp_table_name.clone()),
                Arc::new(table),
            )
            .context(UnableToAddDataSnafu)?;

        let sql_stmt = match self.update_type {
            UpdateType::Overwrite => format!(
                r#"CREATE OR REPLACE TABLE "{name}" AS SELECT * FROM "{temp_table_name}""#,
                name = self.name,
                temp_table_name = temp_table_name,
            ),
            UpdateType::Append => {
                self.create_table_if_not_exists()?;
                format!(
                    r#"INSERT INTO "{name}" SELECT * FROM "{temp_table_name}""#,
                    name = self.name,
                    temp_table_name = temp_table_name,
                )
            }
        };

        tracing::trace!("Inserting data with SQL: {sql_stmt}");
        MemTableBackend::sql(Arc::clone(&self.ctx), &sql_stmt).await?;

        Ok(())
    }

    fn create_table_if_not_exists(&self) -> Result<()> {
        let table_exists = self
            .ctx
            .table_exist(TableReference::bare(self.name.clone()))
            .unwrap_or(false);

        if !table_exists {
            let schema = self.data[0].schema();
            let table =
                MemTable::try_new(schema, vec![self.data.clone()]).context(UnableToAddDataSnafu)?;

            self.ctx
                .register_table(TableReference::bare(self.name.clone()), Arc::new(table))
                .context(UnableToAddDataSnafu)?;

            tracing::trace!("Created table {} in memory", self.name);
        };
        Ok(())
    }
}

impl Drop for MemTableUpdate {
    fn drop(&mut self) {
        let temp_table_name = TableReference::bare(temp_table_name(self.name.as_str()));
        let deregister_result = self.ctx.deregister_table(temp_table_name);
        if let Err(e) = deregister_result {
            tracing::error!("Error dropping temp table: {e:?}");
        }
    }
}
