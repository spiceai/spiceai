use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::sync::Arc;

use crate::{
    datapublisher::{AddDataResult, DataPublisher},
    dataupdate::{DataUpdate, UpdateType},
};
use arrow::record_batch::RecordBatch;
use datafusion::{
    datasource::MemTable,
    error::DataFusionError,
    execution::context::SessionContext,
    physical_plan::collect,
    sql::{
        parser::DFParser,
        sqlparser::{self, dialect::PostgreSqlDialect},
        TableReference,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to add data: {source}"))]
    UnableToAddData { source: DataFusionError },

    #[snafu(display("Unable to parse SQL: {source}"))]
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
}

impl DataPublisher for MemTableBackend {
    fn add_data(&self, _dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult {
        Box::pin(async move {
            if data_update.data.is_empty() {
                tracing::trace!("No data to add");
                return Ok(());
            }

            let table_update = MemTableUpdate {
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

impl MemTableUpdate {
    fn temp_table_name(name: &str) -> String {
        format!("{name}_temp")
    }

    async fn update(&self) -> Result<()> {
        let temp_table_name = MemTableUpdate::temp_table_name(&self.name);
        let sql_stmt = match self.update_type {
            UpdateType::Overwrite => format!(
                r#"CREATE OR REPLACE TABLE "{name}" AS SELECT * FROM "{temp_table_name}""#,
                name = self.name,
                temp_table_name = temp_table_name,
            ),
            UpdateType::Append => {
                let table_created = self.create_table_if_not_exists()?;
                // If the table was created then it will have been populated with the data.
                if table_created {
                    return Ok(());
                }

                format!(
                    r#"INSERT INTO "{name}" SELECT * FROM "{temp_table_name}""#,
                    name = self.name,
                    temp_table_name = temp_table_name,
                )
            }
        };

        // There is probably a better way to do this than registering a temp table
        let schema = self.data[0].schema();
        let table =
            MemTable::try_new(schema, vec![self.data.clone()]).context(UnableToAddDataSnafu)?;
        self.ctx
            .register_table(
                TableReference::bare(temp_table_name.clone()),
                Arc::new(table),
            )
            .context(UnableToAddDataSnafu)?;

        tracing::trace!("Inserting data with SQL: {sql_stmt}");
        let statements = DFParser::parse_sql_with_dialect(&sql_stmt, &PostgreSqlDialect {})
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

    fn create_table_if_not_exists(&self) -> Result<bool> {
        let table_exists = self
            .ctx
            .table_exist(TableReference::bare(self.name.clone()))
            .unwrap_or(false);

        if !table_exists {
            tracing::trace!("Creating table");
            let schema = self.data[0].schema();
            let table =
                MemTable::try_new(schema, vec![self.data.clone()]).context(UnableToAddDataSnafu)?;

            self.ctx
                .register_table(TableReference::bare(self.name.clone()), Arc::new(table))
                .context(UnableToAddDataSnafu)?;

            tracing::trace!("Created table");

            return Ok(true);
        };
        Ok(false)
    }
}

impl Drop for MemTableUpdate {
    fn drop(&mut self) {
        let temp_table_name =
            TableReference::bare(MemTableUpdate::temp_table_name(self.name.as_str()));
        let deregister_result = self.ctx.deregister_table(temp_table_name);
        if let Err(e) = deregister_result {
            tracing::error!("Error dropping temp table: {e:?}");
        }
    }
}
