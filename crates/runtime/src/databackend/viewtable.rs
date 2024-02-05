use std::{pin::Pin, sync::Arc, thread, time::Duration};

use datafusion::{
    datasource::ViewTable,
    error::DataFusionError,
    execution::context::SessionContext,
    sql::{parser::DFParser, sqlparser::dialect::AnsiDialect},
};
use futures_core::Future;
use snafu::prelude::*;

use super::{
    DataBackend, DataBackendType, Result, UnableToAddDataSnafu, UnableToCreateTableDataFusionSnafu,
    UnableToCreateTableSnafu, UnableToParseSqlSnafu, UnsupportedOperationSnafu,
};

pub struct ViewTableBackend {
    _ctx: Arc<SessionContext>,
    _name: String,
    _sql: String,
}

impl ViewTableBackend {
    pub async fn new(ctx: Arc<SessionContext>, name: &str, sql: &str) -> Result<Self> {
        let statements = DFParser::parse_sql_with_dialect(sql, &AnsiDialect {})
            .context(UnableToParseSqlSnafu)?;
        if statements.len() != 1 {
            return UnableToCreateTableSnafu {
                reason: format!(
                    "Expected 1 statement to create view, received {}",
                    statements.len()
                )
                .to_string(),
            }
            .fail();
        }

        let view: ViewTable;
        loop {
            let plan_result = ctx.state().statement_to_plan(statements[0].clone()).await;

            match plan_result {
                Ok(plan) => {
                    view = ViewTable::try_new(plan, Some(sql.to_string()))
                        .context(UnableToAddDataSnafu)?;
                    break;
                }
                Err(e) => match e {
                    DataFusionError::Plan(_) => {
                        tracing::error!(
                            "Plan error for {}, waiting 1 second and retrying: {}",
                            name,
                            e
                        );
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                    _ => return Err(e).context(UnableToCreateTableDataFusionSnafu),
                },
            }
        }

        ctx.register_table(name, Arc::new(view))
            .context(UnableToCreateTableDataFusionSnafu)?;

        Ok(ViewTableBackend {
            _ctx: ctx,
            _name: name.to_owned(),
            _sql: sql.to_owned(),
        })
    }
}

impl DataBackend for ViewTableBackend {
    fn add_data(
        &self,
        _data_update: crate::dataupdate::DataUpdate,
    ) -> Pin<Box<(dyn Future<Output = Result<()>> + Send + '_)>> {
        Box::pin(async move {
            UnsupportedOperationSnafu {
                operation: "add_data".to_string(),
                backend: DataBackendType::ViewTable,
            }
            .fail()
        })
    }
}
