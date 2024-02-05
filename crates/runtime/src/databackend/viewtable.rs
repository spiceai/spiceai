use std::{pin::Pin, sync::Arc, thread, time::Duration};

use datafusion::{
    datasource::ViewTable,
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
        // TODO: Tables are currently lazily created (i.e. not created until first data is received) so that we know the table schema.
        // This means that we can't create a view on top of a table until the first data is received for all dependent tables and therefore
        // the tables are created. We need a way to know that all dependent tables have been created and then create the view. For now, just sleep.
        thread::sleep(Duration::from_secs(1));

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

        let plan = ctx
            .state()
            .statement_to_plan(statements[0].clone())
            .await
            .context(UnableToCreateTableDataFusionSnafu)?;
        let view = ViewTable::try_new(plan, Some(sql.to_string())).context(UnableToAddDataSnafu)?;
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
