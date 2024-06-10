use datafusion::logical_expr::Expr;
use snafu::prelude::*;
use sql_provider_datafusion::expr::{self, Engine};

pub mod indexes;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: expr::Error },
}

pub fn filters_to_sql(filters: &[Expr], engine: Option<Engine>) -> Result<String, Error> {
    Ok(filters
        .iter()
        .map(|expr| expr::to_sql_with_engine(expr, engine))
        .collect::<expr::Result<Vec<_>>>()
        .context(UnableToGenerateSQLSnafu)?
        .join(" AND "))
}
