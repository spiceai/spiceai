use datafusion::logical_expr::Expr;
use snafu::prelude::*;
use sql_provider_datafusion::expr;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: expr::Error },
}

pub fn filters_to_sql(filters: &[Expr]) -> Result<String, Error> {
    Ok(filters
        .iter()
        .map(expr::to_sql)
        .collect::<expr::Result<Vec<_>>>()
        .context(UnableToGenerateSQLSnafu)?
        .join(" AND "))
}
