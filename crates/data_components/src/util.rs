use datafusion::logical_expr::Expr;
use snafu::prelude::*;
use sql_provider_datafusion::expr::{self, Engine};
use std::{collections::HashMap, hash::Hash};

pub mod column_reference;
pub mod constraints;
pub mod indexes;
pub mod on_conflict;

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

#[must_use]
pub fn hashmap_from_option_string<K, V>(hashmap_option_str: &str) -> HashMap<K, V>
where
    K: for<'a> From<&'a str> + Eq + Hash,
    V: for<'a> From<&'a str> + Default,
{
    hashmap_option_str
        .split(';')
        .map(|index| {
            let parts: Vec<&str> = index.split(':').collect();
            if parts.len() == 2 {
                (K::from(parts[0]), V::from(parts[1]))
            } else {
                (K::from(index), V::default())
            }
        })
        .collect()
}
