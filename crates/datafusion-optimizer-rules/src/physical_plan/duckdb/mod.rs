use datafusion::sql::sqlparser;

pub mod aggregate_pushdown;
pub mod intermediate_index_cte;

pub static PARSER_DIALECT: sqlparser::dialect::DuckDbDialect = sqlparser::dialect::DuckDbDialect {};
