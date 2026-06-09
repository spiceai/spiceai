use datafusion::sql::sqlparser;
use datafusion_table_providers::duckdb::sql_table::DuckSqlExec;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::DuckDBParameter;
use duckdb::DuckdbConnectionManager;
use r2d2::PooledConnection;

pub mod aggregate_pushdown;
pub mod intermediate_index_cte;

pub type ConcreteDuckSqlExec =
    DuckSqlExec<PooledConnection<DuckdbConnectionManager>, DuckDBParameter>;
pub static PARSER_DIALECT: sqlparser::dialect::DuckDbDialect = sqlparser::dialect::DuckDbDialect {};
