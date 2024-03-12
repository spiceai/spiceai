use std::{
    collections::HashMap,
    fmt, mem,
    sync::{Arc, PoisonError},
};

use arrow::{record_batch::RecordBatch};
use datafusion::{execution::context::SessionContext, sql::TableReference};
use duckdb::{vtab::arrow::arrow_recordbatch_to_query_params, DuckdbConnectionManager, Row, ToSql};
use snafu::{prelude::*, ResultExt};
use spicepod::component::dataset::Dataset;
use sql_provider_datafusion::{
    dbconnection::{self, duckdbconn::DuckDbConnection, DbConnection},
    dbconnectionpool::{duckdbpool::DuckDbConnectionPool, DbConnectionPool, Mode},
    SqlTable,
};

use crate::{
    datapublisher::{AddDataResult, DataPublisher},
    dataupdate::{DataUpdate, UpdateType},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: sql_provider_datafusion::dbconnection::Error,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError {
        source: sql_provider_datafusion::dbconnectionpool::Error,
    },

    #[snafu(display("DuckDBError: {source}"))]
    DuckDB { source: duckdb::Error },

    #[snafu(display("DuckDBDataFusionError: {source}"))]
    DuckDBDataFusion {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("DataFusionError: {source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Lock is poisoned: {message}"))]
    LockPoisoned { message: String },

    #[snafu(display("Unable to downcast DbConnection to DuckDbConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("Unable to convert DuckDB VARCHAR bytes value: {varchar}"))]
    UnableToConvertBytes{varchar: String},
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDBBackend {
    ctx: Arc<SessionContext>,
    name: String,
    pool: Arc<
        dyn DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>
            + Send
            + Sync,
    >,
    create_mutex: std::sync::Mutex<()>,
    _primary_keys: Option<Vec<String>>,
}

impl DataPublisher for DuckDBBackend {
    fn add_data(&self, _dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult {
        let pool = Arc::clone(&self.pool);
        let name = self.name.clone();
        Box::pin(async move {
            let mut conn = pool.connect().await.context(DbConnectionPoolSnafu)?;
            let Some(conn) = conn.as_any_mut().downcast_mut::<DuckDbConnection>() else {
                return Err(
                    Box::new(Error::UnableToDowncastDbConnection {}) as Box<dyn std::error::Error>
                );
            };

            let mut duckdb_update = DuckDBUpdate {
                name,
                data: data_update.data,
                update_type: data_update.update_type,
                duckdb_conn: conn,
                create_mutex: &self.create_mutex,
            };

            duckdb_update.update()?;

            self.initialize_datafusion().await?;
            Ok(())
        })
    }

    fn name(&self) -> &str {
        "DuckDB"
    }
}

impl DuckDBBackend {
    #[allow(clippy::needless_pass_by_value)]
    pub async fn new(
        ctx: Arc<SessionContext>,
        name: &str,
        mode: Mode,
        params: Arc<Option<HashMap<String, String>>>,
        primary_keys: Option<Vec<String>>,
    ) -> Result<Self> {
        let pool = DuckDbConnectionPool::new(name, mode, params)
            .await
            .context(DbConnectionPoolSnafu)?;
        Ok(DuckDBBackend {
            ctx,
            name: name.to_string(),
            pool: Arc::new(pool),
            create_mutex: std::sync::Mutex::new(()),
            _primary_keys: primary_keys,
        })
    }

    async fn initialize_datafusion(&self) -> Result<()> {
        let table_exists = self
            .ctx
            .table_exist(TableReference::bare(self.name.clone()))
            .context(DataFusionSnafu)?;
        if table_exists {
            return Ok(());
        }

        let table = match SqlTable::new(&self.pool, TableReference::bare(self.name.clone()))
            .await
            .context(DuckDBDataFusionSnafu)
        {
            Ok(table) => table,
            Err(e) => return Err(e),
        };

        self.ctx
            .register_table(&self.name, Arc::new(table))
            .context(DataFusionSnafu)?;

        Ok(())
    }
}

struct DuckDBUpdate<'a> {
    name: String,
    data: Vec<RecordBatch>,
    update_type: UpdateType,
    duckdb_conn: &'a mut dbconnection::duckdbconn::DuckDbConnection,
    create_mutex: &'a std::sync::Mutex<()>,
}

impl<'a> DuckDBUpdate<'a> {
    fn update(&mut self) -> Result<()> {
        match self.update_type {
            UpdateType::Overwrite => self.create_table(true)?,
            UpdateType::Append => {
                if !self.table_exists() {
                    self.create_table(false)?;
                }
            }
        };

        let data = mem::take(&mut self.data);
        for batch in data {
            self.insert_batch(batch)?;
        }

        self.monitor_duckdb()?;
        tracing::trace!("Processed update to DuckDB table {name}", name = self.name,);

        Ok(())
    }

    fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let params = arrow_recordbatch_to_query_params(batch);
        let sql = format!(
            r#"INSERT INTO "{name}" SELECT * FROM arrow(?, ?)"#,
            name = self.name
        );
        tracing::trace!("{sql}");

        self.duckdb_conn
            .execute(
                &sql,
                &params.iter().map(|p| p as &dyn ToSql).collect::<Vec<_>>(),
            )
            .context(DbConnectionSnafu)?;

        Ok(())
    }

    fn create_table(&mut self, drop_if_exists: bool) -> Result<()> {
        let _lock = self.create_mutex.lock().map_err(handle_poison)?;

        if self.table_exists() {
            if drop_if_exists {
                let sql = format!(r#"DROP TABLE "{}""#, self.name);
                tracing::trace!("{sql}");
                self.duckdb_conn
                    .execute(&sql, &[])
                    .context(DbConnectionSnafu)?;
            } else {
                return Ok(());
            }
        }

        let Some(batch) = self.data.pop() else {
            return Ok(());
        };

        let arrow_params = arrow_recordbatch_to_query_params(batch);
        let sql = format!(
            r#"CREATE TABLE "{name}" AS SELECT * FROM arrow(?, ?)"#,
            name = self.name
        );
        tracing::trace!("{sql}");

        self.duckdb_conn
            .execute(
                &sql,
                &arrow_params
                    .iter()
                    .map(|p| p as &dyn ToSql)
                    .collect::<Vec<_>>(),
            )
            .context(DbConnectionSnafu)?;

        Ok(())
    }

    fn table_exists(&self) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables 
              WHERE table_name = '{name}'
            )"#,
            name = self.name
        );
        tracing::trace!("{sql}");

        self.duckdb_conn
            .conn
            .query_row(&sql, [], |row| row.get::<usize, bool>(0))
            .unwrap_or(false)
    }


    fn monitor_duckdb(&self) -> Result<()> {
        self.duckdb_conn.conn.query_row("select database_size, wal_size, memory_usage, memory_limit FROM pragma_database_size();", [], |row| {
            log_bytes_measure_from_db_row(row, 0, "database_size");
            log_bytes_measure_from_db_row(row, 1, "wal_size");
            log_bytes_measure_from_db_row(row, 2, "memory_usage");
            log_bytes_measure_from_db_row(row, 3, "memory_limit");
            Ok(())
        }).context(DuckDBSnafu)
    }
}

fn log_bytes_measure_from_db_row(r: &Row, i: usize, key_name: &'static str) {
    tracing::warn!("loading {} at i={}", key_name, i);
    match r.get::<usize, String>(i) {
        Ok(measure_bytes_str) => {
            tracing::warn!("  Got {}", measure_bytes_str);
            match pg_size_bytes(measure_bytes_str.as_str()) {
                Ok(bytes) => metrics::gauge!(key_name).set(bytes),
                Err(e) => tracing::warn!("couldn't parse DuckDB measures: {}", e)
            }
        },
        Err(e) => {
            tracing::warn!("failed to get {}", e)
        }
    };
}

#[allow(clippy::needless_pass_by_value)]
fn handle_poison<T: fmt::Debug>(e: PoisonError<T>) -> Error {
    Error::LockPoisoned {
        message: format!("{e:?}"),
    }
}

/// Converts disk sizes from a string format to a floating-point number representing the size in bytes.
///
/// This function is designed to parse string representations of disk sizes (as often found in database formats like DuckDB)
/// and convert them into a numeric format (`f64`) that represents the size in bytes. It supports conversions for a range of units
/// from bytes (B) up to yobibytes (YiB), adhering to the binary (IEC) standard where 1 KiB = 1024 bytes.
///
/// # Arguments
///
/// * `size_str` - A string slice that holds the size to be converted. The expected format is "<number> <unit>",
/// where <unit> can be KiB, MiB..., or KB, MB, ... (case insensitive).
///
/// # Returns
///
/// This function returns a `Result<f64>`. On success, it provides the size in bytes as an `f64`. On failure,
/// it returns an `Error::UnableToConvertBytes` variant, indicating either an unsupported format or a parsing error.
///
/// # Examples
///
/// Basic usage:
///
/// ```
/// let size_in_bytes = pg_size_bytes("1 GiB").unwrap();
/// assert_eq!(size_in_bytes, 1073741824.0);
///
/// let size_in_bytes = pg_size_bytes("1024 KiB").unwrap();
/// assert_eq!(size_in_bytes, 1048576.0);
///
/// let error = pg_size_bytes("10 megabytes").unwrap_err();
/// // This will return an error due to the unsupported unit "megabytes"
/// ```
///
/// # Errors
///
/// This function will return an `Error::UnableToConvertBytes` if:
///
/// - The input string does not adhere to the expected format ("<number> <unit>").
/// - The number cannot be parsed into a floating-point number.
/// - The unit is not one of the supported IEC binary units (B, KiB, MiB, GiB, TiB, PiB, EiB, ZiB, YiB).
///
/// Note: The function handles the special case of "0 bytes" directly, returning `0.0` without error.
///
fn pg_size_bytes(size_str: &str) -> Result<f64> {
    if size_str == "0 bytes" { // Edge case
        return Ok(0.0)
    }
    let mut chars = size_str.chars().peekable();
    let mut number_str = String::new();
    while let Some(&c) = chars.peek() {
        if c.is_digit(10) || c == '.' {
            number_str.push(chars.next().unwrap());
        } else {
            break;
        }
    }
    let number: f64 = match number_str.parse() {
        Ok(num) => num,
        Err(_) => return Err(Error::UnableToConvertBytes{varchar: size_str.to_string()}),
    };

    let bytes = match chars.collect::<String>().to_uppercase().as_str() {
        "B" => number,
        "KB" => number * 1000.0,
        "MB" => number * 1000.0_f64.powi(2),
        "GB" => number * 1000.0_f64.powi(3),
        "TB" => number * 1000.0_f64.powi(4),
        "PB" => number * 1000.0_f64.powi(5),
        "EB" => number * 1000.0_f64.powi(6),
        "ZB" => number * 1000.0_f64.powi(7),
        "YB" => number * 1000.0_f64.powi(8),
        "KIB" => number * 1024.0,
        "MIB" => number * 1024.0_f64.powi(2),
        "GIB" => number * 1024.0_f64.powi(3),
        "TIB" => number * 1024.0_f64.powi(4),
        "PIB" => number * 1024.0_f64.powi(5),
        "EIB" => number * 1024.0_f64.powi(6),
        "ZIB" => number * 1024.0_f64.powi(7),
        "YIB" => number * 1024.0_f64.powi(8),
        _ => return Err(Error::UnableToConvertBytes{varchar: size_str.to_string()}),
    };
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn test_add_data() {
        let ctx = Arc::new(SessionContext::new());
        let name = "test_add_data";
        let backend =
            DuckDBBackend::new(Arc::clone(&ctx), name, Mode::Memory, Arc::new(None), None)
                .await
                .expect("Unable to create DuckDBBackend");

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let data = if let Ok(batch) = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            ],
        ) {
            vec![batch]
        } else {
            panic!("Unable to create record batch");
        };
        let data_update = DataUpdate {
            data,
            update_type: UpdateType::Overwrite,
        };

        let dataset = Arc::new(Dataset::new("test".to_string(), "test".to_string()));

        backend
            .add_data(dataset, data_update)
            .await
            .expect("Unable to add data");

        let df = ctx
            .sql("SELECT * FROM test_add_data")
            .await
            .expect("Unable to execute query");
        let _ = df.show().await;
    }
}
