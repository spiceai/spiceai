use datafusion::error::DataFusionError;
use datafusion::execution::{context::SessionContext, options::ParquetReadOptions};
use snafu::prelude::*;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to register parquet file"))]
    RegisterParquet { source: DataFusionError},
}

pub struct DataFusion {
    pub ctx: SessionContext,
}

impl DataFusion {
    #[must_use]
    pub fn new() -> Self {
        DataFusion { ctx: SessionContext::new() }
    }

    pub async fn register_parquet(&self, table_name: &str, path: &str) -> Result<()> {
        self.ctx.register_parquet(table_name, path, ParquetReadOptions::default())
            .await
            .context(RegisterParquetSnafu)
    }
}