use async_trait::async_trait;
use bb8_postgres::tokio_postgres::types::ToSql;
use bb8_postgres::tokio_postgres::NoTls;
use bb8_postgres::PostgresConnectionManager;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use snafu::prelude::*;
use sql_provider_datafusion::dbconnectionpool::postgrespool::PostgresConnectionPool;
use sql_provider_datafusion::dbconnectionpool::DbConnectionPool;
use sql_provider_datafusion::SqlTable;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use arrow::array::RecordBatch;
use spicepod::component::dataset::Dataset;

use crate::secrets::Secret;

use super::DataConnector;
use super::Result;
use super::UnableToGetTableProviderSnafu;

pub struct Postgres {
    pool: Arc<
        dyn DbConnectionPool<
                bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
                &'static (dyn ToSql + Sync),
            > + Send
            + Sync,
    >,
}

#[async_trait]
impl DataConnector for Postgres {
    fn new(
        _secret: Option<Secret>,
        _params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = Result<Self>> + Send>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let pool: Arc<
                dyn DbConnectionPool<
                        bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
                        &'static (dyn ToSql + Sync),
                    > + Send
                    + Sync,
            > = Arc::new(
                PostgresConnectionPool::new()
                    .await
                    .context(UnableToGetTableProviderSnafu)?,
            );

            Ok(Self { pool })
        })
    }

    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<RecordBatch>> + Send>> {
        let path = dataset.path().clone();
        let pool = Arc::clone(&self.pool);
        Box::pin(async move {
            let conn = match pool.connect().await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::error!("Failed to connect to Postgres: {:?}", e);
                    return vec![];
                }
            };

            let Some(async_conn) = conn.as_async() else {
                tracing::error!("Failed to convert postgres conn to async connection",);
                return vec![];
            };

            let record_batch_stream = match async_conn
                .query_arrow(format!("SELECT * FROM {path}").as_str(), &[])
                .await
            {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!("Failed to query Postgres: {:?}", e);
                    return vec![];
                }
            };

            let recs: Vec<RecordBatch> =
                match record_batch_stream.try_collect::<Vec<RecordBatch>>().await {
                    Ok(recs) => recs,
                    Err(e) => {
                        tracing::error!("Failed to collect record batches from Postgres: {:?}", e);
                        return vec![];
                    }
                };

            recs
        })
    }

    fn has_table_provider(&self) -> bool {
        true
    }

    async fn get_table_provider(
        &self,
        dataset: &Dataset,
    ) -> Result<Arc<dyn TableProvider + 'static>> {
        let pool = Arc::clone(&self.pool);
        let table_provider = SqlTable::new(&pool, TableReference::bare(dataset.path()))
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(UnableToGetTableProviderSnafu)?;

        Ok(Arc::new(table_provider))
    }
}
