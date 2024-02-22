use std::{collections::HashMap, str::FromStr, sync::Arc};

use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use postgres::types::ToSql;
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use snafu::{prelude::*, ResultExt};
use tonic::transport::Endpoint;

use super::{DbConnectionPool, Mode, Result};
use crate::dbconnection::{postgresconn::PostgresConnection, DbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("PostgresError: {source}"))]
    PostgresError { source: postgres::Error },

    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: r2d2::Error },

    #[snafu(display("Unable to connect to endpoint: {source}"))]
    UnableToConnectToEndpoint { source: tonic::transport::Error },
}

pub struct PostgresConnectionPool {
    pool: Arc<r2d2::Pool<PostgresConnectionManager<NoTls>>>,
    flight_sql_client: FlightSqlServiceClient<tonic::transport::Channel>,
}

#[async_trait]
impl DbConnectionPool<PostgresConnectionManager<NoTls>, &'static (dyn ToSql + Sync)>
    for PostgresConnectionPool
{
    async fn new(
        _name: &str,
        _mode: Mode,
        _params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Self> {
        let parsed_config = "host=localhost user=postgres"
            .parse()
            .context(PostgresSnafu)?;
        let manager = PostgresConnectionManager::new(parsed_config, NoTls);
        let pool = Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?);

        let channel = Endpoint::from_str("grpc://127.0.0.1:15432")
            .context(UnableToConnectToEndpointSnafu)?
            .connect()
            .await
            .context(UnableToConnectToEndpointSnafu)?;
        let flight_sql_client = FlightSqlServiceClient::new(channel);

        Ok(PostgresConnectionPool {
            pool,
            flight_sql_client,
        })
    }

    fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<PostgresConnectionManager<NoTls>, &'static (dyn ToSql + Sync)>>>
    {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get().context(ConnectionPoolSnafu)?;
        Ok(Box::new(PostgresConnection::new(
            conn,
            Some(self.flight_sql_client.clone()),
        )))
    }
}
