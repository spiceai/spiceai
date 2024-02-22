use std::{collections::HashMap, sync::Arc};

use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use postgres::types::ToSql;
use snafu::{prelude::*, ResultExt};
use tonic::transport::Channel;

use super::{DbConnectionPool, Mode, Result};
use crate::dbconnection::{postgresconn::PostgresConnection, DbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("PostgresError: {source}"))]
    PostgresError { source: postgres::Error },

    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: r2d2::Error },

    #[snafu(display("Unable to connect to endpoint: {source}"))]
    UnableToConnectToEndpoint { source: flight_client::tls::Error },

    #[snafu(display("Unable to connect to endpoint: {source}"))]
    ArrowError { source: arrow_schema::ArrowError },
}

pub struct PostgresConnectionPool {
    pool: FlightSqlServiceClient<Channel>,
}

#[async_trait]
impl DbConnectionPool<FlightSqlServiceClient<Channel>, &'static (dyn ToSql + Sync)>
    for PostgresConnectionPool
{
    async fn new(
        _name: &str,
        _mode: Mode,
        _params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Self> {
        let channel = flight_client::tls::new_tls_flight_channel("http://127.0.0.1:15432")
            .await
            .context(UnableToConnectToEndpointSnafu)?;
        let pool = FlightSqlServiceClient::new(channel);

        Ok(PostgresConnectionPool { pool })
    }

    fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<FlightSqlServiceClient<Channel>, &'static (dyn ToSql + Sync)>>>
    {
        Ok(Box::new(PostgresConnection::new(self.pool.clone())))
    }
}
