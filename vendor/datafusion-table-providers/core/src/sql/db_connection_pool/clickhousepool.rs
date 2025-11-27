use std::collections::HashMap;

use clickhouse::{Client, Compression};
use secrecy::{ExposeSecret, SecretString};
use snafu::{ResultExt, Snafu};

use super::{dbconnection::DbConnection, DbConnectionPool, JoinPushDown};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ClickHouse connection failed. {source}"))]
    ConnectionError { source: clickhouse::error::Error },

    #[snafu(display("Invalid connection string for ClickHouse. {source}"))]
    InvalidConnectionString { source: url::ParseError },

    #[snafu(display("Invalid value for parameter {parameter_name}. Ensure the value is valid for parameter {parameter_name}"))]
    InvalidParameterError { parameter_name: String },
}

#[derive(Clone)]
pub struct ClickHouseConnectionPool {
    pub client: Client,
    pub join_push_down: JoinPushDown,
}

impl std::fmt::Debug for ClickHouseConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseConnectionPool")
            .field("join_push_down", &self.join_push_down)
            .finish()
    }
}

impl ClickHouseConnectionPool {
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self, Error> {
        let mut client = Client::default();
        let mut url = None;
        let mut database = None;

        for (key, value) in &params {
            let value = value.expose_secret();
            match key.as_str() {
                "url" => {
                    client = client.with_url(value);
                    url = Some(value)
                }
                "database" => {
                    client = client.with_database(value);
                    database = Some(value)
                }
                "user" => {
                    client = client.with_user(value);
                }
                "password" => {
                    client = client.with_password(value);
                }
                "access_token" => {
                    client = client.with_access_token(value);
                }
                "compression" => {
                    client = match value.to_lowercase().as_str() {
                        "lz4" => client.with_compression(Compression::Lz4),
                        "none" => client.with_compression(Compression::None),
                        other => {
                            return Err(Error::InvalidParameterError {
                                parameter_name: format!("compression = {}", other),
                            });
                        }
                    };
                }
                key if key.starts_with("option_") => {
                    let opt = &key["option_".len()..];
                    client = client.with_option(opt, value);
                }
                key if key.starts_with("header_") => {
                    let header = &key["header_".len()..];
                    client = client.with_header(header, value);
                }
                key if key.starts_with("product_") => {
                    let name = &key["product_".len()..];
                    client = client.with_product_info(name, value);
                }
                _ => {
                    // Unknown keys are ignored silently or optionally warn
                }
            }
        }

        client
            .query("SELECT 1")
            .fetch_all::<u8>()
            .await
            .context(ConnectionSnafu)?;

        let join_push_down = {
            let mut ctx = format!("url={}", url.unwrap_or("default"));
            if let Some(db) = database {
                ctx.push_str(&format!(",db={}", db));
            }
            JoinPushDown::AllowedFor(ctx)
        };

        Ok(Self {
            client,
            join_push_down,
        })
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }
}

#[async_trait::async_trait]
impl DbConnectionPool<Client, ()> for ClickHouseConnectionPool {
    async fn connect(&self) -> super::Result<Box<dyn DbConnection<Client, ()>>> {
        Ok(Box::new(self.client()))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}
