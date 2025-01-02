/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use async_trait::async_trait;
use bb8::Pool;
use snafu::ResultExt;
use tiberius::{Client, Config};
use tokio::net::TcpStream;

use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use super::SqlServerAccessSnafu;

pub type SqlServerConnectionPool = Pool<SqlServerConnectionManager>;

#[derive(Clone, Debug)]
pub struct SqlServerConnectionManager {
    config: Config,
}

impl SqlServerConnectionManager {
    fn new(config: Config) -> SqlServerConnectionManager {
        Self { config }
    }

    pub async fn create(config: Config) -> super::Result<SqlServerConnectionPool> {
        let manager = SqlServerConnectionManager::new(config);
        let pool = bb8::Pool::builder()
            .build(manager)
            .await
            .context(SqlServerAccessSnafu)?;
        Ok(pool)
    }
}

#[async_trait]
impl bb8::ManageConnection for SqlServerConnectionManager {
    type Connection = Client<Compat<TcpStream>>;
    type Error = tiberius::error::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let tcp = TcpStream::connect(&self.config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        Client::connect(self.config.clone(), tcp.compat_write()).await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.simple_query("SELECT 1").await?.into_row().await?;
        Ok(())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
