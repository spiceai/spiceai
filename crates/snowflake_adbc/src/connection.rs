/*
Copyright 2025 The Spice.ai OSS Authors

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

use crate::client::SnowflakeClient;
use crate::error::Result;
use crate::statement::Statement;
use crate::Database;
use std::sync::Arc;

/// An active connection to Snowflake.
///
/// Connections are not thread-safe but can be used from multiple threads
/// with proper synchronization.
#[derive(Debug, Clone)]
pub struct Connection {
    client: Arc<SnowflakeClient>,
    database: Database,
}

impl Connection {
    pub(crate) async fn new(database: Database) -> Result<Self> {
        let client = SnowflakeClient::new(database.account()).await?;

        client
            .authenticate(
                database.auth_config(),
                database.warehouse(),
                database.database(),
                database.schema(),
                database.role(),
            )
            .await?;

        Ok(Self {
            client: Arc::new(client),
            database,
        })
    }

    /// Create a new statement for query execution.
    pub fn create_statement(&self) -> Statement {
        Statement::new(self.client.clone())
    }

    /// Execute a query and return a statement with results.
    pub async fn query(&self, sql: &str) -> Result<Statement> {
        let mut stmt = self.create_statement();
        stmt.set_sql_query(sql);
        stmt.execute().await?;
        Ok(stmt)
    }
}
