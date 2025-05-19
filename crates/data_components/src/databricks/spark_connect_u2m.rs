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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{datasource::TableProvider, sql::TableReference};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{Read, spark_connect::SparkConnect};
use token_provider::TokenProvider;

type SparkSession = SparkConnect;

#[derive(Clone)]
pub struct DatabricksSparkConnectU2M {
    /// Base connection string (without token or session_id).
    base_connection: String,

    session_pool: Arc<RwLock<HashMap<String, Arc<RwLock<SparkSession>>>>>,

    token_provider: Arc<dyn TokenProvider>,
}

impl DatabricksSparkConnectU2M {
    pub fn from_token_provider(
        endpoint: String,
        cluster_id: String,
        databricks_use_ssl: bool,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Self {
        let user_agent = super::user_agent();
        let base_connection = format!(
            "sc://{endpoint}:443/;use_ssl={databricks_use_ssl};user_id=spice.ai;x-databricks-cluster-id={cluster_id};user_agent={user_agent};"
        );
        Self {
            base_connection,
            session_pool: Arc::new(RwLock::new(HashMap::new())),
            token_provider,
        }
    }

    async fn get_session(
        &self,
    ) -> Result<Arc<RwLock<SparkSession>>, Box<dyn std::error::Error + Send + Sync>> {
        let access_token = self.token_provider.get_token();

        {
            let pool = self.session_pool.read().await;
            if let Some(session) = pool.get(&access_token) {
                return Ok(session.clone());
            }
        }

        let mut pool = self.session_pool.write().await;

        let session_id = Uuid::new_v4();
        let connection = format!(
            "{}token={};session_id={};",
            self.base_connection, access_token, session_id
        );

        let spark_connect = SparkConnect::from_connection(connection.as_str()).await?;
        let session = Arc::new(RwLock::new(spark_connect));
        pool.insert(access_token, session.clone());
        Ok(session)
    }
}

#[async_trait]
impl Read for DatabricksSparkConnectU2M {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let session = self.get_session().await?;
        let spark_connect = session.read().await;
        Ok(spark_connect
            .table_provider(table_reference, schema)
            .await?)
    }
}
