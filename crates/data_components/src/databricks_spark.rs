/*
Copyright 2024 The Spice.ai OSS Authors

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
use datafusion::{datasource::TableProvider, sql::TableReference};
use std::{error::Error, sync::Arc};
use uuid::Uuid;

use crate::{spark_connect::SparkConnect, Read, ReadWrite};

#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct DatabricksSparkConnect {
    spark_connect: SparkConnect,
}
impl DatabricksSparkConnect {
    pub async fn new(
        endpoint: String,
        user: Option<String>,
        cluster_id: String,
        token: String,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let user = user.unwrap_or("spice.ai".to_string());
        let session_id = Uuid::new_v4();
        let connection = format!("sc://{endpoint}:443/;use_ssl=true;ser_id={user};session_id={session_id};token={token};x-databricks-cluster-id={cluster_id}");
        Ok(Self {
            spark_connect: SparkConnect::from_connection(connection.as_str()).await?,
        })
    }
}

#[async_trait]
impl ReadWrite for DatabricksSparkConnect {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        Ok(ReadWrite::table_provider(&self.spark_connect, table_reference).await?)
    }
}

#[async_trait]
impl Read for DatabricksSparkConnect {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        Ok(Read::table_provider(&self.spark_connect, table_reference).await?)
    }
}
