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
use datafusion::{common::OwnedTableReference, datasource::TableProvider};
use secrets::Secret;
use spark_connect_rs::{SparkSession, SparkSessionBuilder};
use std::{collections::HashMap, error::Error, sync::Arc};

use crate::{spark_connect, Read, ReadWrite};

#[derive(Clone)]
pub struct Databricks {
    pub secret: Arc<Option<Secret>>,
    pub params: Arc<Option<HashMap<String, String>>>,
    session: Arc<SparkSession>,
}
impl Databricks {
    #[must_use]
    pub async fn new(
        secret: Arc<Option<Secret>>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let param_deref = match params.as_ref() {
            None => return Err("Dataset params not found".into()),
            Some(params) => params,
        };

        let Some(endpoint) = param_deref.get("endpoint") else {
            return Err("Databricks Workspace not found in dataset params".into());
        };
        let Some(cluster_id) = param_deref.get("databricks-cluster-id") else {
            return Err(
                "Databricks Cluster ID (databricks-cluster-id) not found in dataset params".into(),
            );
        };
        let Some(secrets) = secret.as_ref() else {
            return Err("No secret found, DATABRICKS TOKEN not available".into());
        };
        let Some(token) = secrets.get("token") else {
            return Err("Secrets found, but DATABRICKS TOKEN not available".into());
        };
        let mut user = "spice.ai";
        if let Some(user_val) = param_deref.get("endpoint") {
            user = user_val.as_str();
        };
        let connection =format!("sc://{endpoint}:443/;user_id={user};session_id=0d2af2a9-cc3c-4d4b-bf27-e2fefeaca233;token={token};x-databricks-cluster-id={cluster_id}");
        let session = Arc::new(
            SparkSessionBuilder::remote(connection.as_str())
                .build()
                .await?,
        );
        Ok(Self {
            secret,
            params,
            session,
        })
    }
}

#[async_trait]
impl ReadWrite for Databricks {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        let provider =
            spark_connect::get_table_provider(self.session.clone(), table_reference).await?;
        Ok(provider)
    }
}

#[async_trait]
impl Read for Databricks {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        let provider =
            spark_connect::get_table_provider(self.session.clone(), table_reference).await?;
        Ok(provider)
    }
}
