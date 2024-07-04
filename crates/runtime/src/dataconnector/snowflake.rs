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

use super::DataConnector;
use super::DataConnectorFactory;
use async_trait::async_trait;
use data_components::snowflake::SnowflakeTableFactory;
use data_components::Read;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;

use crate::component::dataset::Dataset;
use crate::secrets::Secret;
use crate::secrets::SecretMap;
use datafusion::datasource::TableProvider;
use db_connection_pool::snowflakepool::SnowflakeConnectionPool;
use itertools::Itertools;
use snafu::prelude::*;
use snowflake_api::SnowflakeApi;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    UnableToCreateSnowflakeConnectionPool {
        source: db_connection_pool::snowflakepool::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Snowflake {
    table_factory: SnowflakeTableFactory,
}

impl DataConnectorFactory for Snowflake {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        let mut params: SecretMap = params.as_ref().into();
        if let Some(secret) = secret {
            secret.insert_to_params(&mut params, "username_key", "username");
            secret.insert_to_params(&mut params, "account_key", "account");
            secret.insert_to_params(
                &mut params,
                "snowflake_warehouse_key",
                "snowflake_warehouse",
            );
            secret.insert_to_params(&mut params, "snowflake_role_key", "snowflake_role");
            secret.insert_to_params(
                &mut params,
                "snowflake_auth_type_key",
                "snowflake_auth_type",
            );
            secret.insert_to_params(&mut params, "password_key", "password");
            secret.insert_to_params(
                &mut params,
                "snowflake_private_key_path_key",
                "snowflake_private_key_path",
            );
            secret.insert_to_params(
                &mut params,
                "snowflake_private_key_passphrase_key",
                "snowflake_private_key_passphrase",
            );
        }

        Box::pin(async move {
            let pool: Arc<
                dyn DbConnectionPool<Arc<SnowflakeApi>, &'static (dyn Sync)> + Send + Sync,
            > = Arc::new(
                SnowflakeConnectionPool::new(&params)
                    .await
                    .context(UnableToCreateSnowflakeConnectionPoolSnafu)?,
            );

            let table_factory = SnowflakeTableFactory::new(pool);

            Ok(Arc::new(Self { table_factory }) as Arc<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for Snowflake {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset
            .path()
            .split('.')
            .map(|x| {
                if x.starts_with('"') && x.ends_with('"') {
                    return x.into();
                }

                format!("\"{x}\"")
            })
            .join(".");

        Ok(Read::table_provider(&self.table_factory, path.into())
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "snowflake",
            })?)
    }
}
