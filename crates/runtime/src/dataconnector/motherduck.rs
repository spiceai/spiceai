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
use data_components::duckdb::DuckDBTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use secrets::{get_secret_or_param, Secret};
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorError, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct MotherDuck {
    duckdb_factory: DuckDBTableFactory,
}

impl DataConnectorFactory for MotherDuck {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let motherduck_token = get_param(&params, &secret, "motherduck_token").ok_or(
                DataConnectorError::InvalidConfiguration {
                    dataconnector: "motherduck".to_string(),
                    message: "Missing required parameter motherduck_token.".to_string(),
                    source: "Missing motherduck_token".into(),
                },
            )?;

            let conn_params = format!("md:?motherduck_token={motherduck_token}&saas_mode=true");

            let pool = Arc::new(
                DuckDbConnectionPool::new_file(&conn_params, &AccessMode::ReadOnly).map_err(
                    |e| DataConnectorError::UnableToConnectInternal {
                        dataconnector: "motherduck".to_string(),
                        source: e,
                    },
                )?,
            );

            let duckdb_factory = DuckDBTableFactory::new(pool);

            Ok(Arc::new(Self { duckdb_factory }) as Arc<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for MotherDuck {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(
            Read::table_provider(&self.duckdb_factory, dataset.path().into())
                .await
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "motherduck",
                })?,
        )
    }
}

fn get_param(
    params: &Arc<Option<HashMap<String, String>>>,
    secret: &Option<Secret>,
    param_name: &str,
) -> Option<String> {
    return get_secret_or_param(
        params.as_ref().as_ref(),
        secret,
        &format!("{param_name}_key"),
        param_name,
    );
}
