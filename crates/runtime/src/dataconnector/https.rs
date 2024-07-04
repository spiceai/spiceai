/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this Https except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use crate::component::dataset::Dataset;
use crate::secrets::{get_secret_or_param, Secret};
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::Url;

use super::{
    DataConnector, DataConnectorError, DataConnectorFactory, DataConnectorResult,
    ListingTableConnector,
};

pub struct Https {
    params: Arc<HashMap<String, String>>,
    secret: Option<Secret>,
}

impl std::fmt::Display for Https {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "https")
    }
}

impl DataConnectorFactory for Https {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(Self { params, secret }) as Arc<dyn DataConnector>) })
    }
}

impl ListingTableConnector for Https {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &HashMap<String, String> {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let mut u = Url::parse(&dataset.from).boxed().map_err(|e| {
            DataConnectorError::InvalidConfiguration {
                dataconnector: "https".to_string(),
                message: format!("Invalid URL: {e}"),
                source: e,
            }
        })?;

        if let Some(p) = self.params.get("http_port") {
            let n = match p.parse::<u16>() {
                Ok(n) => n,
                Err(e) => {
                    return Err(DataConnectorError::InvalidConfiguration {
                        dataconnector: "https".to_string(),
                        message: format!("Invalid port parameter: {e}"),
                        source: Box::new(e),
                    });
                }
            };
            let _ = u.set_port(Some(n));
        };

        if let Some(p) = get_secret_or_param(
            &self.params,
            &self.secret,
            "http_password_key",
            "http_password",
        ) {
            if u.set_password(Some(&p)).is_err() {
                return Err(
                    DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                        dataconnector: "https".to_string(),
                    },
                );
            };
        }

        if let Some(p) = self.params.get("http_username") {
            if u.set_username(p).is_err() {
                return Err(
                    DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                        dataconnector: "https".to_string(),
                    },
                );
            };
        }
        Ok(u)
    }
}
