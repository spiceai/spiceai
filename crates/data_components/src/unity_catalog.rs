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

use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::collections::HashMap;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: {parameter}"))]
    MissingParameter { parameter: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An ergonomic wrapper around calling Unity Catalog APIs.
///
/// Could be replaced once <https://crates.io/crates/unitycatalog-client> is available.
pub struct UnityCatalog {
    endpoint: String,
    token: SecretString,
}

impl UnityCatalog {
    pub fn new(endpoint: impl Into<String>, token: SecretString) -> Self {
        Self {
            endpoint: endpoint.into(),
            token,
        }
    }

    pub fn from_params(params: &HashMap<String, SecretString>) -> Result<Self, Error> {
        let endpoint = params
            .get("endpoint")
            .ok_or(Error::MissingParameter {
                parameter: "endpoint".into(),
            })?
            .expose_secret()
            .to_string();

        let token = params
            .get("token")
            .ok_or(Error::MissingDatabricksToken {
                parameter: "token".into(),
            })?
            .clone();

        Ok(Self::new(endpoint, token))
    }
}
