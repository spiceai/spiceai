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

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::param::SecretParam;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct PostgresParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_string: Option<SecretParam<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<SecretParam<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pass: Option<SecretParam<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<SecretParam<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<SecretParam<u16>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<SecretParam<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sslmode: Option<SecretParam<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sslrootcert: Option<SecretParam<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_pool_min_idle: Option<SecretParam<u32>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_pool_size: Option<SecretParam<u32>>,
}
