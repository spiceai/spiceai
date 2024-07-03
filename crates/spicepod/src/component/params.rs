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

use std::collections::HashMap;

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(untagged)]
pub enum ParamValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl ParamValue {
    #[must_use]
    pub fn as_string(&self) -> String {
        match self {
            ParamValue::String(value) => value.clone(),
            ParamValue::Int(value) => value.to_string(),
            ParamValue::Float(value) => value.to_string(),
            ParamValue::Bool(value) => value.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Params {
    #[cfg_attr(feature = "schemars", schemars(flatten))]
    pub data: HashMap<String, ParamValue>,
}

impl Params {
    #[must_use]
    pub fn as_string_map(&self) -> HashMap<String, String> {
        self.data
            .iter()
            .map(|(k, v)| (k.clone(), v.as_string()))
            .collect()
    }

    #[must_use]
    pub fn from_string_map(data: HashMap<String, String>) -> Self {
        let mut params = HashMap::new();
        for (k, v) in data {
            params.insert(k, ParamValue::String(v));
        }
        Params { data: params }
    }
}

impl Serialize for Params {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.data.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Params {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let params = HashMap::<String, ParamValue>::deserialize(deserializer)?;
        Ok(Params { data: params })
    }
}
