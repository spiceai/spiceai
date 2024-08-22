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
#![allow(clippy::missing_errors_doc)]
use async_trait::async_trait;
use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

use crate::Runtime;

pub mod builtin;
pub mod options;

/// Tools that implement the [`SpiceModelTool`] trait can automatically be used by LLMs in the runtime.
#[async_trait]
pub trait SpiceModelTool: Sync + Send {
    fn name(&self) -> &'static str;
    fn description(&self) -> Option<&'static str>;
    fn parameters(&self) -> Option<Value>;
    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;
}

/// Construct a [`serde_json::Value`] from a [`JsonSchema`] type.
fn parameters<T: JsonSchema + Serialize>() -> Option<Value> {
    match serde_json::to_value(schema_for!(T)) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::error!("Unexpectedly cannot serialize schema: {e}",);
            None
        }
    }
}
