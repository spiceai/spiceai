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

use crate::SpiceModelTool;
use async_trait::async_trait;
use serde_json::Value;
use std::{borrow::Cow, sync::Arc};

/// Recreate a tool with a new name.
///
/// Underlying tool is not modified.
pub fn with_name(tool: &Arc<dyn SpiceModelTool>, name: &str) -> Arc<dyn SpiceModelTool> {
    Arc::new(RenamedTool {
        name: name.into(),
        tool: Arc::clone(tool),
    })
}

/// Wraps [`SpiceModelTool`]s to enable renaming them.
///
/// Not intended for broad use, solely [`with_name`].
struct RenamedTool {
    name: String,
    tool: Arc<dyn SpiceModelTool>,
}

#[async_trait]
impl SpiceModelTool for RenamedTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        self.tool.description()
    }

    fn parameters(&self) -> Option<Value> {
        self.tool.parameters()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.tool.call(arg).await
    }
}
