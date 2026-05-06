/*
Copyright 2026 The Spice.ai OSS Authors

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
use std::borrow::Cow;

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use serde_json::Value;
use snafu::ResultExt;
use tools::SpiceModelTool;

pub struct GetCurrentDateTimeTool {
    name: String,
    description: String,
}

impl GetCurrentDateTimeTool {
    #[must_use]
    pub fn new(name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            name: name.unwrap_or("get_current_datetime").to_string(),
            description: description
                .unwrap_or("Return the current UTC date and time as an ISO 8601 timestamp. Call this whenever the model needs the actual current time to compute relative durations, evaluate freshness, or stamp generated content; do not guess or rely on training-data dates. Takes no arguments. Returns a string like '2026-05-06T13:45:00Z'.")
                .to_string(),
        }
    }
}

#[async_trait]
impl SpiceModelTool for GetCurrentDateTimeTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        None
    }

    async fn call(&self, _arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::get_current_datetime", tool = self.name().to_string());
        let datetime = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let value = Value::String(datetime);
        let captured_output_json = serde_json::to_string(&value).boxed()?;
        tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;

    #[tokio::test]
    async fn call_returns_iso_8601_utc_datetime() {
        let tool = GetCurrentDateTimeTool::new(None, None);

        let value = tool
            .call("")
            .await
            .expect("get_current_datetime should return the current datetime");
        let datetime = value
            .as_str()
            .expect("get_current_datetime should return a string");

        DateTime::parse_from_rfc3339(datetime)
            .expect("datetime should be parseable as RFC 3339 / ISO 8601");
        assert!(datetime.ends_with('Z'));
    }
}
