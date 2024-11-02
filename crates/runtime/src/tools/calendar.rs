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
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use spicepod::component::tool::Tool;
use std::{collections::HashMap, sync::Arc};

use data_components::sharepoint::calendar::CalendarClient;
use graph_rs_sdk::{
    identity::{
        AuthorizationCodeCredential, ConfidentialClientApplication, PublicClientApplication,
    },
    GraphClient,
};
use snafu::ResultExt;
use url::Url;

use crate::Runtime;

use super::{parameters, SpiceModelTool};

pub struct CalendarTool {
    name: String,
    description: Option<String>,
    client: CalendarClient,
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct CalendarToolParams {
    /// TODO description for
    user: String,
}

#[async_trait]
impl SpiceModelTool for CalendarTool {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<CalendarToolParams>()
    }

    async fn call(
        &self,
        arg: &str,
        _rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::calendar_events", tool = self.name(), input = arg);

        let req: CalendarToolParams = serde_json::from_str(arg)?;

        let events = self.client.get_events_for(req.user).await?;
        let captured_output_json = serde_json::to_string(&events).boxed()?;
        tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);

        serde_json::to_value(events).boxed()
    }
}

fn client_from_params(
    params: &HashMap<String, String>,
) -> Result<CalendarClient, Box<dyn std::error::Error + Send + Sync>> {
    let client_id = params.get("client_id").expect("client_id").as_str();

    let tenant_id = params.get("tenant_id").expect("tenant_id");

    let client_secret = params.get("client_secret");
    let auth_code = params.get("auth_code");

    let graph_client = match (client_secret, auth_code) {
        (Some(client_secret), None) => GraphClient::from(
            &ConfidentialClientApplication::builder(client_id)
                .with_client_secret(client_secret)
                .with_tenant(tenant_id)
                .with_scope([".default"])
                .build(),
        ),
        (Some(_) | None, Some(auth_code)) => {
            tracing::warn!("Both `params.client_secret` and `params.auth_code` are provided. Using `params.auth_code`.");
            // Must match the redirect URL used in `spice login sharepoint...`.
            let redirect_url = Url::parse("http://localhost:8091").boxed()?;
            GraphClient::from(&PublicClientApplication::from(
                AuthorizationCodeCredential::new_with_redirect_uri(
                    tenant_id,
                    client_id,
                    "",
                    auth_code,
                    redirect_url,
                ),
            ))
        }
        (None, None) => {
            panic!("mahhhhh either 'client_secret' or 'auth_code' must be provided");
        }
    };
    Ok(CalendarClient::new(Arc::new(graph_client)))
}

impl TryFrom<&Tool> for CalendarTool {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: &Tool) -> Result<Self, Self::Error> {
        let client = client_from_params(&value.params)?;
        Ok(CalendarTool {
            name: "get_calendar_events".to_string(),
            description: Some("Get the events from a user's schedule".to_string()),
            client,
        })
    }
}
