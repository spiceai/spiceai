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

use std::sync::Arc;

use graph_rs_sdk::{GraphClient, ODataQuery};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonEvent {
    #[serde(rename = "@odata.etag")]
    pub odata_etag: String,
    pub id: String,
    pub subject: String,
    pub body: Option<Body>,
    pub start: DateTime,
    pub end: DateTime,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Body {
    #[serde(rename = "contentType")]
    pub content_type: String,
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DateTime {
    #[serde(rename = "dateTime")]
    pub date_time: String,
    #[serde(rename = "timeZone")]
    pub time_zone: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GrphResponse {
    #[serde(rename = "@odata.context")]
    pub odata_context: String,
    pub value: Vec<JsonEvent>,
    #[serde(rename = "@odata.nextLink")]
    pub odata_next_link: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Event {
    pub start: String,
    pub end: String,
    pub body: Option<String>,
    pub subject: String,
}

#[derive(Clone)]
pub struct CalendarClient {
    client: Arc<GraphClient>,
}

impl CalendarClient {
    pub fn new(client: Arc<GraphClient>) -> Self {
        Self { client }
    }
}

impl CalendarClient {
    pub async fn get_events_for(
        &self,
        user: String,
        limit: usize,
    ) -> Result<Vec<Event>, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .client
            .user(user.clone())
            .events()
            .list_events()
            .select(&["start", "end", "body", "subject"])
            .order_by(&["lastModifiedDateTime DESC"])
            .top(limit.to_string())
            .send()
            .await
            .boxed()?
            .json::<GrphResponse>()
            .await
            .boxed()?;

        Ok(resp
            .value
            .iter()
            .map(|j| Event {
                start: format!("{} {}", j.start.date_time, j.start.time_zone),
                end: format!("{} {}", j.end.date_time, j.end.time_zone),
                body: j.body.as_ref().map(|b| b.content.clone()),
                subject: j.subject.clone(),
            })
            .collect())
    }
}
