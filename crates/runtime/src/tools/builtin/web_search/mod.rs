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

use std::fmt::{self, Display, Formatter};

use llms::perplexity::PerplexitySonar;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

mod perplexity;
mod tool;

pub enum SearchEngineType {
    Perplexity,
}

impl Display for SearchEngineType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            SearchEngineType::Perplexity => write!(f, "perplexity"),
        }
    }
}

impl SearchEngineType {
    #[must_use] pub fn description(&self) -> String {
        match self {
            SearchEngineType::Perplexity => {
                "Search the web with Perplexity's Sonar API.".to_string()
            }
        }
    }
}

pub enum SearchEngine {
    Perplexity(PerplexitySonar),
}

impl SearchEngine {
    #[must_use] pub fn engine_type(&self) -> SearchEngineType {
        match self {
            SearchEngine::Perplexity(_) => SearchEngineType::Perplexity,
        }
    }

    pub async fn search(
        &self,
        req: WebSearchParams,
    ) -> Result<WebSearchResponse, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            SearchEngine::Perplexity(engine) => Ok(engine
                .search_request(req.try_into().boxed()?)
                .await
                .boxed()?
                .into()),
        }
    }
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct WebSearchParams {
    /// The query to search the web for.
    pub query: String,

    /// The number of results to return. If None, the default limit from the search engine is used.
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize, Default)]
pub struct WebSearchResponse {
    summary: Option<String>,
    results: Vec<WebSearchResult>,
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct WebSearchResult {
    url: String,
    title: Option<String>,
    #[serde(rename = "type")]
    result_type: WebSearchResultType,
    content: Option<String>,
}

impl WebSearchResult {
    #[must_use] pub fn image_url(url: String) -> Self {
        Self {
            url,
            title: None,
            result_type: WebSearchResultType::Image,
            content: None,
        }
    }
    #[must_use] pub fn webpage(url: String) -> Self {
        Self {
            url,
            title: None,
            result_type: WebSearchResultType::Webpage,
            content: None,
        }
    }
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub enum WebSearchResultType {
    Webpage,
    Image,
}
