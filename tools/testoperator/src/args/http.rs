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

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use test_framework::{anyhow, spicetest::http::component::HttpComponent};

use super::CommonArgs;

#[derive(Parser, Debug, Clone, Deserialize, Serialize)]
pub struct HttpTestArgs {
    /// The embedding model (named in spicepod) to test against. Cannot be used in conjunction with `model`.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) embedding: Option<String>,

    /// The language model (named in spicepod) to test against. Cannot be used in conjunction with `embedding`.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) model: Option<String>,

    /// The path to a file containing payloads to use in testing. Either JSONL of compatible request bodies, or individual string payloads. Cannot not be used in conjunction with `payload`.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) payload_file: Option<PathBuf>,

    /// The payload to use in testing. Either JSONL of compatible request bodies, or individual string payloads. Cannot not be used in conjunction with `payload_file`.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) payload: Option<Vec<String>>,
}

const DEFAULT_API_BASE: &str = "http://localhost:8090/v1";

impl HttpTestArgs {
    pub(crate) fn get_http_component(&self) -> anyhow::Result<HttpComponent> {
        match (&self.model, &self.embedding) {
            (Some(_), Some(_)) => Err(anyhow::anyhow!(
                "Cannot specify both --model and --embedding"
            )),
            (None, None) => Err(anyhow::anyhow!(
                "Must specify either --model or --embedding"
            )),
            (Some(model), None) => Ok(HttpComponent::Model {
                model: model.clone(),
                api_base: DEFAULT_API_BASE.to_string(),
            }),
            (None, Some(embedding)) => Ok(HttpComponent::Embedding {
                embedding: embedding.clone(),
                api_base: DEFAULT_API_BASE.to_string(),
            }),
        }
    }

    pub(crate) fn get_payloads(&self) -> anyhow::Result<Vec<String>> {
        match (&self.payload_file, &self.payload) {
            (Some(_), Some(_)) => Err(anyhow::anyhow!(
                "Cannot specify both --payload-file and --payload"
            )),
            (None, None) => Err(anyhow::anyhow!(
                "Must specify either --payload-file or --payload"
            )),
            (Some(file), None) => Ok(std::fs::read_to_string(file)?
                .lines()
                .map(std::string::ToString::to_string)
                .collect()),
            (None, Some(payload)) => Ok(payload.clone()),
        }
    }
}

#[derive(Parser)]
pub struct HttpConsistencyTestArgs {
    #[clap(flatten)]
    pub(crate) common: CommonArgs,

    #[command(flatten)]
    pub(crate) http: HttpTestArgs,

    #[arg(long, default_value = "0")]
    pub warmup: u64,

    /// The number of buckets to divide the test duration into.
    #[arg(long, default_value = "10")]
    pub(crate) buckets: usize,

    /// The threshold for the increase in percentile latency between the first and last bucket of the test.
    #[arg(long, default_value = "1.1")]
    pub(crate) increase_threshold: f64,
}

#[derive(Parser)]
pub struct HttpOverheadTestArgs {
    #[clap(flatten)]
    pub(crate) common: CommonArgs,

    #[clap(flatten)]
    pub(crate) http: HttpTestArgs,

    /// The threshold for the increase in percentile latency between the spice component and the underlying HTTP connection.
    #[arg(long, default_value = "1.10")]
    pub(crate) increase_threshold: f64,

    /// The base URL of the underlying HTTP service to test against.
    #[arg(long)]
    pub(crate) base_url: String,

    /// Headers to use when making requests to the base URL.
    #[arg(long)]
    pub(crate) base_header: Option<Vec<String>>,

    /// If the component has a different name between the spicepod and the HTTP service, specify the name of the component in the HTTP service.
    #[arg(long)]
    pub(crate) base_component: Option<String>,

    /// The path to a file containing request body to use in testing the baseline component. Expects a request body compatible payloads. Cannot not be used in conjunction with `base_payload`.
    #[arg(long)]
    pub(crate) base_payload_file: Option<PathBuf>,

    /// The request body(s) to use in testing. Expects a request body compatible payloads.Cannot not be used in conjunction with `base_payload_file`.
    #[arg(long)]
    pub(crate) base_payload: Option<Vec<String>>,
}

impl HttpOverheadTestArgs {
    pub(crate) fn base_payload(&self) -> anyhow::Result<Option<Vec<String>>> {
        match (&self.base_payload_file, &self.base_payload) {
            (Some(_), Some(_)) => Err(anyhow::anyhow!(
                "Cannot specify both --payload-file and --payload"
            )),
            (None, None) => Ok(None),
            (Some(file), None) => Ok(Some(
                std::fs::read_to_string(file)?
                    .lines()
                    .map(std::string::ToString::to_string)
                    .collect(),
            )),
            (None, Some(payload)) => Ok(Some(payload.clone())),
        }
    }
}
