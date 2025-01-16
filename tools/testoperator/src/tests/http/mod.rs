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

use crate::commands::HttpTestArgs;
use test_framework::{anyhow, spicetest::http::component::HttpComponent};

const DEFAULT_API_BASE: &str = "http://localhost:8090/v1";

mod consistency;
pub(crate) use consistency::consistency_run;

mod overhead;
pub(crate) use overhead::overhead_run;

fn get_http_component(args: &HttpTestArgs) -> anyhow::Result<HttpComponent> {
    match (&args.model, &args.embedding) {
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

fn get_payloads(args: &HttpTestArgs) -> anyhow::Result<Vec<String>> {
    match (&args.payload_file, &args.payload) {
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
