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

use graph_rs_sdk::{
    error::{ErrorMessage, ErrorStatus},
    GraphFailure,
};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid drive format: {}", input))]
    InvalidDriveFormat { input: String },

    #[snafu(display("Drive not found: {}", drive))]
    DriveNotFound { drive: String },

    #[snafu(display("Group not found: {}", group))]
    GroupNotFound { group: String },

    #[snafu(display("Group '{}' has no drive", group))]
    GroupHasNoDrive { group: String },

    #[snafu(display("Site {} has no drive", site))]
    SiteHasNoDrive { site: String },

    #[snafu(display("Site not found: {}", site))]
    SiteNotFound { site: String },

    #[snafu(display("Error interacting with Microsoft Sharepoint: {e}", e=resolve_graph_failure(source)))]
    MicrosoftGraphFailure { source: GraphFailure },

    #[snafu(display("Error parsing document: {source}"))]
    DocumentParsing { source: document_parse::Error },
}

/// Resolves a `GraphFailure` into a human-readable error message.
fn resolve_graph_failure(e: &GraphFailure) -> String {
    tracing::debug!("GraphFailure occurred: {:#?}", e);

    match e {
        GraphFailure::SilentTokenAuth{message, response} => {
            let description = if let Ok(resp) = response.body() {
                let description = resp.get("error_description").and_then(|v| v.as_str());
                if description.is_none() {
                    tracing::debug!("Unexpected microsoft graph failure for 'SilentTokenAuth', response={resp:#?}");
                }
                description
            } else {
                None
            };
            Some(format!("token-based authentication failed: {message}, {}", description.unwrap_or_default()))
        },
        GraphFailure::ErrorMessage(ErrorMessage{error: ErrorStatus{code, message, inner_error }}) => {
            if code.is_none() && message.is_none() && inner_error.is_none() {
                Some("Unknown Microsoft Graph error. This is likely a permission issue.".to_string())
            } else {
                Some(format!(
                    "Generic Microsoft error. Code: {code}, Message: {message}, Inner error: {inner_error:#?}",
                        code=code.as_deref().unwrap_or("None"),
                        message=message.as_deref().unwrap_or("None"),
                        inner_error=inner_error
                    )
                )
            }
        }
        _ => None,
    }.unwrap_or(e.to_string())
}
