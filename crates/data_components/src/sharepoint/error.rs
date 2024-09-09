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

use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid drive format: {}", input))]
    InvalidDriveFormat { input: String },

    #[snafu(display("Drive not found: {}", drive))]
    DriveNotFound { drive: String },

    #[snafu(display("Group not found: {}", group))]
    GroupNotFound { group: String },

    #[snafu(display("Site not found: {}", site))]
    SiteNotFound { site: String },

    #[snafu(display("Error interacting with Microsoft graph: {source}"))]
    MicrosoftGraphFailure {
        source: graph_rs_sdk::error::GraphFailure,
    },
}
