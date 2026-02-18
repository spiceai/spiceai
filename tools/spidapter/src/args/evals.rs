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

use super::CommonArgs;
use clap::Parser;

#[derive(Parser)]
pub struct EvalsTestArgs {
    #[clap(flatten)]
    pub(crate) common: CommonArgs,

    /// The language model (as named in Spicepod) to test against.
    /// If not specified, the first model from the Spicepod definition will be used.
    #[arg(long)]
    pub(crate) model: Option<String>,

    /// The eval name (as named in Spicepod) to test against.
    /// If not specified, the first eval from the Spicepod definition will be used.
    #[arg(long)]
    pub(crate) eval: Option<String>,
}
