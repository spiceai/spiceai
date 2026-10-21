/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use super::CommonArgs;

/// Arguments for the schema test command.
///
/// The schema test queries `information_schema.tables` and `information_schema.columns`
/// to validate that catalog connectors correctly discover and register tables and schemas.
#[derive(Parser, Debug, Clone)]
pub struct SchemaTestArgs {
    #[command(flatten)]
    pub(crate) common: CommonArgs,

    /// Minimum number of tables expected in the catalog.
    /// If set, the test fails when fewer tables are discovered.
    #[arg(long)]
    pub(crate) min_tables: Option<usize>,
}
