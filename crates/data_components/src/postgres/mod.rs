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

pub mod provider;

// There is deliberately no `impl Read` (or `ReadWrite`) for the fork's
// `PostgresTableFactory`. Its read path federates with no Spice function
// deny-list and has no seam to install one, so a provider built from it as
// `Arc<dyn Read>` unparses every Spice-only UDF into the SQL sent to
// `PostgreSQL`, which rejects it (#13664). A `PostgreSQL` read provider is built
// through `create_spice_federated_table_provider` with
// `deny_spice_functions_for_postgres_table_providers()`: see the dataset
// connector's `federated_postgres_table_provider` and the catalog connector's
// `build_table_factory`.
