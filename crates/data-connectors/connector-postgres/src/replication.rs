/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Glue between Spice's connector params and the `postgres_replication` module.
//!
//! Responsibilities:
//!   - Parse connection & replication params out of `runtime::parameters::Parameters`.
//!   - Fall back to sensible per-replica defaults for slot & publication names.
//!   - Look up the source table schema (via the federated table) and hand everything
//!     off to `data_components::postgres_replication::start_replication_stream`.

use std::sync::Arc;
use std::time::Duration;

use async_stream::try_stream;
use data_components::cdc::{ChangesStream, StreamError};
use data_components::postgres_replication::{
    ReplicationParams, ReplicationStreamInput, config, start_replication_stream,
};
use datafusion::sql::TableReference;
use futures::StreamExt;
use runtime::component::dataset::Dataset;
use runtime::federated_table::FederatedTable;
use runtime::parameters::{ExposedParamLookup, Parameters};
use secrecy::SecretString;

const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);

pub fn build_changes_stream(
    params: &Parameters,
    dataset: &Dataset,
    federated_table: Arc<FederatedTable>,
) -> ChangesStream {
    let dataset_name = dataset.name.to_string();
    let (schema_name, table_name) = split_schema_table(&dataset.from);

    let params_for_stream = match replication_params_from_connector_params(
        params,
        &dataset_name,
    ) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("postgres replication: {e}");
            return Box::pin(futures::stream::once(async move { Err(StreamError::External(msg)) }));
        }
    };

    Box::pin(try_stream! {
        let table_provider = federated_table.table_provider().await;
        let schema = table_provider.schema();

        let primary_keys = extract_primary_keys(&table_provider);

        let input = ReplicationStreamInput {
            dataset_name: dataset_name.clone(),
            params: params_for_stream,
            schema,
            primary_keys,
            schema_name,
            table_name,
        };

        let mut inner = start_replication_stream(input);
        while let Some(item) = inner.next().await {
            yield item?;
        }
    })
}

fn replication_params_from_connector_params(
    params: &Parameters,
    dataset_name: &str,
) -> std::result::Result<ReplicationParams, String> {
    let host = required_string(params, "host")?;
    let port = optional_string(params, "port")
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(5432);
    let user = required_string(params, "user")?;
    let password_str = required_secret(params, "pass")?;
    let database = required_string(params, "db")?;
    let sslmode = config::SslMode::from_str_or_default(optional_string(params, "sslmode").as_deref());

    let slot_name = optional_string(params, "replication_slot")
        .unwrap_or_else(|| config::default_slot_name(dataset_name));
    let publication_name = optional_string(params, "publication")
        .unwrap_or_else(|| config::default_publication_name(dataset_name));
    let initial_snapshot = optional_string(params, "replication_initial_snapshot")
        .map(|s| parse_bool_default_true(&s))
        .unwrap_or(true);
    let temporary_slot = optional_string(params, "replication_temporary_slot")
        .map(|s| parse_bool_default_false(&s))
        .unwrap_or(false);
    let status_interval = optional_string(params, "replication_status_interval")
        .and_then(|s| fundu::parse_duration(&s).ok())
        .unwrap_or(DEFAULT_STATUS_INTERVAL);

    Ok(ReplicationParams {
        host,
        port,
        user,
        password: SecretString::from(password_str),
        database,
        sslmode,
        slot_name,
        publication_name,
        initial_snapshot,
        temporary_slot,
        status_interval,
    })
}

fn required_string(params: &Parameters, key: &str) -> std::result::Result<String, String> {
    match params.get(key).expose() {
        ExposedParamLookup::Present(v) => Ok(v.to_string()),
        ExposedParamLookup::Absent(name) => {
            Err(format!("missing required parameter `{name}`"))
        }
    }
}

fn required_secret(params: &Parameters, key: &str) -> std::result::Result<String, String> {
    match params.get(key).expose() {
        ExposedParamLookup::Present(v) => Ok(v.to_string()),
        ExposedParamLookup::Absent(name) => {
            Err(format!("missing required secret `{name}`"))
        }
    }
}

fn optional_string(params: &Parameters, key: &str) -> Option<String> {
    match params.get(key).expose() {
        ExposedParamLookup::Present(v) => Some(v.to_string()),
        ExposedParamLookup::Absent(_) => None,
    }
}

fn parse_bool_default_true(s: &str) -> bool {
    matches!(s.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y") || s.is_empty()
}

fn parse_bool_default_false(s: &str) -> bool {
    matches!(s.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y")
}

/// Splits `dataset.from` like `"postgres:public.users"` into (schema, table).
/// Falls back to ("public", <rest>) if unqualified.
fn split_schema_table(from: &str) -> (String, String) {
    let path = from.strip_prefix("postgres:").unwrap_or(from);
    // Use TableReference to respect quoting.
    let r = TableReference::from(path);
    match (r.schema(), r.table()) {
        (Some(schema), table) => (schema.to_string(), table.to_string()),
        (None, table) => ("public".to_string(), table.to_string()),
    }
}

fn extract_primary_keys(provider: &Arc<dyn datafusion::datasource::TableProvider>) -> Vec<String> {
    use datafusion::common::Constraint;
    let Some(constraints) = provider.constraints() else {
        return Vec::new();
    };
    let schema = provider.schema();
    for c in constraints.iter() {
        if let Constraint::PrimaryKey(indices) = c {
            return indices
                .iter()
                .filter_map(|i| schema.fields().get(*i).map(|f| f.name().clone()))
                .collect();
        }
    }
    Vec::new()
}
