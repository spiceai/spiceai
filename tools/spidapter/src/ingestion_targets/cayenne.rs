// Copyright 2026 Spice AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::ComponentOrReference;
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{
    ApiKey, ApiKeyAuth, Auth, Flight, Query, Runtime, TelemetryConfig,
};
use spicepod::param::Params;
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::{AdbcDriver, DatasetConfig, EtlSinkType, SetupResponse};
use uuid::Uuid;

use crate::args::StdioArgs;
use super::super::{RunState, SetupConfig, resolve_aws_region};

pub(crate) fn build_cayenne_setup_response(
    etl_sink_type: Option<EtlSinkType>,
    state: &RunState,
) -> SetupResponse {
    let mut db_kwargs = HashMap::from([
        (
            "uri".to_string(),
            serde_json::Value::String(state.flight_url().to_string()),
        ),
        (
            "username".to_string(),
            serde_json::Value::String(String::new()),
        ),
        (
            "password".to_string(),
            serde_json::Value::String(state.password().to_string()),
        ),
    ]);

    if let RunState::Local(local_state) = state
        && let Some(api_key) = &local_state.flight_api_key
    {
        db_kwargs.insert(
            "adbc.flight.sql.rpc.call_header.authorization".to_string(),
            serde_json::Value::String(format!("Bearer {api_key}")),
        );
    }

    SetupResponse {
        driver: AdbcDriver::Flightsql,
        db_kwargs,
        catalog_namespace: etl_sink_type
            .as_ref()
            .filter(|sink_type| matches!(sink_type, EtlSinkType::Adbc))
            .map(|_| "spicebench.bench".to_string()),
        read_driver: None,
    }
}

/// Generate the spicepod for Cayenne ADBC sink mode: a Cayenne catalog with
/// Flight SQL auth, rate-limit disabled, and optional data/metadata dirs.
pub(crate) fn generate_cayenne_sink_spicepod(
    run_id: &Uuid,
    flight_api_key: Option<&str>,
    args: &StdioArgs,
) -> SpicepodDefinition {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();

    let mut spicepod = SpicepodDefinition::new(format!("spidapter-{short_id}"));
    spicepod.runtime = Runtime {
        telemetry: TelemetryConfig {
            enabled: false,
            ..TelemetryConfig::default()
        },
        auth: flight_api_key.map(|key| Auth {
            api_key: Some(ApiKeyAuth {
                enabled: true,
                keys: vec![ApiKey::ReadWrite {
                    key: key.to_string(),
                }],
            }),
        }),
        flight: Some(Flight {
            do_put_rate_limit_enabled: false,
            ..Flight::default()
        }),
        query: Some(Query {
            memory_limit: args
                .query_memory_limit
                .clone()
                .or(Some("150Gi".to_string())),
            ..Query::default()
        }),
        ..Runtime::default()
    };

    let mut cayenne_catalog = Catalog::new("cayenne".to_string(), "spicebench".to_string())
        .with_access(AccessMode::ReadWriteCreate);

    let mut params_map = HashMap::new();

    if let Some(cayenne_data_dir) = &args
        .cayenne_data_dir
        .clone()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        params_map.insert("cayenne_data_dir".to_string(), cayenne_data_dir.clone());
    }

    if let Some(cayenne_metadata_dir) = &args
        .cayenne_metadata_dir
        .clone()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        params_map.insert(
            "cayenne_metadata_dir".to_string(),
            cayenne_metadata_dir.clone(),
        );
    }

    if !params_map.is_empty() {
        cayenne_catalog.params = Some(Params::from_string_map(params_map));
    }

    spicepod
        .catalogs
        .push(ComponentOrReference::Component(cayenne_catalog));
    spicepod
}

/// Generate a spicepod that accelerates S3/hive-partitioned parquet data into
/// cayenne (file mode). Used for the default (non-ADBC, non-CDC) benchmark path.
pub(crate) fn generate_hive_spicepod(
    run_id: &Uuid,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<SpicepodDefinition> {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    let region = resolve_aws_region(setup_config);

    let mut spicepod = SpicepodDefinition::new(format!("spidapter-{short_id}"));
    spicepod.runtime = Runtime {
        telemetry: TelemetryConfig {
            enabled: false,
            ..TelemetryConfig::default()
        },
        ..Runtime::default()
    };

    for (dataset_name, config) in datasets {
        let from = config.location.as_deref().ok_or_else(|| {
            anyhow::anyhow!("Dataset '{dataset_name}' is missing a 'from' URI in its config")
        })?;

        let mut param_map = HashMap::from([
            ("file_format".to_string(), "parquet".to_string()),
            ("s3_auth".to_string(), "public".to_string()),
            ("s3_region".to_string(), region.clone()),
            ("hive_partitioning_enabled".to_string(), "true".to_string()),
        ]);
        if let Some(endpoint) = &setup_config.endpoint {
            param_map.insert("s3_endpoint".to_string(), endpoint.clone());
            if endpoint.starts_with("http://") {
                param_map.insert("allow_http".to_string(), "true".to_string());
            }
        }

        let mut dataset = Dataset::new(from, dataset_name.as_str());
        dataset.params = Some(Params::from_string_map(param_map));
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("cayenne".to_string()),
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Full),
            ..Acceleration::default()
        });

        spicepod
            .datasets
            .push(ComponentOrReference::Component(dataset));
    }

    Ok(spicepod)
}
