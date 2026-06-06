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

use spicepod::component::ComponentOrReference;
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::component::runtime::{
    ApiKey, ApiKeyAuth, Auth, Flight, Query, Runtime, TelemetryConfig,
};
use spicepod::param::Params;
use spicepod::spec::SpicepodDefinition;
use uuid::Uuid;

use crate::scenario::CayenneConfig;

/// Generate the spicepod for Cayenne ADBC sink mode: a Cayenne catalog with
/// Flight SQL auth, rate-limit disabled, and optional data/metadata dirs.
pub(crate) fn generate_cayenne_sink_spicepod(
    run_id: &Uuid,
    flight_api_key: Option<&str>,
    cayenne: Option<&CayenneConfig>,
    query_memory_limit: Option<&str>,
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
            memory_limit: query_memory_limit
                .map(str::to_string)
                .or(Some("150Gi".to_string())),
            ..Query::default()
        }),
        ..Runtime::default()
    };

    let mut cayenne_catalog = Catalog::new("cayenne".to_string(), "spicebench".to_string())
        .with_access(AccessMode::ReadWriteCreate);

    let mut params_map = HashMap::new();

    if let Some(cfg) = cayenne {
        if let Some(data_dir) = cfg
            .data_dir
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            params_map.insert("cayenne_data_dir".to_string(), data_dir.to_string());
        }

        if let Some(metadata_dir) = cfg
            .metadata_dir
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            params_map.insert("cayenne_metadata_dir".to_string(), metadata_dir.to_string());
        }
    }

    if !params_map.is_empty() {
        cayenne_catalog.params = Some(Params::from_string_map(params_map));
    }

    spicepod
        .catalogs
        .push(ComponentOrReference::Component(cayenne_catalog));
    spicepod
}
