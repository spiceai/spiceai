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

use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode, RefreshOnStartup, ZeroResultsAction};
use spicepod::component::ComponentOrReference;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime, TelemetryConfig};
use spicepod::semantic::Column;
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::DatasetConfig;
use uuid::Uuid;

use super::arrow_type_to_spicepod_str;

/// Database name used for all `MongoDB` benchmark collections.
const MONGODB_DATABASE: &str = "spicebench";

pub(crate) fn generate_mongodb_spicepod(
    run_id: &Uuid,
    uri: &str,
    datasets: &HashMap<String, DatasetConfig>,
    acceleration_engine: &str,
    auto_load_complete: bool,
) -> SpicepodDefinition {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();

    let mut spicepod = SpicepodDefinition::new(format!("spidapter-{short_id}"));
    spicepod.runtime = Runtime {
        telemetry: TelemetryConfig {
            enabled: false,
            ..TelemetryConfig::default()
        },
        ..Runtime::default()
    };

    for (dataset_name, dataset_config) in datasets {
        // `connection_string` is the correct connector param (not mongodb_*-prefixed).
        // `sslmode: disabled` prevents the driver from attempting TLS on plain connections.
        let mut param_map = HashMap::from([
            ("mongodb_connection_string".to_string(), uri.to_string()),
            ("mongodb_sslmode".to_string(), "disabled".to_string()),
        ]);

        let pks = &dataset_config.primary_key_columns;
        let primary_key = match pks.len() {
            0 => None,
            1 => Some(pks[0].clone()),
            _ => Some(format!("({})", pks.join(", "))),
        };
        let on_conflict = match &primary_key {
            None => HashMap::new(),
            Some(pk) => HashMap::from([(pk.clone(), OnConflictBehavior::Upsert)]),
        };

        let mut dataset = Dataset::new(
            format!("mongodb:{MONGODB_DATABASE}.{dataset_name}"),
            dataset_name.as_str(),
        );
        dataset.params = Some(spicepod::param::Params::from_string_map(param_map));
        dataset.columns = dataset_config
            .schema
            .fields()
            .iter()
            .map(|field| {
                Column::new(field.name())
                    .with_type(arrow_type_to_spicepod_str(field.data_type()))
                    .with_nullable(field.is_nullable())
            })
            .collect();
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some(acceleration_engine.to_string()),
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Changes),
            primary_key,
            on_conflict,
            ..Acceleration::default()
        });

        spicepod
            .datasets
            .push(ComponentOrReference::Component(dataset));
    }

    spicepod
}
