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

use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode};
use spicepod::component::ComponentOrReference;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime, TelemetryConfig};
use spicepod::semantic::Column;
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::DatasetConfig;
use uuid::Uuid;

use super::mongodb_arrow_type_to_spicepod_str;

/// Secret name under which the MongoDB connection string is stored on a Spice
/// Cloud app and referenced from the spicepod as
/// `${secrets:MONGO_CONNECTION_STRING}`.
pub(crate) const MONGO_CONNECTION_STRING_SECRET: &str = "MONGO_CONNECTION_STRING";

pub(crate) fn generate_mongodb_spicepod(
    run_id: &Uuid,
    uri: &str,
    datasets: &HashMap<String, DatasetConfig>,
    acceleration_engine: &str,
    use_connection_secret: bool,
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
        // On Spice Cloud the connection string is stored as a secret and the
        // spicepod references it as `${secrets:MONGO_CONNECTION_STRING}` (the
        // caller sets that secret to the real URI). Locally there is no secret
        // store wired up, so inline the URI verbatim — the caller is responsible
        // for any TLS settings (EC2-provisioned mongo encodes tls=false in its
        // URI; connect-mode URIs like Atlas carry their own tls/ssl settings).
        let conn_str = if use_connection_secret {
            format!("${{secrets:{MONGO_CONNECTION_STRING_SECRET}}}")
        } else {
            uri.to_string()
        };
        let param_map = HashMap::from([
            ("mongodb_connection_string".to_string(), conn_str),
            // Increase cursor batch sizes to reduce round-trips on high-volume streams.
            ("change_stream_batch_size".to_string(), "10000".to_string()),
            (
                "change_stream_batch_max_size".to_string(),
                "10000".to_string(),
            ),
        ]);

        // MongoDB change stream delete events only carry `_id`, so we use `_id` as
        // the acceleration primary key. The sink computes `_id` from the TPC-H PK columns.
        let primary_key = Some("_id".to_string());
        let on_conflict = HashMap::from([("_id".to_string(), OnConflictBehavior::Upsert)]);

        // Use just the collection name in `from:` — database comes from the connection_string URI.
        // Using `db.collection` in `from:` can conflict with the URI's database and break CDC.
        let mut dataset = Dataset::new(format!("mongodb:{dataset_name}"), dataset_name.as_str());
        dataset.params = Some(spicepod::param::Params::from_string_map(param_map));

        // Add `_id` as the first column (string key the sink writes), then all data columns.
        let mut columns: Vec<Column> =
            vec![Column::new("_id").with_type("Utf8").with_nullable(true)];
        columns.extend(dataset_config.schema.fields().iter().map(|field| {
            Column::new(field.name())
                .with_type(mongodb_arrow_type_to_spicepod_str(field.data_type()))
                .with_nullable(field.is_nullable())
        }));
        dataset.columns = columns;

        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some(acceleration_engine.to_string()),
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Changes),
            primary_key,
            on_conflict,
            params: super::cayenne_acceleration_params(acceleration_engine),
            ..Acceleration::default()
        });

        spicepod
            .datasets
            .push(ComponentOrReference::Component(dataset));
    }

    spicepod
}
