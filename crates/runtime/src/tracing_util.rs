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

use crate::component::dataset::{
    acceleration::{Acceleration, Mode, ZeroResultsAction},
    Dataset,
};

// Format: Dataset taxi_trips registered (s3://spiceai-demo-datasets/taxi_trips/2024/), acceleration (duckdb), results cache enabled.
pub fn dataset_registered_trace(ds: &Dataset, results_cache_enabled: bool) -> String {
    let mut info = format!("Dataset {} registered ({})", &ds.name, &ds.from);
    if let Some(acceleration) = &ds.acceleration {
        if acceleration.enabled {
            info.push_str(&format!(
                ", acceleration ({})",
                dataset_acceleration_info(acceleration)
            ));
        }
    }

    if results_cache_enabled {
        info.push_str(", results cache enabled");
    }

    info.push('.');
    info
}

// Format: sqlite:file, 30s refresh, 1hr retention, fallback on source on empty result
fn dataset_acceleration_info(acceleration: &Acceleration) -> String {
    let mut info: String = acceleration.engine.to_string();

    if acceleration.mode == Mode::File {
        info.push_str(":file");
    }

    if let Some(refresh_interval) = &acceleration.refresh_check_interval {
        info.push_str(&format!(", {refresh_interval} refresh"));
    }
    if let Some(retention_check_interval) = &acceleration.retention_check_interval {
        if acceleration.retention_check_enabled {
            info.push_str(&format!(", {retention_check_interval} retention"));
        }
    }
    if acceleration.on_zero_results == ZeroResultsAction::UseSource {
        info.push_str(", fallback on source on empty result");
    }
    info
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::acceleration::Engine;

    #[test]
    fn test_dataset_registered_trace_no_acceleration() {
        let ds = Dataset::try_new("s3://taxi_trips/2024/".to_string(), "taxi_trips")
            .expect("to create dataset");

        let info = dataset_registered_trace(&ds, false);
        assert_eq!(
            info,
            "Dataset taxi_trips registered (s3://taxi_trips/2024/)."
        );
    }

    #[test]
    fn test_dataset_registered_trace_default_acceleration_cache() {
        let acceleration = Acceleration {
            enabled: true,
            ..Default::default()
        };

        let mut ds = Dataset::try_new("s3://taxi_trips/2024/".to_string(), "taxi_trips")
            .expect("to create dataset");
        ds.acceleration = Some(acceleration);

        let info = dataset_registered_trace(&ds, true);
        assert_eq!(info, "Dataset taxi_trips registered (s3://taxi_trips/2024/), acceleration (arrow), results cache enabled.");
    }

    #[test]
    fn test_dataset_registered_trace_with_acceleration_complex() {
        let acceleration = Acceleration {
            enabled: true,
            engine: Engine::DuckDB,
            mode: Mode::File,
            refresh_check_interval: Some("30s".to_string()),
            retention_check_interval: Some("1hr".to_string()),
            retention_check_enabled: true,
            on_zero_results: ZeroResultsAction::UseSource,
            ..Default::default()
        };

        let mut ds = Dataset::try_new("s3://taxi_trips/2024/".to_string(), "taxi_trips")
            .expect("to create dataset");
        ds.acceleration = Some(acceleration);

        let info = dataset_registered_trace(&ds, false);
        assert_eq!(info, "Dataset taxi_trips registered (s3://taxi_trips/2024/), acceleration (duckdb:file, 30s refresh, 1hr retention, fallback on source on empty result).");
    }
}
