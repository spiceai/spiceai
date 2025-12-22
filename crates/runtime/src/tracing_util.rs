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

use datafusion::sql::TableReference;
use opentelemetry::trace::TraceId;
use rand::RngCore;

use crate::{
    component::dataset::{
        Dataset,
        acceleration::{self, Acceleration, Mode, RefreshMode, ZeroResultsAction},
    },
    dataconnector::DataConnector,
};

use std::fmt::Write;

// Format: Dataset taxi_trips registered (s3://spiceai-demo-datasets/taxi_trips/2024/), acceleration (duckdb), results cache enabled.
pub fn dataset_registered_trace(
    data_connector: &dyn DataConnector,
    ds: &Dataset,
    results_cache_enabled: bool,
) -> String {
    let mut info = format!("Dataset {} registered ({})", &ds.name, &ds.from);
    if let Some(acceleration) = &ds.acceleration
        && acceleration.enabled
    {
        let _ = write!(
            info,
            ", acceleration ({})",
            acceleration_info(Some(data_connector), acceleration)
        );
    }

    if results_cache_enabled {
        info.push_str(", results cache enabled");
    }

    info.push('.');
    info
}

// Format: View taxi_trips_vw registered, acceleration (duckdb)
pub fn view_registered_trace(
    table: &TableReference,
    acceleration: Option<&acceleration::Acceleration>,
) -> String {
    let mut info = format!("View {table} registered");
    if let Some(acceleration) = acceleration
        && acceleration.enabled
    {
        let _ = write!(
            info,
            ", acceleration ({})",
            acceleration_info(None, acceleration)
        );
    }

    info.push('.');
    info
}

// Format: sqlite:file, 30s refresh, 1hr retention, fallback on source on empty result
fn acceleration_info(
    data_connector: Option<&dyn DataConnector>,
    acceleration: &Acceleration,
) -> String {
    let mut info: String = acceleration.engine.to_string();

    if matches!(acceleration.mode, Mode::File | Mode::FileCreate) {
        info.push_str(":file");
    }

    let refresh_mode = if let Some(data_connector) = data_connector {
        data_connector.resolve_refresh_mode(acceleration.refresh_mode)
    } else {
        acceleration.refresh_mode.unwrap_or(RefreshMode::Disabled)
    };

    match refresh_mode {
        RefreshMode::Full | RefreshMode::Disabled => {}
        RefreshMode::Append => {
            info.push_str(", append");
        }
        RefreshMode::Changes => {
            info.push_str(", changes");
        }
        RefreshMode::Caching => {
            info.push_str(", caching");
        }
    }

    if let Some(refresh_interval) = &acceleration.refresh_check_interval {
        let _ = write!(info, ", {refresh_interval:#?} refresh");
    }
    if let Some(retention_check_interval) = &acceleration.retention_check_interval
        && acceleration.retention_check_enabled
    {
        let _ = write!(info, ", {retention_check_interval} retention");
    }
    if acceleration.on_zero_results == ZeroResultsAction::UseSource {
        info.push_str(", fallback on source on empty result");
    }
    info
}

pub fn random_trace_id() -> TraceId {
    let mut bytes = [0u8; 16];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut bytes);

    // Ensure the TraceId is not all zeros
    if bytes.iter().all(|&b| b == 0) {
        return random_trace_id();
    }

    TraceId::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::acceleration::Engine;
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataconnector::DataConnectorResult;
    use async_trait::async_trait;
    use datafusion::datasource::TableProvider;
    use std::any::Any;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug)]
    struct TestDataConnector {}

    #[async_trait]
    impl DataConnector for TestDataConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn read_provider(
            &self,
            _dataset: &Dataset,
        ) -> DataConnectorResult<Arc<dyn TableProvider>> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_dataset_registered_trace_no_acceleration() {
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let ds = DatasetBuilder::try_new("s3://taxi_trips/2024/".to_string(), "taxi_trips")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

        let test_data_connector: Arc<dyn DataConnector> = Arc::new(TestDataConnector {});
        let info = dataset_registered_trace(test_data_connector.as_ref(), &ds, false);
        assert_eq!(
            info,
            "Dataset taxi_trips registered (s3://taxi_trips/2024/)."
        );
    }

    #[tokio::test]
    async fn test_dataset_registered_trace_default_acceleration_cache() {
        let acceleration = Acceleration {
            enabled: true,
            ..Default::default()
        };

        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let mut ds = DatasetBuilder::try_new("s3://taxi_trips/2024/".to_string(), "taxi_trips")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");
        ds.acceleration = Some(acceleration);

        let test_data_connector: Arc<dyn DataConnector> = Arc::new(TestDataConnector {});
        let info = dataset_registered_trace(test_data_connector.as_ref(), &ds, true);
        assert_eq!(
            info,
            "Dataset taxi_trips registered (s3://taxi_trips/2024/), acceleration (arrow), results cache enabled."
        );
    }

    #[tokio::test]
    async fn test_dataset_registered_trace_with_acceleration_complex() {
        let acceleration = Acceleration {
            enabled: true,
            engine: Engine::DuckDB,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Append),
            refresh_check_interval: Some(Duration::from_secs(30)),
            retention_check_interval: Some("1hr".to_string()),
            retention_check_enabled: true,
            on_zero_results: ZeroResultsAction::UseSource,
            ..Default::default()
        };

        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let mut ds = DatasetBuilder::try_new("s3://taxi_trips/2024/".to_string(), "taxi_trips")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");
        ds.acceleration = Some(acceleration);

        let test_data_connector: Arc<dyn DataConnector> = Arc::new(TestDataConnector {});
        let info = dataset_registered_trace(test_data_connector.as_ref(), &ds, false);
        assert_eq!(
            info,
            "Dataset taxi_trips registered (s3://taxi_trips/2024/), acceleration (duckdb:file, append, 30s refresh, 1hr retention, fallback on source on empty result)."
        );
    }

    #[test]
    fn test_view_registered_trace_no_acceleration() {
        let table_ref = TableReference::from("taxi_trips_vw");
        let info = view_registered_trace(&table_ref, None);
        assert_eq!(info, "View taxi_trips_vw registered.");
    }

    #[test]
    fn test_view_registered_trace_with_default_acceleration() {
        let table_ref = TableReference::from("taxi_trips_vw");
        let acceleration = Some(Acceleration {
            enabled: true,
            ..Default::default()
        });
        let info = view_registered_trace(&table_ref, acceleration.as_ref());
        assert_eq!(info, "View taxi_trips_vw registered, acceleration (arrow).");
    }

    #[test]
    fn test_view_registered_trace_with_complex_acceleration() {
        let table_ref = TableReference::from("taxi_trips_vw");
        let acceleration = Some(Acceleration {
            enabled: true,
            engine: Engine::DuckDB,
            mode: Mode::File,
            refresh_check_interval: Some(Duration::from_secs(30)),
            ..Default::default()
        });
        let info = view_registered_trace(&table_ref, acceleration.as_ref());
        assert_eq!(
            info,
            "View taxi_trips_vw registered, acceleration (duckdb:file, 30s refresh)."
        );
    }
}
