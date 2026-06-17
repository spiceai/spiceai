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

//! Catalog connector for Cayenne catalogs backed by local file storage.
//!
//! Cayenne catalogs use `SQLite` for metadata and Vortex files for columnar data,
//! with data stored on local disk.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::{
    Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams,
    parameters::Parameters,
};
use async_trait::async_trait;
use cayenne::{CayenneCatalogProvider, CayenneCatalogProviderConfig};
use data_components::RefreshableCatalogProvider as _;
use std::any::Any;
use std::sync::Arc;

pub mod provider;

/// Catalog connector prefix for Cayenne catalogs.
pub static PREFIX: &str = "cayenne";

/// Parameters for configuring a Cayenne catalog.
pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("data_dir")
        .description("Local directory for table data files. Defaults to spice data directory."),
    ParameterSpec::component("metadata_dir").description(
        "Local directory for Cayenne SQLite metadata. Defaults to spice data directory.",
    ),
    ParameterSpec::component("segment_cache_mb")
        .description("Vortex segment cache size in MB. Default: 256.")
        .default("256"),
    ParameterSpec::component("target_file_size_mb")
        .description("Target Vortex file size in MB. Default: 256.")
        .default("256"),
    ParameterSpec::component("compression_strategy")
        .description("Compression: 'btrblocks' (default) or 'zstd'.")
        .default("btrblocks"),
    ParameterSpec::component("pk_conflict_detection")
        .description("Whether Cayenne scans existing primary keys on insert. 'auto' (default) detects conflicts; 'none' skips conflict detection and is only safe when the source enforces primary-key uniqueness and ingestion cannot replay existing rows.")
        .one_of(&["auto", "none"])
        .default("auto"),
    ParameterSpec::component("upload_concurrency")
        .description("Maximum number of concurrent file uploads when writing multiple Vortex files. Defaults to available CPU parallelism."),
    ParameterSpec::component("write_concurrency")
        .description("Optional writer partition override for unsorted Cayenne ingests. Defaults to runtime.query.target_partitions."),
    ParameterSpec::component("inline_max_rows")
        .description("Maximum rows in a single write that can be inlined into the Cayenne metastore instead of writing a Vortex file. Set to 0 to disable write-entry inlining. Default: 1024.")
        .default("1024"),
    ParameterSpec::component("inline_max_bytes")
        .description("Maximum serialized Arrow IPC bytes in a single inlined Cayenne metastore entry. Set to 0 to disable write-entry inlining. Default: 1048576.")
        .default("1048576"),
    ParameterSpec::component("inline_max_buffer_bytes")
        .description("Maximum Arrow in-memory bytes buffered while deciding whether to inline a write. Set to 0 to force the Vortex write path after the first buffered batch. Default: 4194304.")
        .default("4194304"),
    ParameterSpec::component("inline_flush_max_rows")
        .description("Maximum inline rows before checkpointing inline data to Vortex. Default: 10000.")
        .default("10000"),
    ParameterSpec::component("inline_flush_max_segments")
        .description("Maximum inline entries before checkpointing inline data to Vortex. Default: 64.")
        .default("64"),
    ParameterSpec::component("inline_flush_max_bytes")
        .description("Maximum inline IPC bytes before checkpointing inline data to Vortex. Default: 8388608.")
        .default("8388608"),
    ParameterSpec::component("tuning")
        .description("Auto-tuning mode. 'auto' (default): use static, hardware-derived defaults. 'adaptive': additionally run a per-table closed-feedback controller that measures the live CDC ingest rate AND the runtime's whole-system response (apply latency vs offered load, read amplification, cgroup-aware memory pressure) and adjusts the inline-memtable flush caps, compaction cadence/trigger, and write concurrency over time, within a hardware-derived [floor, ceiling]. The controller's bounds anchor to the seeded knob values, which are derived from the detected host (cores + cgroup-aware memory + storage class) — no schema inference is needed on the catalog path. An explicit per-knob value (e.g. cayenne_inline_flush_max_bytes) overrides the seed and is pinned under 'adaptive'.")
        .one_of(&["auto", "adaptive"])
        .default("auto"),
];

/// A catalog connector for Cayenne lakehouse catalogs.
///
/// Cayenne catalogs provide a high-performance lakehouse format combining:
/// - `SQLite` for transactional metadata management (stored locally)
/// - Vortex columnar files for data (stored locally)
///
/// Used as `from: cayenne` in the spicepod. Does not support a catalog ID.
#[derive(Clone)]
pub struct CayenneCatalogConnector {
    params: Parameters,
}

impl CayenneCatalogConnector {
    /// Create a new Cayenne catalog connector from the given parameters.
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self {
            params: params.parameters,
        })
    }

    async fn parse_provider_config(&self) -> CayenneCatalogProviderConfig {
        // Parse a numeric catalog parameter, warning (and ignoring) on a value
        // that does not parse, so a typo surfaces instead of being silently
        // dropped — matching the acceleration-param path's behavior.
        fn parse_num_param<T: std::str::FromStr>(value: &str, key: &str) -> Option<T> {
            if let Ok(parsed) = value.parse::<T>() {
                Some(parsed)
            } else {
                tracing::warn!(
                    "Invalid Cayenne catalog parameter `{key}` value `{value}`; expected a number, ignoring it. See https://spiceai.org/docs/components/catalogs/cayenne"
                );
                None
            }
        }
        let data_dir = self
            .params
            .get("data_dir")
            .expose()
            .ok()
            .map(ToOwned::to_owned);
        let metadata_dir = self
            .params
            .get("metadata_dir")
            .expose()
            .ok()
            .map(ToOwned::to_owned);

        let segment_cache_mb = self
            .params
            .get("segment_cache_mb")
            .expose()
            .ok()
            .and_then(|v| parse_num_param::<usize>(v, "segment_cache_mb"));
        let target_file_size_mb = self
            .params
            .get("target_file_size_mb")
            .expose()
            .ok()
            .and_then(|v| parse_num_param::<usize>(v, "target_file_size_mb"));
        let compression_strategy = self
            .params
            .get("compression_strategy")
            .expose()
            .ok()
            .and_then(|v| match v.to_lowercase().as_str() {
                "zstd" => Some(cayenne::metadata::CompressionStrategy::Zstd),
                "btrblocks" => Some(cayenne::metadata::CompressionStrategy::Btrblocks),
                other => {
                    tracing::warn!(
                        "Invalid Cayenne catalog parameter `compression_strategy` value `{other}`; expected `zstd` or `btrblocks`, ignoring it."
                    );
                    None
                }
            });
        let pk_conflict_detection = self
            .params
            .get("pk_conflict_detection")
            .expose()
            .ok()
            .and_then(|v| {
                cayenne::metadata::PkConflictDetection::parse(v).or_else(|| {
                    tracing::warn!(
                        "Invalid Cayenne catalog parameter `pk_conflict_detection` value `{v}`; expected `auto` or `none`, ignoring it."
                    );
                    None
                })
            });
        let upload_concurrency = self
            .params
            .get("upload_concurrency")
            .expose()
            .ok()
            .and_then(|v| parse_num_param::<usize>(v, "upload_concurrency"))
            .map(|v| v.max(1));
        let write_concurrency = self
            .params
            .get("write_concurrency")
            .expose()
            .ok()
            .and_then(|v| parse_num_param::<usize>(v, "write_concurrency"))
            .map(|v| v.max(1));
        let inline_max_rows = self
            .params
            .get("inline_max_rows")
            .expose()
            .ok()
            .and_then(|v| parse_num_param::<usize>(v, "inline_max_rows"));
        let inline_max_bytes = self
            .params
            .get("inline_max_bytes")
            .expose()
            .ok()
            .and_then(|v| parse_num_param::<usize>(v, "inline_max_bytes"));
        let inline_max_buffer_bytes = self
            .params
            .get("inline_max_buffer_bytes")
            .expose()
            .ok()
            .and_then(|v| parse_num_param::<usize>(v, "inline_max_buffer_bytes"));
        let inline_flush_max_rows = self
            .params
            .get("inline_flush_max_rows")
            .expose()
            .ok()
            .and_then(|v| parse_num_param::<i64>(v, "inline_flush_max_rows"))
            .map(|v| v.max(0));
        let inline_flush_max_segments = self
            .params
            .get("inline_flush_max_segments")
            .expose()
            .ok()
            .and_then(|v| parse_num_param::<i64>(v, "inline_flush_max_segments"))
            .map(|v| v.max(0));
        let inline_flush_max_bytes = self
            .params
            .get("inline_flush_max_bytes")
            .expose()
            .ok()
            .and_then(|v| parse_num_param::<i64>(v, "inline_flush_max_bytes"))
            .map(|v| v.max(0));

        // Tuning mode (`cayenne_tuning`): `auto` (default) keeps the static,
        // hardware-derived knobs; `adaptive` additionally runs the closed-loop
        // controller in `cayenne::provider::context`. Unlike the accelerator
        // path, the catalog path has no schema inference, so `adaptive` is seeded
        // purely from the detected `HardwareProfile` — the controller's bounds
        // anchor to `[floor, 4×seed]`, so a host-appropriate seed is essential.
        let tuning_mode = self
            .params
            .get("tuning")
            .expose()
            .ok()
            .map(|v| v.trim().to_ascii_lowercase());
        if let Some(mode) = &tuning_mode
            && mode != "auto"
            && mode != "adaptive"
        {
            tracing::warn!(
                "Invalid Cayenne catalog parameter `tuning` value `{mode}`; expected `auto` or `adaptive`, defaulting to `auto`."
            );
        }
        let dynamic_tuning = tuning_mode.as_deref() == Some("adaptive");

        // Seed the adaptive-tunable knobs from the host hardware profile (only
        // when adaptive is requested — `auto` keeps the engine defaults so the
        // static path is byte-identical to prior behavior). The seed values are
        // ONLY applied where the operator did not pin the knob explicitly, so an
        // explicit `cayenne_*` value still wins.
        let (
            seed_compaction_background_interval_ms,
            seed_compaction_trigger_files,
            seed_inline_flush_max_rows,
            seed_inline_flush_max_segments,
            seed_inline_flush_max_bytes,
            seed_write_concurrency,
        ) = if dynamic_tuning {
            use crate::dataaccelerator::cayenne::autotune::{HardwareProfile, WorkloadProfile};
            // Probe storage under the resolved data/metadata dirs (falling back to
            // the spice data base path), mirroring the accelerator's detection.
            let base = crate::spice_data_base_path();
            let data_path = data_dir.clone().unwrap_or_else(|| base.clone());
            let metastore_path = metadata_dir.clone().unwrap_or(base);
            // No StorageProfile override is plumbed on the catalog path; auto-detect.
            let hw = HardwareProfile::detect(
                crate::component::dataset::acceleration::StorageProfile::Auto,
                &data_path,
                &metastore_path,
            )
            .await;
            // Hardware-only workload profile (no inferred row_count / row width).
            let wl = WorkloadProfile::default();
            let caps = hw.inline_flush_caps(&wl);
            (
                // Small-write/CDC cadence so the controller has a tick to ride.
                Some(10_000_u64),
                Some(4_usize),
                Some(caps.max_rows),
                Some(caps.max_segments),
                Some(caps.max_bytes),
                // Seed write concurrency to the host core count so the controller's
                // [1, cores] window is host-appropriate.
                Some(hw.cores),
            )
        } else {
            (None, None, None, None, None, None)
        };

        // The seed only applies where the operator did not set the knob; an
        // explicit catalog param always wins.
        let inline_flush_max_rows = inline_flush_max_rows.or(seed_inline_flush_max_rows);
        let inline_flush_max_segments =
            inline_flush_max_segments.or(seed_inline_flush_max_segments);
        let inline_flush_max_bytes = inline_flush_max_bytes.or(seed_inline_flush_max_bytes);
        let write_concurrency = write_concurrency.or(seed_write_concurrency);

        CayenneCatalogProviderConfig {
            data_dir,
            metadata_dir,
            spice_data_base_path: crate::spice_data_base_path(),
            footer_cache_mb: None,
            segment_cache_mb,
            target_file_size_mb,
            compression_strategy,
            pk_conflict_detection,
            upload_concurrency,
            write_concurrency,
            inline_max_rows,
            inline_max_bytes,
            inline_max_buffer_bytes,
            inline_flush_max_rows,
            inline_flush_max_segments,
            inline_flush_max_bytes,
            dynamic_tuning,
            compaction_background_interval_ms: seed_compaction_background_interval_ms,
            compaction_trigger_files: seed_compaction_trigger_files,
        }
    }
}

#[async_trait]
impl CatalogConnector for CayenneCatalogConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn data_components::RefreshableCatalogProvider>> {
        if runtime
            .datafusion()
            .cluster_config
            .effective_role()
            .is_none()
        {
            return Err(super::Error::InvalidConfigurationNoSource {
                connector: PREFIX.to_string(),
                connector_component: ConnectorComponent::from(catalog),
                message: "Cayenne catalog is only supported in distributed Spice mode. Start Spice with `--role scheduler` or `--role executor` (with `--scheduler-address`). See https://spiceai.org/docs/features/distributed-query".to_string(),
            });
        }

        let runtime_env = runtime.datafusion().ctx.runtime_env();
        let provider_config = self.parse_provider_config().await;
        let refreshable_provider = Arc::new(
            CayenneCatalogProvider::try_new(provider_config, runtime_env)
                .await
                .map_err(|e| super::Error::UnableToGetCatalogProvider {
                    connector: PREFIX.to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?,
        );

        refreshable_provider.refresh().await.map_err(|source| {
            super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: ConnectorComponent::from(catalog),
                source,
            }
        })?;

        Ok(refreshable_provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parameters::Parameters;
    use runtime_secrets::Secrets;
    use secrecy::SecretString;
    use tokio::sync::RwLock;

    #[test]
    fn catalog_parameter_specs_render_single_cayenne_prefix() {
        let display_names: Vec<String> = PARAMETERS
            .iter()
            .map(|parameter| parameter.display_name(PREFIX))
            .collect();

        assert!(display_names.contains(&"cayenne_upload_concurrency".to_string()));
        assert!(display_names.contains(&"cayenne_write_concurrency".to_string()));
        assert!(display_names.contains(&"cayenne_inline_max_rows".to_string()));
        assert!(display_names.contains(&"cayenne_inline_flush_max_bytes".to_string()));
        assert!(display_names.contains(&"cayenne_pk_conflict_detection".to_string()));
        assert!(
            display_names
                .iter()
                .all(|name| !name.starts_with("cayenne_cayenne_")),
            "Cayenne catalog parameter specs should not include the prefix in component names"
        );
    }

    #[tokio::test]
    async fn parse_provider_config_uses_normalized_catalog_params() {
        let params = Parameters::try_new(
            "connector cayenne",
            vec![
                (
                    "cayenne_data_dir".to_string(),
                    SecretString::new("/tmp/cayenne-data".to_string().into()),
                ),
                (
                    "cayenne_upload_concurrency".to_string(),
                    SecretString::new("0".to_string().into()),
                ),
                (
                    "cayenne_write_concurrency".to_string(),
                    SecretString::new("8".to_string().into()),
                ),
                (
                    "cayenne_inline_max_rows".to_string(),
                    SecretString::new("0".to_string().into()),
                ),
                (
                    "cayenne_inline_flush_max_bytes".to_string(),
                    SecretString::new("2097152".to_string().into()),
                ),
                (
                    "cayenne_pk_conflict_detection".to_string(),
                    SecretString::new("none".to_string().into()),
                ),
            ],
            PREFIX,
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("single-prefixed Cayenne catalog params should validate");
        let connector = CayenneCatalogConnector { params };

        let config = connector.parse_provider_config().await;

        assert_eq!(config.data_dir.as_deref(), Some("/tmp/cayenne-data"));
        assert_eq!(config.upload_concurrency, Some(1));
        assert_eq!(config.write_concurrency, Some(8));
        assert_eq!(config.inline_max_rows, Some(0));
        assert_eq!(config.inline_flush_max_bytes, Some(2_097_152));
        assert_eq!(
            config.pk_conflict_detection,
            Some(cayenne::metadata::PkConflictDetection::None)
        );
    }
}
