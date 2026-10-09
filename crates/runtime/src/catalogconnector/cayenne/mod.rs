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
    Runtime,
    component::catalog::{Catalog, table_selector},
    dataconnector::parameters::ConnectorParams,
    parameters::Parameters,
};
use async_trait::async_trait;
use cayenne::{CayenneCatalogProvider, CayenneCatalogProviderConfig};
use data_components::RefreshableCatalogProvider as _;
use std::any::Any;
use std::collections::HashMap;
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
        .description("Ignored: the Vortex segment cache is now one budget shared by every Cayenne table rather than a cache per catalog, so a per-catalog size no longer has anything to size. Set runtime.params.cayenne_segment_cache_mb instead (in MB; 0 disables caching). A value set here is reported at startup and otherwise has no effect."),
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
    // Retired: tuning and the goals moved to `runtime.params` (`adaptive_tuning`, `target_*`). Listed only so `Parameters` drops
    // it without a second, generic warning; left out of the published schema.
    ParameterSpec::component("tuning").moved_to("runtime.params.adaptive_tuning"),
    ParameterSpec::component("goal_replication_lag")
        .moved_to("runtime.params.target_replication_lag"),
    ParameterSpec::component("goal_freshness").moved_to("runtime.params.target_freshness"),
    ParameterSpec::component("goal_query_latency")
        .moved_to("runtime.params.target_query_latency"),
    ParameterSpec::component("goal_convergence_window")
        .moved_to("runtime.params.target_convergence_window"),
    ParameterSpec::component("goal_qph").moved_to("runtime.params.target_qph"),
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

    /// `runtime_params` is the runtime's `runtime.params`, which carry the runtime-wide
    /// `adaptive_tuning` mode and `target_*` setpoints.
    async fn parse_provider_config(
        &self,
        catalog_name: Option<&str>,
        runtime_params: &HashMap<String, String>,
    ) -> CayenneCatalogProviderConfig {
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

        // The segment cache is process-wide, so a per-catalog size has nothing to
        // size. Report it the same way the per-dataset parameter is reported,
        // rather than discarding it silently — a catalog that set `0` to disable
        // caching would otherwise be given a cache with no indication why.
        let segment_cache_mb = self
            .params
            .get("segment_cache_mb")
            .expose()
            .ok()
            .and_then(|v| parse_num_param::<usize>(v, "segment_cache_mb"));
        if self.params.get("segment_cache_mb").expose().ok().is_some() {
            tracing::warn!(
                "catalog.params.cayenne_segment_cache_mb is ignored. The Vortex segment cache is now a single budget shared by every Cayenne table instead of one cache per catalog. To control it, set runtime.params.cayenne_segment_cache_mb (in MB; 0 disables caching). See: https://spiceai.org/docs/components/catalogs/cayenne"
            );
        }
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

        // Tuning mode (`runtime.params.adaptive_tuning`): `disabled` (default) keeps the static,
        // hardware-derived knobs; `enabled` additionally runs the closed-loop
        // controller in `cayenne::provider::context`. Unlike the accelerator
        // path, the catalog path has no schema inference, so `enabled` is seeded
        // purely from the detected `HardwareProfile` — the controller's bounds
        // anchor to `[floor, 4×seed]`, so a host-appropriate seed is essential.
        let raw_tuning = runtime_params.get("adaptive_tuning").map(String::as_str);

        // Probe under the resolved data/metadata dirs, falling back to the data base path.
        let base = crate::spice_data_base_path();
        let data_path = data_dir.clone().unwrap_or_else(|| base.clone());
        let metastore_path = metadata_dir.clone().unwrap_or(base);

        // The engine owns both the `adaptive_tuning` vocabulary and the hardware probe: a catalog
        // has no schema inference, so the seed comes from the host alone, and the
        // controller anchors its bounds to `[floor, 4x seed]` — a seed that ignored the
        // host would leave it riding the wrong window. Asked through the registration
        // slice rather than by naming the engine crate.
        let tuning = data_accelerator_api::DATA_ACCELERATOR_REGISTRATIONS
            .iter()
            .find(|registration| registration.engine == runtime_acceleration::Engine::Cayenne)
            // Built only to ask for tuning seeds, which are derived from the host rather
            // than from any runtime-level setting.
            .and_then(data_accelerator_api::AcceleratorRegistration::build_with_defaults);
        let outcome = if let Some(engine) = tuning {
            engine
                .adaptive_tuning_seeds(runtime_params, &data_path, &metastore_path)
                .await
        } else {
            // This catalog builds its provider from the `cayenne` library, so it works in a
            // binary that links no Cayenne *accelerator* — but only the engine can seed the
            // controller, so `enabled` cannot be honoured here. Say so rather than quietly
            // serving a statically-tuned catalog, which is the same configuration an
            // operator would get by setting `disabled`.
            //
            // The engine owns the `adaptive_tuning` vocabulary; the two names are recognized here
            // only to decide which warning a build that cannot ask should emit, so that a
            // typo is still reported rather than passing as a valid mode.
            let value = raw_tuning.map(str::trim).unwrap_or_default();
            if value.eq_ignore_ascii_case("enabled") {
                tracing::warn!(
                    "`runtime.params.adaptive_tuning` is `enabled`, but this build links no Cayenne accelerator to size the controller, so this catalog runs with static tuning (`disabled`) instead. Link the `accelerator-cayenne` crate to enable adaptive tuning. See: https://spiceai.org/docs/components/catalogs/cayenne"
                );
            }
            data_accelerator_api::AdaptiveTuningOutcome {
                tuning_value_invalid: !value.is_empty()
                    && !value.eq_ignore_ascii_case("disabled")
                    && !value.eq_ignore_ascii_case("enabled"),
                seeds: None,
                targets: data_accelerator_api::TuningTargets::default(),
            }
        };

        // A target steers the closed loop and never turns it on.
        if outcome.targets.any_set() && outcome.seeds.is_none() {
            tracing::warn!(
                "`runtime.params.target_*` is set but `runtime.params.adaptive_tuning` is `disabled`, so catalog '{}' ignores the targets. Set `runtime.params.adaptive_tuning` to `enabled` to enable target-seeking. See: https://spiceai.org/docs/reference/spicepod/runtime",
                catalog_name.unwrap_or_default()
            );
        }

        if outcome.tuning_value_invalid {
            tracing::warn!(
                "{} This catalog runs with static tuning (`disabled`) instead.",
                spicepod::component::runtime::invalid_tuning_message(
                    raw_tuning.unwrap_or_default().trim()
                )
            );
        }
        let dynamic_tuning = outcome.seeds.is_some();

        let (
            seed_compaction_background_interval_ms,
            seed_compaction_trigger_files,
            seed_inline_flush_max_rows,
            seed_inline_flush_max_segments,
            seed_inline_flush_max_bytes,
            seed_write_concurrency,
        ) = match outcome.seeds {
            Some(seeds) => (
                Some(seeds.compaction_background_interval_ms),
                Some(seeds.compaction_trigger_files),
                Some(seeds.inline_flush_max_rows),
                Some(seeds.inline_flush_max_segments),
                Some(seeds.inline_flush_max_bytes),
                Some(seeds.write_concurrency),
            ),
            None => (None, None, None, None, None, None),
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
            catalog_name: catalog_name.map(ToOwned::to_owned),
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
            target_replication_lag_secs: outcome.targets.replication_lag_secs,
            target_freshness_secs: outcome.targets.freshness_secs,
            target_query_latency_ms: outcome.targets.query_latency_ms,
            target_convergence_window_secs: outcome.targets.convergence_window_secs,
            target_qph: outcome.targets.qph,
            compaction_background_interval_ms: seed_compaction_background_interval_ms,
            compaction_trigger_files: seed_compaction_trigger_files,
            // The catalog path keeps the engine default (50_000) as the bake-trigger
            // anchor; the adaptive controller can still move it within its bounds.
            bake_deletion_index_trigger: None,
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

        for warning in spicepod::component::runtime::retired_tuning_param_warnings(
            "catalog",
            &catalog.name,
            &catalog.params,
            spicepod::component::runtime::RETIRED_CATALOG_TUNING_PARAMS,
        ) {
            tracing::warn!("{warning}");
        }

        let runtime_params = runtime
            .app()
            .read()
            .await
            .as_ref()
            .map(|app| app.runtime.params.clone())
            .unwrap_or_default();
        let runtime_env = runtime.datafusion().ctx.runtime_env();
        let provider_config = self
            .parse_provider_config(Some(catalog.name.as_str()), &runtime_params)
            .await;
        let refreshable_provider = Arc::new(
            CayenneCatalogProvider::try_new(provider_config, runtime_env, table_selector(catalog))
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

        let config = connector
            .parse_provider_config(Some("warehouse"), &HashMap::new())
            .await;

        // Carried for diagnostics only — the storage paths stay keyed on the constant, so
        // a rename must not relocate anybody's data.
        assert_eq!(config.catalog_name.as_deref(), Some("warehouse"));
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

    #[tokio::test]
    async fn retired_catalog_tuning_param_is_not_applied() {
        let params = Parameters::try_new(
            "connector cayenne",
            vec![(
                "cayenne_tuning".to_string(),
                SecretString::new("enabled".to_string().into()),
            )],
            PREFIX,
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("a retired parameter must not fail validation");
        assert!(
            !params.to_secret_map().contains_key("tuning"),
            "`cayenne_tuning` must be dropped, not carried into the catalog config"
        );
    }

    #[test]
    fn retired_catalog_params_are_listed_so_they_are_dropped_silently() {
        for name in [
            "tuning",
            "goal_replication_lag",
            "goal_freshness",
            "goal_query_latency",
            "goal_convergence_window",
            "goal_qph",
        ] {
            let spec = PARAMETERS
                .iter()
                .find(|p| p.name == name)
                .expect("a retired catalog parameter must stay listed so it is dropped silently");
            assert!(spec.is_retired(), "`{name}` must be retired");
        }
    }

    #[tokio::test]
    async fn retired_catalog_goal_params_are_not_applied() {
        let params = Parameters::try_new(
            "connector cayenne",
            vec![
                (
                    "cayenne_goal_freshness".to_string(),
                    SecretString::new("5s".to_string().into()),
                ),
                (
                    "goal_qph".to_string(),
                    SecretString::new("100".to_string().into()),
                ),
            ],
            PREFIX,
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("a retired parameter must not fail validation");
        let map = params.to_secret_map();
        assert!(!map.contains_key("goal_freshness") && !map.contains_key("goal_qph"));
    }
}
