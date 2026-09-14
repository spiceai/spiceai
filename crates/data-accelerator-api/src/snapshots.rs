/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Snapshot bootstrap for a file-backed acceleration: download one before the engine
//! creates its table, and create one before an engine recreates it.
//!
//! Lives beside the accelerator contract rather than in `runtime` because the engines
//! that call it do — they are linked by the binary, not by the orchestrator. The one
//! thing it needs from the runtime, the acceleration checkpoint to reconcile a
//! downloaded snapshot against, arrives through
//! [`AccelerationSource::checkpointer_factory`].

use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Instant};

use runtime_acceleration::BootstrapStatus;
use runtime_acceleration::acceleration::{Acceleration, Mode, RefreshMode};
use runtime_acceleration::acceleration_source::{
    AccelerationSource, MaterializationSource, SourceDefinition,
};
use runtime_acceleration::snapshot::engine::SnapshotEngine;
use runtime_acceleration::snapshot::{
    AccelerationEngine, AccelerationLayout, ForceCreate, SnapshotBehavior, SnapshotManager, metrics,
};
use snafu::Snafu;

use crate::{AcceleratorEngineRegistry, acceleration_file_path};

/// Whether `mode: file_create` still permits bootstrapping from a snapshot.
///
/// `file_create` snapshots the outgoing acceleration and deletes it so the next refresh
/// rebuilds from the source. Bootstrapping straight back from that snapshot would undo
/// the delete, so a refresh that replays everything must not bootstrap. A refresh that
/// does *not* replay from the beginning still needs it, or rows nothing can re-send are
/// gone.
///
/// `refresh_mode` arrives resolved: resolving it consults the connector, which this
/// crate cannot reach. Erring toward keeping the bootstrap costs `file_create` some of
/// its effect on a CDC dataset; erring the other way destroys rows nothing can re-send.
fn mode_allows_snapshot_bootstrap(acceleration: &Acceleration, refresh_mode: RefreshMode) -> bool {
    if acceleration.mode != Mode::FileCreate {
        return true;
    }

    !matches!(refresh_mode, RefreshMode::Full | RefreshMode::Caching)
}

pub async fn download_snapshot_if_needed(
    acceleration: &Acceleration,
    source: &dyn AccelerationSource,
    layout: AccelerationLayout,
    engine: AccelerationEngine,
    engine_override: Option<Arc<dyn SnapshotEngine>>,
    refresh_mode: RefreshMode,
) -> BootstrapStatus {
    if !acceleration.snapshot_behavior.bootstrap_enabled() {
        return BootstrapStatus::none();
    }

    if !mode_allows_snapshot_bootstrap(acceleration, refresh_mode) {
        tracing::info!(
            "Acceleration mode is 'file_create' for dataset {}, skipping snapshot bootstrap so the next refresh rebuilds the acceleration from the source",
            source.name()
        );
        return BootstrapStatus::none();
    }

    if !layout.is_enabled() {
        tracing::debug!("No storage paths for the acceleration layout, skipping download");
        return BootstrapStatus::none();
    }

    // Asks the layout whether an acceleration is already present, rather than testing one
    // path for existence. For a directory-layout engine the accelerator has already
    // created its directories by the time this runs — Cayenne's metastore creates the
    // metadata directory the moment it opens, and that directory is shared by every
    // Cayenne dataset in the pod — so an existence test on a path would answer "yes"
    // unconditionally and silently skip every bootstrap.
    if layout.has_existing_acceleration() {
        tracing::info!(
            "Acceleration for '{}' already exists at {}, skipping snapshot download",
            source.name(),
            layout.data_path().map_or_else(
                || "the configured location".to_string(),
                |p| p.display().to_string()
            )
        );
        return BootstrapStatus::none();
    }

    let dataset_name = source.name().to_string();
    // The source opens its own checkpoint: each engine's checkpointer carries that
    // engine's sidecar SQL and lives in its own `runtime-checkpoint-*` crate, so it
    // reaches here as a factory behind the `AccelerationSource` contract rather than
    // as a type this crate names.
    let checkpoint_factory = source.checkpointer_factory(acceleration.snapshot_behavior.clone());
    if let Some(manager) = SnapshotManager::try_new(
        dataset_name.clone(),
        acceleration.snapshot_behavior.clone(),
        layout,
        engine,
    )
    .await
    {
        let mut manager = manager.with_checkpointer_factory(checkpoint_factory);
        if let Some(engine_override) = engine_override {
            manager = manager.with_snapshot_engine(engine_override);
        }
        // A source whose rows are the result of a definition (a view's SQL) must not
        // bootstrap an archive materialized from a different one: the rows would be
        // wrong rather than merely old, and no schema check would catch it.
        if let Some(definition) = source.definition_fingerprint() {
            manager = manager.with_source_definition(definition);
        }
        let start_time = Instant::now();
        match manager.download_latest_snapshot().await {
            Ok(Some(info)) => {
                let duration_ms = start_time.elapsed().as_secs_f64() * 1000.0;
                metrics::record_bootstrap_metrics(
                    &dataset_name,
                    duration_ms,
                    info.bytes_downloaded,
                    &info.checksum,
                );
                BootstrapStatus::bootstrapped(info)
            }
            Ok(None) => BootstrapStatus::none(),
            Err(e) => {
                tracing::error!(dataset = %dataset_name, error = %e, "Failed to download snapshot");
                BootstrapStatus::none()
            }
        }
    } else {
        BootstrapStatus::none()
    }
}

/// Creates a snapshot of the existing acceleration file before it is deleted or recreated.
///
/// Called during `file_create` and `file_update` (on schema mismatch) modes to preserve
/// a copy of the current acceleration data before it is destroyed.
///
/// This is a best-effort operation: if snapshotting fails, a warning is logged and the
/// caller proceeds with recreation.
///
/// `engine_override` parallels [`download_snapshot_if_needed`].
pub async fn snapshot_before_recreate(
    acceleration: &Acceleration,
    source: &dyn AccelerationSource,
    layout: AccelerationLayout,
    engine: AccelerationEngine,
    schema: Arc<arrow_schema::Schema>,
    engine_override: Option<Arc<dyn SnapshotEngine>>,
    refresh_mode: RefreshMode,
) {
    if !acceleration.snapshot_behavior.create_enabled() {
        return;
    }

    let dataset_name = source.name().to_string();

    // A source whose rows are the result of a query cannot publish from here. This runs
    // inside the accelerator's `init`, before the runtime has planned the definition, so
    // there is no way to establish that the outgoing materialization came from a single
    // read — and publishing makes whatever it holds the store's current snapshot. The live
    // publish path decides that question with the compiled plan in hand; this one would be
    // guessing, and the cost of guessing wrong is a durable wrong answer rather than a
    // missing backup.
    //
    // Asks what produced the rows rather than whether a fingerprint exists: a dataset also
    // carries one (its `from:` and `refresh_sql`), and treating that as "cannot publish"
    // would silently drop the pre-recreation backup for every dataset.
    if source
        .definition_fingerprint()
        .is_some_and(|definition| definition.materialization == MaterializationSource::PlannedQuery)
    {
        tracing::warn!(
            "Skipped snapshotting the outgoing acceleration of '{dataset_name}' before recreating it, so the snapshot series keeps its previously published contents: Spice cannot confirm from here that those rows came from a single consistent read of this view's sources"
        );
        return;
    }

    // `refresh_mode: snapshot` is a read-only consumer of the snapshot store, so it must
    // never publish. Its local acceleration is a copy of a snapshot someone else owns,
    // and creating one makes the uploaded bytes the store's `current-snapshot-id`: a
    // replica lagging behind would publish its stale copy under a higher id and roll
    // every other reader back onto it.
    if refresh_mode == RefreshMode::Snapshot {
        tracing::debug!(
            dataset = %dataset_name,
            "refresh_mode: snapshot consumes snapshots without publishing them; skipping pre-recreation snapshot"
        );
        return;
    }

    // A partitioned Cayenne dataset's metastore slice is incomplete: `export_dataset`
    // selects every dependent table by the parent's table id, so the partition child
    // `cayenne_table` rows (and each child's metadata) are not exported, and a restore
    // fails at `infer_existing_partitions` with `TableNotFound` once the drop cascade has
    // removed the live child rows. Publishing would make that unrestorable archive the
    // store's `current-snapshot-id`, so skip until the slice covers child tables.
    // `build_snapshot_creation_config` applies the same gate to the periodic publish path.
    if engine == AccelerationEngine::Cayenne && !acceleration.partition_by.is_empty() {
        tracing::warn!(
            dataset = %dataset_name,
            "Skipping the pre-recreation snapshot: snapshots of a partitioned Cayenne acceleration are not yet supported, and an archive without the partitions' metadata could not be restored"
        );
        return;
    }

    // A Cayenne bootstrap needs the per-dataset metastore slice that only
    // `CayenneSnapshotEngine` writes, and creating a snapshot makes whatever it uploads
    // the store's `current-snapshot-id`. Publishing a default-engine archive (a raw
    // `cayenne.db`, no slice) would replace a restorable current snapshot with one
    // nothing can load, which is worse than keeping no backup of this wipe. The caller
    // still recreates the acceleration either way.
    if engine == AccelerationEngine::Cayenne && engine_override.is_none() {
        tracing::warn!(
            dataset = %dataset_name,
            "Skipping the pre-recreation snapshot: this dataset's Cayenne metastore catalog is unavailable, and an archive without its metastore slice could not be restored"
        );
        return;
    }

    let Some(manager) = SnapshotManager::try_new(
        dataset_name.clone(),
        acceleration.snapshot_behavior.clone(),
        layout,
        engine,
    )
    .await
    else {
        return;
    };
    let manager = if let Some(engine_override) = engine_override {
        manager.with_snapshot_engine(engine_override)
    } else {
        manager
    };

    // The newly loaded Spicepod is the *incoming* definition. These rows were
    // produced under the outgoing one. Stamping the incoming fingerprint would
    // make a later cold start accept those old rows as current after a
    // same-schema `from:` / params change. Publish only when the outgoing
    // fingerprint was persisted with the *local* checkpoint — the remote
    // snapshot stamp is the last published identity, which after a withheld
    // override is not what these rows are.
    let local_fingerprint = local_materialization_fingerprint(
        source,
        acceleration.snapshot_behavior.clone(),
        &dataset_name,
    )
    .await;
    let Some(definition) = outgoing_definition_for_pre_recreation(
        pre_recreation_stamp_fingerprint(local_fingerprint.as_deref(), None),
    ) else {
        tracing::warn!(
            "Skipped snapshotting the outgoing acceleration of '{dataset_name}' before recreating it, so the snapshot series keeps its previously published contents: the outgoing definition was not persisted with this materialization, and stamping the newly loaded definition would label those rows as current"
        );
        return;
    };
    let manager = manager.with_source_definition(definition);

    // If the caller provided an empty schema (e.g. during file_create init when the table
    // provider isn't available yet), try to read the real schema from existing snapshot
    // metadata. If no stored schema exists either, skip the snapshot to avoid storing an
    // empty schema that would make this snapshot unrestorable.
    let snapshot_schema = if schema.fields().is_empty() {
        let Some(stored) = manager.current_stored_schema().await else {
            tracing::debug!(dataset = %dataset_name, "No stored schema available for pre-recreation snapshot; skipping");
            return;
        };
        stored
    } else {
        Arc::clone(&schema)
    };

    // Create a mutex just for this one-off snapshot; no other operations are concurrent at init time.
    let mutex = Arc::new(tokio::sync::Mutex::new(()));
    let lock_guard = mutex.lock_owned().await;

    match manager
        .create_snapshot(&snapshot_schema, lock_guard, None, None, ForceCreate(true))
        .await
    {
        Ok(Some(path)) => {
            tracing::info!(dataset = %dataset_name, snapshot = %path, "Created pre-recreation snapshot");
        }
        Ok(None) => {
            tracing::debug!(dataset = %dataset_name, "No snapshot created before recreation");
        }
        Err(e) => {
            tracing::warn!(dataset = %dataset_name, error = %e, "Failed to create pre-recreation snapshot; proceeding with recreation");
        }
    }
}

/// Rejects a configuration in which two snapshot-enabled components snapshot to the same
/// path.
///
/// Views take part alongside datasets, so the collision is reported with each component's
/// own label rather than calling everything a dataset.
///
/// # Errors
///
/// Returns [`SharedAccelerationSnapshotError`] naming the components that collide.
pub async fn validate_snapshot_paths(
    sources: Vec<Arc<dyn AccelerationSource>>,
    registry: &AcceleratorEngineRegistry,
) -> Result<(), SharedAccelerationSnapshotError> {
    let mut paths: HashMap<PathBuf, Vec<String>> = HashMap::new();

    for source in sources {
        let Some(acceleration) = source.acceleration() else {
            continue;
        };

        if matches!(acceleration.snapshot_behavior, SnapshotBehavior::Disabled) {
            continue;
        }

        if !source.is_file_accelerated() {
            continue;
        }

        match acceleration_file_path(source.as_ref(), registry).await {
            Ok(path) => {
                paths.entry(path).or_default().push(format!(
                    "{} '{}'",
                    source.component_label(),
                    source.name()
                ));
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to resolve the acceleration file path of {} '{}', so Spice cannot check whether it shares that file with another snapshot-enabled component. Cause: {err}",
                    source.component_label(),
                    source.name()
                );
            }
        }
    }

    if let Some((path, components)) = paths.into_iter().find(|(_, c)| c.len() > 1) {
        return Err(SharedAccelerationSnapshotError::DuckDbSharedFile {
            components: components.join(", "),
            path: path.display().to_string(),
        });
    }

    Ok(())
}

/// The identity persisted with the local acceleration, if any.
///
/// Opens the local checkpoint rather than reading remote snapshot metadata:
/// after snapshot A is published and a request-scoped override replaces the
/// local rows with B (publication withheld), the remote store still holds A.
async fn local_materialization_fingerprint(
    source: &dyn AccelerationSource,
    snapshot_behavior: SnapshotBehavior,
    dataset_name: &str,
) -> Option<String> {
    let factory = source.checkpointer_factory(snapshot_behavior);
    match factory().await {
        Ok(checkpointer) => match checkpointer.get_source_fingerprint().await {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                tracing::debug!(
                    dataset = %dataset_name,
                    error = %error,
                    "Could not read the local acceleration's persisted definition, so the pre-recreation snapshot will be skipped"
                );
                None
            }
        },
        Err(error) => {
            tracing::debug!(
                dataset = %dataset_name,
                error = %error,
                "Could not open the local acceleration checkpoint, so the pre-recreation snapshot will be skipped"
            );
            None
        }
    }
}

/// The fingerprint a pre-recreation archive may carry.
///
/// Only the identity persisted with the local checkpoint is used.
/// `remote_snapshot_fingerprint` is accepted so the override-B / stamp-A
/// failure mode is testable: after A is published and a request-scoped
/// override replaces the local rows with B, the remote store still holds A.
/// Consulting that remote value would publish B stamped as A.
fn pre_recreation_stamp_fingerprint(
    local_checkpoint_fingerprint: Option<&str>,
    _remote_snapshot_fingerprint: Option<&str>,
) -> Option<String> {
    local_checkpoint_fingerprint.map(ToString::to_string)
}

/// The definition stamp a pre-recreation archive may carry.
///
/// Those rows were produced under the previously persisted fingerprint, not the
/// newly loaded Spicepod. If that outgoing fingerprint was not persisted with
/// the local materialization, the archive must not be published: stamping the
/// incoming definition would make a later cold start accept the old rows as
/// current.
fn outgoing_definition_for_pre_recreation(
    persisted_outgoing_fingerprint: Option<String>,
) -> Option<SourceDefinition> {
    Some(SourceDefinition {
        fingerprint: persisted_outgoing_fingerprint?,
        accept_unstamped: false,
        materialization: MaterializationSource::SourceTable,
    })
}

#[derive(Debug, Snafu)]
pub enum SharedAccelerationSnapshotError {
    #[snafu(display(
        "DuckDB doesn't support snapshots for shared acceleration, so none of these can be \
        snapshotted: {components} all share the acceleration file '{path}'. \
        Give each one its own file with `duckdb_file`. \
        See: https://spiceai.org/docs/components/data-accelerators/duckdb"
    ))]
    DuckDbSharedFile { components: String, path: String },
}

#[derive(Debug, Snafu)]
pub enum CayenneSnapshotValidationError {
    #[snafu(display(
        "Cayenne components sharing the metadata directory '{metadata_dir}' disagree about \
        snapshots, so none of them load. Snapshots enabled: {enabled_components}. \
        Snapshots disabled: {disabled_components}. \
        Set the same `snapshots` value on all of them, or give them separate metadata directories. \
        See: https://spiceai.org/docs/components/data-accelerators/cayenne#snapshots"
    ))]
    InconsistentSnapshotSettings {
        metadata_dir: String,
        enabled_components: String,
        disabled_components: String,
    },

    #[snafu(display(
        "Cayenne doesn't support snapshots for shared acceleration, so none of these can be \
        snapshotted: {components} all share the metadata directory '{metadata_dir}'. \
        Give each one its own metadata directory. \
        See: https://spiceai.org/docs/components/data-accelerators/cayenne#snapshots"
    ))]
    SharedAcceleration {
        metadata_dir: String,
        components: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These messages are the only explanation an operator gets for a component that
    /// refused to load, and views now reach them alongside datasets — so a reword must not
    /// quietly go back to calling everything a dataset, drop the resource, or drop the fix.
    #[test]
    fn a_shared_file_names_each_component_by_its_own_label() {
        let message = SharedAccelerationSnapshotError::DuckDbSharedFile {
            components: "view 'orders_us', dataset 'orders'".to_string(),
            path: "/data/accel.db".to_string(),
        }
        .to_string();

        assert!(
            message.contains("view 'orders_us'") && message.contains("dataset 'orders'"),
            "each colliding component must be named with its own label: {message}"
        );
        assert!(
            !message.contains("Datasets ["),
            "a collision involving a view must not be reported as a dataset-only problem: {message}"
        );
        assert!(
            message.contains("/data/accel.db") && message.contains("duckdb_file"),
            "the message must name the shared file and the parameter that separates them: {message}"
        );
    }

    /// After snapshot A is published, a request-scoped override can replace the
    /// local acceleration with rows B while publication is withheld. The remote
    /// store still holds A's fingerprint; the local checkpoint does not. The
    /// stamp must come from local provenance only — using remote A would
    /// publish B as A.
    #[test]
    fn pre_recreation_does_not_stamp_override_rows_with_the_remote_snapshot() {
        assert!(
            pre_recreation_stamp_fingerprint(None, Some("sha256:A")).is_none(),
            "pre_recreate_uses_remote_fingerprint=false local_materialization_provenance_is_consulted=true: override-B rows must not inherit remote snapshot A's stamp"
        );
        assert!(
            outgoing_definition_for_pre_recreation(pre_recreation_stamp_fingerprint(
                None,
                Some("sha256:A")
            ))
            .is_none(),
            "a pre-recreation archive of override-B rows must be skipped, not stamped as A"
        );

        assert_eq!(
            pre_recreation_stamp_fingerprint(Some("sha256:A"), Some("sha256:A")).as_deref(),
            Some("sha256:A"),
            "configured local rows may be stamped with the identity persisted beside them"
        );

        assert_eq!(
            pre_recreation_stamp_fingerprint(Some("sha256:B"), Some("sha256:A")).as_deref(),
            Some("sha256:B"),
            "when local provenance exists it is used even if the remote snapshot disagrees"
        );
    }

    /// Same-schema `from:` / params change: the outgoing file still has the old
    /// rows. The newly loaded Spicepod fingerprint must not become the stamp —
    /// that would make a cold-start bootstrap accept those rows as current.
    #[test]
    fn pre_recreation_does_not_stamp_outgoing_rows_with_the_incoming_definition() {
        let incoming = "sha256:new-from";
        let outgoing = "sha256:old-from";

        assert!(
            outgoing_definition_for_pre_recreation(None).is_none(),
            "without a persisted outgoing fingerprint the archive must be skipped, not stamped with {incoming}"
        );

        let stamped = outgoing_definition_for_pre_recreation(Some(outgoing.to_string())).expect(
            "a persisted outgoing fingerprint is enough to publish the pre-recreation archive",
        );
        assert_eq!(
            stamped.fingerprint, outgoing,
            "the stamp must be the definition that produced the rows"
        );
        assert_ne!(
            stamped.fingerprint, incoming,
            "stamping the newly loaded Spicepod would make old_rows_definition_matches_stamp=false and bootstrap_accepts_old_rows_as_current=true"
        );
        assert!(
            !stamped.accept_unstamped,
            "a pre-recreation archive must still refuse a later unstamped bootstrap"
        );
    }

    #[test]
    fn inconsistent_snapshot_settings_names_both_sides_by_label() {
        let message = CayenneSnapshotValidationError::InconsistentSnapshotSettings {
            metadata_dir: "/data/cayenne".to_string(),
            enabled_components: "view 'orders_us'".to_string(),
            disabled_components: "dataset 'orders'".to_string(),
        }
        .to_string();

        assert!(
            message.contains("view 'orders_us'") && message.contains("dataset 'orders'"),
            "both sides of the disagreement must be named with their own labels: {message}"
        );
        assert!(
            !message.contains("Cayenne datasets sharing"),
            "a disagreement involving a view must not be reported as dataset-only: {message}"
        );
        assert!(
            message.contains("/data/cayenne") && message.contains("snapshots"),
            "the message must name the shared directory and the setting to align: {message}"
        );
    }
}
