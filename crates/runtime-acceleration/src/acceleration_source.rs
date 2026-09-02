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

use crate::acceleration::Acceleration;
use crate::schema_change::OnSchemaChange;
use datafusion::common::TableReference;
use runtime_secrets::Secrets;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::RwLock;

/// What produced the rows a snapshot of a source would contain.
///
/// Distinguishes the two questions a caller may need to answer about a definition-bearing
/// source: *did the definition change* (every source answers that with a fingerprint) and
/// *can this caller establish the rows came from a single consistent read* (only a caller
/// holding the compiled plan can, and only for a query).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializationSource {
    /// A copy of a named source table. The rows on disk stand on their own, so any caller
    /// that can see them may publish them.
    SourceTable,
    /// The result of a query. Whether the rows came from a single consistent read of the
    /// query's sources is only answerable with the compiled plan in hand, so a caller
    /// without one must not publish them — see `snapshot_before_recreate`.
    PlannedQuery,
}

/// Identity of the definition a source's rows were materialized from, and how to treat a
/// snapshot that records no definition at all.
#[derive(Debug, Clone)]
pub struct SourceDefinition {
    /// Stable hash of the definition. A snapshot recording a *different* value is always
    /// refused: its rows answer a different question.
    pub fingerprint: String,
    /// Whether a snapshot recording NO definition may still be restored.
    ///
    /// `false` where every archive the source could have published carries a stamp, so an
    /// unstamped one cannot belong to its series — true of accelerated views, for which
    /// snapshots have never existed without one.
    ///
    /// `true` where stamping was introduced after archives already existed. Refusing those
    /// would strand every snapshot taken before the upgrade, for a series whose definition
    /// has most likely not changed at all — a large, certain cost to close a smaller,
    /// possible hole. Accepting them still refuses every *mismatch*, so the protection
    /// applies in full to everything published from here on.
    pub accept_unstamped: bool,
    /// What produced these rows. A fingerprint alone cannot answer this: every
    /// definition-bearing source has one, but only a query's rows need a compiled plan
    /// before they may be published.
    pub materialization: MaterializationSource,
}

pub type InitializedSourcesFuture<'a> =
    Pin<Box<dyn Future<Output = Vec<Arc<dyn AccelerationSource>>> + Send + 'a>>;

/// Represents an acceleration source component, such as a dataset or a view.
/// Provides additional information about the source, such as its name and associated metadata.
pub trait AccelerationSource: Send + Sync {
    /// Returns a clone of the source as an `Arc<dyn AccelerationSource>`
    fn clone_arc(&self) -> Arc<dyn AccelerationSource>;

    /// Returns true if the source uses file-based acceleration
    fn is_file_accelerated(&self) -> bool;

    /// Returns the application associated with this source
    fn app(&self) -> Arc<app::App>;

    /// Returns the secrets store associated with this source
    fn secrets(&self) -> Arc<RwLock<Secrets>>;

    /// Returns the acceleration configuration if it exists
    fn acceleration(&self) -> Option<&Acceleration>;

    /// Returns the name of this source
    fn name(&self) -> &TableReference;

    /// The name of the connector this source's rows arrive from — the `from:`
    /// prefix (`debezium`, `cdc`, `sink`, `postgres`, …) — or `None` for a source
    /// with no connector.
    ///
    /// Load-bearing because `DataConnector::resolve_refresh_mode` fills in an unset
    /// `refresh_mode` and its result is never written back into [`Acceleration`]:
    /// `acceleration().refresh_mode` is still `None` for a genuine `debezium:`/`cdc:`
    /// stream. A consumer that must know the mode the source will actually run with
    /// maps this name through the connector-default table instead of reading the
    /// field raw (see [`crate::acceleration::unset_refresh_mode_for_connector`]).
    ///
    /// Deliberately has NO default implementation: every impl states its own answer,
    /// so a new source cannot silently inherit a wrong `None` and misclassify itself.
    fn connector_name(&self) -> Option<&str>;

    /// The `on_schema_change` policy this source declares, or `None` for a source that
    /// has no such policy — a view, or a table created by DDL.
    ///
    /// An engine that can widen its stored schema in place asks here instead of
    /// downcasting to the source's concrete type: `None` is the answer that keeps schema
    /// evolution off, which is what a source with no policy to state must resolve to.
    ///
    /// Deliberately has NO default implementation, for the same reason as
    /// [`Self::connector_name`]: a default would let a new source silently inherit
    /// somebody else's schema-change policy.
    fn on_schema_change(&self) -> Option<OnSchemaChange>;

    /// Whether rows can reach this source through anything other than its refresh path
    /// — `access: read_write` on a dataset, or DML against a DDL-created table.
    ///
    /// Load-bearing for scan freshness: only a source that provably takes no writes of
    /// its own can serve a scan from a slightly older view of its accelerator, because
    /// for anything else a pre-mutation view is a stale (wrong) result. A source that
    /// cannot prove it is write-free answers `true`.
    ///
    /// Deliberately has NO default implementation: the safe answer here is the
    /// permissive one, and a default would hand a new source the *restrictive* answer
    /// and with it a silently stale read.
    fn allows_write(&self) -> bool;

    /// Returns the time column name if configured, None otherwise.
    /// Views always return None as they don't support time-based append mode.
    fn time_column(&self) -> Option<&str>;

    /// Returns a reference to `Any` for downcasting
    fn as_any(&self) -> &dyn std::any::Any;

    /// Returns all initialized acceleration sources (datasets and views) known to this source's
    /// runtime. Used by `DuckDB` to attach peer file-mode databases. Default returns empty.
    fn initialized_sources(&self) -> InitializedSourcesFuture<'_> {
        Box::pin(async { vec![] })
    }

    /// Opens this source's acceleration checkpoint, read-only, for the snapshot
    /// bootstrap to compare a downloaded snapshot against.
    ///
    /// Returns a factory rather than the checkpointer itself because opening one
    /// touches the accelerator, which the caller may decide not to do.
    ///
    /// The source resolves the accelerator itself instead of taking a registry
    /// parameter, and that is load-bearing rather than stylistic: the registry type
    /// lives in `data-accelerator-api`, which sits **above** this crate, so a
    /// signature naming it would not compile. Keeping the resolution on the impl
    /// side is what lets the snapshot bootstrap live below `runtime`.
    ///
    /// Deliberately has NO default implementation, for the same reason as
    /// [`Self::connector_name`]: a default returning a no-op checkpointer would
    /// silently disable snapshot-vs-checkpoint reconciliation for any new source.
    fn checkpointer_factory(
        &self,
        snapshot_behavior: crate::snapshot::SnapshotBehavior,
    ) -> crate::dataset_checkpoint::DatasetCheckpointerFactory;

    /// How to name this source in a user-facing message — `"dataset"`, `"view"`, `"table"`.
    ///
    /// The snapshot paths are shared by datasets and views, so a message that hardcodes
    /// "dataset" tells an operator to go fix a component that does not exist. Only the
    /// source knows what it is.
    ///
    /// Deliberately has NO default implementation, for the same reason as
    /// [`Self::connector_name`]: a default would silently mislabel a new source in every
    /// error it can raise.
    fn component_label(&self) -> &'static str;

    /// Stable identity of the definition whose result this source's rows are, or `None`
    /// for a source whose rows are not a function of one.
    ///
    /// A dataset answers with its `from:` and `refresh_sql`, which together decide which
    /// rows it copies and how they are filtered — rebinding `from:` to a same-schema table
    /// changes what the rows mean while leaving the schema identical. A view answers with
    /// its whole definition closure, because its rows are that query's *result* —
    /// change the query and the archived rows answer a question nobody is asking any
    /// more. The schema check cannot stand in for it: `WHERE region = 'us'` and `WHERE
    /// region = 'eu'` share a schema and share nothing else.
    ///
    /// Recorded in the snapshot metadata on publish and re-checked on bootstrap, so a
    /// restored archive can never be served under a definition it was not materialized
    /// from.
    ///
    /// Deliberately has NO default implementation, for the same reason as
    /// [`Self::connector_name`]: a default `None` would silently opt a new
    /// definition-bearing source out of that check.
    fn definition_fingerprint(&self) -> Option<SourceDefinition>;
}

/// The refresh mode `source` actually runs with, applying the connector's fill-in for an
/// unset `refresh_mode`.
///
/// `DataConnector::resolve_refresh_mode` decides that fill-in and its result is never
/// written back into the [`Acceleration`], so `acceleration.refresh_mode` is still `None`
/// for a genuine `debezium:`/`cdc:` stream or a `sink:` dataset. Mapping the source's
/// connector name through [`crate::acceleration::unset_refresh_mode_for_connector`] — the
/// same table the runtime builder classifies the pod with — recovers it.
///
/// A source with no connector (a view, an Iceberg DDL table) has no default to apply and
/// falls back to `full`, which is what those paths resolve an unset mode to.
#[must_use]
pub fn resolved_refresh_mode(
    source: &dyn AccelerationSource,
    acceleration: &Acceleration,
) -> crate::acceleration::RefreshMode {
    acceleration.refresh_mode.unwrap_or_else(|| {
        source.connector_name().map_or(
            crate::acceleration::RefreshMode::Full,
            crate::acceleration::unset_refresh_mode_for_connector,
        )
    })
}
