/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Catalog-level CDC acceleration for the `PostgreSQL` catalog connector.
//!
//! [`AcceleratedCatalogProvider`] discovers schemas/tables the same way
//! [`data_components::postgres::provider::PostgresCatalogProvider`] does, but
//! instead of exposing plain federated tables it synthesizes a normal,
//! per-table `Dataset` (as if the user had hand-written a spicepod
//! `datasets:` entry) for every discovered table, and drives it through the
//! exact same dataset lifecycle as any spicepod-declared dataset (connector
//! creation, `AcceleratedTable` construction, refresh loop, status/metrics —
//! see [`Runtime::load_synthesized_dataset`]).
//!
//! Every synthesized dataset is given the same explicit replication slot
//! name (derived once from the catalog's own name), so every table shares
//! one replication connection and one publication instead of each opening
//! its own — WAL is decoded once for the whole catalog, not once per table.
//!
//! The catalog's `acceleration.mode` and `acceleration.params` are applied
//! uniformly to every table. The default `mode: memory` holds the acceleration
//! only in RAM, so each table re-runs its initial snapshot on every start; a
//! file mode (with `params.cayenne_file_path`) persists it and resumes from the
//! shared slot instead. `new` warns when the configured mode is not durable.
//!
//! Each table is accelerated according to its `PostgreSQL` `REPLICA IDENTITY`
//! (see `classify_replica_identity`): `DEFAULT` + primary key and `USING INDEX`
//! (keyed by the nominated unique index) replicate normally; `FULL` + primary
//! key replicates too but logs a warning (heavier -- the full old-row image is
//! written to the WAL on every `UPDATE`/`DELETE`). A table with no usable CDC
//! key -- `NOTHING`, keyless `DEFAULT`, `FULL` without a key, or an unusable
//! `USING INDEX` -- is skipped with an actionable warning and simply absent from
//! the catalog namespace, rather than failing the whole catalog. `include`/
//! `exclude` narrow scope and suppress the skip warning for known-ineligible
//! tables (handle those via federation and/or `refresh_mode: full` instead).
//!
//! Before touching any table, `refresh()` validates the `PostgreSQL`
//! prerequisites CDC needs (`wal_level = logical`, replication privilege)
//! and fails fast with a specific, actionable error if either is missing —
//! a clear pass/fail, not a full per-table CDC-readiness report.
//!
//! There is deliberately no federated stand-in for an accelerated table
//! while its dataset is still bootstrapping — `AcceleratedSchemaProvider`
//! reports it as not-yet-present rather than serving reads through the
//! source.
//!
//! Each synthesized dataset declares a [`crate::datafusion::DatasetPlacement`]
//! before it is loaded, so the dataset lifecycle installs its table provider
//! into this catalog's own [`AcceleratedSchemaProvider`] rather than the default
//! catalog. A user therefore reaches each table only as
//! `{catalog}.{schema}.{table}`; the synthesized registration name is never
//! queryable, and the catalog owns its providers the same way every other
//! catalog connector's schema provider does.
//!
//! The datasets are otherwise completely ordinary — status, metrics, health
//! monitoring, retry and the refresh loop are all keyed on the dataset name and
//! are unaffected by where the provider lands.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use app::App;
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::catalog_filter::TableSelector;
use data_components::postgres::provider::{
    ReplicaIdentityOutcome, check_cdc_prerequisites, classify_replica_identity,
    ensure_replication_slot_capacity, list_schemas, list_tables, list_views, replica_identity,
    replication_slot_status, wal_sender_timeout_ms,
};
use data_components::postgres_replication::config::catalog_slot_name;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::TableReference;
use datafusion::common::utils::quote_identifier;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use parking_lot::RwLock;
use snafu::prelude::*;
use spicepod::acceleration::{Acceleration as SpicepodAcceleration, OnConflictBehavior};
use spicepod::component::dataset::Dataset as SpicepodDataset;
use spicepod::param::Params;

use crate::Runtime;
use crate::component::catalog::{Catalog, CatalogAcceleration, table_selector};
use crate::component::dataset::builder::DatasetBuilder;

/// Dataset param key carrying an explicit replication slot name (see
/// `connector-postgres`'s `replication_slot` parameter spec, exposed to
/// datasets as `pg_replication_slot`). Every synthesized per-table dataset
/// is given the *same* slot name so they share one replication connection
/// and one publication instead of each opening its own -- this is the
/// catalog's single shared slot.
const REPLICATION_SLOT_PARAM: &str = "pg_replication_slot";

/// Docs link appended to every user-facing warning and error this module emits
/// (the skip warning, the REPLICA IDENTITY FULL warning, the view "not
/// replicated" warning, and the no-eligible-tables error), so each points at the
/// same actionable reference (see item D of #11850: standardize messages on
/// "primary key" / "REPLICA IDENTITY FULL / USING INDEX" and always include a
/// docs link). The purely informational USING INDEX acceleration line is not a
/// warning/error and omits it.
const DOCS_URL: &str = "https://spiceai.org/docs/components/data-connectors/postgres";

/// Escapes one `PostgreSQL` identifier into a component of a synthesized dataset
/// name, so the joined result is always a valid SQL identifier word and no two
/// distinct `(catalog, schema, table)` triples can encode to the same string.
///
/// The encoding keeps ordinary names readable — `orders` escapes to itself:
///
///   * `_`                          -> `_u`
///   * any other non-`[A-Za-z0-9]`  -> `_x{byte:02x}` (per UTF-8 byte)
///   * everything else              -> verbatim
///
/// Every `_` an encoded component contains is therefore followed by `u` or `x`,
/// so **`__` can never occur inside one**. Joining components with `__` (see
/// [`synthesized_dataset_name`]) is what makes the whole name injective: the
/// first `__` is always the first separator, so the split — and hence the
/// original triple — is uniquely recoverable.
///
/// Escaping `_` as `__` and separating with a single `_` would *not* be
/// injective, even though a lone `_` never appears inside a component: the
/// separator is the same character as the doubled one, so boundaries shift.
/// `("x", "_y")` and `("x_", "y")` both encode to `x___y`. Two source tables
/// would then share one dataset name, one component-status key, and one data
/// directory — see the regression test.
fn escape_name_component(s: &str) -> String {
    use std::fmt::Write;
    s.bytes()
        .fold(String::with_capacity(s.len()), |mut out, b| {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' => out.push(char::from(b)),
                b'_' => out.push_str("_u"),
                other => {
                    let _ = write!(out, "_x{other:02x}");
                }
            }
            out
        })
}

/// A sanitized, collision-safe internal name for the per-table dataset
/// synthesized for `catalog_name.schema_name.table_name`.
///
/// This name is never queryable: the dataset's provider is installed into the
/// catalog's own schema provider rather than the default catalog (see
/// [`AcceleratedSchemaProvider`]), so users only ever reach the table as
/// `{catalog}.{schema}.{table}`.
///
/// It is still visible, and still has to be unique and stable. It appears in
/// logs, component status (`dataset:<name>`), and metric attributes, and under a
/// file mode it is the table's directory beneath `cayenne_file_path` — so two
/// tables colliding here would share a status key and a data directory, and a
/// name that changed between restarts would orphan the acceleration on disk.
/// Hence escaping rather than hex-encoding: the common case stays legible
/// (`__catalog_accel_pg__cdc_public_orders`) while any character a quoted
/// `PostgreSQL` identifier allows (`-`, spaces, dots, non-ASCII) still round-trips
/// into a valid identifier — see [`escape_name_component`].
fn synthesized_dataset_name(catalog_name: &str, schema_name: &str, table_name: &str) -> String {
    // Components are joined with `__`, which no encoded component can contain
    // (see `escape_name_component`) -- that, not the escaping alone, is what
    // makes the name injective.
    format!(
        "__catalog_accel_{}__{}__{}",
        escape_name_component(catalog_name),
        escape_name_component(schema_name),
        escape_name_component(table_name)
    )
}

/// Render `key` in the string form `ColumnReference` parses (see
/// `datafusion_table_providers`'s `ColumnReference::try_from`): a single column
/// verbatim, a compound key as `(a, b, ...)`. Used for BOTH the dataset's
/// `primary_key` and its `on_conflict` upsert target so both parse to the same
/// `ColumnReference` and the CDC path's upsert-on-key check matches.
fn column_reference_string(key: &[String]) -> String {
    match key {
        [single] => single.clone(),
        many => format!("({})", many.join(", ")),
    }
}

/// How a table's `REPLICA IDENTITY` lets it be CDC-accelerated -- the reporting
/// counterpart of the eligible [`ReplicaIdentityOutcome`] variants. Carried so
/// the startup summary and metrics can break accelerated tables down by the key
/// that drives their upsert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccelerationKind {
    /// `DEFAULT` + primary key.
    PrimaryKey,
    /// `USING INDEX`, keyed by the nominated unique index.
    UniqueIndex,
    /// `FULL` + primary key.
    Full,
}

impl AccelerationKind {
    /// The eligible outcome's kind, or `None` for `Skip`.
    fn from_outcome(outcome: &ReplicaIdentityOutcome) -> Option<Self> {
        match outcome {
            ReplicaIdentityOutcome::AccelerateViaPrimaryKey { .. } => Some(Self::PrimaryKey),
            ReplicaIdentityOutcome::AccelerateViaUniqueIndex { .. } => Some(Self::UniqueIndex),
            ReplicaIdentityOutcome::AccelerateFullReplicaIdentity { .. } => Some(Self::Full),
            ReplicaIdentityOutcome::Skip { .. } => None,
        }
    }

    /// Stable metric-attribute value (never a display string).
    fn metric_label(self) -> &'static str {
        match self {
            Self::PrimaryKey => "primary_key",
            Self::UniqueIndex => "unique_index",
            Self::Full => "full",
        }
    }
}

/// Widen a table count to the `u64` the metrics API takes, saturating rather
/// than panicking (counts never approach `u64::MAX`; this only satisfies the
/// no-`unwrap` lint without an `as` cast).
fn count_as_u64(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// The per-catalog tally of how discovery resolved every table it looked at:
/// accelerated (broken down by [`AccelerationKind`]), skipped for lacking a
/// usable `REPLICA IDENTITY`, or excluded by `include`/`exclude`. Accumulated
/// across schemas (see [`AccelerationSummary::add`]) into the startup summary
/// log line and the catalog acceleration metrics, and used to fail loudly when
/// nothing is eligible.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct AccelerationSummary {
    primary_key: usize,
    unique_index: usize,
    full: usize,
    skipped: usize,
    excluded: usize,
    /// View-like relations (views, materialized views, foreign tables) selected
    /// by `include`/`exclude` that cannot be CDC-accelerated. Not counted among
    /// the discovered *tables* -- reported separately so a view produces a
    /// "not replicated" warning instead of being dropped silently (#11911).
    views_not_replicated: usize,
}

impl AccelerationSummary {
    fn record_accelerated(&mut self, kind: AccelerationKind) {
        match kind {
            AccelerationKind::PrimaryKey => self.primary_key += 1,
            AccelerationKind::UniqueIndex => self.unique_index += 1,
            AccelerationKind::Full => self.full += 1,
        }
    }

    /// Total tables that will be CDC-accelerated (across all kinds).
    fn accelerated_total(&self) -> usize {
        self.primary_key + self.unique_index + self.full
    }

    /// Total *tables* discovery looked at: accelerated (any kind) + skipped for
    /// lacking a usable replica identity + excluded by filters. Excludes
    /// view-like relations, which aren't tables. Used for the "0 of N discovered
    /// tables" fail-loud message.
    fn discovered_tables(&self) -> usize {
        self.accelerated_total() + self.skipped + self.excluded
    }

    /// Fold another schema's summary into this one.
    fn add(&mut self, other: &Self) {
        self.primary_key += other.primary_key;
        self.unique_index += other.unique_index;
        self.full += other.full;
        self.skipped += other.skipped;
        self.excluded += other.excluded;
        self.views_not_replicated += other.views_not_replicated;
    }

    /// The single startup summary line, naming the shared slot and breaking the
    /// accelerated count down by acceleration kind. Closes with a note on the
    /// discovery scope: `refresh()` re-runs on the catalog's periodic interval,
    /// so a table *added* to a selected schema later is picked up and accelerated
    /// on the next refresh -- but schema changes to existing tables and
    /// renamed/dropped tables are not tracked (documented non-goals).
    fn summary_message(&self, catalog_name: &str, slot_name: &str) -> String {
        // Only mention views when there are some, so the common (view-free) case
        // stays terse.
        let views_clause = if self.views_not_replicated > 0 {
            format!(
                " {} view(s)/materialized view(s)/foreign table(s) not replicated (see warnings);",
                self.views_not_replicated
            )
        } else {
            String::new()
        };
        format!(
            "Catalog '{catalog_name}': accelerating {} table(s) via CDC ({} via primary key, {} via REPLICA IDENTITY USING INDEX, {} via REPLICA IDENTITY FULL; shared replication slot '{slot_name}'); {} table(s) excluded by include/exclude filters; {} table(s) skipped (no usable replica identity -- see warnings);{views_clause} tables added to these schema(s) later are picked up on the periodic catalog refresh; schema changes to existing tables, and renamed or dropped tables, are not tracked.",
            self.accelerated_total(),
            self.primary_key,
            self.unique_index,
            self.full,
            self.excluded,
            self.skipped,
        )
    }

    /// Emit the catalog acceleration gauges (accelerated/skipped/excluded counts
    /// and the by-kind breakdown) for `catalog_name`.
    fn emit_metrics(&self, catalog_name: &str) {
        use opentelemetry::KeyValue;
        let category = |value: &'static str| {
            [
                KeyValue::new("catalog", catalog_name.to_string()),
                KeyValue::new("category", value),
            ]
        };
        runtime_metrics::catalogs::ACCELERATION_TABLES.record(
            count_as_u64(self.accelerated_total()),
            &category("accelerated"),
        );
        runtime_metrics::catalogs::ACCELERATION_TABLES
            .record(count_as_u64(self.skipped), &category("skipped"));
        runtime_metrics::catalogs::ACCELERATION_TABLES
            .record(count_as_u64(self.excluded), &category("excluded"));
        runtime_metrics::catalogs::ACCELERATION_TABLES.record(
            count_as_u64(self.views_not_replicated),
            &category("views_not_replicated"),
        );

        let kind = |k: AccelerationKind, n: usize| {
            runtime_metrics::catalogs::ACCELERATION_TABLES_BY_KIND.record(
                count_as_u64(n),
                &[
                    KeyValue::new("catalog", catalog_name.to_string()),
                    KeyValue::new("kind", k.metric_label()),
                ],
            );
        };
        kind(AccelerationKind::PrimaryKey, self.primary_key);
        kind(AccelerationKind::UniqueIndex, self.unique_index);
        kind(AccelerationKind::Full, self.full);
    }
}

/// A table already handed off to a background bootstrap/CDC task, tracked across
/// refreshes: the dataset name it was registered under and how it is
/// accelerated (so re-plans re-report its [`AccelerationKind`] without
/// re-querying its replica identity).
#[derive(Debug, Clone)]
struct SpawnedTable {
    dataset_name: String,
    kind: AccelerationKind,
}

/// Discovery found no CDC-eligible table in the catalog. A hard, actionable
/// startup error (see [`AcceleratedCatalogProvider::refresh`]): failing the
/// *initial* refresh means the catalog never registers -- and so never gets a
/// periodic refresh to reconsider -- so an empty result is a configuration
/// problem to surface, not an empty catalog to register silently. `postgres.rs`
/// maps this to a permanent configuration error.
#[derive(Debug, Snafu)]
#[snafu(display(
    "Catalog '{catalog}': 0 of {discovered} discovered table(s) are eligible for CDC acceleration ({skipped} have no primary key or usable REPLICA IDENTITY; {excluded} excluded by include/exclude filters).{views_note} Give each table a usable CDC key -- a primary key (which REPLICA IDENTITY DEFAULT or FULL then keys on), or a UNIQUE NOT NULL index set as REPLICA IDENTITY USING INDEX -- and ensure the catalog's `include`/`exclude` patterns match them. Note that REPLICA IDENTITY FULL without a primary key is still skipped. Docs: {DOCS_URL}"
))]
pub(crate) struct NoEligibleTablesError {
    catalog: String,
    /// Total tables looked at (skipped + excluded here, since accelerated is 0).
    /// Excludes view-like relations, which aren't tables -- so a schema of only
    /// views yields `0 of 0`; `views_note` then explains where those relations
    /// went (see [`AcceleratedCatalogProvider::refresh`]).
    discovered: usize,
    excluded: usize,
    skipped: usize,
    /// Pre-rendered so the `Display` needs no conditional formatting: a leading-
    /// space sentence naming any view-like relations found (so a `0 of 0`
    /// only-views case isn't confusing), or empty when there were none.
    views_note: String,
}

/// The catalog's deterministic replication slot is already actively held by
/// another consumer. Because the slot name is derived purely from the catalog
/// (see [`catalog_slot_name`]) and `PostgreSQL` permits a single consumer per
/// slot, this means a second Spice instance (or process) is already streaming
/// this catalog's changes. Surfaced only after a bounded wait (see
/// [`AcceleratedCatalogProvider::ensure_catalog_slot_available`]) that already
/// absorbs the legitimate hand-off cases -- a fast self-restart, or a rolling
/// deploy whose predecessor is shutting down -- by giving the server up to
/// `wal_sender_timeout` to release a now-dead consumer's slot. If a *live*
/// consumer still holds it past that window, `postgres.rs` maps this to a
/// permanent configuration error (terminal ERROR status, no retry loop): running
/// two instances against one catalog is a misconfiguration to surface loudly,
/// not to silently keep retrying.
#[derive(Debug, Snafu)]
#[snafu(display(
    "Catalog '{catalog}': replication slot '{slot_name}' is already in use by {active_consumer} after waiting {waited_secs}s. Another Spice instance (or process) is already streaming this catalog's changes -- PostgreSQL permits only one consumer per replication slot. Ensure only one Spice instance accelerates this catalog, or stop the other consumer. Docs: https://spiceai.org/docs/components/data-connectors/postgres"
))]
pub(crate) struct SlotInUseError {
    catalog: String,
    slot_name: String,
    /// Pre-rendered so the `Display` string needs no `Option` formatting, e.g.
    /// `"PID 1234"` or `"an unknown backend"`.
    active_consumer: String,
    waited_secs: u64,
}

/// How long to wait, beyond the server's `wal_sender_timeout`, for a slot that a
/// crashed consumer still holds `active` to be released before concluding a
/// different live consumer owns it. `PostgreSQL` frees the slot within
/// milliseconds of the walsender exiting (at ~`wal_sender_timeout`), so this is
/// a small buffer for scheduling/poll granularity, not a second full timeout.
const SLOT_RELEASE_GRACE: Duration = Duration::from_secs(5);
/// Fallback wait budget when `wal_sender_timeout` is `0` (disabled), since then
/// the server won't time out a dropped consumer on its own.
const SLOT_WAIT_BUDGET_WHEN_TIMEOUT_DISABLED: Duration = Duration::from_secs(90);
/// Absolute safety ceiling on the slot-availability wait. The wait is normally
/// the server's `wal_sender_timeout` (+ [`SLOT_RELEASE_GRACE`]) so we wait
/// exactly as long as `PostgreSQL` takes to release a dead consumer's slot; this
/// only bounds a pathologically large (or misconfigured) `wal_sender_timeout` so
/// catalog startup can't hang for an unreasonable time. The default 60s
/// `wal_sender_timeout` is far under this.
const SLOT_WAIT_BUDGET_CAP: Duration = Duration::from_mins(10);
/// How often to re-poll the slot's activity while waiting for it to free.
const SLOT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// A catalog provider that CDC-accelerates every table it discovers (subject
/// to `include`/`exclude`), holding its own `PostgreSQL` connection directly
/// rather than wrapping the plain federated catalog provider.
pub struct AcceleratedCatalogProvider {
    catalog_name: String,
    pool: Arc<PostgresConnectionPool>,
    runtime: Arc<Runtime>,
    app: Arc<App>,
    /// Connection params shared with every synthesized per-table dataset —
    /// the same `pg_host`/`pg_port`/... the catalog itself was configured
    /// with.
    dataset_params: HashMap<String, String>,
    /// One replication slot name shared by every synthesized dataset in
    /// this catalog, so WAL is decoded once by one shared connection rather
    /// than once per table.
    slot_name: String,
    /// The acceleration block written onto every synthesized dataset, before the
    /// per-table `primary_key`/`on_conflict` are filled in
    /// (`CatalogAcceleration::to_dataset_acceleration`). Its `mode` -- the
    /// catalog's -- decides persistence: `Memory`, the default, is fully in-RAM,
    /// so every table re-snapshots from the source on each restart, while a file
    /// mode persists and resumes from the shared slot instead.
    table_acceleration: SpicepodAcceleration,
    selector: TableSelector,
    schemas: RwLock<HashMap<String, Arc<AcceleratedSchemaProvider>>>,
    /// `(schema_name, table_name)` -> the dataset name it was already
    /// spawned under, tracked across refreshes so a periodic `refresh()`
    /// (every `refresh_check_interval`, default 1 minute -- see
    /// `RefreshingCatalogProvider::start_refresh`) doesn't re-spawn a
    /// duplicate bootstrap/CDC task for a table that's already running.
    ///
    /// Keyed by the `(schema, table)` pair rather than a joined
    /// `"{schema}.{table}"` string -- `PostgreSQL` allows `.` in a quoted
    /// identifier, so a joined string key can collide (e.g. schema `"a.b"`,
    /// table `"c"` vs. schema `"a"`, table `"b.c"`), which would make one
    /// table reuse another's dataset name and silently route queries to the
    /// wrong accelerated table.
    spawned: RwLock<HashMap<(String, String), SpawnedTable>>,
}

impl std::fmt::Debug for AcceleratedCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceleratedCatalogProvider")
            .field("catalog_name", &self.catalog_name)
            .finish_non_exhaustive()
    }
}

impl AcceleratedCatalogProvider {
    #[must_use]
    pub fn new(
        catalog: &Catalog,
        acceleration: &CatalogAcceleration,
        pool: Arc<PostgresConnectionPool>,
    ) -> Self {
        let slot_name = catalog_slot_name(&catalog.name);

        // Seed from the catalog's connection params, then let `dataset_params`
        // override -- the same precedence other catalog connectors use (e.g.
        // `UnityCatalog`, `Databricks`) for per-dataset overrides of
        // catalog-level connection settings.
        let mut dataset_params = catalog.params.clone();
        dataset_params.extend(catalog.dataset_params.clone());

        let table_acceleration = acceleration.to_dataset_acceleration();
        let acceleration_mode = &table_acceleration.mode;

        // `mode: memory` (the default) and `mode: file_create` both start empty
        // on every boot. That is a supported configuration, not an error -- but
        // it is easy to configure by accident and its cost is invisible until a
        // restart, so state it once at startup rather than letting an operator
        // discover it as apparent data loss.
        //
        // The message names `cayenne_cdc_durability` too, because that is the
        // setting operators reach for when they mean "buffer writes in RAM":
        // `mode` decides whether the acceleration exists on disk at all, while
        // `cayenne_cdc_durability` only defers the durable write of a file-backed
        // one. Choosing `mode: memory` for the throughput of the second is the
        // most likely way to arrive here by accident.
        if !acceleration.is_durable() {
            tracing::warn!(
                "Catalog '{}': acceleration `mode: {acceleration_mode}` does not persist across restarts -- nothing is written to disk, so the acceleration starts empty and every table re-runs its initial snapshot from the source on every start. Set `acceleration.mode: file` with `acceleration.params.cayenne_file_path` to keep the acceleration across restarts and resume from the replication slot instead. If the goal was to keep CDC writes off the disk hot path rather than to discard them, use a file mode with `acceleration.params.cayenne_cdc_durability: memory`, which buffers in RAM but still drains to durable storage. Docs: {DOCS_URL}",
                catalog.name,
            );
        }

        Self {
            catalog_name: catalog.name.clone(),
            pool,
            runtime: catalog.runtime(),
            app: catalog.app(),
            dataset_params,
            slot_name,
            table_acceleration,
            selector: table_selector(catalog),
            schemas: RwLock::new(HashMap::new()),
            spawned: RwLock::new(HashMap::new()),
        }
    }

    /// Fail loudly, before spawning anything, if this catalog's deterministic
    /// replication slot is already **actively** held by another consumer.
    ///
    /// Because [`catalog_slot_name`] derives the slot name purely from the
    /// catalog (no instance component), a second Spice instance pointed at the
    /// same catalog resolves to the *same* slot -- and `PostgreSQL` permits only
    /// one consumer per slot. So:
    ///
    ///   - slot absent -> a new slot will be created, so first check the server
    ///     has capacity ([`ensure_replication_slot_capacity`]) and fail loudly if
    ///     `max_replication_slots` is exhausted; otherwise return;
    ///   - slot present but inactive -> return; it is reused (a restart/reschedule
    ///     resumes from its `restart_lsn`, no re-snapshot), so no capacity is used;
    ///   - slot present and active -> another consumer holds it. But a fast
    ///     self-restart after an ungraceful exit can *also* see the slot active
    ///     (the server keeps the dead consumer's slot active until
    ///     `wal_sender_timeout`), so we poll for a bounded window sized from the
    ///     server's `wal_sender_timeout` before concluding a *different* live
    ///     consumer owns it and returning [`SlotInUseError`].
    ///
    /// Only meaningful before this catalog owns the slot; callers guard on
    /// "nothing spawned yet" so a periodic refresh never trips over *our own*
    /// active slot.
    async fn ensure_catalog_slot_available(
        &self,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Size the wait from the server's wal_sender_timeout: that is how long
        // PostgreSQL keeps a crashed consumer's slot marked active before its
        // walsender exits and the slot is released. Wait exactly that long (plus
        // a small grace for the server to actually clear the flag) so a legitimate
        // hand-off -- a fast self-restart, or a rolling deploy whose predecessor
        // is still shutting down -- reclaims the slot before we treat it as taken
        // by a live consumer. `0` disables the timeout, so fall back to a fixed
        // window; [`SLOT_WAIT_BUDGET_CAP`] only bounds a pathologically large
        // value so startup can't hang.
        let timeout_ms = wal_sender_timeout_ms(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        let budget = if timeout_ms > 0 {
            Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(u64::MAX))
                .saturating_add(SLOT_RELEASE_GRACE)
                .min(SLOT_WAIT_BUDGET_CAP)
        } else {
            SLOT_WAIT_BUDGET_WHEN_TIMEOUT_DISABLED
        };
        // Human-readable derivation of `budget`, for the wait log.
        let wait_basis = if timeout_ms > 0 {
            format!(
                "wal_sender_timeout {}s + {}s grace",
                timeout_ms / 1000,
                SLOT_RELEASE_GRACE.as_secs(),
            )
        } else {
            format!(
                "wal_sender_timeout disabled, {}s fallback",
                SLOT_WAIT_BUDGET_WHEN_TIMEOUT_DISABLED.as_secs(),
            )
        };

        let start = tokio::time::Instant::now();
        let deadline = start + budget;
        // Warn only once, when the wait begins -- the loop re-polls every
        // `SLOT_POLL_INTERVAL`, so warning each iteration would emit up to
        // hundreds of lines during a full conflict window. Subsequent polls log
        // at debug.
        let mut warned = false;
        loop {
            let status = replication_slot_status(&self.pool, &self.slot_name)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            match status {
                // Absent: a new slot will be created for this catalog, so verify
                // the server has capacity now -- an exhausted `max_replication_slots`
                // otherwise fails later, deep in replication setup, with a cryptic
                // error instead of an actionable one at startup.
                None => {
                    ensure_replication_slot_capacity(&self.pool)
                        .await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    return Ok(());
                }
                // Present but inactive: reused, not created -- no capacity consumed.
                Some(status) if !status.active => return Ok(()),
                Some(status) => {
                    let active_consumer = status.active_pid.map_or_else(
                        || "an unknown backend".to_string(),
                        |pid| format!("PID {pid}"),
                    );
                    if tokio::time::Instant::now() >= deadline {
                        return Err(Box::new(SlotInUseError {
                            catalog: self.catalog_name.clone(),
                            slot_name: self.slot_name.clone(),
                            active_consumer,
                            waited_secs: start.elapsed().as_secs(),
                        }));
                    }
                    if warned {
                        tracing::debug!(
                            "Catalog '{}': replication slot '{}' still in use by {}; continuing to wait (up to {}s total).",
                            self.catalog_name,
                            self.slot_name,
                            active_consumer,
                            budget.as_secs(),
                        );
                    } else {
                        tracing::warn!(
                            "Catalog '{}': replication slot '{}' is currently in use by {} -- a previous instance may still be shutting down. Waiting up to {}s ({}) for the slot to be released before failing.",
                            self.catalog_name,
                            self.slot_name,
                            active_consumer,
                            budget.as_secs(),
                            wait_basis,
                        );
                        warned = true;
                    }
                    tokio::time::sleep(SLOT_POLL_INTERVAL).await;
                }
            }
        }
    }

    /// Synthesizes (but does NOT spawn) the per-table CDC dataset for
    /// `schema_name.table_name`, keyed by `key` (the columns resolved from the
    /// table's replica identity -- see `classify_replica_identity`). Returns the
    /// name it will be registered under and the built `Dataset`. Building is
    /// separated from spawning so the whole catalog can be validated before any
    /// background bootstrap/CDC task starts (see `refresh`); a build failure
    /// aborts the refresh with nothing spawned.
    #[expect(clippy::result_large_err)]
    fn build_accelerated_dataset(
        &self,
        schema_name: &str,
        table_name: &str,
        key: &[String],
    ) -> Result<(String, crate::component::dataset::Dataset), crate::Error> {
        let dataset_name = synthesized_dataset_name(&self.catalog_name, schema_name, table_name);

        let mut params = self.dataset_params.clone();
        params.insert(REPLICATION_SLOT_PARAM.to_string(), self.slot_name.clone());

        // Each component is quoted (only when required) via the same
        // DataFusion helper `foreign_key_target` uses, so the joined path
        // round-trips back through `TableReference::parse_str` (which
        // `dataset.path()` is parsed with downstream) even when a component
        // needs quoting to resolve correctly -- e.g. mixed case, embedded
        // `.`, or embedded spaces. See #11727 for the same class of bug.
        let mut spicepod_ds = SpicepodDataset::new(
            format!(
                "postgres:{}.{}",
                quote_identifier(schema_name),
                quote_identifier(table_name)
            ),
            dataset_name.clone(),
        )
        .with_params(Params::from_string_map(params));
        // Declare the CDC key EXPLICITLY rather than relying on schema inference:
        // inference only derives a primary key from `indisprimary`, so a
        // `REPLICA IDENTITY USING INDEX` table (no formal PK) would otherwise get
        // no key. The same string keys both `primary_key` and the `on_conflict`
        // upsert target so they parse to the same `ColumnReference` (which
        // `connector-postgres`'s CDC path requires -- otherwise UPDATE events
        // append duplicate rows). Inference still fills sort/secondary-indexes.
        let key_ref = column_reference_string(key);
        // The catalog's engine, storage mode and accelerator params apply uniformly
        // to every table it accelerates; only the key is per-table. Under a file
        // mode each table lands in its own directory beneath `cayenne_file_path`,
        // named for the dataset (see `synthesized_dataset_name`).
        spicepod_ds.acceleration = Some(SpicepodAcceleration {
            primary_key: Some(key_ref.clone()),
            on_conflict: HashMap::from([(key_ref, OnConflictBehavior::Upsert)]),
            ..self.table_acceleration.clone()
        });

        let dataset = DatasetBuilder::try_from(spicepod_ds)?
            .with_app(Arc::clone(&self.app))
            .with_runtime(Arc::clone(&self.runtime))
            .build()
            .context(crate::UnableToBuildDatasetSnafu {
                dataset: dataset_name.clone(),
            })?;

        Ok((dataset_name, dataset))
    }

    /// Returns the schema provider along with the number of discovered
    /// tables that `include`/`exclude` excluded from it, for the catalog's
    /// startup summary.
    ///
    /// Classifies and builds (but does NOT spawn) every selected table by its
    /// `REPLICA IDENTITY` (see `classify_replica_identity`): tables with a usable
    /// CDC key are built keyed by it (emitting a per-table info/warn describing
    /// how they replicate), tables without one are skipped with an actionable
    /// warning and counted in `SchemaPlan::skipped`. Only a genuine connection /
    /// query / build failure aborts the refresh. Spawning is deferred to
    /// `refresh` so the whole catalog is validated before any background task
    /// starts. See [`SchemaPlan`].
    async fn plan_schema_provider(
        &self,
        schema_name: &str,
    ) -> Result<SchemaPlan, Box<dyn std::error::Error + Send + Sync>> {
        // Views can't be CDC-accelerated (no replica identity) -- exclude them at
        // discovery so they never reach the per-table classification below.
        let table_names = list_tables(&self.pool, schema_name, false)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let mut tables = HashMap::new();
        let mut summary = AccelerationSummary::default();
        let mut to_spawn = Vec::new();
        for table_name in table_names {
            if !self.selector.selects_table(schema_name, &table_name) {
                summary.excluded += 1;
                continue;
            }

            // Already running from a previous refresh -- reuse it rather
            // than re-classifying and re-spawning a duplicate bootstrap/CDC
            // task for a table `refresh()` already knows about. Its stored
            // acceleration kind is re-counted so re-plans keep the by-kind
            // summary/metrics accurate without re-querying its replica identity.
            let spawn_key = (schema_name.to_string(), table_name.clone());
            let already_spawned = {
                let guard = self.spawned.read();
                guard.get(&spawn_key).cloned()
            };

            let dataset_name = if let Some(spawned) = already_spawned {
                summary.record_accelerated(spawned.kind);
                spawned.dataset_name
            } else {
                // Quote each component (only when required) so the per-table
                // warnings below unambiguously identify the table and round-trip
                // -- a bare `{schema}.{table}` is misleading for identifiers that
                // need quoting (spaces, mixed case) or contain dots. Matches how
                // `build_accelerated_dataset` and `foreign_key_target` quote
                // (see #11727).
                let table_path = format!(
                    "{}.{}",
                    quote_identifier(schema_name),
                    quote_identifier(&table_name)
                );
                let identity = replica_identity(&self.pool, schema_name, &table_name)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                // Resolve the CDC upsert key from the table's replica identity, or
                // skip (not error) a table that has no usable one so the rest of
                // the catalog still replicates. `DEFAULT` + primary key is the
                // normal case and logs nothing (counted in the summary); the
                // notable cases each get one line.
                let outcome = classify_replica_identity(&identity);
                let key = match &outcome {
                    ReplicaIdentityOutcome::Skip { reason } => {
                        summary.skipped += 1;
                        tracing::warn!(
                            "Catalog '{}': skipping table {table_path}: {}. Exclude it via the catalog's `include`/`exclude` patterns to suppress this warning. Docs: {DOCS_URL}",
                            self.catalog_name,
                            reason.explanation(),
                        );
                        continue;
                    }
                    ReplicaIdentityOutcome::AccelerateViaPrimaryKey { key } => key.clone(),
                    ReplicaIdentityOutcome::AccelerateViaUniqueIndex { key } => {
                        tracing::info!(
                            "Catalog '{}': accelerating table {table_path} via its REPLICA IDENTITY unique index ({}).",
                            self.catalog_name,
                            key.join(", "),
                        );
                        key.clone()
                    }
                    ReplicaIdentityOutcome::AccelerateFullReplicaIdentity { key } => {
                        tracing::warn!(
                            "Catalog '{}': accelerating table {table_path} with REPLICA IDENTITY FULL (keyed by ({})) -- heavier than DEFAULT/USING INDEX: PostgreSQL logs the full old-row image on every UPDATE/DELETE. Prefer a primary key or USING INDEX where possible. Docs: {DOCS_URL}",
                            self.catalog_name,
                            key.join(", "),
                        );
                        key.clone()
                    }
                };
                // Skip returned above; every remaining outcome has a kind.
                let Some(kind) = AccelerationKind::from_outcome(&outcome) else {
                    continue;
                };
                summary.record_accelerated(kind);

                // Build (validate) now; defer spawning to `refresh` so a later
                // unbuildable table can't leave this one orphaned.
                let (dataset_name, dataset) = self
                    .build_accelerated_dataset(schema_name, &table_name, &key)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                to_spawn.push((table_name.clone(), dataset_name.clone(), kind, dataset));
                dataset_name
            };

            tables.insert(table_name, dataset_name);
        }

        // View-like relations (views, materialized views, foreign tables) can't
        // be CDC-accelerated. Rather than dropping them silently, warn once per
        // selected relation that it won't be replicated (#11911 tracks adding
        // support). `include`/`exclude` suppress the warning the same way they do
        // for skipped tables. These are counted separately from the discovered
        // *tables* -- a view is neither accelerated, skipped-for-replica-identity,
        // nor a filtered-out table.
        let views = list_views(&self.pool, schema_name)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        for view in views {
            if !self.selector.selects_table(schema_name, &view.name) {
                continue;
            }
            summary.views_not_replicated += 1;
            let view_path = format!(
                "{}.{}",
                quote_identifier(schema_name),
                quote_identifier(&view.name)
            );
            tracing::warn!(
                "Catalog '{}': {} {view_path} is not replicated -- {}s cannot be CDC-accelerated (no REPLICA IDENTITY). It is absent from the accelerated catalog; query it through a non-accelerated catalog or dataset instead. Exclude it via the catalog's `include`/`exclude` patterns to suppress this warning. Docs: {DOCS_URL}",
                self.catalog_name,
                view.kind,
                view.kind,
            );
        }

        Ok(SchemaPlan {
            tables,
            summary,
            to_spawn,
        })
    }
}

/// A validated, not-yet-spawned plan for one schema (see
/// [`AcceleratedCatalogProvider::refresh`]): the `table_name -> dataset
/// registration name` map for the schema provider, the schema's
/// [`AccelerationSummary`] (accelerated-by-kind / skipped / excluded counts),
/// and the datasets that are new this refresh and still need spawning.
/// Classification + build completes for the whole catalog before any dataset is
/// spawned, so a later unbuildable table can't leave earlier tables' bootstrap/
/// CDC tasks running orphaned against a catalog that never registers.
struct SchemaPlan {
    tables: HashMap<String, String>,
    summary: AccelerationSummary,
    /// `(table_name, dataset_name, acceleration kind, built dataset)` for each
    /// new table.
    to_spawn: Vec<(
        String,
        String,
        AccelerationKind,
        crate::component::dataset::Dataset,
    )>,
}

#[async_trait]
impl RefreshableCatalogProvider for AcceleratedCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Fail fast with a clear, actionable error before touching any
        // tables, rather than only surfacing a wal_level/permission problem
        // later when the first table's CDC pump tries (and fails) to open
        // a replication connection.
        check_cdc_prerequisites(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Before this catalog owns its shared slot, fail loudly if another live
        // consumer already holds it (its deterministic name means a second Spice
        // instance would otherwise silently compete for the single-consumer
        // slot). Guarded to the pre-ownership window -- once we've spawned our own
        // datasets, the slot is active because *we* hold it, so a periodic refresh
        // must not re-run this check and trip over ourselves.
        if self.spawned.read().is_empty() {
            self.ensure_catalog_slot_available().await?;
        }

        let schema_names = list_schemas(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Phase 1: discover, classify, and build every selected table across all
        // schemas BEFORE spawning anything. Tables with no usable replica
        // identity are skipped (with a warning), not fatal; only a genuine
        // connection/query/build failure aborts here -- and when it does, nothing
        // has been spawned, so it can't leave earlier tables' bootstrap/CDC tasks
        // running orphaned against a catalog that then never registers.
        let mut plans = Vec::new();
        let mut summary = AccelerationSummary::default();
        for schema_name in &schema_names {
            let plan = self.plan_schema_provider(schema_name).await?;
            summary.add(&plan.summary);
            plans.push((schema_name.clone(), plan));
        }

        // Fail loudly when nothing is eligible: an empty result is a
        // configuration problem to surface, not an empty catalog to register
        // silently. Returned before spawning or swapping `self.schemas`, so a
        // *later* periodic refresh that transiently sees zero leaves any
        // previously-registered schemas intact. On the *initial* refresh,
        // `postgres.rs` maps this to a permanent configuration error (ERROR
        // status, catalog never registers -- see `NoEligibleTablesError`).
        if summary.accelerated_total() == 0 {
            // When the only relations in scope are view-like (a `0 of 0`
            // discovered-tables case), say so explicitly -- otherwise the message
            // reads as an empty schema when in fact views/matviews/foreign tables
            // were found and simply can't be CDC-accelerated.
            let views_note = if summary.views_not_replicated > 0 {
                format!(
                    " {} view-like relation(s) (views/materialized views/foreign tables) were found in scope but cannot be CDC-accelerated.",
                    summary.views_not_replicated
                )
            } else {
                String::new()
            };
            return Err(Box::new(NoEligibleTablesError {
                catalog: self.catalog_name.clone(),
                discovered: summary.discovered_tables(),
                excluded: summary.excluded,
                skipped: summary.skipped,
                views_note,
            }));
        }

        // Phase 2: the whole catalog validated -- now spawn the new datasets
        // (fire-and-forget, same retry-forever semantics as any spicepod-declared
        // dataset) and build the schema providers. These steps are infallible, so
        // registration can no longer be left partially applied.
        let mut schemas = HashMap::new();
        for (schema_name, plan) in plans {
            // Reuse this schema's existing provider when there is one. It owns
            // the providers of every table already loaded, so replacing it on a
            // periodic refresh would drop them and make ready tables vanish
            // until their datasets happened to reload.
            let provider = self.schemas.read().get(&schema_name).map_or_else(
                || Arc::new(AcceleratedSchemaProvider::default()),
                Arc::clone,
            );
            *provider.tables.write() = plan.tables;

            for (table_name, dataset_name, kind, dataset) in plan.to_spawn {
                // Declare where the provider belongs *before* spawning, so the
                // dataset is never briefly registered under the default catalog:
                // users reach these tables through this catalog's own namespace,
                // and the synthesized registration name is not part of its
                // interface.
                provider
                    .dataset_to_table
                    .write()
                    .insert(dataset_name.clone(), table_name.clone());
                self.runtime.df.set_dataset_placement(
                    &TableReference::bare(dataset_name.clone()),
                    Arc::clone(&provider) as Arc<dyn crate::datafusion::DatasetPlacement>,
                );
                self.spawned.write().insert(
                    (schema_name.clone(), table_name),
                    SpawnedTable { dataset_name, kind },
                );
                tokio::spawn(Arc::clone(&self.runtime).load_synthesized_dataset(Arc::new(dataset)));
            }
            schemas.insert(schema_name, provider);
        }

        {
            let mut guard = self.schemas.write();
            *guard = schemas;
        }

        summary.emit_metrics(&self.catalog_name);
        tracing::info!(
            "{}",
            summary.summary_message(&self.catalog_name, &self.slot_name)
        );

        Ok(())
    }
}

impl CatalogProvider for AcceleratedCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let guard = self.schemas.read();
        guard.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let guard = self.schemas.read();
        guard
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}

/// A schema provider that owns the accelerated tables of one schema, the same
/// way every other catalog connector's schema provider owns its own
/// `TableProvider`s.
///
/// `tables` maps the source table name to the name of the dataset synthesized
/// for it; `providers` holds each table's provider once its dataset finishes
/// loading and the dataset lifecycle installs it here (see
/// [`DatasetPlacement`]). A table that is discovered but still bootstrapping is
/// present in `tables` and absent from `providers`, which is exactly the
/// "not yet queryable" state.
impl AcceleratedSchemaProvider {
    /// The provider for `name`, but only while `name` is still part of the
    /// current plan.
    ///
    /// Both halves are required. `providers` alone would keep serving a table
    /// that a later `refresh()` dropped from `tables` — because the source
    /// dropped it, or because `include`/`exclude` now filter it out — leaving a
    /// ghost that `table_names()` no longer lists but queries still resolve,
    /// backed by an accelerator no longer being fed. `tables` alone would report
    /// a discovered-but-still-bootstrapping table as present, letting
    /// `normalize_table_reference` resolve something whose `table()` returns
    /// `None`, which then fails as "not found" at query time and can shadow a
    /// ready same-named table in another schema.
    fn installed_provider(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        if !self.tables.read().contains_key(name) {
            return None;
        }
        self.providers.read().get(name).map(Arc::clone)
    }
}

#[derive(Default)]
struct AcceleratedSchemaProvider {
    tables: RwLock<HashMap<String, String>>,
    /// `table_name` -> its accelerated provider. Keyed by the *source* table
    /// name, since that is what `SchemaProvider` is asked for.
    providers: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    /// Reverse index (`dataset name` -> `table name`) so an install, which
    /// arrives keyed by dataset name, can find the table it belongs to.
    dataset_to_table: RwLock<HashMap<String, String>>,
}

impl std::fmt::Debug for AcceleratedSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceleratedSchemaProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for AcceleratedSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        let guard = self.tables.read();
        guard.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.installed_provider(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.installed_provider(name).is_some()
    }
}

impl crate::datafusion::DatasetPlacement for AcceleratedSchemaProvider {
    /// Take ownership of a synthesized dataset's provider instead of letting it
    /// be registered under the default catalog.
    fn install(
        &self,
        name: &TableReference,
        provider: Arc<dyn TableProvider>,
    ) -> crate::datafusion::Result<()> {
        let dataset_name = name.to_string();
        // The catalog registers the placement before spawning the dataset, so an
        // install for an unknown dataset means the two have gone out of sync.
        // Fail rather than log-and-continue: returning `Ok` here would let the
        // lifecycle mark the dataset Ready while its provider was dropped on the
        // floor, so the table would be permanently unqueryable and reported
        // healthy. Failing surfaces it as a dataset error and retries.
        let table_name = self
            .dataset_to_table
            .read()
            .get(&dataset_name)
            .cloned()
            .ok_or_else(
                || crate::datafusion::Error::UnableToRegisterTableToDataFusion {
                    source: datafusion::error::DataFusionError::Internal(format!(
                        "accelerated catalog has no table registered for dataset '{dataset_name}'"
                    )),
                },
            )?;
        self.providers.write().insert(table_name, provider);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_synthesized_dataset_name_is_valid_identifier_for_special_chars() {
        // Real PostgreSQL identifiers permitted via quoting can contain
        // characters (e.g. `-`) that plain concatenation would carry into a
        // name `validate_identifier` rejects.
        let name = synthesized_dataset_name("my-catalog", "my schema", "my-table");
        crate::component::validate_identifier(&name).expect("should be a valid identifier");

        // Non-ASCII is escaped per UTF-8 byte, so it stays a valid identifier.
        let unicode = synthesized_dataset_name("cat", "public", "ürders");
        crate::component::validate_identifier(&unicode).expect("should be a valid identifier");
    }

    #[test]
    fn test_synthesized_dataset_name_is_readable_for_ordinary_identifiers() {
        // The name shows up in logs, status, metric attributes and -- under a
        // file mode -- as the table's directory under `cayenne_file_path`, so
        // the common all-alphanumeric case must stay legible.
        assert_eq!(
            synthesized_dataset_name("pg_cdc", "public", "order_items"),
            "__catalog_accel_pg_ucdc__public__order_uitems"
        );
    }

    /// Regression: escaping `_` as `__` and separating with a single `_` was
    /// NOT injective, because the separator was the same character as the
    /// doubled one, so a `_` at a component boundary shifted the split.
    /// `("x", "_y")` and `("x_", "y")` both produced `..._x___y`, which would
    /// give two source tables one dataset name, one component-status key and
    /// one data directory.
    #[test]
    fn test_synthesized_dataset_name_survives_underscores_at_component_boundaries() {
        assert_ne!(
            synthesized_dataset_name("cat", "x", "_y"),
            synthesized_dataset_name("cat", "x_", "y"),
        );
        assert_ne!(
            synthesized_dataset_name("cat", "x_", "_y"),
            synthesized_dataset_name("cat", "x", "__y"),
        );
        assert_ne!(
            synthesized_dataset_name("cat_", "x", "y"),
            synthesized_dataset_name("cat", "_x", "y"),
        );
    }

    /// Brute-force the property the whole scheme rests on, rather than trusting
    /// the argument for it: over every triple drawn from a set of adversarial
    /// components (empty, underscores in every position, the escape sequences
    /// themselves, non-ASCII), no two distinct triples may share a name.
    #[test]
    fn test_synthesized_dataset_name_is_injective_over_adversarial_triples() {
        let parts = [
            "", "a", "_", "__", "___", "a_", "_a", "a_b", "ab", "a__b", "u", "_u", "x61", "_x61",
            "a_u", "a_x61", "-", " ", ".", "ü",
        ];
        let mut seen: HashMap<String, (&str, &str, &str)> = HashMap::new();
        for c in parts {
            for s in parts {
                for t in parts {
                    let name = synthesized_dataset_name(c, s, t);
                    if let Some(prev) = seen.insert(name.clone(), (c, s, t)) {
                        panic!("{:?} and {:?} both encode to {name:?}", (c, s, t), prev);
                    }
                }
            }
        }
    }

    #[test]
    fn test_synthesized_dataset_name_is_deterministic_and_distinct() {
        let a = synthesized_dataset_name("cat", "public", "orders");
        let b = synthesized_dataset_name("cat", "public", "orders");
        assert_eq!(a, b, "same inputs must produce the same name");

        let different_table = synthesized_dataset_name("cat", "public", "items");
        assert_ne!(a, different_table);

        // A naive "join with '_'" scheme collides here: schema="a_b",
        // table="c" and schema="a", table="b_c" both naively join to
        // "a_b_c". Doubling `_` inside each component rules this out: a lone
        // `_` then only ever appears as the separator between components.
        let shifted_left = synthesized_dataset_name("cat", "a_b", "c");
        let shifted_right = synthesized_dataset_name("cat", "a", "b_c");
        assert_ne!(shifted_left, shifted_right);

        // The same shift, but with the escape sequence itself in the input --
        // a table literally named `x61` must not collide with the escaping of
        // some other character.
        assert_ne!(
            synthesized_dataset_name("cat", "public", "_x61"),
            synthesized_dataset_name("cat", "public", "a"),
        );
    }

    #[test]
    fn test_escape_name_component_is_injective_over_awkward_identifiers() {
        // Every distinct input must produce a distinct component; a collision
        // here means two source tables share one accelerated dataset, and one
        // silently serves the other's rows.
        let inputs = [
            "", "a", "A", "_", "__", "a_", "_a", "a_b", "ab", "a-b", "a b", "a.b", "a_x62", "x62",
            "_x62", "ürders", "0", "_0",
        ];
        let mut seen = HashMap::new();
        for input in inputs {
            let escaped = escape_name_component(input);
            if let Some(previous) = seen.insert(escaped.clone(), input) {
                panic!("{input:?} and {previous:?} both escape to {escaped:?}");
            }
        }
    }

    fn empty_provider() -> Arc<dyn TableProvider> {
        Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::new(
            datafusion::arrow::datatypes::Schema::empty(),
        )))
    }

    /// The schema provider owns its tables outright -- the same contract every
    /// other catalog connector's schema provider has. A discovered table is not
    /// queryable until its synthesized dataset finishes loading and installs a
    /// provider here; reporting it earlier would let it resolve and then fail as
    /// "not found" at query time.
    #[tokio::test]
    async fn a_table_is_queryable_only_once_its_provider_is_installed() {
        use crate::datafusion::DatasetPlacement;

        let schema = AcceleratedSchemaProvider::default();
        *schema.tables.write() =
            HashMap::from([("orders".to_string(), "__catalog_accel_x".to_string())]);
        schema
            .dataset_to_table
            .write()
            .insert("__catalog_accel_x".to_string(), "orders".to_string());

        // Discovered, still bootstrapping: listed, but not yet resolvable.
        assert_eq!(schema.table_names(), vec!["orders".to_string()]);
        assert!(!schema.table_exist("orders"));
        assert!(schema.table("orders").await.expect("lookup").is_none());

        schema
            .install(&TableReference::bare("__catalog_accel_x"), empty_provider())
            .expect("install");

        assert!(schema.table_exist("orders"));
        assert!(schema.table("orders").await.expect("lookup").is_some());
    }

    /// A periodic refresh rebuilds the per-schema table map, but must not
    /// discard providers already installed -- doing so would make every ready
    /// table vanish until its dataset happened to reload.
    #[tokio::test]
    async fn refreshing_the_table_map_keeps_installed_providers() {
        use crate::datafusion::DatasetPlacement;

        let schema = AcceleratedSchemaProvider::default();
        schema
            .dataset_to_table
            .write()
            .insert("__catalog_accel_x".to_string(), "orders".to_string());
        schema
            .install(&TableReference::bare("__catalog_accel_x"), empty_provider())
            .expect("install");

        // What `refresh()` does to a reused provider: replace the table map.
        *schema.tables.write() = HashMap::from([
            ("orders".to_string(), "__catalog_accel_x".to_string()),
            ("items".to_string(), "__catalog_accel_y".to_string()),
        ]);

        assert!(
            schema.table("orders").await.expect("lookup").is_some(),
            "an already-loaded table must survive a re-plan"
        );
        assert!(
            !schema.table_exist("items"),
            "a newly discovered table is not queryable until its provider lands"
        );
    }

    /// An install for a dataset the schema does not know about means the
    /// placement registry and the table map have diverged. Dropping the provider
    /// silently would leave that table permanently unqueryable with nothing in
    /// the logs, so the mismatch is surfaced instead.
    #[tokio::test]
    async fn an_install_for_an_unknown_dataset_is_not_silently_dropped() {
        use crate::datafusion::DatasetPlacement;

        let schema = AcceleratedSchemaProvider::default();
        let result = schema.install(
            &TableReference::bare("__catalog_accel_missing"),
            empty_provider(),
        );

        assert!(
            result.is_err(),
            "an install with nowhere to go must fail the dataset rather than let it \
             report Ready while its table is unqueryable"
        );
        assert!(schema.table_names().is_empty());
        assert!(schema.providers.read().is_empty());
    }

    /// A table dropped from the plan by a later `refresh()` -- removed at the
    /// source, or newly matched by `exclude` -- must stop resolving, not linger
    /// as a ghost backed by an accelerator that is no longer fed.
    #[tokio::test]
    async fn a_table_removed_from_the_plan_stops_resolving() {
        use crate::datafusion::DatasetPlacement;

        let schema = AcceleratedSchemaProvider::default();
        *schema.tables.write() =
            HashMap::from([("orders".to_string(), "__catalog_accel_x".to_string())]);
        schema
            .dataset_to_table
            .write()
            .insert("__catalog_accel_x".to_string(), "orders".to_string());
        schema
            .install(&TableReference::bare("__catalog_accel_x"), empty_provider())
            .expect("install");
        assert!(schema.table_exist("orders"));

        // The next refresh no longer discovers `orders`.
        schema.tables.write().clear();

        assert!(
            !schema.table_exist("orders"),
            "removed table must not resolve"
        );
        assert!(schema.table("orders").await.expect("lookup").is_none());
        assert!(schema.table_names().is_empty());
    }

    #[test]
    fn acceleration_kind_maps_from_eligible_outcomes_only() {
        use data_components::postgres::provider::SkipReason;
        assert_eq!(
            AccelerationKind::from_outcome(&ReplicaIdentityOutcome::AccelerateViaPrimaryKey {
                key: vec!["id".to_string()],
            }),
            Some(AccelerationKind::PrimaryKey)
        );
        assert_eq!(
            AccelerationKind::from_outcome(&ReplicaIdentityOutcome::AccelerateViaUniqueIndex {
                key: vec!["uid".to_string()],
            }),
            Some(AccelerationKind::UniqueIndex)
        );
        assert_eq!(
            AccelerationKind::from_outcome(
                &ReplicaIdentityOutcome::AccelerateFullReplicaIdentity {
                    key: vec!["id".to_string()],
                }
            ),
            Some(AccelerationKind::Full)
        );
        assert_eq!(
            AccelerationKind::from_outcome(&ReplicaIdentityOutcome::Skip {
                reason: SkipReason::KeylessDefault,
            }),
            None
        );
    }

    #[test]
    fn acceleration_summary_counts_by_kind_and_total() {
        let mut summary = AccelerationSummary::default();
        summary.record_accelerated(AccelerationKind::PrimaryKey);
        summary.record_accelerated(AccelerationKind::PrimaryKey);
        summary.record_accelerated(AccelerationKind::UniqueIndex);
        summary.record_accelerated(AccelerationKind::Full);
        summary.skipped = 2;
        summary.excluded = 1;

        assert_eq!(summary.primary_key, 2);
        assert_eq!(summary.unique_index, 1);
        assert_eq!(summary.full, 1);
        // Skipped/excluded do not count toward accelerated total.
        assert_eq!(summary.accelerated_total(), 4);
    }

    #[test]
    fn acceleration_summary_add_folds_every_field() {
        let mut a = AccelerationSummary {
            primary_key: 1,
            unique_index: 2,
            full: 3,
            skipped: 4,
            excluded: 5,
            views_not_replicated: 6,
        };
        let b = AccelerationSummary {
            primary_key: 10,
            unique_index: 20,
            full: 30,
            skipped: 40,
            excluded: 50,
            views_not_replicated: 60,
        };
        a.add(&b);
        assert_eq!(
            a,
            AccelerationSummary {
                primary_key: 11,
                unique_index: 22,
                full: 33,
                skipped: 44,
                excluded: 55,
                views_not_replicated: 66,
            }
        );
    }

    #[test]
    fn acceleration_summary_detects_zero_eligible() {
        // Only skipped/excluded tables => nothing eligible => the fail-loud path.
        let summary = AccelerationSummary {
            skipped: 3,
            excluded: 2,
            ..AccelerationSummary::default()
        };
        assert_eq!(summary.accelerated_total(), 0);
        // Discovered tables count skipped + excluded (accelerated is 0 here);
        // views are NOT tables and don't inflate the discovered count.
        assert_eq!(summary.discovered_tables(), 5);
    }

    #[test]
    fn discovered_tables_excludes_views() {
        let summary = AccelerationSummary {
            primary_key: 2,
            unique_index: 1,
            full: 1,
            skipped: 3,
            excluded: 2,
            views_not_replicated: 10,
        };
        // 4 accelerated + 3 skipped + 2 excluded = 9; the 10 views don't count.
        assert_eq!(summary.discovered_tables(), 9);
    }

    #[test]
    fn acceleration_summary_message_reports_counts_and_slot() {
        let summary = AccelerationSummary {
            primary_key: 2,
            unique_index: 1,
            full: 1,
            skipped: 2,
            excluded: 3,
            views_not_replicated: 5,
        };
        let message = summary.summary_message("my_pg", "spice_my_pg_slot");
        assert!(message.contains("my_pg"), "{message}");
        assert!(message.contains("spice_my_pg_slot"), "{message}");
        assert!(
            message.contains("4 table(s)"),
            "accelerated total: {message}"
        );
        assert!(message.contains("2 via primary key"), "{message}");
        assert!(
            message.contains("1 via REPLICA IDENTITY USING INDEX"),
            "{message}"
        );
        assert!(message.contains("1 via REPLICA IDENTITY FULL"), "{message}");
        assert!(message.contains("3 table(s) excluded"), "{message}");
        assert!(message.contains("2 table(s) skipped"), "{message}");
        assert!(message.contains("5 view(s)"), "views clause: {message}");
        // The discovery-scope note is stated in the summary.
        assert!(
            message.contains("picked up on the periodic catalog refresh"),
            "discovery-scope note: {message}"
        );
    }

    #[test]
    fn summary_message_omits_views_clause_when_none() {
        let summary = AccelerationSummary {
            primary_key: 1,
            ..AccelerationSummary::default()
        };
        let message = summary.summary_message("my_pg", "slot");
        assert!(!message.contains("not replicated"), "{message}");
        // The discovery-scope note is unconditional.
        assert!(
            message.contains("picked up on the periodic catalog refresh"),
            "{message}"
        );
    }

    #[test]
    fn no_eligible_tables_error_is_actionable_with_docs_link() {
        let err = NoEligibleTablesError {
            catalog: "my_pg".to_string(),
            discovered: 5,
            excluded: 3,
            skipped: 2,
            views_note: String::new(),
        }
        .to_string();
        assert!(err.contains("my_pg"), "{err}");
        // "0 of N discovered" shape from the spec example (#11850).
        assert!(err.contains("0 of 5 discovered"), "{err}");
        assert!(err.contains("2 have no primary key"), "{err}");
        assert!(err.contains("3 excluded"), "{err}");
        assert!(err.contains("REPLICA IDENTITY"), "{err}");
        assert!(err.contains("USING INDEX"), "{err}");
        assert!(err.contains("https://spiceai.org/docs"), "{err}");
    }

    #[test]
    fn no_eligible_tables_error_notes_view_like_relations_when_present() {
        // The only-views case: 0 of 0 discovered *tables*, but view-like
        // relations were found -- the message must explain that so it doesn't
        // read as an empty schema.
        let err = NoEligibleTablesError {
            catalog: "my_pg".to_string(),
            discovered: 0,
            excluded: 0,
            skipped: 0,
            views_note: " 3 view-like relation(s) (views/materialized views/foreign tables) were found in scope but cannot be CDC-accelerated.".to_string(),
        }
        .to_string();
        assert!(err.contains("0 of 0 discovered"), "{err}");
        assert!(err.contains("3 view-like relation(s)"), "{err}");
    }
}
