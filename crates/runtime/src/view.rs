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
use crate::{
    accelerated::AcceleratedTable, component::view::View,
    embeddings::index::table::wrap_table_as_index,
    search::full_text::table::add_full_text_search_to_table,
};
use ::datafusion::sql::{TableReference, parser, sqlparser::ast};
use async_trait::async_trait;
use datafusion::{
    catalog::{ScanArgs, ScanResult, Session, TableProvider},
    common::tree_node::TreeNodeRecursion,
    datasource::ViewTable,
    error::{DataFusionError, Result},
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};
use datafusion_federation::{FederatedPlanNode, FederatedTableProviderAdaptor};
use runtime_acceleration::snapshot::SnapshotPublishGate;
use runtime_datafusion::refresh_scan::session_is_refresh_scan;
use runtime_search::embeddings::{table::EmbeddingTable, warm_index_on_zero_results};
use runtime_table::accelerated::materialization::MaterializationIdentity;
use sha2::{Digest, Sha256};
use snafu::ResultExt;
use spice_table::TableLayer;
use spicepod::component::embeddings::ColumnEmbeddingConfig;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

/// The binding half of the accelerated-view snapshot consistency check.
///
/// The load-time check in `create_accelerated_view` exists to fail fast with a message an
/// operator can act on — including a `bootstrap_only` consumer that would otherwise
/// restore an `accept_skew` archive without opting out — but it cannot be the whole
/// answer: the compiled plan follows catalog state, statistics and federation pushdown,
/// so a view that reads once at registration can read twice later without its SQL
/// changing.
///
/// This gate consumes the read-shape recorded from the plan that *executed* the
/// refresh that produced the rows now on disk. It does not re-plan at publish time:
/// a catalog or pushdown change after materialization must not approve rows that
/// came from a multi-read refresh, and must not be required to re-prove a
/// single-read refresh whose sources have since grown a second scan.
///
/// [`AttestingViewProvider`] writes that attestation when the federated view is
/// scanned from a refresh session, stamped with the materialization epoch the
/// refresh began. An ordinary query — including `on_zero_results: use_source`
/// fallback — does not record, so it cannot replace a multi-read refresh with a
/// later single-read scan at the same epoch. No attestation means no refresh in
/// this process has proven the current rows came from a single read, so
/// publication is refused. An attestation from a later epoch than the rows being
/// archived is refused the same way: a refresh can record a new plan shape
/// before it takes the write mutex, and that shape must not approve the
/// previous generation's rows.
///
/// Refusing skips one publish; it does not fail the view or the refresh. The accelerated
/// table stays correct and keeps serving — it just does not add a snapshot this cycle,
/// and a cold start bootstraps whatever was last published.
pub(crate) struct ViewSnapshotPublishGate {
    view_name: TableReference,
    attestation: ViewRefreshReadAttestation,
    /// Epoch sampled with the rows under the write mutex. `None` until
    /// [`SnapshotPublishGate::bind_materialization_epoch`] — tests that only
    /// exercise shape still work; the live snapshot path always binds.
    expected_epoch: parking_lot::Mutex<Option<u64>>,
}

impl ViewSnapshotPublishGate {
    pub(crate) fn new(view_name: TableReference, attestation: ViewRefreshReadAttestation) -> Self {
        Self {
            view_name,
            attestation,
            expected_epoch: parking_lot::Mutex::new(None),
        }
    }
}

fn missing_refresh_attestation_reason(view_name: &TableReference) -> String {
    format!(
        "view '{view_name}' has no refresh-plan attestation, so Spice cannot confirm these rows came from a single consistent read of its sources"
    )
}

fn attestation_epoch_mismatch_reason(view_name: &TableReference) -> String {
    format!(
        "view '{view_name}' has a refresh-plan attestation from a later materialization than the rows being archived, so Spice cannot confirm these rows came from a single consistent read of its sources"
    )
}

#[async_trait]
impl SnapshotPublishGate for ViewSnapshotPublishGate {
    async fn check_publish(&self) -> Result<(), String> {
        match self.attestation.last_stamped() {
            None => Err(missing_refresh_attestation_reason(&self.view_name)),
            Some((epoch, shape)) => {
                if let Some(expected) = *self.expected_epoch.lock()
                    && epoch != expected
                {
                    return Err(attestation_epoch_mismatch_reason(&self.view_name));
                }
                shape.refusal_reason().map_or(Ok(()), Err)
            }
        }
    }

    fn bind_materialization_epoch(&self, epoch: u64) {
        *self.expected_epoch.lock() = Some(epoch);
    }
}

/// Last refresh-plan read shape written by the executing refresh scan, read by
/// [`ViewSnapshotPublishGate`]. `None` means no refresh has attested this process.
/// Each record is stamped with the [`MaterializationIdentity`] epoch at scan time.
/// Production writes go through [`AttestingViewProvider`], which only calls
/// [`Self::record`] on a refresh session.
#[derive(Clone, Debug)]
pub(crate) struct ViewRefreshReadAttestation {
    identity: MaterializationIdentity,
    shape: Arc<parking_lot::RwLock<Option<(u64, ViewReadShape)>>>,
}

impl ViewRefreshReadAttestation {
    #[must_use]
    pub(crate) fn with_identity(identity: MaterializationIdentity) -> Self {
        Self {
            identity,
            shape: Arc::new(parking_lot::RwLock::new(None)),
        }
    }

    pub(crate) fn record(&self, shape: ViewReadShape) {
        let epoch = self.identity.epoch();
        *self.shape.write() = Some((epoch, shape));
    }

    #[must_use]
    pub(crate) fn last_stamped(&self) -> Option<(u64, ViewReadShape)> {
        self.shape.read().clone()
    }
}

/// Records the read shape of the [`ExecutionPlan`] a refresh will run.
///
/// Stacked as a [`spice_table::TableLayer`] so layer walks see through to the
/// federated view instead of stopping on a wrapping `TableProvider`.
pub(crate) fn wrap_view_refresh_attestation(
    inner: Arc<dyn TableProvider>,
    attestation: ViewRefreshReadAttestation,
) -> Arc<dyn TableProvider> {
    spice_table::SpiceTable::over(Arc::new(AttestingViewProvider { attestation }), inner)
}

/// Federated-side layer for an accelerated view. `scan_with_args` classifies the
/// plan returned by the table beneath and stores it for the publish gate only
/// when the session is a refresh execution. An ordinary query, including
/// `on_zero_results: use_source` fallback, does not record. Every other
/// [`TableLayer`] method keeps its default and forwards to `below`.
struct AttestingViewProvider {
    attestation: ViewRefreshReadAttestation,
}

impl std::fmt::Debug for AttestingViewProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttestingViewProvider")
            .field("attestation", &self.attestation)
            .finish()
    }
}

impl AttestingViewProvider {
    fn record_executed(&self, plan: &dyn ExecutionPlan) {
        self.attestation.record(classify_executed_read(plan));
    }
}

#[async_trait]
impl TableLayer for AttestingViewProvider {
    async fn scan_with_args<'a>(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        let result = below.scan_with_args(state, args).await?;
        let plan = result.into_inner();
        if session_is_refresh_scan(state) {
            self.record_executed(plan.as_ref());
        }
        Ok(ScanResult::new(plan))
    }
}

/// Plan `sql` and classify the read shape of the result.
///
/// Runs the full `optimize` pass rather than stopping at the raw plan: federation is an
/// analyzer rule and the optimizer changes the scan count, so only the compiled plan
/// reports the reads that will actually happen. See [`classify_view_read`].
pub(crate) async fn analyzed_view_read_shape(
    ctx: &SessionContext,
    sql: &str,
) -> Result<ViewReadShape> {
    let state = ctx.state();
    let plan = state.create_logical_plan(sql).await?;
    // `SessionState::optimize` is synchronous and runs the whole analyzer + logical
    // optimizer, which is milliseconds of uninterrupted CPU for a multi-join view — far
    // past the ~100us an async worker may hold. The state is already an owned clone, so
    // it moves cleanly.
    let analyzed = tokio::task::spawn_blocking(move || state.optimize(&plan))
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;
    Ok(classify_view_read(&analyzed))
}

/// How many independent reads of its sources a view's materialization spans.
///
/// A view materializes a query. Every `TableScan` in the compiled plan becomes one
/// `TableProvider::scan` call, and each of those resolves its own read view — so a plan
/// with two scans captures its sources at two different positions and can materialize
/// rows that never existed together. That is not a staleness problem the next refresh fixes;
/// it is a row set that corresponds to no state the source was ever in, and publishing
/// it as a snapshot makes it durable and reusable.
///
/// Counted on the **compiled** plan rather than the SQL, for two reasons. The analyzer
/// and optimizer change the scan count — subquery decorrelation, join elimination and
/// union flattening all add or remove `TableScan` nodes — so a count taken from the AST
/// is neither an upper nor a lower bound on the reads that actually happen. And
/// federation is applied as an analyzer rule, so a plan that pushes down entirely to one
/// source is only visible after analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ViewReadShape {
    /// At most one `TableScan`, nothing federated. The whole materialization descends
    /// from a single `scan` call, so it is a single read by construction. A view over no
    /// table at all (`SELECT 1`) lands here too — zero reads cannot disagree.
    SingleScan { tables: Vec<TableReference> },
    /// The entire plan collapsed into one federated sub-plan, which executes as a single
    /// statement at one source.
    ///
    /// Admitted, for the same reason [`Self::SingleScan`] is: this gate measures how many
    /// times a materialization reads its sources, and this reads once. Whether that read
    /// is *isolated* at the source is a separate property, and not one this gate checks
    /// for a local scan either — a `TableScan` of an object-store listing has no better
    /// guarantee than an HTTP endpoint. Holding federation to the stricter standard would
    /// refuse the ordinary case, since the federation analyzer rule is on by default and a
    /// view over a single federated dataset compiles to exactly this.
    FederatedSingleStatement { tables: Vec<TableReference> },
    /// Several independent reads: several scans, several federated sub-plans, or a mix.
    MultipleReads {
        reads: usize,
        tables: Vec<TableReference>,
    },
}

impl ViewReadShape {
    /// A cause clause explaining why this shape cannot publish a snapshot. `None` when
    /// it can.
    pub(crate) fn refusal_reason(&self) -> Option<String> {
        match self {
            // Both read their sources exactly once, which is the whole question here.
            ViewReadShape::SingleScan { .. } | ViewReadShape::FederatedSingleStatement { .. } => {
                None
            }
            ViewReadShape::MultipleReads { reads, tables } => Some(format!(
                "its query reads its sources {reads} times ({}), so a snapshot would \
                 capture each read at a different source position and could store rows \
                 that never existed together in the source",
                quoted_list(tables)
            )),
        }
    }
}

fn quoted_list(tables: &[TableReference]) -> String {
    if tables.is_empty() {
        return "no tables".to_string();
    }
    tables
        .iter()
        .map(|t| format!("'{t}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Whether this scan's provider delegates to several child providers, so that one logical
/// `TableScan` performs more than one independent read.
///
/// A partitioned acceleration is the case in the tree today: `PartitionTableProvider::scan`
/// loops over its children and calls each child's `TableProvider::scan`, and each Cayenne
/// child captures its own read view. A cross-partition write commits every catalog pointer
/// in one transaction but publishes the partitions' in-memory state one at a time, so a
/// scan can take one partition from before that publish and another from after — a
/// combination that was never a single state of the table.
///
/// Counting `TableScan` nodes cannot see this: the provider hides its children (its
/// `get_logical_plan` returns `None`), so the plan shows one node either way.
fn scan_fans_out(scan: &datafusion::logical_expr::TableScan) -> bool {
    // `source_as_provider` is the supported way back to the provider behind a scan; the
    // `TableSource` trait's own downcast is not reachable in this fork.
    let Ok(provider) = datafusion::datasource::source_as_provider(&scan.source) else {
        // A source shape this code cannot resolve cannot be shown to read once. Say so
        // rather than assuming the friendly answer.
        return true;
    };
    provider_fans_out(&provider)
}

/// Whether `provider`, or any provider beneath it, fans one scan out across child
/// providers.
///
/// A registered accelerated dataset never presents its accelerator directly: the scan
/// resolves to the layered `SpiceTable`/`AcceleratedTable` stack — with a
/// `FederatedTableProviderAdaptor` above it when the source is federated — and the
/// partition provider sits behind `AcceleratedTable::get_accelerator()`. Testing only the
/// outermost provider reports no fan-out for exactly the accelerated views this gate
/// exists to refuse, so the layers are peeled first.
/// `DataFusion::partition_expr_from_table_provider` walks the same shape.
fn provider_fans_out(provider: &Arc<dyn TableProvider>) -> bool {
    if provider.is::<runtime_table_partition::provider::PartitionTableProvider>() {
        return true;
    }

    if let Some(accelerated) =
        spice_table::find_layer::<AcceleratedTable>(provider.as_ref(), spice_table::LayerWalk::Read)
    {
        return provider_fans_out(&accelerated.get_accelerator());
    }

    if let Some(layered) = provider.downcast_ref::<spice_table::SpiceTable>() {
        return provider_fans_out(layered.below());
    }

    if let Some(adaptor) = provider.downcast_ref::<FederatedTableProviderAdaptor>()
        && let Some(inner) = adaptor.table_provider.as_ref()
    {
        return provider_fans_out(inner);
    }

    false
}

/// Every table scanned by `plan`, including inside subqueries, **without**
/// deduplicating: two scans of one table are two independent reads, which is exactly the
/// case this classification exists to catch. (`prepare_transaction` walks the same way
/// but dedupes, because it wants a participant set rather than a read count.)
fn scanned_tables(plan: &LogicalPlan) -> Vec<TableReference> {
    let mut tables = Vec::new();
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            tables.push(scan.table_name.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    tables
}

/// Classify the read shape of an **analyzed** logical plan.
///
/// The caller must pass a plan that has been through the analyzer (`SessionState::optimize`
/// runs it): the federation rule is an analyzer rule, so on a raw plan every federated
/// sub-plan still appears as loose `TableScan`s and a fully-pushed-down view would be
/// misreported as a multi-read.
pub(crate) fn classify_view_read(plan: &LogicalPlan) -> ViewReadShape {
    let mut scans: Vec<TableReference> = Vec::new();
    let mut federated: Vec<Vec<TableReference>> = Vec::new();
    let mut opaque: usize = 0;
    let mut fan_out: usize = 0;

    let _ = plan.apply_with_subqueries(|node| {
        match node {
            LogicalPlan::TableScan(scan) => {
                if scan_fans_out(scan) {
                    // One logical scan, many actual reads: this provider's `scan` loops
                    // over child providers and calls each one's `scan` independently, so
                    // each child captures its own read view. Counting the node would report
                    // one read for a materialization that took several.
                    fan_out += 1;
                } else {
                    scans.push(scan.table_name.clone());
                }
            }
            LogicalPlan::Extension(ext) => {
                // `FederatedPlanNode::inputs()` is empty by design, so the walk stops at
                // the boundary and the sub-plan's scans have to be collected explicitly.
                // That is what makes "one federated node and no loose scans" a
                // recognizable shape rather than an invisible one.
                if let Some(fed) = ext.node.as_any().downcast_ref::<FederatedPlanNode>() {
                    federated.push(scanned_tables(fed.plan()));
                } else if ext.node.inputs().is_empty() {
                    // An extension this code does not recognize, with no inputs to descend
                    // into, may hide any number of reads. Counting it as zero admits it;
                    // count it as unknown so the plan is refused instead.
                    opaque += 1;
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    });

    match (scans.len(), federated.len(), opaque + fan_out) {
        (0 | 1, 0, 0) => ViewReadShape::SingleScan { tables: scans },
        (0, 1, 0) => ViewReadShape::FederatedSingleStatement {
            tables: federated.into_iter().next().unwrap_or_default(),
        },
        (scan_count, federated_count, opaque_count) => {
            let mut tables = scans;
            for inner in federated {
                tables.extend(inner);
            }
            ViewReadShape::MultipleReads {
                reads: scan_count + federated_count + opaque_count,
                tables,
            }
        }
    }
}

/// Classify the read shape of the [`ExecutionPlan`] a refresh is about to run.
///
/// Counted on the physical plan because that is what executes: a later re-plan of
/// the same SQL can see a different catalog or pushdown and disagree with the
/// rows already on disk. Federation that collapsed into one remote statement is
/// one leaf (or a named federated node), so it is admitted on the same footing
/// as [`ViewReadShape::FederatedSingleStatement`]. Unknown leaves are treated as
/// reads so a shape this walk cannot explain is refused rather than published.
pub(crate) fn classify_executed_read(plan: &dyn ExecutionPlan) -> ViewReadShape {
    let mut reads = Vec::new();
    collect_executed_reads(plan, &mut reads);
    match reads.as_slice() {
        [] => ViewReadShape::SingleScan { tables: Vec::new() },
        [ExecutedRead::Federated] => ViewReadShape::FederatedSingleStatement { tables: Vec::new() },
        [ExecutedRead::Scan { label }] => ViewReadShape::SingleScan {
            tables: vec![TableReference::bare(label.clone())],
        },
        many => ViewReadShape::MultipleReads {
            reads: many.len(),
            tables: many
                .iter()
                .map(|read| match read {
                    ExecutedRead::Scan { label } => TableReference::bare(label.clone()),
                    ExecutedRead::Federated => TableReference::bare("federated"),
                })
                .collect(),
        },
    }
}

#[derive(Debug)]
enum ExecutedRead {
    Scan { label: String },
    Federated,
}

fn collect_executed_reads(plan: &dyn ExecutionPlan, reads: &mut Vec<ExecutedRead>) {
    let name = plan.name();
    if is_federated_exec(name) {
        reads.push(ExecutedRead::Federated);
        return;
    }

    let children = plan.children();
    if children.is_empty() {
        if !is_non_read_leaf(name) {
            reads.push(ExecutedRead::Scan {
                label: name.to_string(),
            });
        }
        return;
    }

    for child in children {
        collect_executed_reads(child.as_ref(), reads);
    }
}

fn is_federated_exec(name: &str) -> bool {
    name.contains("Federat") || name.contains("federat")
}

fn is_non_read_leaf(name: &str) -> bool {
    matches!(name, "EmptyExec" | "PlaceholderRowExec" | "ValuesExec")
}

/// The definition string a view's snapshot identity is computed over: its own SQL plus the
/// SQL of every view it transitively reads.
///
/// A view's rows are the result of its whole dependency closure, not just its outer text.
/// `outer` = `SELECT * FROM inner` keeps identical SQL when `inner` changes from a US
/// filter to an EU one — same schema, entirely different rows — so an identity taken from
/// the outer text alone would accept an archive materialized under the old `inner`. The
/// runtime already treats this as a real dependency: `apply_view_diff` reloads unchanged
/// views whose dependencies changed.
///
/// Dependencies are emitted in sorted order so the string does not depend on traversal
/// order, and each view is visited once so a cycle (which view loading tolerates and warns
/// about) terminates rather than recursing forever.
#[must_use]
pub(crate) fn view_definition_closure(
    name: &TableReference,
    sql: &str,
    columns: &[spicepod::semantic::Column],
    params: &HashMap<String, String>,
    app: &app::App,
) -> String {
    /// Every relation the SQL names, wherever it appears.
    ///
    /// Uses `visit_relations` rather than [`get_dependent_table_names`], which walks only
    /// `FROM` relations and CTEs. That is the right answer for ordering view loads, but not
    /// for an identity: a dependency reached only through an expression subquery
    /// (`WHERE id IN (SELECT id FROM inner)`) would be missed, leaving the fingerprint
    /// unchanged when that view's definition changes and accepting an archive of the rows it
    /// used to produce. A relation named in a position this walk reports but planning does
    /// not actually read costs at most an extra dependency in the identity.
    fn dependencies_of(sql: &str) -> Vec<TableReference> {
        use ::datafusion::sql::sqlparser::ast::visit_relations;
        use std::ops::ControlFlow;

        let Ok(statements) = parser::DFParser::parse_sql_with_dialect(
            sql,
            &::datafusion::sql::sqlparser::dialect::PostgreSqlDialect {},
        ) else {
            return Vec::new();
        };
        let Some(parser::Statement::Statement(statement)) = statements.front() else {
            return Vec::new();
        };

        // A CTE name is not a dependency: it is defined by this very query, and its own
        // body's relations are visited anyway.
        let cte_names: HashSet<String> = {
            let mut names = HashSet::new();
            if let ast::Statement::Query(query) = statement.as_ref()
                && let Some(with) = &query.with
            {
                for cte in &with.cte_tables {
                    names.insert(cte.alias.name.value.to_lowercase());
                }
            }
            names
        };

        let mut found = Vec::new();
        let _ = visit_relations(statement.as_ref(), |relation| {
            let name = relation.to_string();
            if !cte_names.contains(&name.to_lowercase()) {
                found.push(TableReference::parse_str(&name));
            }
            ControlFlow::<()>::Continue(())
        });
        found
    }

    // A Spicepod name and a name parsed out of SQL may spell the same table differently —
    // `public.inner` declared, `inner` referenced. Compare the parsed forms, and treat a
    // qualifier only as a constraint when BOTH sides state it. Matching too widely costs
    // an occasional extra dependency in the fingerprint (a snapshot refused that need not
    // have been); matching too narrowly drops a dependency, which is a snapshot ACCEPTED
    // under a definition that no longer produces its rows.
    //
    // Because a bare reference can match more than one declaration (`public.inner` and
    // `sales.inner` both answer to `inner`), EVERY match is folded in rather than the first.
    // Picking one would mean picking the wrong one half the time: planning resolves the
    // bare name through the default catalog and schema, which this code cannot see, so a
    // change to the view actually being read would leave the fingerprint untouched whenever
    // the arbitrary first match was some other view.
    fn names_match(declared: &str, referenced: &TableReference) -> bool {
        let declared = TableReference::parse_str(declared);
        if declared.table() != referenced.table() {
            return false;
        }
        match (declared.schema(), referenced.schema()) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        }
    }

    let mut closure: BTreeMap<String, String> = BTreeMap::new();
    let mut pending = dependencies_of(sql);
    let mut seen: HashSet<String> = HashSet::from([name.to_string()]);

    while let Some(dependency) = pending.pop() {
        let key = dependency.to_string();
        if !seen.insert(key.clone()) {
            continue;
        }

        for view in app
            .views
            .iter()
            .filter(|candidate| names_match(&candidate.name, &dependency))
        {
            // `sql_ref` names a FILE. Hashing the path would leave the fingerprint
            // unchanged when the file's contents change, which is precisely the
            // substitution this identity exists to catch, so read it — the same way
            // `ViewBuilder` does for the root view. An unreadable file is recorded as
            // such rather than skipped: skipping would silently restore the outer view's
            // old archive, whereas a marker simply fails the match and declines.
            let dependency_sql = match (&view.sql, &view.sql_ref) {
                (Some(inline), _) => Some(inline.clone()),
                (None, Some(path)) => Some(
                    std::fs::read_to_string(path)
                        .unwrap_or_else(|e| format!("-- unreadable sql_ref {path}: {e}")),
                ),
                (None, None) => None,
            };
            if let Some(dependency_sql) = dependency_sql {
                pending.extend(dependencies_of(&dependency_sql));
                // Keyed by the DECLARED name, so two views answering the same bare
                // reference occupy separate entries instead of overwriting each other.
                //
                // Carries the dependency's value-shaping configuration for the same reason
                // the root view's is carried: a view whose rows are read through another
                // view's archive can change its embedding model or file format without
                // touching a line of SQL.
                closure.insert(
                    view.name.clone(),
                    view_definition_identity(
                        &dependency_sql,
                        &view.columns,
                        &view
                            .params
                            .as_ref()
                            .map(spicepod::param::Params::as_string_map)
                            .unwrap_or_default(),
                    ),
                );
            }
        }

        // Datasets contribute too, and are folded in even when a view of the same name
        // already matched: a bare reference is resolved at planning time, so with a
        // `sales.inner` view and a `public.inner` dataset it is the DEFAULT-schema dataset
        // that `inner` reads. Fingerprinting only the view would leave the identity
        // unchanged when that dataset is rebound, which restores an archive the current
        // configuration no longer produces. Same reason every view candidate contributes
        // rather than the first.
        //
        // A dataset's own snapshot series validates ITS archives,
        // but it cannot speak for rows already baked into a view's archive: rebind
        // `orders` to a same-schema table and `SELECT * FROM orders` keeps identical SQL
        // while its materialized rows come from somewhere else entirely.
        for dataset in app
            .datasets
            .iter()
            .filter(|candidate| names_match(&candidate.name, &dependency))
        {
            closure.insert(
                dataset.name.clone(),
                dataset_definition_identity(&dataset.from, &dataset_identity_fields(dataset)),
            );
        }
    }

    // The root view contributes its own value-shaping configuration too, not just its SQL —
    // see `view_definition_identity`.
    let mut definition = String::new();
    push_len_prefixed(
        &mut definition,
        &view_definition_identity(sql, columns, params),
    );
    for (dependency_name, dependency_identity) in closure {
        definition.push_str("\n-- depends on ");
        push_len_prefixed(&mut definition, &dependency_name);
        definition.push('\n');
        // Already a `view_definition_identity` / `dataset_definition_identity`.
        // Trimming it would collapse a last field that differs only by
        // equal-length trailing whitespace (quoted YAML can preserve space vs tab).
        push_len_prefixed(&mut definition, &dependency_identity);
    }
    definition
}

/// Appends `value` as `<byte length>:<value>`, so a value can never be mistaken for the
/// field names and delimiters around it.
///
/// Concatenating raw values with separators is not injective: a dependency whose SQL
/// contains the literal `-- depends on` line, or a parameter whose value contains
/// `\nparam.k=v`, produces the same string as a genuinely different configuration and so
/// the same fingerprint — which is an incompatible archive accepted at bootstrap. A reader
/// of a length-prefixed string knows where every value ends without interpreting its
/// contents, so distinct inputs cannot collide.
fn push_len_prefixed(out: &mut String, value: &str) {
    use std::fmt::Write;
    // Writing into a `String` cannot fail.
    let _ = write!(out, "{}:{value}", value.len());
}

/// The definition string a DATASET's snapshot identity is computed over: what it copies
/// and everything that shapes which rows it copies.
///
/// Shared by the dataset's own identity and by a view's closure over a dataset it reads, so
/// the two cannot disagree about what "the same dataset definition" means.
///
/// `params` is included in full rather than filtered to a known row-shaping subset. Which
/// parameters select rows is connector-specific and unenumerable — `json_pointer` picks a
/// different element of the same document, `file_format` and `csv_has_header` reinterpret
/// the same bytes — and getting that list wrong in the permissive direction accepts an
/// archive whose rows answer a different question. Including a parameter that turns out not
/// to shape rows (a timeout, a pool size) only costs a refused snapshot and a refresh from
/// source.
///
/// These are the Spicepod parameters, so a `${secrets:...}` value is hashed as the
/// *reference*. Rotating a credential would leave the identity alone (the rows did not
/// change), but a row-shaping parameter read from a secret, `json_pointer:
/// ${secrets:pointer}`, can move from `/us` to `/eu` with the reference — and therefore
/// the identity — unchanged. This sync, secret-less path cannot see the resolved value
/// (`get_params_with_secrets` runs where the connector is constructed), so a
/// snapshot-enabled source whose identity `params` contain a `${ store:key }` reference
/// is refused at load rather than accepting a stamp that cannot see that change. See
/// [`first_unresolved_snapshot_identity_param`].
///
/// The cost of including params in full falls on SHARING a snapshot series between
/// deployments, and it is significant: two spiced instances that materialize identical rows
/// but reach the source differently — an IAM role in one and access keys in the other, a
/// different `endpoint` or pool size — compute different identities, so neither bootstraps
/// from the other's archive. Both still snapshot and both still bootstrap from their own
/// series; what they lose is the cross-deployment reuse that is much of the point. Narrowing
/// this safely means asking the connector which of ITS params select rows, rather than
/// guessing centrally from the key name, and defaulting to "all of them" where it does not
/// say — a capability method on `DataConnector` rather than a list maintained here.
///
/// `fields` carries everything else — see [`dataset_identity_fields`] and
/// [`dataset_identity_fields_from_spec`], the two ways it is built.
///
/// Ordered by key so the string does not depend on map iteration order.
#[must_use]
pub(crate) fn dataset_definition_identity(from: &str, fields: &BTreeMap<String, String>) -> String {
    let mut identity = String::from("from=");
    push_len_prefixed(&mut identity, from);
    for (key, value) in fields {
        identity.push('\n');
        push_len_prefixed(&mut identity, key);
        identity.push('=');
        push_len_prefixed(&mut identity, value);
    }
    identity
}

/// The identity fields of a dataset as DECLARED in a Spicepod.
///
/// Paired with [`dataset_identity_fields_from_spec`], which reads the same fields off the
/// parsed configuration; the two are kept adjacent, and
/// `spicepod_and_spec_identities_cover_the_same_fields` fails if one grows a field the
/// other does not. Each path only has to be internally consistent — the identity has to
/// CHANGE when the configuration changes — so the two need not agree byte for byte, but
/// they must cover the same configuration.
fn dataset_identity_fields(
    dataset: &spicepod::component::dataset::Dataset,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();

    let params = dataset
        .params
        .as_ref()
        .map(spicepod::param::Params::as_string_map)
        .unwrap_or_default();
    for (key, value) in params {
        fields.insert(format!("param.{key}"), value);
    }

    push_dataset_shape_fields(
        &mut fields,
        dataset.time_column.as_deref(),
        dataset.time_format.as_ref().map(|f| format!("{f:?}")),
        &dataset.columns,
        &dataset.embeddings,
    );

    if let Some(acceleration) = &dataset.acceleration {
        push_acceleration_row_policies(
            &mut fields,
            acceleration.refresh_sql.as_deref(),
            acceleration.refresh_mode.as_ref().map(|m| format!("{m:?}")),
            acceleration.refresh_data_window.as_deref(),
            acceleration.primary_key.as_deref().map(canonical_columns),
            acceleration
                .on_conflict
                .iter()
                .map(|(column, behavior)| (canonical_columns(column), format!("{behavior:?}")))
                .collect(),
            acceleration
                .indexes
                .iter()
                .map(|(column, index)| (canonical_columns(column), index.to_string()))
                .collect(),
            acceleration.retention_period.as_deref(),
            acceleration.retention_sql.as_deref(),
            acceleration.retention_check_enabled,
        );
    }

    fields
}

/// The definition string a VIEW's snapshot identity is computed over: its SQL plus the
/// configuration that shapes the VALUES its materialization stores.
///
/// SQL alone is not the whole definition. `prepare_view` builds embedding and full-text
/// columns from `columns`, and reads `file_format` out of `params`, so swapping an embedding
/// model for another of the same vector size changes every stored vector while leaving both
/// the SQL and the schema identical — and a bootstrap would serve the old model's vectors as
/// though they were the new model's. Same failure the dataset identity closes, on the path
/// that reaches it through a view instead.
///
/// Used for the root view and for every view in its dependency closure, so a dependency
/// cannot change its embedding configuration invisibly either.
fn view_definition_identity(
    sql: &str,
    columns: &[spicepod::semantic::Column],
    params: &HashMap<String, String>,
) -> String {
    let mut fields = BTreeMap::new();
    if !columns.is_empty() {
        fields.insert("columns".to_string(), identity_value(&columns));
    }
    for (key, value) in params {
        fields.insert(format!("param.{key}"), value.clone());
    }

    let mut identity = String::from("sql=");
    push_len_prefixed(&mut identity, sql.trim());
    for (key, value) in fields {
        identity.push('\n');
        push_len_prefixed(&mut identity, &key);
        identity.push('=');
        push_len_prefixed(&mut identity, &value);
    }
    identity
}

/// A dataset's identity computed from its parsed runtime configuration — the form the
/// dataset itself is registered from. A view reading this dataset arrives at the same
/// fields through [`dataset_identity_fields`] instead, off the Spicepod declaration.
#[must_use]
pub(crate) fn dataset_definition_identity_from_spec(
    spec: &crate::component::dataset::DatasetSpec,
) -> String {
    dataset_definition_identity(&spec.from, &dataset_identity_fields_from_spec(spec))
}

/// The identity fields of a dataset as PARSED into its runtime configuration. See
/// [`dataset_identity_fields`] for why both exist and what keeps them in step.
fn dataset_identity_fields_from_spec(
    spec: &crate::component::dataset::DatasetSpec,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();

    for (key, value) in &spec.params {
        fields.insert(format!("param.{key}"), value.clone());
    }

    push_dataset_shape_fields(
        &mut fields,
        spec.time_column.as_deref(),
        spec.time_format.as_ref().map(|f| format!("{f:?}")),
        &spec.columns,
        &spec.embeddings,
    );

    if let Some(acceleration) = &spec.acceleration {
        push_acceleration_row_policies(
            &mut fields,
            acceleration.refresh_sql.as_deref(),
            acceleration.refresh_mode.as_ref().map(|m| format!("{m:?}")),
            acceleration.refresh_data_window.as_deref(),
            acceleration
                .primary_key
                .as_ref()
                .map(|key| key.iter().collect::<Vec<_>>().join(",")),
            acceleration
                .on_conflict
                .iter()
                .map(|(column, behavior)| {
                    (
                        column.iter().collect::<Vec<_>>().join(","),
                        format!("{behavior:?}"),
                    )
                })
                .collect(),
            acceleration
                .indexes
                .iter()
                .map(|(column, index)| {
                    (
                        column.iter().collect::<Vec<_>>().join(","),
                        index.to_string(),
                    )
                })
                .collect(),
            acceleration.retention_period.as_deref(),
            acceleration.retention_sql.as_deref(),
            acceleration.retention_check_enabled,
        );
    }

    fields
}

/// A column reference in its parsed form, so `(a, b)`, `(a,b)` and `( a , b )` are one
/// identity rather than three.
fn canonical_columns(reference: &str) -> String {
    datafusion_table_providers::util::column_reference::ColumnReference::try_from(reference)
        .map_or_else(
            |_| reference.to_string(),
            |parsed| parsed.iter().collect::<Vec<_>>().join(","),
        )
}

/// Dataset-level configuration that changes which rows are stored or what they contain.
fn push_dataset_shape_fields(
    fields: &mut BTreeMap<String, String>,
    time_column: Option<&str>,
    time_format: Option<String>,
    columns: &[spicepod::semantic::Column],
    embeddings: &[ColumnEmbeddingConfig],
) {
    if let Some(time_column) = time_column {
        fields.insert("time_column".to_string(), time_column.to_string());
    }
    if let Some(time_format) = time_format {
        fields.insert("time_format".to_string(), time_format);
    }
    if !columns.is_empty() {
        fields.insert("columns".to_string(), identity_value(&columns));
    }
    if !embeddings.is_empty() {
        fields.insert("embeddings".to_string(), identity_value(&embeddings));
    }
}

/// Acceleration settings that decide which rows are stored and which survive a write.
///
/// Deliberately a subset: snapshot, scheduling, engine and storage settings decide WHEN and
/// WHERE an archive is written, not what is in it, so tuning them must not strand a series.
/// Everything that can change the stored row set or its values belongs here — a field
/// wrongly included costs a refused snapshot and a refresh from source, whereas one wrongly
/// left out accepts an archive whose rows answer a different question.
#[expect(clippy::too_many_arguments)]
fn push_acceleration_row_policies(
    fields: &mut BTreeMap<String, String>,
    refresh_sql: Option<&str>,
    refresh_mode: Option<String>,
    refresh_data_window: Option<&str>,
    primary_key: Option<String>,
    on_conflict: BTreeMap<String, String>,
    indexes: BTreeMap<String, String>,
    retention_period: Option<&str>,
    retention_sql: Option<&str>,
    retention_check_enabled: bool,
) {
    if let Some(refresh_sql) = refresh_sql {
        fields.insert(
            "acceleration.refresh_sql".to_string(),
            refresh_sql.trim().to_string(),
        );
    }
    if let Some(refresh_mode) = refresh_mode {
        fields.insert("acceleration.refresh_mode".to_string(), refresh_mode);
    }
    if let Some(window) = refresh_data_window {
        fields.insert(
            "acceleration.refresh_data_window".to_string(),
            window.to_string(),
        );
    }
    if let Some(primary_key) = primary_key {
        fields.insert("acceleration.primary_key".to_string(), primary_key);
    }
    for (column, behavior) in on_conflict {
        fields.insert(format!("acceleration.on_conflict.{column}"), behavior);
    }
    for (column, index) in indexes {
        fields.insert(format!("acceleration.index.{column}"), index);
    }
    if let Some(period) = retention_period {
        fields.insert(
            "acceleration.retention_period".to_string(),
            period.to_string(),
        );
    }
    if let Some(retention_sql) = retention_sql {
        fields.insert(
            "acceleration.retention_sql".to_string(),
            retention_sql.trim().to_string(),
        );
    }
    if retention_check_enabled {
        fields.insert(
            "acceleration.retention_check_enabled".to_string(),
            "true".to_string(),
        );
    }
}

/// Canonical JSON for a structured identity field.
///
/// Object keys are sorted at every nesting level. `serde_json` is compiled with
/// `preserve_order` in this crate, and `Column::metadata` is a `HashMap`, so
/// `serde_json::to_string` would otherwise follow the map's per-process iteration
/// order — a restart could compute a different fingerprint for the same Spicepod
/// and refuse its own snapshot.
///
/// A marker is recorded rather than the field dropped: dropping it would silently widen
/// what the identity accepts, which is the direction that restores a wrong archive.
fn identity_value<T: serde::Serialize + ?Sized>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(value) => serde_json::to_string(&canonicalize_json_object_keys(value))
            .unwrap_or_else(|e| format!("-- unserializable: {e}")),
        Err(e) => format!("-- unserializable: {e}"),
    }
}

fn canonicalize_json_object_keys(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(String, serde_json::Value)> = map
                .into_iter()
                .map(|(key, nested)| (key, canonicalize_json_object_keys(nested)))
                .collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            entries
                .into_iter()
                .collect::<serde_json::Map<_, _>>()
                .into()
        }
        serde_json::Value::Array(values) => serde_json::Value::Array(
            values
                .into_iter()
                .map(canonicalize_json_object_keys)
                .collect(),
        ),
        other => other,
    }
}

/// Stable hash of a definition string, recorded alongside a source's snapshots so a
/// bootstrap can refuse an archive materialized from a different definition. A schema
/// check cannot stand in for this: `SELECT a, b FROM t WHERE region = 'us'` and the same
/// query with `region = 'eu'` have identical schemas and completely different contents.
///
/// Shared by both source kinds so a dataset's identity (its `from:` plus `refresh_sql`)
/// and a view's (its definition closure) cannot drift into different hash schemes.
///
/// Hashed verbatim. SQL and refresh SQL are already normalized when their identity
/// fields are built. Trimming the finished string would collapse a length-prefixed
/// identity whose last value differs only by equal-length trailing whitespace —
/// quoted YAML can preserve a trailing space vs tab — so two incompatible
/// row-shaping configs would share a fingerprint.
#[must_use]
pub(crate) fn definition_fingerprint(definition: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(definition.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

/// A Spicepod `params` value that is a `${ store:key }` reference.
///
/// Snapshot identity hashes the reference, not the resolved value, so a row-shaping
/// parameter such as `json_pointer: ${secrets:pointer}` can move from `/us` to `/eu`
/// without changing the stamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnresolvedSnapshotIdentityParam {
    pub param: String,
    pub store: String,
    pub key: String,
}

/// The first identity `params` entry that is a secret or env reference, if any.
///
/// Uses the same `${ store:key }` grammar as `Spicepod` secret expansion
/// ([`runtime_secrets::iter_secret_references`]), including whitespace variants such as
/// `${ secrets:pointer }` and `${env:POINTER}`. Any matching store is refused — `secrets`,
/// `env`, and a user-defined store have the same hole. Keys are considered in sorted
/// order so the named parameter does not depend on `HashMap` iteration.
#[must_use]
pub(crate) fn first_unresolved_snapshot_identity_param(
    params: &HashMap<String, String>,
) -> Option<UnresolvedSnapshotIdentityParam> {
    params
        .iter()
        .filter_map(|(param, value)| {
            runtime_secrets::iter_secret_references(value)
                .next()
                .map(|reference| UnresolvedSnapshotIdentityParam {
                    param: param.clone(),
                    store: reference.store,
                    key: reference.key,
                })
        })
        .min_by(|left, right| left.param.cmp(&right.param))
}

/// User-facing refusal when snapshots are enabled and an identity param is a secret or
/// env reference. Built as a pure function so a reword cannot drop the resource, the
/// consequence, or the docs link.
#[must_use]
pub(crate) fn snapshot_identity_unresolved_param_message(
    component: &str,
    name: &str,
    param: &str,
    store: &str,
    key: &str,
) -> String {
    format!(
        "Failed to enable acceleration snapshots for {component} '{name}': its snapshot identity hashes Spicepod `params.{param}` as the `${{{store}:{key}}}` reference, so a change to the resolved value would not change the stamp and a cold start could restore rows that no longer match. Set a literal value for `params.{param}`, or set `snapshots: disabled`. See: https://spiceai.org/docs/components/data-accelerators/snapshots"
    )
}

pub(crate) fn get_dependent_table_names(statement: &parser::Statement) -> Vec<TableReference> {
    let mut table_names = Vec::new();
    let mut cte_names = HashSet::new();

    if let parser::Statement::Statement(statement) = statement.clone()
        && let ast::Statement::Query(statement) = *statement
    {
        // Collect names of CTEs
        if let Some(with) = statement.with {
            for table in with.cte_tables {
                cte_names.insert(TableReference::bare(table.alias.name.to_string()));
                let cte_table_names = get_dependent_table_names(&parser::Statement::Statement(
                    Box::new(ast::Statement::Query(table.query)),
                ));
                // Extend table_names with names found in CTEs if they reference actual tables
                table_names.extend(cte_table_names);
            }
        }
        // Extract table names from the main query
        table_names.extend(extract_tables_from_set_expr(&statement.body, &cte_names));
    }

    // Filter out CTEs and temporary views (aliases of subqueries)
    table_names
        .into_iter()
        .filter(|name| !cte_names.contains(name))
        .collect()
}

fn extract_tables_from_set_expr(
    expr: &ast::SetExpr,
    cte_names: &HashSet<TableReference>,
) -> Vec<TableReference> {
    match expr {
        ast::SetExpr::Select(select_statement) => {
            let mut table_names = vec![];
            for from in &select_statement.from {
                let mut relations = vec![from.relation.clone()];
                for join in &from.joins {
                    relations.push(join.relation.clone());
                }

                for relation in relations {
                    match relation {
                        ast::TableFactor::Table { name, .. } => {
                            let table_ref = name.to_string().into();
                            if !cte_names.contains(&table_ref) {
                                table_names.push(table_ref);
                            }
                        }
                        ast::TableFactor::Derived { subquery, .. } => {
                            table_names.extend(get_dependent_table_names(
                                &parser::Statement::Statement(Box::new(ast::Statement::Query(
                                    subquery,
                                ))),
                            ));
                        }
                        _ => {}
                    }
                }
            }
            table_names
        }
        ast::SetExpr::SetOperation { left, right, .. } => {
            let mut table_names = extract_tables_from_set_expr(left, cte_names);
            table_names.extend(extract_tables_from_set_expr(right, cte_names));
            table_names
        }
        _ => vec![],
    }
}

pub(crate) async fn prepare_view(
    ctx: &SessionContext,
    statement: &parser::Statement,
    view: &Arc<View>,
) -> Result<Arc<dyn TableProvider>> {
    let plan = ctx.state().statement_to_plan(statement.clone()).await?;
    let view_table = ViewTable::new(plan, Some(view.sql.to_string()));
    let mut tbl_provider = Arc::new(view_table) as Arc<dyn TableProvider>;

    // Add any embedding columns (and vector engine, if applicable)
    if view.has_embeddings() {
        let file_format = view.params.get("file_format").map(String::as_str);
        if let Some(ref vectors) = view.vectors
            && vectors.enabled
        {
            let on_zero_results = warm_index_on_zero_results(view.acceleration.as_ref());

            tbl_provider = wrap_table_as_index(
                &Arc::new(ctx.clone()),
                &view.runtime.embeds(),
                &view.runtime.secrets(),
                &view.name,
                &view.columns,
                file_format,
                tbl_provider,
                vectors,
                on_zero_results,
            )
            .await?;
        } else {
            tbl_provider = EmbeddingTable::from_spicepod_columns(
                tbl_provider,
                view.columns
                    .iter()
                    .flat_map(|col| {
                        col.embeddings.iter().map(|emb| ColumnEmbeddingConfig {
                            column: col.name.clone(),
                            model: emb.model.clone(),
                            primary_keys: emb.row_ids.clone(),
                            chunking: emb.chunking.clone(),
                            vector_size: emb.vector_size,
                            aggregation: emb.aggregation,
                            max_elements_per_row: emb.max_elements_per_row,
                        })
                    })
                    .collect(),
                &view.runtime.embeds(),
                file_format,
            )
            .await
            .boxed()
            .map_err(DataFusionError::External)?;
        }
    }

    // Configure full-text search
    if view.has_full_text_column() {
        tbl_provider =
            add_full_text_search_to_table(&tbl_provider, &view.columns, &view.name, false)?
                as Arc<dyn TableProvider>;
    }

    Ok(tbl_provider)
}

#[cfg(test)]
mod tests {
    use datafusion::sql::{parser::DFParser, sqlparser::dialect::PostgreSqlDialect};

    use super::*;

    #[test]
    fn identity_value_sorts_json_object_keys() {
        // This crate enables serde_json `preserve_order`, so two objects with the
        // same keys in different insertion order encode differently unless we
        // canonicalize. That is the `HashMap` metadata case: a 24-process
        // harness of `serde_json::to_string` on the same four entries produced
        // 13 encodings.
        let z_first: serde_json::Value =
            serde_json::from_str(r#"{"z":1,"a":{"m":2,"b":3}}"#).expect("parse z-first object");
        let a_first: serde_json::Value =
            serde_json::from_str(r#"{"a":{"b":3,"m":2},"z":1}"#).expect("parse a-first object");

        assert_ne!(
            serde_json::to_string(&z_first).expect("encode z-first"),
            serde_json::to_string(&a_first).expect("encode a-first"),
            "precondition: serde_json preserve_order must keep key insertion order"
        );
        assert_eq!(
            identity_value(&z_first),
            identity_value(&a_first),
            "the same object must have one identity regardless of key order"
        );
        assert_eq!(
            identity_value(&z_first),
            r#"{"a":{"b":3,"m":2},"z":1}"#,
            "object keys are sorted lexicographically at every nesting level"
        );
    }

    #[test]
    fn identity_value_preserves_array_order() {
        let first: serde_json::Value =
            serde_json::from_str(r#"[{"z":1},{"a":2}]"#).expect("parse first array");
        let swapped: serde_json::Value =
            serde_json::from_str(r#"[{"a":2},{"z":1}]"#).expect("parse swapped array");

        assert_ne!(
            identity_value(&first),
            identity_value(&swapped),
            "array order is part of the identity; only object keys are sorted"
        );
        assert_eq!(
            identity_value(&first),
            r#"[{"z":1},{"a":2}]"#,
            "objects inside arrays have sorted keys, but the array itself is not sorted"
        );
    }

    #[test]
    fn column_metadata_insertion_order_does_not_change_the_identity() {
        // Same four entries, opposite insertion order. `Column::metadata` is a
        // `HashMap`; without canonicalization, `serde_json::to_string` follows
        // that map's iteration order and a restart can refuse its own snapshot.
        let entries = [
            ("delta", serde_json::json!("4")),
            ("alpha", serde_json::json!("1")),
            ("gamma", serde_json::json!("3")),
            ("beta", serde_json::json!("2")),
        ];
        let mut first_metadata = HashMap::new();
        for (key, value) in &entries {
            first_metadata.insert((*key).to_string(), value.clone());
        }
        let mut second_metadata = HashMap::new();
        for (key, value) in entries.iter().rev() {
            second_metadata.insert((*key).to_string(), value.clone());
        }

        let first = spicepod::semantic::Column::new("body").with_metadata(first_metadata);
        let second = spicepod::semantic::Column::new("body").with_metadata(second_metadata);

        let first_identity = identity_value(std::slice::from_ref(&first));
        let second_identity = identity_value(std::slice::from_ref(&second));
        assert_eq!(
            first_identity, second_identity,
            "the same column metadata must have one identity regardless of HashMap insertion order"
        );
        assert!(
            first_identity
                .contains(r#""metadata":{"alpha":"1","beta":"2","delta":"4","gamma":"3"}"#),
            "metadata keys must be encoded in sorted order, got {first_identity}"
        );

        let fingerprint = |column: spicepod::semantic::Column| {
            definition_fingerprint(&view_definition_identity(
                "SELECT body FROM docs",
                &[column],
                &HashMap::new(),
            ))
        };
        assert_eq!(
            fingerprint(first),
            fingerprint(second),
            "a view fingerprint must not move when only metadata insertion order changes"
        );
    }

    mod read_shape {
        use super::*;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::context::SessionState;
        use datafusion::logical_expr::Extension;
        use datafusion::physical_plan::ExecutionPlan;
        use datafusion_federation::FederationPlanner;
        use runtime_table::accelerated::materialization::MaterializationSample;
        use std::sync::Arc;

        fn ctx_with_tables(names: &[&str]) -> SessionContext {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("region", DataType::Utf8, true),
            ]));
            for name in names {
                let table = MemTable::try_new(Arc::clone(&schema), vec![vec![]])
                    .expect("in-memory test table");
                ctx.register_table(*name, Arc::new(table))
                    .expect("register test table");
            }
            ctx
        }

        async fn shape_of(sql: &str, tables: &[&str]) -> ViewReadShape {
            let ctx = ctx_with_tables(tables);
            analyzed_view_read_shape(&ctx, sql)
                .await
                .expect("view SQL should plan against the registered test tables")
        }

        #[tokio::test]
        async fn single_table_select_is_one_read() {
            let shape = shape_of("SELECT id FROM orders", &["orders"]).await;
            assert!(
                shape.refusal_reason().is_none(),
                "a single-table select reads once: {shape:?}"
            );
            assert!(shape.refusal_reason().is_none());
        }

        #[tokio::test]
        async fn constant_select_has_no_reads_and_is_admissible() {
            // Zero reads cannot disagree with each other, so this must not be refused
            // just because the scan count is not exactly one.
            let shape = shape_of("SELECT 1 AS one", &[]).await;
            assert!(
                shape.refusal_reason().is_none(),
                "a constant view reads nothing: {shape:?}"
            );
        }

        #[tokio::test]
        async fn join_of_two_tables_is_refused() {
            let shape = shape_of(
                "SELECT o.id FROM orders o JOIN customers c ON o.id = c.id",
                &["orders", "customers"],
            )
            .await;
            assert!(shape.refusal_reason().is_some(), "{shape:?}");
            let reason = shape.refusal_reason().expect("a multi-read shape refuses");
            assert!(reason.contains("reads its sources 2 times"), "{reason}");
            assert!(reason.contains("'orders'"), "{reason}");
            assert!(reason.contains("'customers'"), "{reason}");
        }

        /// The case an AST-shaped check misses: one table name, two reads. Two scans of
        /// one table resolve two independent read views, so this is exactly as unsafe as
        /// joining two different tables.
        #[tokio::test]
        async fn self_join_of_one_table_is_refused() {
            let shape = shape_of(
                "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id",
                &["orders"],
            )
            .await;
            assert!(
                shape.refusal_reason().is_some(),
                "a self-join reads the table twice: {shape:?}"
            );
            assert!(matches!(
                shape,
                ViewReadShape::MultipleReads { reads: 2, .. }
            ));
        }

        /// A CTE referenced twice is materialized as two scans, so the same rule applies
        /// even though the SQL names the table once.
        #[tokio::test]
        async fn cte_referenced_twice_is_refused() {
            let shape = shape_of(
                "WITH o AS (SELECT id, region FROM orders) \
                 SELECT l.id FROM o l JOIN o r ON l.id = r.id",
                &["orders"],
            )
            .await;
            assert!(shape.refusal_reason().is_some(), "{shape:?}");
        }

        /// A `UNION ALL` of two filters over one table is two scans, not one.
        #[tokio::test]
        async fn union_over_one_table_is_refused() {
            let shape = shape_of(
                "SELECT id FROM orders WHERE region = 'us' \
                 UNION ALL SELECT id FROM orders WHERE region = 'eu'",
                &["orders"],
            )
            .await;
            assert!(shape.refusal_reason().is_some(), "{shape:?}");
        }

        /// The count has to come from the compiled plan: decorrelation rewrites this
        /// `IN (SELECT ...)` into a join, and both sides are real reads.
        #[tokio::test]
        async fn decorrelated_subquery_is_refused() {
            let shape = shape_of(
                "SELECT id FROM orders WHERE id IN (SELECT id FROM customers)",
                &["orders", "customers"],
            )
            .await;
            assert!(shape.refusal_reason().is_some(), "{shape:?}");
        }

        #[derive(Debug)]
        struct StubFederationPlanner;

        #[async_trait]
        impl FederationPlanner for StubFederationPlanner {
            async fn plan_federation(
                &self,
                _node: &FederatedPlanNode,
                _session_state: &SessionState,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                Err(DataFusionError::NotImplemented(
                    "stub federation planner is never executed".to_string(),
                ))
            }
        }

        /// A plan that collapsed entirely into one federated sub-plan is recognized as
        /// its own shape, not reported as a multi-read — `FederatedPlanNode::inputs()` is
        /// empty, so the scans inside it are invisible to an ordinary tree walk and have
        /// to be collected explicitly.
        #[tokio::test]
        async fn fully_federated_plan_is_its_own_shape() {
            let ctx = ctx_with_tables(&["orders", "customers"]);
            let inner = ctx
                .state()
                .create_logical_plan("SELECT o.id FROM orders o JOIN customers c ON o.id = c.id")
                .await
                .expect("inner plan");

            let federated = LogicalPlan::Extension(Extension {
                node: Arc::new(FederatedPlanNode::new(
                    inner,
                    Arc::new(StubFederationPlanner),
                )),
            });

            let shape = classify_view_read(&federated);
            match &shape {
                ViewReadShape::FederatedSingleStatement { tables } => {
                    assert_eq!(tables.len(), 2, "both scanned tables are reported");
                }
                other => panic!("expected FederatedSingleStatement, got {other:?}"),
            }

            // One federated sub-plan is one read, so it is admitted on the same footing as
            // a single local scan.
            assert!(
                shape.refusal_reason().is_none(),
                "a fully pushed-down plan reads once"
            );
        }

        #[tokio::test]
        async fn executed_single_scan_is_admissible() {
            let ctx = ctx_with_tables(&["orders"]);
            let df = ctx
                .sql("SELECT id FROM orders")
                .await
                .expect("single-table SQL");
            let plan = df.create_physical_plan().await.expect("physical plan");
            let shape = classify_executed_read(plan.as_ref());
            assert!(
                shape.refusal_reason().is_none(),
                "the executing plan of a single-table select reads once: {shape:?}"
            );
        }

        #[tokio::test]
        async fn executed_join_is_multiple_reads() {
            let ctx = ctx_with_tables(&["orders", "customers"]);
            let df = ctx
                .sql("SELECT o.id FROM orders o JOIN customers c ON o.id = c.id")
                .await
                .expect("join SQL");
            let plan = df.create_physical_plan().await.expect("physical plan");
            let shape = classify_executed_read(plan.as_ref());
            assert!(
                matches!(shape, ViewReadShape::MultipleReads { reads: 2, .. }),
                "the executing plan of a two-table join must be two reads: {shape:?}"
            );
        }

        #[tokio::test]
        async fn publish_gate_uses_refresh_attestation_not_a_replanned_query() {
            let attestation =
                ViewRefreshReadAttestation::with_identity(MaterializationIdentity::new());
            let gate = ViewSnapshotPublishGate::new(
                TableReference::bare("orders_us"),
                attestation.clone(),
            );

            let missing = gate
                .check_publish()
                .await
                .expect_err("publish without a refresh attestation must refuse");
            assert!(missing.contains("no refresh-plan attestation"), "{missing}");

            attestation.record(ViewReadShape::MultipleReads {
                reads: 2,
                tables: vec![TableReference::bare("orders")],
            });
            let refused = gate
                .check_publish()
                .await
                .expect_err("a multi-read executing plan must not publish");
            assert!(refused.contains("reads its sources 2 times"), "{refused}");

            attestation.record(ViewReadShape::SingleScan {
                tables: vec![TableReference::bare("orders")],
            });
            gate.check_publish()
                .await
                .expect("a single-read executing-plan attestation may publish");
        }

        /// Sequential model of Copilot `discussion_r4010927556`.
        ///
        /// A snapshot holds the write mutex and samples `configured = true` at
        /// epoch N. A new refresh then begins (retracts provenance, epoch N+1)
        /// and records a single-read attestation while blocked from writing.
        /// The gate must refuse: that plan is not the plan that produced these
        /// rows. Without the epoch, `check_publish` would approve the old
        /// multi-read rows under the new single-read shape.
        #[tokio::test]
        async fn publish_gate_refuses_a_newer_attestation_than_the_sampled_rows() {
            let identity = MaterializationIdentity::new();
            let attestation = ViewRefreshReadAttestation::with_identity(identity.clone());

            let epoch_n = identity.begin_refresh();
            attestation.record(ViewReadShape::MultipleReads {
                reads: 2,
                tables: vec![TableReference::bare("orders")],
            });
            identity.set_configured(true);

            let sampled = identity.sample();
            assert!(sampled.configured, "precondition: rows are configured");
            assert_eq!(sampled.epoch, epoch_n);

            let gate = ViewSnapshotPublishGate::new(
                TableReference::bare("orders_us"),
                attestation.clone(),
            );
            gate.bind_materialization_epoch(sampled.epoch);

            let epoch_next = identity.begin_refresh();
            assert_eq!(epoch_next, epoch_n + 1);
            assert!(
                !identity.is_configured(),
                "dequeue must retract configured on the new epoch"
            );
            attestation.record(ViewReadShape::SingleScan {
                tables: vec![TableReference::bare("orders")],
            });
            assert_eq!(
                attestation.last_stamped().map(|(epoch, _)| epoch),
                Some(epoch_next),
                "the in-flight refresh stamps the new epoch"
            );

            let refused = gate
                .check_publish()
                .await
                .expect_err("old rows must not publish under a later refresh's attestation");
            assert!(
                refused.contains("later materialization"),
                "refusal must name the epoch mismatch, got {refused}"
            );
            assert!(
                refused.contains("orders_us"),
                "refusal must name the view, got {refused}"
            );
        }

        #[tokio::test]
        async fn publish_gate_accepts_attestation_from_the_sampled_epoch() {
            let identity = MaterializationIdentity::new();
            let attestation = ViewRefreshReadAttestation::with_identity(identity.clone());
            let epoch = identity.begin_refresh();
            attestation.record(ViewReadShape::SingleScan {
                tables: vec![TableReference::bare("orders")],
            });
            identity.set_configured(true);

            let gate = ViewSnapshotPublishGate::new(TableReference::bare("orders_us"), attestation);
            gate.bind_materialization_epoch(epoch);
            gate.check_publish()
                .await
                .expect("matching epoch and a single-read plan must publish");
        }

        #[test]
        fn attestation_epoch_mismatch_names_the_view() {
            let name = TableReference::bare("orders_us");
            let message = attestation_epoch_mismatch_reason(&name);
            assert!(
                message.contains("view 'orders_us'"),
                "must name the view, got {message}"
            );
            assert!(
                message.contains("later materialization"),
                "must state the epoch mismatch, got {message}"
            );
            assert!(
                message.contains("single consistent read"),
                "must state the impact, got {message}"
            );
        }

        #[tokio::test]
        async fn attesting_provider_records_the_executed_scan_shape() {
            let ctx = ctx_with_tables(&["orders", "customers"]);
            let logical = ctx
                .state()
                .create_logical_plan("SELECT o.id FROM orders o JOIN customers c ON o.id = c.id")
                .await
                .expect("logical plan");
            let view_table = ViewTable::new(logical, Some("join view".to_string()));
            let attestation =
                ViewRefreshReadAttestation::with_identity(MaterializationIdentity::new());
            let wrapped = wrap_view_refresh_attestation(Arc::new(view_table), attestation.clone());

            let mut state = ctx.state();
            runtime_datafusion::refresh_scan::mark_refresh_scan(&mut state);
            let _plan = wrapped
                .scan(&state, None, &[], None)
                .await
                .expect("view scan");

            let shape = attestation
                .last_stamped()
                .map(|(_, shape)| shape)
                .expect("refresh scan records the executing-plan attestation");
            assert!(
                matches!(shape, ViewReadShape::MultipleReads { .. }),
                "the wrapper must attest the executed join as a multi-read: {shape:?}"
            );
        }

        #[tokio::test]
        async fn attesting_provider_ignores_scans_outside_refresh() {
            let ctx = ctx_with_tables(&["orders", "customers"]);
            let logical = ctx
                .state()
                .create_logical_plan("SELECT o.id FROM orders o JOIN customers c ON o.id = c.id")
                .await
                .expect("logical plan");
            let view_table = ViewTable::new(logical, Some("join view".to_string()));
            let attestation =
                ViewRefreshReadAttestation::with_identity(MaterializationIdentity::new());
            let wrapped = wrap_view_refresh_attestation(Arc::new(view_table), attestation.clone());

            let _plan = wrapped
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("ordinary query scan");

            assert!(
                attestation.last_stamped().is_none(),
                "a non-refresh scan must not write publish-gate attestation"
            );
        }

        /// Sequential model of Copilot `discussion_r4011722534`.
        ///
        /// A refresh at epoch N produced a multi-read materialization and the
        /// snapshot path sampled `(configured=true, epoch=N)`. An ordinary
        /// query with `on_zero_results: use_source` then scans the wrapped
        /// federated view without advancing the epoch. That scan's plan can
        /// be a single read; it must not replace `MultipleReads` with
        /// `SingleScan`. The publish gate must still refuse.
        #[tokio::test]
        async fn fallback_scan_cannot_replace_refresh_attestation_at_the_same_epoch() {
            let identity = MaterializationIdentity::new();
            let attestation = ViewRefreshReadAttestation::with_identity(identity.clone());

            let epoch = identity.begin_refresh();
            attestation.record(ViewReadShape::MultipleReads {
                reads: 2,
                tables: vec![
                    TableReference::bare("orders"),
                    TableReference::bare("customers"),
                ],
            });
            identity.set_configured(true);

            let sampled = identity.sample();
            assert!(sampled.configured, "precondition: rows are configured");
            assert_eq!(sampled.epoch, epoch);

            let gate = ViewSnapshotPublishGate::new(
                TableReference::bare("orders_us"),
                attestation.clone(),
            );
            gate.bind_materialization_epoch(sampled.epoch);

            let before = publish_gate_harness_state(&gate, &sampled).await;
            eprintln!("before_fallback={before}");
            assert_eq!(
                before, "refused: multiple reads",
                "precondition: multi-read refresh must refuse"
            );

            // Ordinary query / `on_zero_results: use_source`: scan the same
            // attestation through a single-table plan with no refresh marker.
            // Ungated, `classify_executed_read` would record `SingleScan` at
            // epoch N and the gate would publish.
            let ctx = ctx_with_tables(&["orders"]);
            let logical = ctx
                .state()
                .create_logical_plan("SELECT id FROM orders")
                .await
                .expect("logical plan");
            let view_table = ViewTable::new(logical, Some("fallback scan".to_string()));
            let wrapped = wrap_view_refresh_attestation(Arc::new(view_table), attestation.clone());
            let _plan = wrapped
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("fallback scan");

            let after = publish_gate_harness_state(&gate, &sampled).await;
            eprintln!("after_fallback={after}");
            assert_eq!(
                after, "refused: multiple reads",
                "fallback must not flip MultipleReads→SingleScan at epoch {epoch}"
            );
            assert_eq!(
                attestation.last_stamped().map(|(stamped_epoch, shape)| {
                    (
                        stamped_epoch,
                        matches!(shape, ViewReadShape::MultipleReads { reads: 2, .. }),
                    )
                }),
                Some((epoch, true)),
                "refresh attestation must survive the fallback scan"
            );
            assert_eq!(
                identity.sample(),
                sampled,
                "epoch+configured publish gating must stay intact"
            );
        }

        async fn publish_gate_harness_state(
            gate: &ViewSnapshotPublishGate,
            sample: &MaterializationSample,
        ) -> String {
            match gate.check_publish().await {
                Ok(()) => format!(
                    "allowed configured={} epoch={}",
                    sample.configured, sample.epoch
                ),
                Err(reason) => {
                    let kind = if reason.contains("reads its sources") {
                        "multiple reads"
                    } else if reason.contains("no refresh-plan attestation") {
                        "missing attestation"
                    } else if reason.contains("later materialization") {
                        "epoch mismatch"
                    } else {
                        "other"
                    };
                    format!("refused: {kind}")
                }
            }
        }

        /// Build an app holding one view and one dataset, to exercise the closure.
        fn app_with(views: &[(&str, &str)], datasets: &[(&str, &str)]) -> app::App {
            let mut builder = app::AppBuilder::new("closure_test");
            for (name, sql) in views {
                let mut view = spicepod::component::view::View::new((*name).to_string());
                view.sql = Some((*sql).to_string());
                builder = builder.with_view(view);
            }
            for (name, from) in datasets {
                builder = builder.with_dataset(spicepod::component::dataset::Dataset::new(
                    (*from).to_string(),
                    (*name).to_string(),
                ));
            }
            builder.build()
        }

        /// A view's rows are the result of its dependencies, so changing a view it reads
        /// must change its identity even though its own SQL is byte-identical.
        #[test]
        fn closure_follows_a_dependency_view() {
            let outer = TableReference::bare("outer");
            let us = view_definition_closure(
                &outer,
                "SELECT * FROM inner",
                &[],
                &HashMap::new(),
                &app_with(
                    &[("inner", "SELECT id FROM orders WHERE region = 'us'")],
                    &[],
                ),
            );
            let eu = view_definition_closure(
                &outer,
                "SELECT * FROM inner",
                &[],
                &HashMap::new(),
                &app_with(
                    &[("inner", "SELECT id FROM orders WHERE region = 'eu'")],
                    &[],
                ),
            );
            assert_ne!(
                definition_fingerprint(&us),
                definition_fingerprint(&eu),
                "the outer view's identity must follow the inner view's definition"
            );
        }

        /// A dataset dependency counts too: the view's archive embeds the dataset's rows,
        /// and the dataset's own snapshot series cannot vouch for rows already baked in.
        #[test]
        fn closure_follows_a_dependency_dataset() {
            let v = TableReference::bare("v");
            let old = view_definition_closure(
                &v,
                "SELECT * FROM orders",
                &[],
                &HashMap::new(),
                &app_with(&[], &[("orders", "postgres:old_orders")]),
            );
            let new = view_definition_closure(
                &v,
                "SELECT * FROM orders",
                &[],
                &HashMap::new(),
                &app_with(&[], &[("orders", "postgres:new_orders")]),
            );
            assert_ne!(
                definition_fingerprint(&old),
                definition_fingerprint(&new),
                "rebinding a dataset the view reads must change the view's identity"
            );
        }

        /// A dependency reached only through an expression subquery still counts: the outer
        /// SQL is unchanged when the inner view's definition changes, so missing it would
        /// accept an archive of the rows that view used to produce.
        #[test]
        fn closure_follows_a_dependency_in_an_expression_subquery() {
            let outer = TableReference::bare("outer");
            let us = view_definition_closure(
                &outer,
                "SELECT id FROM orders WHERE id IN (SELECT id FROM inner)",
                &[],
                &HashMap::new(),
                &app_with(&[("inner", "SELECT id FROM t WHERE region = 'us'")], &[]),
            );
            let eu = view_definition_closure(
                &outer,
                "SELECT id FROM orders WHERE id IN (SELECT id FROM inner)",
                &[],
                &HashMap::new(),
                &app_with(&[("inner", "SELECT id FROM t WHERE region = 'eu'")], &[]),
            );
            assert_ne!(
                definition_fingerprint(&us),
                definition_fingerprint(&eu),
                "a view read only from inside a subquery is still a dependency"
            );
        }

        /// A bare reference that two declarations answer to folds in BOTH, because planning
        /// resolves it through a default catalog/schema this code cannot see. Picking one
        /// would leave the fingerprint unchanged whenever the other is the one being read.
        #[test]
        fn closure_folds_in_every_candidate_for_a_bare_reference() {
            let outer = TableReference::bare("outer");
            let both = view_definition_closure(
                &outer,
                "SELECT * FROM inner",
                &[],
                &HashMap::new(),
                &app_with(
                    &[("public.inner", "SELECT 1"), ("sales.inner", "SELECT 2")],
                    &[],
                ),
            );
            assert!(
                both.contains("SELECT 1") && both.contains("SELECT 2"),
                "both declarations must contribute: {both}"
            );
        }

        /// A Spicepod may declare `public.inner` while the SQL says `inner`. Missing that
        /// match would drop the dependency and accept an archive of the old definition.
        #[test]
        fn closure_matches_a_qualified_declaration() {
            let outer = TableReference::bare("outer");
            let matched = view_definition_closure(
                &outer,
                "SELECT * FROM inner",
                &[],
                &HashMap::new(),
                &app_with(&[("public.inner", "SELECT 1")], &[]),
            );
            assert!(
                matched.contains("SELECT 1"),
                "a qualified declaration must still match a bare reference: {matched}"
            );
        }

        #[test]
        fn fingerprint_tracks_the_definition_not_the_schema() {
            // Same schema, different rows — the case a schema check cannot catch.
            let us = definition_fingerprint("SELECT id FROM orders WHERE region = 'us'");
            let eu = definition_fingerprint("SELECT id FROM orders WHERE region = 'eu'");
            assert_ne!(us, eu);

            // YAML block scalars add surrounding whitespace to SQL. That is
            // normalized when the identity is built, not by hashing a trimmed
            // identity string.
            assert_eq!(
                definition_fingerprint(&view_definition_identity(
                    "SELECT id FROM orders",
                    &[],
                    &HashMap::new(),
                )),
                definition_fingerprint(&view_definition_identity(
                    "  SELECT id FROM orders\n",
                    &[],
                    &HashMap::new(),
                )),
            );
        }

        #[test]
        fn fingerprint_distinguishes_equal_length_trailing_whitespace() {
            // Length-prefixed identity: the last field's trailing byte is part of
            // the value. Quoted YAML can preserve a trailing space vs tab of the
            // same length; hashing a trimmed identity would collapse them.
            let with_space = dataset_definition_identity(
                "s3://bucket",
                &BTreeMap::from([("param.selector".to_string(), "x ".to_string())]),
            );
            let with_tab = dataset_definition_identity(
                "s3://bucket",
                &BTreeMap::from([("param.selector".to_string(), "x\t".to_string())]),
            );
            assert_eq!(
                with_space.len(),
                with_tab.len(),
                "precondition: encodings must be the same length so only the trailing byte differs"
            );
            assert_ne!(
                definition_fingerprint(&with_space),
                definition_fingerprint(&with_tab),
                "a trailing space vs tab in a quoted parameter must not share a fingerprint"
            );
        }

        #[test]
        fn closure_fingerprint_follows_dependency_param_trailing_whitespace() {
            // The nested identity is already canonical. Re-trimming it when
            // folding into the outer closure collapses a dependency param that
            // differs only by a trailing space vs tab.
            let outer = TableReference::bare("outer");
            let closure_for = |selector: &str| {
                let mut inner = spicepod::component::view::View::new("inner".to_string());
                inner.sql = Some("SELECT body FROM docs".to_string());
                inner.params = Some(spicepod::param::Params::from_string_map(HashMap::from([(
                    "selector".to_string(),
                    selector.to_string(),
                )])));
                let app = app::AppBuilder::new("closure_test")
                    .with_view(inner)
                    .build();
                view_definition_closure(&outer, "SELECT * FROM inner", &[], &HashMap::new(), &app)
            };
            let with_space = closure_for("x ");
            let with_tab = closure_for("x\t");
            assert_ne!(
                definition_fingerprint(&with_space),
                definition_fingerprint(&with_tab),
                "a dependency view param that differs only by trailing space vs tab must change the outer fingerprint"
            );
        }

        #[test]
        fn closure_folds_in_a_dataset_shadowed_by_a_same_named_view() {
            // A bare reference resolves at planning time, and with a qualified view and a
            // default-schema dataset both answering `inner` it is the DATASET that is read.
            // Folding in only the view would leave the identity unchanged when the dataset
            // is rebound, restoring an archive the configuration no longer produces.
            let outer = TableReference::bare("outer");
            let closure = view_definition_closure(
                &outer,
                "SELECT * FROM inner",
                &[],
                &HashMap::new(),
                &app_with(
                    &[("sales.inner", "SELECT 2")],
                    &[("public.inner", "s3://a")],
                ),
            );
            assert!(
                closure.contains("SELECT 2"),
                "the view candidate must contribute: {closure}"
            );
            assert!(
                closure.contains("s3://a"),
                "the dataset candidate must contribute even though a view matched: {closure}"
            );
        }

        #[test]
        fn rebinding_a_shadowed_dataset_changes_the_fingerprint() {
            let outer = TableReference::bare("outer");
            let before = view_definition_closure(
                &outer,
                "SELECT * FROM inner",
                &[],
                &HashMap::new(),
                &app_with(
                    &[("sales.inner", "SELECT 2")],
                    &[("public.inner", "s3://a")],
                ),
            );
            let after = view_definition_closure(
                &outer,
                "SELECT * FROM inner",
                &[],
                &HashMap::new(),
                &app_with(
                    &[("sales.inner", "SELECT 2")],
                    &[("public.inner", "s3://b")],
                ),
            );
            assert_ne!(
                definition_fingerprint(&before),
                definition_fingerprint(&after),
                "rebinding the dataset a bare reference resolves to must move the identity"
            );
        }

        /// A dataset carrying `params`, used to exercise the identity encoding.
        fn dataset_with_params(params: &[(&str, &str)]) -> spicepod::component::dataset::Dataset {
            let mut dataset = spicepod::component::dataset::Dataset::new(
                "s3://bucket/data".to_string(),
                "data".to_string(),
            );
            dataset.params = Some(spicepod::param::Params::from_string_map(
                params
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            ));
            dataset
        }

        #[test]
        fn a_parameter_value_cannot_forge_another_parameter() {
            // Concatenating `\nparam.<key>=<value>` is not injective: this one value spells
            // out a second parameter. Length-prefixing every field is what separates them.
            let forged = dataset_with_params(&[("a", "b\nparam.c=d")]);
            let genuine = dataset_with_params(&[("a", "b"), ("c", "d")]);
            assert_ne!(
                definition_fingerprint(&dataset_definition_identity(
                    &forged.from,
                    &dataset_identity_fields(&forged)
                )),
                definition_fingerprint(&dataset_definition_identity(
                    &genuine.from,
                    &dataset_identity_fields(&genuine)
                )),
                "a parameter value must not be able to spell another parameter"
            );
        }

        #[test]
        fn row_shaping_acceleration_policies_move_the_identity() {
            // Each of these changes which rows are stored, or which survive a write, while
            // leaving the schema — and the schema is all the snapshot metadata used to
            // record. An archive taken under one must not bootstrap under another.
            fn identity_with(acceleration: spicepod::acceleration::Acceleration) -> String {
                let mut dataset = dataset_with_params(&[("a", "b")]);
                dataset.acceleration = Some(acceleration);
                dataset_definition_identity(&dataset.from, &dataset_identity_fields(&dataset))
            }
            fn accelerated() -> spicepod::acceleration::Acceleration {
                spicepod::acceleration::Acceleration {
                    enabled: true,
                    ..Default::default()
                }
            }

            let baseline = identity_with(accelerated());

            let mut primary_key = accelerated();
            primary_key.primary_key = Some("id".to_string());
            assert_ne!(
                baseline,
                identity_with(primary_key),
                "primary_key decides which rows an upsert replaces"
            );

            let mut retention_period = accelerated();
            retention_period.retention_period = Some("1d".to_string());
            assert_ne!(
                baseline,
                identity_with(retention_period),
                "retention_period prunes stored rows"
            );

            let mut retention_sql = accelerated();
            retention_sql.retention_sql = Some("SELECT 1".to_string());
            assert_ne!(
                baseline,
                identity_with(retention_sql),
                "retention_sql chooses which rows are pruned"
            );

            let mut window = accelerated();
            window.refresh_data_window = Some("1h".to_string());
            assert_ne!(
                baseline,
                identity_with(window),
                "refresh_data_window bounds which rows are loaded"
            );

            let mut on_conflict = accelerated();
            on_conflict.on_conflict = std::collections::HashMap::from([(
                "id".to_string(),
                spicepod::acceleration::OnConflictBehavior::Drop,
            )]);
            assert_ne!(
                baseline,
                identity_with(on_conflict),
                "on_conflict decides which of two colliding rows is kept"
            );
        }

        #[test]
        fn swapping_a_views_embedding_model_moves_the_fingerprint() {
            // `prepare_view` builds the embedding columns, so the model that produced the
            // stored vectors is part of the definition even though it appears nowhere in
            // the SQL. Two models of the same vector size leave the SQL and the schema
            // identical, so without this the archive of one bootstraps as the other's.
            let view = TableReference::bare("docs_view");
            let column_with_model = |model: &str| {
                vec![spicepod::semantic::Column {
                    name: "body".to_string(),
                    description: None,
                    r#type: None,
                    nullable: None,
                    embeddings: vec![spicepod::semantic::ColumnLevelEmbeddingConfig::model(model)],
                    full_text_search: None,
                    metadata: std::collections::HashMap::default(),
                }]
            };
            let closure_for = |model: &str| {
                view_definition_closure(
                    &view,
                    "SELECT body FROM docs",
                    &column_with_model(model),
                    &HashMap::new(),
                    &app_with(&[], &[("docs", "s3://docs")]),
                )
            };
            assert_ne!(
                definition_fingerprint(&closure_for("model-a")),
                definition_fingerprint(&closure_for("model-b")),
                "the embedding model that produced the stored vectors is part of the definition"
            );
        }

        #[test]
        fn a_views_params_are_part_of_its_definition() {
            // `prepare_view` reads `file_format` out of `params`, which reinterprets the
            // same bytes — the view analogue of the dataset params rationale.
            let view = TableReference::bare("docs_view");
            let closure_for = |format: &str| {
                view_definition_closure(
                    &view,
                    "SELECT body FROM docs",
                    &[],
                    &HashMap::from([("file_format".to_string(), format.to_string())]),
                    &app_with(&[], &[("docs", "s3://docs")]),
                )
            };
            assert_ne!(
                definition_fingerprint(&closure_for("csv")),
                definition_fingerprint(&closure_for("parquet")),
                "a param that reinterprets the source bytes must move the identity"
            );
        }

        #[test]
        fn a_column_reference_is_identified_by_its_columns_not_its_spelling() {
            // Whitespace and column order are spelling: a column reference is sorted when
            // it is parsed, so `(a, b)` and `(b, a)` are one compound key and must not read
            // as two different configurations that refuse each other's archives.
            assert_eq!(canonical_columns("(a, b)"), canonical_columns("(a,b)"));
            assert_eq!(canonical_columns("(a, b)"), canonical_columns("(b, a)"));

            // A different set of columns is a different key.
            assert_ne!(canonical_columns("(a, b)"), canonical_columns("(a, c)"));
            assert_ne!(canonical_columns("a"), canonical_columns("(a, b)"));
        }

        #[test]
        fn swapping_column_metadata_moves_the_fingerprint() {
            let view = TableReference::bare("docs_view");
            let closure_for = |pairs: &[(&str, i64)]| {
                let metadata = pairs
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), serde_json::json!(*value)))
                    .collect();
                view_definition_closure(
                    &view,
                    "SELECT body FROM docs",
                    &[spicepod::semantic::Column::new("body").with_metadata(metadata)],
                    &HashMap::new(),
                    &app_with(&[], &[("docs", "s3://docs")]),
                )
            };
            assert_ne!(
                definition_fingerprint(&closure_for(&[("alpha", 1), ("beta", 2)])),
                definition_fingerprint(&closure_for(&[("alpha", 1), ("beta", 3)])),
                "column metadata that can change stored values is part of the definition"
            );
        }
    }

    #[test]
    fn literal_identity_params_are_not_unresolved_snapshot_refs() {
        let params = HashMap::from([
            ("json_pointer".to_string(), "/us".to_string()),
            ("file_format".to_string(), "parquet".to_string()),
        ]);
        assert!(
            first_unresolved_snapshot_identity_param(&params).is_none(),
            "a literal row-shaping param is visible to the stamp and must not refuse snapshots"
        );
    }

    /// Conservative refuse for Copilot `discussion_r4012511570`: identity hashes
    /// the Spicepod reference, so a row-shaping secret can change without the stamp.
    #[test]
    fn secret_ref_in_a_row_shaping_param_is_an_unresolved_snapshot_ref() {
        let params =
            HashMap::from([("json_pointer".to_string(), "${secrets:pointer}".to_string())]);
        let unresolved = first_unresolved_snapshot_identity_param(&params)
            .expect("a ${secrets:...} row-shaping param must be detected");
        assert_eq!(unresolved.param, "json_pointer");
        assert_eq!(unresolved.store, "secrets");
        assert_eq!(unresolved.key, "pointer");
    }

    #[test]
    fn env_ref_in_a_row_shaping_param_is_an_unresolved_snapshot_ref() {
        let params = HashMap::from([("json_pointer".to_string(), "${env:POINTER}".to_string())]);
        let unresolved = first_unresolved_snapshot_identity_param(&params)
            .expect("a ${env:...} row-shaping param must be detected");
        assert_eq!(unresolved.param, "json_pointer");
        assert_eq!(unresolved.store, "env");
        assert_eq!(unresolved.key, "POINTER");
    }

    #[test]
    fn spaced_secret_ref_matches_the_spicepod_grammar() {
        let params = HashMap::from([(
            "json_pointer".to_string(),
            "${ secrets:pointer }".to_string(),
        )]);
        let unresolved = first_unresolved_snapshot_identity_param(&params)
            .expect("whitespace inside ${ store:key } is still a secret reference");
        assert_eq!(unresolved.store, "secrets");
        assert_eq!(unresolved.key, "pointer");
    }

    #[test]
    fn snapshot_identity_unresolved_param_refusal_names_the_view_and_a_way_out() {
        let message = snapshot_identity_unresolved_param_message(
            "view",
            "orders_us",
            "json_pointer",
            "secrets",
            "pointer",
        );
        for expected in [
            "view",
            "'orders_us'",
            "`params.json_pointer`",
            "${secrets:pointer}",
            "would not change the stamp",
            "cold start could restore rows",
            "`snapshots: disabled`",
            "https://spiceai.org/docs/components/data-accelerators/snapshots",
        ] {
            assert!(
                message.contains(expected),
                "the refusal must contain {expected:?}: {message}"
            );
        }
        assert!(
            !message.contains('\n'),
            "a user-facing refusal must stay on one line: {message:?}"
        );
    }

    #[test]
    fn snapshot_identity_unresolved_param_refusal_names_the_dataset() {
        let message = snapshot_identity_unresolved_param_message(
            "dataset",
            "docs",
            "json_pointer",
            "env",
            "POINTER",
        );
        assert!(
            message.contains("dataset")
                && message.contains("'docs'")
                && message.contains("${env:POINTER}"),
            "the dataset refusal must name the component and the env reference: {message}"
        );
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_simple_query() {
        let sql = r"
            SELECT a, b FROM employees limit 10;
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<_> = vec![TableReference::bare("employees".to_string())]
            .into_iter()
            .collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_schema() {
        let sql = r"
            SELECT a, b FROM dbo.employees limit 10;
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["dbo.employees".into()].into_iter().collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_joins() {
        let sql = r"
            SELECT e.name, d.department_name
            FROM employees e
            JOIN departments d ON e.department_id = d.id
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["employees".into(), "departments".into()]
                .into_iter()
                .collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_cte_and_join() {
        let sql = r"
            WITH tmp AS (
                SELECT * FROM t1
            )
            SELECT tmp.id, t2.name
            FROM tmp
            JOIN t2 ON tmp.id = t2.id;
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["t1".into(), "t2".into()].into_iter().collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_cte_and_union() {
        let sql = r"
            WITH all_sales AS (
                SELECT sales FROM s3_source
                UNION ALL
                SELECT fare_amount + tip_amount AS sales FROM dremio_source
            )
            SELECT SUM(sales) AS total_sales,
                   COUNT(*) AS total_transactions,
                   MAX(sales) AS max_sale,
                   AVG(sales) AS avg_sale
            FROM all_sales;
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["s3_source".into(), "dremio_source".into()]
                .into_iter()
                .collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_nested_subqueries() {
        let sql = r"
            SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM orders
                ) AS subquery1
            ) AS subquery2
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["orders".into()].into_iter().collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    fn extract_table_names_from_sql(sql: &str) -> HashSet<TableReference> {
        let statements =
            DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {}).expect("to parse sql");
        assert_eq!(statements.len(), 1);

        let table_names = get_dependent_table_names(&statements[0]);
        table_names.into_iter().collect()
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_cte_and_multiple_queries() {
        let sql = r"
            WITH cte1 AS (
                SELECT * FROM table1
            ), cte2 AS (
                SELECT * FROM table2
            )
            SELECT * FROM cte1
            UNION ALL
            SELECT * FROM cte2
            UNION
            SELECT * FROM table3
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["table1".into(), "table2".into(), "table3".into()]
                .into_iter()
                .collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_set_operations() {
        let sql = r"
            SELECT * FROM table1
            UNION
            SELECT * FROM table2
            INTERSECT
            SELECT * FROM table3
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["table1".into(), "table2".into(), "table3".into()]
                .into_iter()
                .collect();

        assert_eq!(expected_table_names, actual_table_names);
    }
}
