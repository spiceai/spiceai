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

//! Reusable real-provider fixtures and evidence capture for covering indexes.
//!
//! The harness deliberately keeps its baseline selector outside Cayenne. A
//! future covering implementation can use the indexed pair while the identical
//! unindexed pair remains a stable ordinary Vortex/HashJoin reference path.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int32Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use cayenne::lookup_index::LookupIndexCounters;
use cayenne::metadata::{
    CdcDurability, CreateTableOptions, DeletionMode, ObjectStoreConfig, VortexConfig,
};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_physical_plan::metrics::{MetricValue, MetricsSet};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use parking_lot::Mutex;
use vortex_datafusion::metrics::VortexMetricsFinder;

use super::{BackendType, TestFixture, insert_batches};

const STORE_URL: &str = "s3://covering-index-evidence";
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// The two storage paths whose SQL semantics the fixture exercises.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixtureMode {
    /// Small Arrow batches are forced through Vortex data files.
    File,
    /// Rows stay in Cayenne's memory tier.
    Memory,
}

/// Which physical pair is registered as `a` and `b` for a harness query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PathSelector {
    /// The declared-secondary-index pair under development.
    Indexed,
    /// The unindexed controls: the durable ordinary-scan baseline.
    Baseline,
}

/// A schema-aware, sortable cell value used for bag comparison.
///
/// Text has one value representation for `Utf8` and `Utf8View`; callers also
/// compare the exact Arrow schema, so that normalization cannot mask a type
/// regression.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum TypedValue {
    Int64(Option<i64>),
    Int32(Option<i32>),
    Text(Option<String>),
}

/// One typed result row. A vector, rather than a set, preserves duplicates.
pub type TypedRow = Vec<TypedValue>;

/// Object-store counters captured at one query lifecycle boundary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadCounters {
    /// Non-HEAD `.vortex` `get` requests.
    pub vortex_gets: u64,
    /// `.vortex` `get` requests carrying one byte range.
    pub vortex_range_gets: u64,
    /// Explicit `.vortex` `get_ranges` calls.
    pub vortex_get_ranges: u64,
    /// Individual ranges supplied to explicit `get_ranges` calls.
    pub vortex_range_requests: u64,
    /// Bytes returned for `.vortex` reads, from actual response ranges/bytes.
    pub vortex_bytes: u64,
    /// Requests for non-data objects, including Vortex heads.
    pub metadata_gets: u64,
    /// Metadata-only HEAD requests.
    pub metadata_heads: u64,
    /// Object-store listing calls.
    pub listings: u64,
    /// Objects yielded by listings.
    pub listed_objects: u64,
}

impl ReadCounters {
    /// Return the monotonic counter difference from `before` to this snapshot.
    #[must_use]
    pub fn delta_from(self, before: Self) -> Self {
        Self {
            vortex_gets: self.vortex_gets.saturating_sub(before.vortex_gets),
            vortex_range_gets: self
                .vortex_range_gets
                .saturating_sub(before.vortex_range_gets),
            vortex_get_ranges: self
                .vortex_get_ranges
                .saturating_sub(before.vortex_get_ranges),
            vortex_range_requests: self
                .vortex_range_requests
                .saturating_sub(before.vortex_range_requests),
            vortex_bytes: self.vortex_bytes.saturating_sub(before.vortex_bytes),
            metadata_gets: self.metadata_gets.saturating_sub(before.metadata_gets),
            metadata_heads: self.metadata_heads.saturating_sub(before.metadata_heads),
            listings: self.listings.saturating_sub(before.listings),
            listed_objects: self.listed_objects.saturating_sub(before.listed_objects),
        }
    }

    /// Whether this checkpoint includes any Vortex data transfer.
    #[must_use]
    pub fn has_vortex_data_access(self) -> bool {
        self.vortex_gets > 0 || self.vortex_get_ranges > 0 || self.vortex_bytes > 0
    }
}

/// Read deltas separated by physical planning, execution, and `EXPLAIN ANALYZE`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryReadDeltas {
    pub planning: ReadCounters,
    pub execution: ReadCounters,
    pub explain_analyze: ReadCounters,
}

/// Existing address-index counters at each query checkpoint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AddressIndexCheckpoints {
    pub before: Option<(LookupIndexCounters, LookupIndexCounters)>,
    pub after_planning: Option<(LookupIndexCounters, LookupIndexCounters)>,
    pub after_execution: Option<(LookupIndexCounters, LookupIndexCounters)>,
    pub after_explain_analyze: Option<(LookupIndexCounters, LookupIndexCounters)>,
}

/// Vortex reader metrics attached to the executed physical plan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VortexReadMetrics {
    /// Number of Vortex `DataSourceExec` metric sets seen on the physical plan.
    pub sources: u64,
    /// Instrumented Vortex `read_at` calls.
    pub reads: u64,
    /// Bytes requested by instrumented Vortex `read_at` calls.
    pub bytes: u64,
}

/// Complete evidence for one SQL query.
#[derive(Clone, Debug)]
pub struct QueryEvidence {
    pub selector: PathSelector,
    pub sql: String,
    pub schema: SchemaRef,
    pub rows: Vec<TypedRow>,
    pub physical_plan: String,
    pub explain_analyze: String,
    pub reads: QueryReadDeltas,
    pub address_index: AddressIndexCheckpoints,
    pub vortex_metrics: VortexReadMetrics,
}

impl QueryEvidence {
    /// Print the reproducible artifact needed to characterize a physical path.
    pub fn print(&self, label: &str) {
        println!(
            "=== {label} ({:?}) ===\nSQL:\n{}\nSchema:\n{:?}\nRows:\n{:?}\nPhysical plan:\n{}\nEXPLAIN ANALYZE:\n{}\nRead deltas:\n{:?}\nAddress-index checkpoints:\n{:?}\nVortex reader metrics:\n{:?}",
            self.selector,
            self.sql,
            self.schema,
            self.rows,
            self.physical_plan,
            self.explain_analyze,
            self.reads,
            self.address_index,
            self.vortex_metrics,
        );
    }
}

/// Real Cayenne providers over the fixed covering-index fixture rows.
pub struct CoveringIndexFixture {
    _fixture: TestFixture,
    mode: FixtureMode,
    runtime_env: Arc<RuntimeEnv>,
    store: Option<Arc<CountingObjectStore>>,
    a: Arc<CayenneTableProvider>,
    b: Arc<CayenneTableProvider>,
    a_control: Arc<CayenneTableProvider>,
    b_control: Arc<CayenneTableProvider>,
}

impl CoveringIndexFixture {
    /// Create, populate, and wait for the indexed pair's query capability.
    pub async fn new(mode: FixtureMode) -> Self {
        let fixture = TestFixture::new(BackendType::Sqlite)
            .await
            .expect("create covering-index fixture");
        let runtime_env = Arc::new(RuntimeEnv::default());
        let store = (mode == FixtureMode::File).then(|| {
            Arc::new(CountingObjectStore::new(Arc::new(
                object_store::memory::InMemory::new(),
            )))
        });
        let storage = FixtureStorage {
            mode,
            vortex_config: vortex_config(mode),
            object_store: store.as_ref().map(|store| ObjectStoreConfig {
                url: url::Url::parse(STORE_URL).expect("valid evidence object-store URL"),
                store: Arc::clone(store) as Arc<dyn ObjectStore>,
            }),
        };

        let a = build_table(
            &fixture,
            Arc::clone(&runtime_env),
            "a",
            a_schema(),
            Some(&["some_value"]),
            &storage,
        )
        .await;
        let b = build_table(
            &fixture,
            Arc::clone(&runtime_env),
            "b",
            b_schema(),
            Some(&["id"]),
            &storage,
        )
        .await;
        let a_control = build_table(
            &fixture,
            Arc::clone(&runtime_env),
            "a_control",
            a_schema(),
            None,
            &storage,
        )
        .await;
        let b_control = build_table(
            &fixture,
            Arc::clone(&runtime_env),
            "b_control",
            b_schema(),
            None,
            &storage,
        )
        .await;

        insert_batches(a.as_ref(), vec![a_rows()])
            .await
            .expect("insert indexed a rows");
        insert_batches(b.as_ref(), vec![b_rows()])
            .await
            .expect("insert indexed b rows");
        insert_batches(a_control.as_ref(), vec![a_rows()])
            .await
            .expect("insert control a rows");
        insert_batches(b_control.as_ref(), vec![b_rows()])
            .await
            .expect("insert control b rows");

        let fixture = Self {
            _fixture: fixture,
            mode,
            runtime_env,
            store,
            a,
            b,
            a_control,
            b_control,
        };
        fixture.wait_for_index_capability().await;
        fixture
    }

    /// Return this fixture's storage mode.
    #[must_use]
    pub fn mode(&self) -> FixtureMode {
        self.mode
    }

    /// Capture the current object-store counters. Memory-mode fixtures have no files.
    #[must_use]
    pub fn read_checkpoint(&self) -> ReadCounters {
        self.store
            .as_ref()
            .map_or_else(ReadCounters::default, |store| store.checkpoint())
    }

    /// Execute SQL against the requested pair, retaining data, plan, and I/O evidence.
    pub async fn execute(&self, selector: PathSelector, sql: impl Into<String>) -> QueryEvidence {
        let sql = sql.into();
        let context = self.query_context(selector);
        let before_reads = self.read_checkpoint();
        let before_indexes = self.address_index_checkpoint(selector);
        let dataframe = context.sql(&sql).await.expect("plan evidence query");
        let schema = Arc::new(dataframe.schema().as_arrow().clone());
        let plan = dataframe
            .create_physical_plan()
            .await
            .expect("create evidence physical plan");
        let after_planning_reads = self.read_checkpoint();
        let after_planning_indexes = self.address_index_checkpoint(selector);
        let physical_plan = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        let batches = collect(Arc::clone(&plan), context.task_ctx())
            .await
            .expect("execute evidence physical plan");
        let after_execution_reads = self.read_checkpoint();
        let after_execution_indexes = self.address_index_checkpoint(selector);
        let vortex_metrics = vortex_read_metrics(plan.as_ref());
        let rows = typed_rows(&batches);

        let explained = context
            .sql(&format!("EXPLAIN ANALYZE {sql}"))
            .await
            .expect("plan EXPLAIN ANALYZE")
            .collect()
            .await
            .expect("execute EXPLAIN ANALYZE");
        let after_explain_reads = self.read_checkpoint();
        let after_explain_indexes = self.address_index_checkpoint(selector);

        QueryEvidence {
            selector,
            sql,
            schema,
            rows,
            physical_plan,
            explain_analyze: arrow::util::pretty::pretty_format_batches(&explained)
                .expect("format EXPLAIN ANALYZE")
                .to_string(),
            reads: QueryReadDeltas {
                planning: after_planning_reads.delta_from(before_reads),
                execution: after_execution_reads.delta_from(after_planning_reads),
                explain_analyze: after_explain_reads.delta_from(after_execution_reads),
            },
            address_index: AddressIndexCheckpoints {
                before: before_indexes,
                after_planning: after_planning_indexes,
                after_execution: after_execution_indexes,
                after_explain_analyze: after_explain_indexes,
            },
            vortex_metrics,
        }
    }

    async fn wait_for_index_capability(&self) {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            self.probe_indexes().await;
            let last = self.address_index_checkpoint(PathSelector::Indexed);
            if index_capability_is_ready(self.mode, last) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "covering-index fixture {:?} did not reach the published index/capability condition within {READY_TIMEOUT:?}: {last:?}",
                self.mode,
            );
            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
    }

    async fn probe_indexes(&self) {
        let context = self.query_context(PathSelector::Indexed);
        for sql in [
            "SELECT a_id FROM a WHERE some_value = 'blahblah'",
            "SELECT b_row_id FROM b WHERE id = 10",
        ] {
            context
                .sql(sql)
                .await
                .expect("plan index readiness probe")
                .collect()
                .await
                .expect("execute index readiness probe");
        }
    }

    fn query_context(&self, selector: PathSelector) -> SessionContext {
        let context =
            SessionContext::new_with_config_rt(SessionConfig::new(), Arc::clone(&self.runtime_env));
        let (a, b) = match selector {
            PathSelector::Indexed => (&self.a, &self.b),
            PathSelector::Baseline => (&self.a_control, &self.b_control),
        };
        context
            .register_table("a", Arc::clone(a) as Arc<dyn TableProvider>)
            .expect("register a evidence table");
        context
            .register_table("b", Arc::clone(b) as Arc<dyn TableProvider>)
            .expect("register b evidence table");
        context
    }

    fn address_index_checkpoint(
        &self,
        selector: PathSelector,
    ) -> Option<(LookupIndexCounters, LookupIndexCounters)> {
        let (a, b) = match selector {
            PathSelector::Indexed => (&self.a, &self.b),
            PathSelector::Baseline => (&self.a_control, &self.b_control),
        };
        a.lookup_index_counters().zip(b.lookup_index_counters())
    }
}

/// Assert exact Arrow schema equality and a sorted bag of typed rows.
pub fn assert_same_schema_and_bag(actual: &QueryEvidence, expected: &QueryEvidence) {
    assert_eq!(actual.schema, expected.schema, "query schemas diverged");
    assert_eq!(
        sorted_rows(&actual.rows),
        sorted_rows(&expected.rows),
        "query bags diverged"
    );
}

/// Assert a query has the supplied duplicate-preserving typed result bag.
pub fn assert_typed_bag(actual: &QueryEvidence, expected: &[TypedRow]) {
    assert_eq!(
        sorted_rows(&actual.rows),
        sorted_rows(expected),
        "unexpected query bag"
    );
}

/// Future acceptance check for a covering scan or join.
pub fn assert_no_vortex_data_access(evidence: &QueryEvidence) {
    assert!(
        !evidence.reads.execution.has_vortex_data_access(),
        "covering query read Vortex data during execution: {:?}",
        evidence.reads.execution
    );
    assert_eq!(
        evidence.vortex_metrics.reads, 0,
        "covering query entered the Vortex reader: {:?}",
        evidence.vortex_metrics
    );
}

/// Future acceptance check for the replaced two-table index-join edge.
pub fn assert_index_join_path(evidence: &QueryEvidence) {
    assert!(
        evidence.explain_analyze.contains("CayenneIndexJoinExec"),
        "expected a Cayenne index join:\n{}",
        evidence.explain_analyze
    );
    assert!(
        evidence.explain_analyze.contains("index_probe_keys=")
            && !evidence.explain_analyze.contains("index_probe_keys=0"),
        "index join reported no probe activity:\n{}",
        evidence.explain_analyze
    );
    assert!(
        !evidence.explain_analyze.contains("HashJoinExec"),
        "the replaced join edge still used HashJoinExec:\n{}",
        evidence.explain_analyze
    );
}

/// Fixed table `a` rows, including the nullable foreign key.
#[must_use]
pub fn a_rows() -> RecordBatch {
    RecordBatch::try_new(
        a_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "blahblah", "blahblah", "blahblah", "other", "blahblah",
            ])),
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(10),
                None,
                Some(20),
                Some(99),
            ])),
            Arc::new(StringArray::from(vec!["a1", "a2", "a3", "a4", "a5"])),
        ],
    )
    .expect("fixed a fixture batch")
}

/// Fixed table `b` rows, including duplicate and nullable non-unique keys.
#[must_use]
pub fn b_rows() -> RecordBatch {
    RecordBatch::try_new(
        b_schema(),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103, 104])),
            Arc::new(Int64Array::from(vec![Some(10), Some(10), Some(20), None])),
            Arc::new(StringArray::from(vec!["b1", "b2", "b3", "b4"])),
            Arc::new(Int32Array::from(vec![1, 0, 1, 1])),
        ],
    )
    .expect("fixed b fixture batch")
}

fn a_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a_id", DataType::Int64, false),
        Field::new("some_value", DataType::Utf8, false),
        Field::new("foreign_id", DataType::Int64, true),
        Field::new("one", DataType::Utf8, false),
    ]))
}

fn b_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("b_row_id", DataType::Int64, false),
        Field::new("id", DataType::Int64, true),
        Field::new("two", DataType::Utf8, false),
        Field::new("active", DataType::Int32, false),
    ]))
}

fn vortex_config(mode: FixtureMode) -> VortexConfig {
    let file_config = VortexConfig {
        target_vortex_file_size_mb: 1,
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        compaction_background_interval_ms: 0,
        compaction_trigger_files: usize::MAX,
        compaction_trigger_protected_snapshots: usize::MAX,
        compaction_trigger_snapshot_age_ms: u64::MAX,
        ..VortexConfig::default()
    };
    match mode {
        FixtureMode::File => file_config,
        FixtureMode::Memory => VortexConfig {
            memory_mode: true,
            cdc_mem_tier_shards: 1,
            cdc_mem_tier_max_age_ms: 0,
            cdc_mem_tier_checkpoint_interval_ms: 0,
            cdc_mem_tier_seal_age_ms: 0,
            cdc_mem_tier_max_bytes: 0,
            cdc_durability: CdcDurability::Memory,
            deletion_mode: DeletionMode::Key,
            cold_tier_location: None,
            ..file_config
        },
    }
}

#[derive(Clone)]
struct FixtureStorage {
    mode: FixtureMode,
    vortex_config: VortexConfig,
    object_store: Option<ObjectStoreConfig>,
}

async fn build_table(
    fixture: &TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
    schema: SchemaRef,
    index: Option<&[&str]>,
    storage: &FixtureStorage,
) -> Arc<CayenneTableProvider> {
    let context = CayenneContext::new(&storage.vortex_config, Arc::clone(&runtime_env), name);
    let base_path = match storage.mode {
        FixtureMode::File => format!("{STORE_URL}/fixture-data"),
        FixtureMode::Memory => fixture.data_path.to_string_lossy().into_owned(),
    };
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema,
        primary_key: Vec::new(),
        on_conflict: None,
        base_path,
        partition_column: None,
        vortex_config: storage.vortex_config.clone(),
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let mut builder = CayenneTableProviderBuilder::new(catalog, runtime_env)
        .with_context(context)
        .with_secondary_indexes(index.map_or_else(Vec::new, |columns| {
            vec![columns.iter().map(|column| (*column).to_string()).collect()]
        }));
    if let Some(config) = storage.object_store.clone() {
        builder = builder.with_object_store(config);
    }
    Arc::new(builder.create(options).await.expect("create fixture table"))
}

fn index_capability_is_ready(
    mode: FixtureMode,
    counters: Option<(LookupIndexCounters, LookupIndexCounters)>,
) -> bool {
    let Some((a, b)) = counters else {
        return false;
    };
    match mode {
        FixtureMode::File => a.access_plans_attached > 0 && b.access_plans_attached > 0,
        FixtureMode::Memory => a.selected > 0 && b.selected > 0,
    }
}

fn typed_rows(batches: &[RecordBatch]) -> Vec<TypedRow> {
    batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(|row| typed_row(batch, row)))
        .collect()
}

fn typed_row(batch: &RecordBatch, row: usize) -> TypedRow {
    batch
        .columns()
        .iter()
        .map(|column| match column.data_type() {
            DataType::Int64 => TypedValue::Int64((!column.is_null(row)).then(|| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 result column")
                    .value(row)
            })),
            DataType::Int32 => TypedValue::Int32((!column.is_null(row)).then(|| {
                column
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Int32 result column")
                    .value(row)
            })),
            DataType::Utf8 => TypedValue::Text((!column.is_null(row)).then(|| {
                column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 result column")
                    .value(row)
                    .to_string()
            })),
            DataType::Utf8View => TypedValue::Text((!column.is_null(row)).then(|| {
                column
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("Utf8View result column")
                    .value(row)
                    .to_string()
            })),
            data_type => panic!("unsupported evidence result type {data_type}"),
        })
        .collect()
}

fn sorted_rows(rows: &[TypedRow]) -> Vec<TypedRow> {
    let mut rows = rows.to_vec();
    rows.sort();
    rows
}

fn vortex_read_metrics(plan: &dyn datafusion_physical_plan::ExecutionPlan) -> VortexReadMetrics {
    let metric_sets = VortexMetricsFinder::find_all(plan);
    let mut metrics = VortexReadMetrics {
        sources: u64::try_from(metric_sets.len()).expect("metric set count fits u64"),
        ..VortexReadMetrics::default()
    };
    for metric in metric_sets.iter().flat_map(MetricsSet::iter) {
        if let MetricValue::Count { name, count } = metric.value() {
            if name == "vortex.io.read.size_count" {
                metrics.reads += u64::try_from(count.value()).expect("Vortex read count fits u64");
            } else if name == "vortex.io.read.total_size" {
                metrics.bytes += u64::try_from(count.value()).expect("Vortex read bytes fit u64");
            }
        }
    }
    metrics
}

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<Mutex<ReadCounters>>,
}

impl CountingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            counters: Arc::new(Mutex::new(ReadCounters::default())),
        }
    }

    fn checkpoint(&self) -> ReadCounters {
        *self.counters.lock()
    }

    fn record_get(&self, location: &Path, is_head: bool, is_range: bool, response: &GetResult) {
        let mut counters = self.counters.lock();
        if is_vortex_data(location) && !is_head {
            counters.vortex_gets += 1;
            counters.vortex_range_gets += u64::from(is_range);
            counters.vortex_bytes += response.range.end.saturating_sub(response.range.start);
        } else {
            counters.metadata_gets += 1;
            counters.metadata_heads += u64::from(is_head);
        }
    }

    fn record_get_ranges(&self, location: &Path, ranges: &[Range<u64>], bytes: &[bytes::Bytes]) {
        if !is_vortex_data(location) {
            return;
        }
        let mut counters = self.counters.lock();
        counters.vortex_get_ranges += 1;
        counters.vortex_range_requests +=
            u64::try_from(ranges.len()).expect("range count fits u64");
        counters.vortex_bytes += bytes
            .iter()
            .map(|bytes| u64::try_from(bytes.len()).expect("byte count fits u64"))
            .sum::<u64>();
    }

    fn record_listing(&self) {
        self.counters.lock().listings += 1;
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CountingObjectStore")
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let is_head = options.head;
        let is_range = options.range.is_some();
        let result = self.inner.get_opts(location, options).await?;
        self.record_get(location, is_head, is_range, &result);
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        let result = self.inner.get_ranges(location, ranges).await?;
        self.record_get_ranges(location, ranges, &result);
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.record_listing();
        let counters = Arc::clone(&self.counters);
        self.inner
            .list(prefix)
            .inspect_ok(move |_| counters.lock().listed_objects += 1)
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.record_listing();
        let counters = Arc::clone(&self.counters);
        self.inner
            .list_with_offset(prefix, offset)
            .inspect_ok(move |_| counters.lock().listed_objects += 1)
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.record_listing();
        let result = self.inner.list_with_delimiter(prefix).await?;
        self.counters.lock().listed_objects +=
            u64::try_from(result.objects.len()).expect("listed object count fits u64");
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

fn is_vortex_data(location: &Path) -> bool {
    location
        .filename()
        .is_some_and(|name| name.ends_with(".vortex"))
}
