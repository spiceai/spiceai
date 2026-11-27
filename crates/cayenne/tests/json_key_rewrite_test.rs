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

//! The JSON key-column rewrite changes no answer.
//!
//! Every query runs three ways: over the Cayenne table in a session with
//! `CayenneJsonKeyRewrite`, over the same table in a session without it, and over
//! an in-memory copy of the rows the table should hold (a `MemTable`, which shares
//! no code with Cayenne's scan). The rows must agree as multisets.
//!
//! The table takes the shapes its rows can be in: Vortex files and the inline
//! corpus; files and the in-memory CDC tier, with upserts hiding older file rows;
//! and a table that gained a JSON column after files were written without it.
//! The documents include escaped, duplicate and non-ASCII keys, non-object
//! documents, malformed and truncated values, empty strings and NULLs.
//!
//! The rewrite must also fire: the status-by-path query's scan must read the two
//! key columns and not the JSON column, so a run where the rule never applied
//! cannot pass.

use crate::common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::display::array_value_to_string;
use cayenne::json_shred::{
    CayenneJsonKeyRewrite, JsonKeyColumn, JsonKeyColumnScan, JsonKeyScanTable,
};
use cayenne::metadata::{CdcDurability, CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, SlotAdvancer};
use common::TestFixture;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::{DefaultTableSource, MemTable, TableProvider};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

test_with_backends!(json_keys_over_files_and_inline_rows);
test_with_backends!(json_keys_over_files_and_memory_tier);
test_with_backends!(json_keys_over_a_table_widened_after_its_files);

/// Queries over table `{t}`, each compared across the three ways it is run.
const QUERIES: &[&str] = &[
    // The status-by-path shape: two keys of one column in one query.
    "SELECT json_get_str(attributes, 'url.path') AS p, \
            json_get_int(attributes, 'http.response.status_code') AS s, count(*) AS c \
     FROM {t} GROUP BY 1, 2",
    // Every rewritten function over one key.
    "SELECT id, json_get_str(attributes, 'url.path'), json_as_text(attributes, 'url.path'), \
            json_get_json(attributes, 'url.path'), json_get(attributes, 'url.path'), \
            json_length(attributes, 'url.path') FROM {t}",
    "SELECT id, json_get_int(attributes, 'http.response.status_code'), \
            json_get_float(attributes, 'duration'), json_get_bool(attributes, 'flag'), \
            json_get_array(attributes, 'arr'), json_length(attributes, 'arr') FROM {t}",
    // A remaining path after the key.
    "SELECT id, json_get_int(attributes, 'nested', 'a'), json_get_str(attributes, 'nested', 'b'), \
            json_get_str(attributes, 'arr', 0), json_get_json(attributes, 'nested', 'a'), \
            json_get_float(attributes, 'nested', 'a') FROM {t}",
    // Duplicate, escaped and non-ASCII keys.
    "SELECT id, json_get_str(attributes, 'dup'), json_get_json(attributes, 'dup'), \
            json_get_str(attributes, 'ké'), json_get_str(attributes, 'esc.aped') FROM {t}",
    // Integer-like keys index arrays, so they are never rewritten.
    "SELECT id, json_get_str(attributes, '0'), json_get_json(attributes, '1') FROM {t}",
    // The JSON column read beside a key.
    "SELECT id, attributes, json_get_str(attributes, 'url.path') FROM {t}",
    // Filters on another column, and on a key.
    "SELECT id, json_get_str(attributes, 'url.path') FROM {t} WHERE id % 3 = 0",
    "SELECT id, ts FROM {t} WHERE json_get_str(attributes, 'url.path') = '/api/v1/items'",
    "SELECT json_get_int(attributes, 'http.response.status_code') AS s, count(*) FROM {t} \
     WHERE json_get_str(attributes, 'url.path') LIKE '/api%' GROUP BY 1",
    // Order and limit.
    "SELECT id, json_get_str(attributes, 'url.path') FROM {t} ORDER BY id LIMIT 7",
    // A call over a key's value.
    "SELECT id, json_get_str(json_get_json(attributes, 'nested'), 'b') FROM {t}",
    // Aggregates over keys.
    "SELECT max(json_get_float(attributes, 'duration')), \
            sum(json_get_int(attributes, 'http.response.status_code')), \
            count(json_get_str(attributes, 'url.path')) FROM {t}",
];

/// Queries over the JSON column a widened table gained.
const WIDENED_QUERIES: &[&str] = &[
    "SELECT json_get_str(extra, 'url.path') AS p, count(*) FROM {t} GROUP BY 1",
    "SELECT id, json_get_str(extra, 'url.path'), json_get_int(attributes, 'http.response.status_code') \
     FROM {t}",
];

const STATUS_BY_PATH: &str = "SELECT json_get_str(attributes, 'url.path') AS p, \
     json_get_int(attributes, 'http.response.status_code') AS s, count(*) AS c \
     FROM {t} GROUP BY 1, 2";

/// A small seeded generator, so a failure names the seed that reproduces it.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n).expect("n fits")).expect("fits")
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

const PATHS: [&str; 5] = [
    "\"/a\"",
    "\"/api/v1/items\"",
    "\"/api/v1/items/with/a/value/longer/than/a/view\"",
    "\"/esc\\\"aped\\\\path\"",
    "null",
];
const STATUSES: [&str; 5] = ["200", "404", "500", "-1", "9223372036854775807"];

/// One document: usually an OTLP-like attributes object, sometimes one of the
/// shapes the functions read differently.
fn document(rng: &mut Rng) -> Option<String> {
    let ws = *rng.pick(&["", " ", "\n\t "]);
    let doc = match rng.below(24) {
        0 => return None,
        1 => String::new(),
        2 => "not json".to_string(),
        3 => "[1, {\"url.path\": \"/in-array\"}]".to_string(),
        4 => "\"a string\"".to_string(),
        5 => "42".to_string(),
        // A malformed value: the functions read a prefix of it in place.
        6 => r#"{"url.path": [1, oops], "http.response.status_code": 200}"#.to_string(),
        7 => r#"{"http.response.status_code": 404, "url.path": "/trunc"#.to_string(),
        // Escaped keys decode to the keys the queries ask for.
        8 => format!(
            r#"{{"url.path":{ws}"/escaped/key","esc.aped": "e","http.response.status_code": 201}}"#
        ),
        // A duplicate key: the first one answers.
        9 => r#"{"dup": "first", "url.path": "/dup", "dup": "second", "http.response.status_code": 200}"#
            .to_string(),
        10 => r#"{"ké": "accent", "url.path": "/k", "0": "zero"}"#.to_string(),
        _ => {
            let mut members = vec![
                format!("\"url.path\":{ws}{}", rng.pick(&PATHS)),
                format!(
                    "\"http.response.status_code\":{ws}{}",
                    rng.pick(&STATUSES)
                ),
                format!("\"duration\":{ws}{}", rng.pick(&["1.5", "-0.0", "1e5", "3"])),
                format!("\"flag\":{ws}{}", rng.pick(&["true", "false", "null"])),
                format!(
                    "\"nested\":{ws}{{\"a\":{ws}{},\"b\":{ws}\"bee\"}}",
                    rng.below(5)
                ),
                format!("\"arr\":{ws}[{}, \"x\"]", rng.below(3)),
            ];
            // Shuffle so keys appear in every order, and drop some.
            for i in (1..members.len()).rev() {
                let j = rng.below(i + 1);
                members.swap(i, j);
            }
            members.truncate(2 + rng.below(members.len() - 1));
            format!("{{{ws}{}{ws}}}", members.join(&format!(",{ws}")))
        }
    };
    Some(doc)
}

fn documents(seed: u64, rows: usize) -> Vec<Option<String>> {
    let mut rng = Rng(seed);
    (0..rows).map(|_| document(&mut rng)).collect()
}

fn base_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("attributes", DataType::Utf8, true),
    ]))
}

fn widened_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("extra", DataType::Utf8, true),
    ]))
}

/// Rows `ids` with `docs` as their attributes (and, for a widened table, `extra`).
fn rows(
    schema: &SchemaRef,
    ids: std::ops::Range<i64>,
    docs: &[Option<String>],
    extra: Option<&[Option<String>]>,
) -> TestResult<RecordBatch> {
    let ids: Vec<i64> = ids.collect();
    assert_eq!(ids.len(), docs.len(), "one document per row");
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(Int64Array::from(ids.clone())),
        Arc::new(Int64Array::from(
            ids.iter().map(|id| id * 1000).collect::<Vec<_>>(),
        )),
        Arc::new(docs.iter().map(Option::as_deref).collect::<StringArray>()),
    ];
    if let Some(extra) = extra {
        columns.push(Arc::new(
            extra.iter().map(Option::as_deref).collect::<StringArray>(),
        ));
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

/// Which rewrite a session runs.
#[derive(Clone, Copy, Debug)]
enum Rewrite {
    /// None: the plan as `DataFusion` optimizes it.
    Off,
    /// The rule, with the Cayenne scan serving the key columns.
    Served,
    /// The rule, with a table that says it serves key columns and then declines
    /// to, so the rewritten scan computes them above a plain scan: what a scan of
    /// an acceleration that is not ready does.
    Computed,
}

/// A table that claims key columns and never plans them.
struct DeclinesKeys;

#[async_trait::async_trait]
impl JsonKeyColumnScan for DeclinesKeys {
    fn serves_json_keys(&self) -> bool {
        true
    }

    async fn scan_with_json_keys(
        &self,
        _state: &dyn Session,
        _projection: &[usize],
        _keys: &[JsonKeyColumn],
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }
}

static DECLINES_KEYS: DeclinesKeys = DeclinesKeys;

/// A session with `DataFusion`'s rules and the JSON functions, plus the rewrite
/// (after the default rules, where the runtime runs it).
fn session(rewrite: Rewrite) -> TestResult<SessionContext> {
    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .with_default_features();
    match rewrite {
        Rewrite::Off => {}
        Rewrite::Served => {
            builder = builder.with_optimizer_rule(Arc::new(CayenneJsonKeyRewrite::new()));
        }
        Rewrite::Computed => {
            builder =
                builder.with_optimizer_rule(Arc::new(CayenneJsonKeyRewrite::new_with_resolver(
                    Arc::new(|provider: &dyn TableProvider| {
                        provider
                            .is::<CayenneTableProvider>()
                            .then_some(&DECLINES_KEYS as &dyn JsonKeyColumnScan)
                    }),
                )));
        }
    }
    let mut state = builder.build();
    datafusion_functions_json::register_all(&mut state)?;
    Ok(SessionContext::new_with_state(state))
}

/// Every row rendered as `a|b|…`, sorted, so the comparison ignores row order.
async fn sorted_rows(ctx: &SessionContext, sql: &str) -> TestResult<Vec<String>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut rendered = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                rendered.push(if column.is_null(row) {
                    "NULL".to_string()
                } else {
                    array_value_to_string(column, row)?
                });
            }
            rows.push(rendered.join("|"));
        }
    }
    rows.sort_unstable();
    Ok(rows)
}

/// The columns each rewritten scan in `sql`'s optimized plan reads.
async fn rewritten_scans(ctx: &SessionContext, sql: &str) -> TestResult<Vec<Vec<String>>> {
    let plan = ctx.sql(sql).await?.into_optimized_plan()?;
    let mut scans = Vec::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && scan
                .source
                .downcast_ref::<DefaultTableSource>()
                .is_some_and(|source| source.table_provider.is::<JsonKeyScanTable>())
        {
            scans.push(
                scan.projected_schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect(),
            );
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(scans)
}

/// Runs every query over `table` three ways and requires one answer, then requires
/// that the rewrite fired where it should and only there.
async fn assert_rewrite_changes_no_answer(
    table: Arc<CayenneTableProvider>,
    oracle: MemTable,
    queries: &[&str],
) -> TestResult {
    let reference = session(Rewrite::Off)?;
    reference.register_table("t", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    reference.register_table("oracle", Arc::new(oracle))?;
    let mut rewritten = Vec::new();
    for rewrite in [Rewrite::Served, Rewrite::Computed] {
        let ctx = session(rewrite)?;
        ctx.register_table("t", Arc::clone(&table) as Arc<dyn TableProvider>)?;
        rewritten.push((rewrite, ctx));
    }

    let mut wrong = Vec::new();
    for query in queries {
        let sql = query.replace("{t}", "t");
        let expected = sorted_rows(&reference, &query.replace("{t}", "oracle")).await?;
        assert!(!expected.is_empty(), "{sql}: the oracle returned no rows");
        let differing = |rows: &[String]| {
            rows.iter()
                .filter(|row| !expected.contains(row))
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
        };
        let unrewritten = sorted_rows(&reference, &sql).await?;
        if unrewritten != expected {
            wrong.push(format!(
                "{sql}\n  without rewrite: {} rows, e.g. {:?}; oracle: {} rows",
                unrewritten.len(),
                differing(&unrewritten),
                expected.len()
            ));
        }
        for (rewrite, ctx) in &rewritten {
            let got = sorted_rows(ctx, &sql).await?;
            if got != expected {
                wrong.push(format!(
                    "{sql}\n  {rewrite:?}: {} rows, e.g. {:?}; oracle: {} rows",
                    got.len(),
                    differing(&got),
                    expected.len()
                ));
            }
        }
        assert!(
            rewritten_scans(&reference, &sql).await?.is_empty(),
            "{sql}: the session without the rule rewrote a scan"
        );
    }
    assert!(wrong.is_empty(), "answers differ:\n{}", wrong.join("\n"));
    Ok(())
}

/// The status-by-path scan reads its two key columns and not the JSON column,
/// whether the table serves them or they are computed above it; the plan derives
/// them; and the answer has several non-NULL paths, so the comparison was not
/// vacuous.
async fn assert_status_by_path_reads_keys(table: Arc<CayenneTableProvider>) -> TestResult {
    let sql = STATUS_BY_PATH.replace("{t}", "t");
    let status_key = "__cayenne_json_key:10:attributes:http.response.status_code".to_string();
    let path_key = "__cayenne_json_key:10:attributes:url.path".to_string();
    for rewrite in [Rewrite::Served, Rewrite::Computed] {
        let ctx = session(rewrite)?;
        ctx.register_table("t", Arc::clone(&table) as Arc<dyn TableProvider>)?;
        let mut scans = rewritten_scans(&ctx, &sql).await?;
        for scan in &mut scans {
            scan.sort_unstable();
        }
        assert_eq!(
            scans,
            vec![vec![status_key.clone(), path_key.clone()]],
            "{rewrite:?}: the scan must read the two key columns and nothing else"
        );
        let physical = ctx.sql(&sql).await?.create_physical_plan().await?;
        let rendered = displayable(physical.as_ref()).indent(true).to_string();
        assert!(
            rendered.contains("__cayenne_json_top_level_value"),
            "{rewrite:?}: the plan must derive the key columns:\n{rendered}"
        );
        assert!(
            !rendered.contains("json_get_str(attributes"),
            "{rewrite:?}: no function may read the documents:\n{rendered}"
        );
        let rows = sorted_rows(&ctx, &sql).await?;
        let answered = rows.iter().filter(|row| !row.starts_with("NULL|")).count();
        assert!(
            answered >= 8,
            "{rewrite:?}: only {answered} groups with a path, the documents exercise too little ({rows:?})"
        );
    }

    // Reading the JSON column beside a key keeps it in the scan; an integer key
    // is not rewritten.
    let rewrite = session(Rewrite::Served)?;
    rewrite.register_table("t", table as Arc<dyn TableProvider>)?;
    let with_json = rewritten_scans(
        &rewrite,
        "SELECT attributes, json_get_str(attributes, 'url.path') FROM t",
    )
    .await?;
    assert!(
        with_json.len() == 1 && with_json[0].contains(&"attributes".to_string()),
        "the JSON column must stay in the scan when it is read: {with_json:?}"
    );
    assert!(
        rewritten_scans(&rewrite, "SELECT json_get_str(attributes, '0') FROM t")
            .await?
            .is_empty(),
        "an integer-like key must not be rewritten"
    );
    Ok(())
}

struct NoopSlotAdvancer;

#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

fn oracle(schema: &SchemaRef, batches: Vec<RecordBatch>) -> TestResult<MemTable> {
    Ok(MemTable::try_new(Arc::clone(schema), vec![batches])?)
}

/// Rows in a Vortex file (a write above the inline limit) and in the inline corpus.
async fn json_keys_over_files_and_inline_rows(fixture: TestFixture) -> TestResult {
    let schema = base_schema();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "json_keys_inline".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    let file_docs = documents(1, 1500);
    let inline_docs = documents(0x5eed, 60);
    let file_rows = rows(&schema, 0..1500, &file_docs, None)?;
    let inline_rows = rows(&schema, 1500..1560, &inline_docs, None)?;
    common::insert_batch(&table, file_rows.clone()).await?;
    common::insert_batch(&table, inline_rows.clone()).await?;
    let table_id = fixture
        .catalog
        .get_table("json_keys_inline")
        .await?
        .table_id;
    assert_eq!(
        fixture.catalog.get_inlined_data_count(&table_id).await?,
        60,
        "precondition: the second write must be inline"
    );

    assert_status_by_path_reads_keys(Arc::clone(&table)).await?;
    assert_rewrite_changes_no_answer(
        table,
        oracle(&schema, vec![file_rows, inline_rows])?,
        QUERIES,
    )
    .await
}

/// Rows in files and in the in-memory CDC tier, where upserts in memory hide the
/// file rows they replace.
async fn json_keys_over_files_and_memory_tier(fixture: TestFixture) -> TestResult {
    let schema = base_schema();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .create(CreateTableOptions {
            table_name: "json_keys_memory".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: fixture
                .data_path
                .join("json_keys_memory")
                .to_string_lossy()
                .to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                cdc_durability: CdcDurability::Memory,
                cdc_mem_tier_checkpoint_interval_ms: 0,
                cdc_mem_tier_max_age_ms: 0,
                cdc_mem_tier_seal_age_ms: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        })
        .await?,
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));

    let first_docs = documents(7, 1200);
    let later_docs = documents(20_261_127, 600);
    let first = rows(&schema, 0..1200, &first_docs, None)?;
    let later = rows(&schema, 900..1500, &later_docs, None)?;
    cdc_apply(&table, first.clone()).await?;
    assert!(
        table.checkpoint_mem_tier().await? > 0,
        "precondition: the first rows must move into a file"
    );
    cdc_apply(&table, later.clone()).await?;

    // Ids 900..1200 were upserted: the oracle keeps the later copy.
    let kept_first = first.slice(0, 900);
    assert_status_by_path_reads_keys(Arc::clone(&table)).await?;
    assert_rewrite_changes_no_answer(table, oracle(&schema, vec![kept_first, later])?, QUERIES)
        .await
}

async fn cdc_apply(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> TestResult {
    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(common::single_batch_stream(batch), &ctx.task_ctx())
        .await?;
    if write.has_pending_finalize() {
        write.finish().await?;
    }
    Ok(())
}

/// Files written before the table gained a JSON column, which they lack, and rows
/// written after, in a file and inline.
async fn json_keys_over_a_table_widened_after_its_files(fixture: TestFixture) -> TestResult {
    let schema = base_schema();
    let widened = widened_schema();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "json_keys_widened".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    let old_docs = documents(3, 1500);
    common::insert_batch(&table, rows(&schema, 0..1500, &old_docs, None)?).await?;

    let evolution = arrow_tools::schema_evolution::EvolutionContext {
        constraint_columns: &[],
    };
    let arrow_tools::schema_evolution::SchemaEvolution::Widening(plan) =
        arrow_tools::schema_evolution::classify(&schema, &widened, &evolution)
    else {
        return Err("adding a nullable column must classify as a widening".into());
    };
    table.evolve_schema_live(&plan).await?;

    let new_docs = documents(4, 1500);
    let new_extra = documents(5, 1500);
    let inline_docs = documents(6, 40);
    let inline_extra = documents(8, 40);
    let new_rows = rows(&widened, 1500..3000, &new_docs, Some(&new_extra))?;
    let inline_rows = rows(&widened, 3000..3040, &inline_docs, Some(&inline_extra))?;
    common::insert_batch(&table, new_rows.clone()).await?;
    common::insert_batch(&table, inline_rows.clone()).await?;

    let no_extra = vec![None; 1500];
    let old_rows = rows(&widened, 0..1500, &old_docs, Some(&no_extra))?;
    let queries: Vec<&str> = QUERIES.iter().chain(WIDENED_QUERIES).copied().collect();
    assert_rewrite_changes_no_answer(
        table,
        oracle(&widened, vec![old_rows, new_rows, inline_rows])?,
        &queries,
    )
    .await
}
