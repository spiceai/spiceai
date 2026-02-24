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

//! Tests for staged append writes.
//!
//! Validates that append writes go through a `_staging/` directory and that
//! partial writes from stream errors are cleaned up without polluting the
//! active snapshot.

#![allow(clippy::expect_used)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog, STAGING_DIR_NAME};

use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::*;
use datafusion_common::DataFusionError;
use datafusion_execution::TaskContext;
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType, Partitioning};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

// ============================================================================
// Test 1: Basic staged append — data correct, staging empty after write
// ============================================================================

test_with_backends!(test_staged_append_basic_impl);

async fn test_staged_append_basic_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "staged_basic").await;

    ctx.sql("INSERT INTO staged_basic VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await?
        .collect()
        .await?;

    let rows = query_all(&ctx, "staged_basic").await;
    assert_eq!(
        rows,
        vec![
            (1, "Alice".to_string()),
            (2, "Bob".to_string()),
            (3, "Charlie".to_string()),
        ]
    );

    assert_staging_empty(&staging_dir(&table));

    Ok(())
}

// ============================================================================
// Test 2: Stream error — partial writes cleaned up, no data corruption
// ============================================================================

test_with_backends!(test_staged_append_stream_error_no_partial_data_impl);

async fn test_staged_append_stream_error_no_partial_data_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "staged_err").await;

    // Insert baseline data
    ctx.sql("INSERT INTO staged_err VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await?
        .collect()
        .await?;

    assert_eq!(row_count(&ctx, "staged_err").await, 3);

    // Build a stream that yields 2 batches then errors
    let schema = test_schema();
    let batch1 = make_batch(&[10, 11], &["X", "Y"]);
    let batch2 = make_batch(&[12, 13], &["Z", "W"]);

    let items: Vec<datafusion_common::Result<RecordBatch>> = vec![
        Ok(batch1),
        Ok(batch2),
        Err(DataFusionError::Execution(
            "simulated stream error".to_string(),
        )),
    ];
    let failing_stream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::iter(items),
    ));

    let input = Arc::new(FailingStreamExec::new(Arc::clone(&schema), failing_stream));

    let insert_plan = table
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await?;

    let result = collect(insert_plan, ctx.task_ctx()).await;
    assert!(result.is_err(), "Expected stream error to propagate");

    // Verify: only original 3 rows remain — no partial data from failed write
    let rows = query_all(&ctx, "staged_err").await;
    assert_eq!(
        rows,
        vec![
            (1, "Alice".to_string()),
            (2, "Bob".to_string()),
            (3, "Charlie".to_string()),
        ],
        "Partial data from failed stream should not be visible"
    );

    // Verify: staging dir is clean
    assert_staging_empty(&staging_dir(&table));

    Ok(())
}

// ============================================================================
// Test 3: Self-healing — leftover files in _staging/ cleaned on next append
// ============================================================================

test_with_backends!(test_staged_append_self_healing_leftover_impl);

async fn test_staged_append_self_healing_leftover_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "staged_heal").await;

    // Insert initial data to ensure table dirs exist
    ctx.sql("INSERT INTO staged_heal VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;

    // Manually plant a leftover file in _staging/ (simulates crash)
    let staging = staging_dir(&table);
    std::fs::create_dir_all(&staging)?;
    std::fs::write(staging.join("leftover.vortex"), b"fake leftover data")?;
    assert!(staging.join("leftover.vortex").exists());

    // Next append should clear _staging/ first (self-healing)
    ctx.sql("INSERT INTO staged_heal VALUES (2, 'Bob')")
        .await?
        .collect()
        .await?;

    // Leftover is gone
    assert!(!staging.join("leftover.vortex").exists());
    assert_staging_empty(&staging);

    // Only real data is queryable
    let rows = query_all(&ctx, "staged_heal").await;
    assert_eq!(
        rows,
        vec![(1, "Alice".to_string()), (2, "Bob".to_string()),]
    );

    Ok(())
}

// ============================================================================
// Test 4: Multiple appends accumulate correctly
// ============================================================================

test_with_backends!(test_staged_append_multi_append_accumulates_impl);

async fn test_staged_append_multi_append_accumulates_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "staged_multi").await;
    let staging = staging_dir(&table);

    // Append 1
    ctx.sql("INSERT INTO staged_multi VALUES (1, 'A'), (2, 'B'), (3, 'C')")
        .await?
        .collect()
        .await?;
    assert_eq!(row_count(&ctx, "staged_multi").await, 3);
    assert_staging_empty(&staging);

    // Append 2
    ctx.sql("INSERT INTO staged_multi VALUES (4, 'D'), (5, 'E'), (6, 'F')")
        .await?
        .collect()
        .await?;
    assert_eq!(row_count(&ctx, "staged_multi").await, 6);
    assert_staging_empty(&staging);

    // Append 3
    ctx.sql("INSERT INTO staged_multi VALUES (7, 'G'), (8, 'H'), (9, 'I')")
        .await?
        .collect()
        .await?;
    assert_eq!(row_count(&ctx, "staged_multi").await, 9);
    assert_staging_empty(&staging);

    // Verify all data
    let rows = query_all(&ctx, "staged_multi").await;
    assert_eq!(rows.len(), 9);
    assert_eq!(rows[0], (1, "A".to_string()));
    assert_eq!(rows[8], (9, "I".to_string()));

    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

// ---------------------------------------------------------------------------
// Minimal ExecutionPlan that wraps a SendableRecordBatchStream — used to
// inject a failing stream into `insert_into` without depending on cayenne's
// `pub(crate)` StreamingExec.
// ---------------------------------------------------------------------------

struct FailingStreamExec {
    schema: SchemaRef,
    stream: std::sync::Mutex<Option<SendableRecordBatchStream>>,
    properties: PlanProperties,
}

impl FailingStreamExec {
    fn new(schema: SchemaRef, stream: SendableRecordBatchStream) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Self {
            schema,
            stream: std::sync::Mutex::new(Some(stream)),
            properties,
        }
    }
}

impl std::fmt::Debug for FailingStreamExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailingStreamExec").finish()
    }
}

impl DisplayAs for FailingStreamExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FailingStreamExec")
    }
}

impl ExecutionPlan for FailingStreamExec {
    fn name(&self) -> &'static str {
        "FailingStreamExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self
            .stream
            .lock()
            .map_err(|e| DataFusionError::Execution(format!("Stream lock poisoned: {e}")))?
            .take()
            .ok_or_else(|| DataFusionError::Execution("Stream already consumed".to_string()))?;
        Ok(stream)
    }
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn make_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .expect("valid batch")
}

/// Build the `_staging/` directory path for a table.
fn staging_dir(table: &CayenneTableProvider) -> PathBuf {
    let meta = table.metadata();
    PathBuf::from(&meta.path)
        .join(meta.table_id.to_string())
        .join(STAGING_DIR_NAME)
}

/// Assert that `_staging/` is empty (no files).
fn assert_staging_empty(staging: &std::path::Path) {
    if !staging.exists() {
        return; // non-existent is fine — means nothing was left behind
    }
    let entries: Vec<_> = std::fs::read_dir(staging)
        .expect("read staging dir")
        .collect();
    assert!(
        entries.is_empty(),
        "Expected _staging/ to be empty but found {} entries",
        entries.len()
    );
}

/// Query total row count from a registered table.
async fn row_count(ctx: &SessionContext, table_name: &str) -> usize {
    let df = ctx
        .sql(&format!("SELECT * FROM {table_name}"))
        .await
        .expect("query");
    let results = df.collect().await.expect("collect");
    results.iter().map(RecordBatch::num_rows).sum()
}

/// Query all (id, name) pairs ordered by id.
async fn query_all(ctx: &SessionContext, table_name: &str) -> Vec<(i64, String)> {
    let df = ctx
        .sql(&format!("SELECT id, name FROM {table_name} ORDER BY id"))
        .await
        .expect("query");
    let batches = df.collect().await.expect("collect");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    rows
}

/// Create a table and register it with a `SessionContext`.
async fn setup_table(
    fixture: &common::TestFixture,
    table_name: &str,
) -> (Arc<CayenneTableProvider>, SessionContext) {
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: test_schema(),
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = CayenneTableProvider::create_table(catalog, table_options)
        .await
        .expect("create table");
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");

    (table, ctx)
}
