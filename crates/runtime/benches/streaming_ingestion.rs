#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use data_components::cdc::{
    ChangeBatch, ChangeEnvelope, ChangesStream, CommitChange, CommitError,
    wrap_data_as_change_batch,
};
use datafusion::catalog::TableProvider;
use datafusion::common::{Constraints, ToDFSchema};
use datafusion::logical_expr::CreateExternalTable;
use datafusion::sql::TableReference;
use futures::stream;
use runtime::accelerated_table::refresh::Refresh;
use runtime::accelerated_table::refresh_task::RefreshTask;
use runtime::component::dataset::acceleration::RefreshMode;
use runtime::dataaccelerator::DataAccelerator;
use runtime::federated_table::FederatedTable;
use runtime::status::RuntimeStatus;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, Notify, RwLock};

mod utils;

struct NoOpCommitter;
impl CommitChange for NoOpCommitter {
    fn commit(&self) -> Result<(), CommitError> {
        Ok(())
    }
}

fn bench_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn generate_record_batch(schema: &Arc<Schema>, batch_size: usize) -> RecordBatch {
    let ids: Vec<i32> = (0..batch_size as i32).collect();
    let names: Vec<String> = (0..batch_size).map(|i| format!("item_{i}")).collect();
    let values: Vec<f64> = (0..batch_size).map(|i| i as f64 * 1.5).collect();

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
        ],
    )
        .expect("failed to create record batch")
}

fn create_changes_stream(
    schema: &Arc<Schema>,
    batch_size: usize,
    num_batches: usize,
) -> ChangesStream {
    let batches: Vec<ChangeBatch> = (0..num_batches)
        .map(|_| {
            let record_batch = generate_record_batch(schema, batch_size);
            wrap_data_as_change_batch(schema, &record_batch)
                .expect("failed to wrap as change batch")
        })
        .collect();

    let envelopes: Vec<Result<ChangeEnvelope, _>> = batches
        .into_iter()
        .enumerate()
        .map(|(i, batch)| {
            let is_ready = i == 0;
            Ok(ChangeEnvelope::new(Box::new(NoOpCommitter), batch, is_ready))
        })
        .collect();

    Box::pin(stream::iter(envelopes))
}

fn make_create_external_table_cmd(
    schema: &Arc<Schema>,
    options: HashMap<String, String>,
) -> CreateExternalTable {
    let df_schema =
        ToDFSchema::to_dfschema_ref(Arc::clone(schema)).expect("failed to create DFSchema");

    CreateExternalTable {
        schema: df_schema,
        name: TableReference::bare("bench_table"),
        location: String::new(),
        file_type: String::new(),
        table_partition_cols: vec![],
        if_not_exists: true,
        or_replace: false,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options,
        constraints: Constraints::new_unverified(vec![]),
        column_defaults: HashMap::default(),
        temporary: false,
    }
}

#[cfg(feature = "duckdb")]
async fn create_duckdb_table(
    schema: &Arc<Schema>,
    db_path: &std::path::Path,
) -> Arc<dyn TableProvider> {
    use runtime::dataaccelerator::duckdb::DuckDBAccelerator;

    let mut options = HashMap::new();
    options.insert("open".to_string(), db_path.display().to_string());

    let cmd = make_create_external_table_cmd(schema, options);

    let engine = DuckDBAccelerator::new();
    engine
        .create_external_table(cmd, None, vec![], None)
        .await
        .expect("DuckDB table creation failed")
}

/// Holds the shared state for cayenne benchmarks (created once, reused across iterations).
struct CayenneSetup {
    dataset: runtime::component::dataset::Dataset,
}

impl CayenneSetup {
    async fn new() -> Self {
        use runtime::component::dataset::acceleration::{Acceleration, Engine, Mode};

        let app = Arc::new(app::AppBuilder::new("bench_app").build());
        let rt = Arc::new(runtime::Runtime::builder().build().await);

        let acceleration = Acceleration {
            enabled: true,
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Changes),
            ..Default::default()
        };

        let mut builder = runtime::component::dataset::builder::DatasetBuilder::try_new(
            "bench:bench_table".to_string(),
            "bench_table",
        )
            .expect("failed to create dataset builder");
        builder.acceleration = Some(acceleration);

        let dataset = builder
            .with_app(app)
            .with_runtime(rt)
            .build()
            .expect("failed to build dataset");

        Self { dataset }
    }

    async fn create_table(
        &self,
        schema: &Arc<Schema>,
        temp_dir: &std::path::Path,
    ) -> Arc<dyn TableProvider> {
        use runtime::dataaccelerator::AccelerationSource;
        use runtime::dataaccelerator::cayenne::CayenneAccelerator;

        let data_dir = temp_dir.join("data");
        let metadata_dir = temp_dir.join("metadata");
        std::fs::create_dir_all(&data_dir).expect("failed to create data dir");
        std::fs::create_dir_all(&metadata_dir).expect("failed to create metadata dir");

        let mut options = HashMap::new();
        options.insert(
            "cayenne_file_path".to_string(),
            data_dir.display().to_string(),
        );
        options.insert(
            "cayenne_metadata_dir".to_string(),
            metadata_dir.display().to_string(),
        );

        let cmd = make_create_external_table_cmd(schema, options);
        let runtime_env = Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default());

        let engine = CayenneAccelerator::new();
        engine
            .create_external_table(
                cmd,
                Some(&self.dataset as &dyn AccelerationSource),
                vec![],
                Some(runtime_env),
            )
            .await
            .expect("Cayenne table creation failed")
    }
}

async fn run_streaming_benchmark(
    accelerator: Arc<dyn TableProvider>,
    schema: &Arc<Schema>,
    batch_size: usize,
    num_batches: usize,
) {
    let changes_stream = create_changes_stream(schema, batch_size, num_batches);

    let empty_batch = RecordBatch::new_empty(Arc::clone(schema));
    let mem_table = datafusion::datasource::MemTable::try_new(
        Arc::clone(schema),
        vec![vec![empty_batch]],
    )
        .expect("failed to create MemTable");
    let federated = Arc::new(FederatedTable::new_unchecked(Arc::new(mem_table)));

    let refresh_task = RefreshTask::builder(
        RuntimeStatus::new(),
        TableReference::bare("bench_table"),
        federated,
        None,
        accelerator,
        Handle::current(),
        Arc::new(Mutex::new(())),
    )
        .build();

    let refresh = Arc::new(RwLock::new(Refresh::new(RefreshMode::Changes)));
    let ready_sender = Some(Arc::new(Notify::new()));
    let initial_load_completed = Arc::new(AtomicBool::new(false));

    refresh_task
        .start_changes_stream(
            refresh,
            changes_stream,
            None,
            ready_sender,
            initial_load_completed,
        )
        .await
        .expect("start_changes_stream failed");
}

fn bench_streaming_ingestion(c: &mut Criterion) {
    utils::init_tracing(Some("warn"));

    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");

    // Create cayenne setup once (avoids Runtime::build per iteration)
    let cayenne_setup = Arc::new(rt.block_on(CayenneSetup::new()));

    let mut group = c.benchmark_group("streaming_ingestion");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(120));

    let schema = bench_schema();
    let batch_sizes = [100, 1_000, 10_000];
    let num_batches = 10;

    for batch_size in batch_sizes {
        #[cfg(feature = "duckdb")]
        {
            group.bench_with_input(
                BenchmarkId::new("duckdb", batch_size),
                &batch_size,
                |b, &batch_size| {
                    b.to_async(&rt).iter(|| {
                        let schema = Arc::clone(&schema);
                        async move {
                            let temp_dir =
                                tempfile::tempdir().expect("failed to create temp dir");
                            let db_path = temp_dir.path().join("bench.db");
                            let accelerator = create_duckdb_table(&schema, &db_path).await;
                            run_streaming_benchmark(accelerator, &schema, batch_size, num_batches)
                                .await;
                        }
                    });
                },
            );
        }

        group.bench_with_input(
            BenchmarkId::new("cayenne", batch_size),
            &batch_size,
            |b, &batch_size| {
                let cayenne_setup = Arc::clone(&cayenne_setup);
                b.to_async(&rt).iter(|| {
                    let schema = Arc::clone(&schema);
                    let cayenne_setup = Arc::clone(&cayenne_setup);
                    async move {
                        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
                        let accelerator =
                            cayenne_setup.create_table(&schema, temp_dir.path()).await;
                        run_streaming_benchmark(accelerator, &schema, batch_size, num_batches)
                            .await;
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_streaming_ingestion);
criterion_main!(benches);
