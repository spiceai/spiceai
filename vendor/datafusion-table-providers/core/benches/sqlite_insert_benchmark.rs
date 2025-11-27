use std::sync::Arc;
use std::time::Instant;

use arrow::{
    array::{Float64Array, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{
    catalog::TableProviderFactory,
    common::{Constraints, TableReference, ToDFSchema},
    execution::context::SessionContext,
    logical_expr::{dml::InsertOp, CreateExternalTable},
    physical_plan::collect,
};
use datafusion_table_providers::{sqlite::SqliteTableProviderFactory, util::test::MockExec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InsertMethod {
    Prepared,
    Inline,
}

impl InsertMethod {
    fn name(&self) -> &str {
        match self {
            InsertMethod::Prepared => "insert_batch_prepared (NEW)",
            InsertMethod::Inline => "insert_batch (OLD)",
        }
    }
}

/// Benchmark for SQLite insert performance comparing prepared statements vs inline SQL
///
/// This benchmark measures the performance of inserting data into SQLite
/// using both the new prepared statement approach and the old inline SQL approach.
///
/// Set the environment variable SQLITE_INSERT_METHOD to control which method to test:
/// - "prepared" (default): Use prepared statements
/// - "inline": Use inline SQL generation
/// - "both": Test both methods and compare
#[tokio::main]
async fn main() {
    println!("\n=== SQLite Insert Performance Benchmark ===\n");

    // Determine which method(s) to test
    let test_mode = std::env::var("SQLITE_INSERT_METHOD")
        .unwrap_or_else(|_| "both".to_string())
        .to_lowercase();

    let methods_to_test = match test_mode.as_str() {
        "inline" => vec![InsertMethod::Inline],
        "prepared" => vec![InsertMethod::Prepared],
        "both" => vec![InsertMethod::Inline, InsertMethod::Prepared],
        _ => vec![InsertMethod::Inline, InsertMethod::Prepared],
    };

    // Test configurations: (num_batches, rows_per_batch)
    let test_configs = vec![
        (10, 1),
        (10, 10),
        (10, 100),
        (1, 1000),
        (10, 1000),
        (100, 1000),
        (10, 10000),
        (5, 50000),
        (5, 100000),
        (5, 1000000),
    ];

    // Store results for comparison
    type BenchmarkResults = Vec<(InsertMethod, Vec<(usize, f64, f64)>)>;
    let mut results: BenchmarkResults = Vec::new();

    for method in &methods_to_test {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("Testing Method: {}", method.name());
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

        let mut method_results = Vec::new();

        for (num_batches, rows_per_batch) in &test_configs {
            let total_rows = num_batches * rows_per_batch;
            println!(
                "  Config: {} batches × {} rows = {} total rows",
                num_batches, rows_per_batch, total_rows
            );

            let duration = run_benchmark(*num_batches, *rows_per_batch, *method).await;
            let rows_per_sec = total_rows as f64 / duration.as_secs_f64();
            let time_per_row = duration.as_micros() as f64 / total_rows as f64;

            println!("    ⏱️  Time taken: {:.3}s", duration.as_secs_f64());
            println!("    🚀 Throughput: {:.0} rows/sec", rows_per_sec);
            println!("    📊 Per-row time: {:.2}µs\n", time_per_row);

            method_results.push((total_rows, rows_per_sec, time_per_row));
        }

        results.push((*method, method_results));
        println!();
    }

    // Print comparison summary if both methods were tested
    if methods_to_test.len() > 1 {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("Performance Comparison");
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

        println!(
            "{:<15} {:<20} {:<20} {:<15}",
            "Total Rows", "OLD (rows/sec)", "NEW (rows/sec)", "Speedup"
        );
        println!("{}", "─".repeat(75));

        for i in 0..test_configs.len() {
            let (total_rows, old_throughput, _) = results[0].1[i];
            let (_, new_throughput, _) = results[1].1[i];
            let speedup = new_throughput / old_throughput;

            println!(
                "{:<15} {:<20.0} {:<20.0} {:.2}x",
                total_rows, old_throughput, new_throughput, speedup
            );
        }

        println!("\n{}", "─".repeat(75));

        // Calculate average speedup
        let avg_speedup: f64 = (0..test_configs.len())
            .map(|i| results[1].1[i].1 / results[0].1[i].1)
            .sum::<f64>()
            / test_configs.len() as f64;

        println!(
            "\n📊 Average speedup: {:.2}x faster with prepared statements",
            avg_speedup
        );
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    }
}

async fn run_benchmark(
    num_batches: usize,
    rows_per_batch: usize,
    method: InsertMethod,
) -> std::time::Duration {
    // Create schema with multiple column types
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
    ]));

    let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

    // Create a unique table name to avoid conflicts
    let table_name = format!(
        "bench_table_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let external_table = CreateExternalTable {
        schema: df_schema,
        name: TableReference::bare(table_name),
        location: String::new(),
        file_type: String::new(),
        table_partition_cols: vec![],
        if_not_exists: true,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options: std::collections::HashMap::new(),
        constraints: Constraints::new_unverified(vec![]),
        column_defaults: std::collections::HashMap::default(),
        temporary: false,
    };

    let ctx = SessionContext::new();

    // Configure the factory based on which method we're testing
    let use_prepared = match method {
        InsertMethod::Prepared => true,
        InsertMethod::Inline => false,
    };

    let table = SqliteTableProviderFactory::default()
        .with_batch_insert_use_prepared_statements(use_prepared)
        .create(&ctx.state(), &external_table)
        .await
        .expect("table should be created");

    // Generate batches
    let batches: Vec<Result<RecordBatch, datafusion::error::DataFusionError>> = (0..num_batches)
        .map(|batch_idx| {
            let start_id = batch_idx * rows_per_batch;

            let ids: Vec<i64> = (start_id..(start_id + rows_per_batch))
                .map(|i| i as i64)
                .collect();

            let names: Vec<String> = (start_id..(start_id + rows_per_batch))
                .map(|i| format!("name_{}", i))
                .collect();

            let values: Vec<f64> = (start_id..(start_id + rows_per_batch))
                .map(|i| (i as f64) * 1.5)
                .collect();

            let categories: Vec<Option<String>> = (start_id..(start_id + rows_per_batch))
                .map(|i| Some(format!("category_{}", i % 10)))
                .collect();

            let counts: Vec<Option<i64>> = (start_id..(start_id + rows_per_batch))
                .map(|i| {
                    if i % 3 == 0 {
                        Some((i % 100) as i64)
                    } else {
                        None
                    }
                })
                .collect();

            let id_array = Int64Array::from(ids);
            let name_array = StringArray::from(names);
            let value_array = Float64Array::from(values);
            let category_array = StringArray::from(categories);
            let count_array = Int64Array::from(counts);

            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(id_array),
                    Arc::new(name_array),
                    Arc::new(value_array),
                    Arc::new(category_array),
                    Arc::new(count_array),
                ],
            )
            .expect("batch should be created"))
        })
        .collect();

    let exec = MockExec::new(batches, schema);

    // Start timing
    let start = Instant::now();

    let insertion = table
        .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
        .await
        .expect("insertion should be successful");

    collect(insertion, ctx.task_ctx())
        .await
        .expect("insert successful");

    // End timing
    start.elapsed()
}
