/*
Copyright 2024 The Spice.ai OSS Authors

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

use app::AppBuilder;
use arrow::datatypes::DecimalType;
use arrow::{
    array::{Decimal128Array, RecordBatch},
    datatypes::{DataType, Decimal128Type},
};
use runtime::Runtime;
use spicepod::component::dataset::{
    acceleration::{Acceleration, Mode},
    Dataset,
};

use crate::{
    dataset_ready_check, get_test_datafusion, init_tracing, run_query_and_check_results, ValidateFn,
};

fn make_sqlite_decimal_dataset(mode: Mode) -> Dataset {
    let mut ds = Dataset::new("https://public-data.spiceai.org/decimal.parquet", "decimal");
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("sqlite".to_string()),
        mode,
        ..Default::default()
    });
    ds
}

type QueryTests<'a> = Vec<(&'a str, Vec<&'a str>, Option<Box<ValidateFn>>)>;

fn decimal_queries() -> QueryTests<'static> {
    vec![
    ("SELECT SUM(small_decimal), SUM(medium_decimal), SUM(large_decimal), SUM(precise_decimal) FROM decimal", vec![
        "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        "| plan_type     | plan                                                                                                                                                              |",
        "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        "| logical_plan  | Aggregate: groupBy=[[]], aggr=[[sum(decimal.small_decimal), sum(decimal.medium_decimal), sum(decimal.large_decimal), sum(decimal.precise_decimal)]]               |",
        "|               |   BytesProcessedNode                                                                                                                                              |",
        "|               |     TableScan: decimal projection=[small_decimal, medium_decimal, large_decimal, precise_decimal]                                                                 |",
        "| physical_plan | AggregateExec: mode=Final, gby=[], aggr=[sum(decimal.small_decimal), sum(decimal.medium_decimal), sum(decimal.large_decimal), sum(decimal.precise_decimal)]       |",
        "|               |   CoalescePartitionsExec                                                                                                                                          |",
        "|               |     AggregateExec: mode=Partial, gby=[], aggr=[sum(decimal.small_decimal), sum(decimal.medium_decimal), sum(decimal.large_decimal), sum(decimal.precise_decimal)] |",
        "|               |       BytesProcessedExec                                                                                                                                          |",
        "|               |         SchemaCastScanExec                                                                                                                                        |",
        "|               |           RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                    |",
        "|               |             SQLiteSqlExec sql=SELECT \"small_decimal\", \"medium_decimal\", \"large_decimal\", \"precise_decimal\" FROM decimal                                           |",
        "|               |                                                                                                                                                                   |",
        "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
    ], Some(Box::new(
        |results: Vec<RecordBatch>| {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].num_columns(), 4);
            assert_eq!(results[0].num_rows(), 1);
            assert_eq!(results[0].column(0).as_any().downcast_ref::<Decimal128Array>().expect("decimal array").value(0).to_string(), "22381");
            let schema = results[0].schema();

            // small_decimal
            let DataType::Decimal128(precision, scale) = schema.field(0).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = results[0].column(0).as_any().downcast_ref::<Decimal128Array>().expect("decimal array");
            assert_eq!(Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale), "223.81");

            // medium_decimal
            let DataType::Decimal128(precision, scale) = schema.field(1).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = results[0].column(1).as_any().downcast_ref::<Decimal128Array>().expect("decimal array");
            assert_eq!(Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale), "186109.5051");

            // large_decimal
            let DataType::Decimal128(precision, scale) = schema.field(2).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = results[0].column(2).as_any().downcast_ref::<Decimal128Array>().expect("decimal array");
            assert_eq!(Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale), "10866582.506250");

            // precise_decimal
            let DataType::Decimal128(precision, scale) = schema.field(3).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = results[0].column(3).as_any().downcast_ref::<Decimal128Array>().expect("decimal array");
            assert_eq!(Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale), "-1.7443152324");
        }
    )))]
}

#[tokio::test]
async fn test_sqlite_decimal_memory() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);

    let app = AppBuilder::new("test_sqlite_decimal_memory")
        .with_dataset(make_sqlite_decimal_dataset(Mode::Memory))
        .build();

    let df = get_test_datafusion();

    let mut rt = Runtime::builder()
        .with_app(app)
        .with_datafusion(df)
        .build()
        .await;

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = rt.load_components() => {}
    }

    dataset_ready_check(&rt, "SELECT * FROM decimal LIMIT 1").await;

    for (query, expected_plan, validate_result) in decimal_queries() {
        run_query_and_check_results(&mut rt, query, &expected_plan, validate_result)
            .await
            .expect("query to succeed");
    }

    Ok(())
}

#[tokio::test]
async fn test_sqlite_decimal_file() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);

    let app = AppBuilder::new("test_sqlite_decimal_file")
        .with_dataset(make_sqlite_decimal_dataset(Mode::File))
        .build();

    let df = get_test_datafusion();

    let mut rt = Runtime::builder()
        .with_app(app)
        .with_datafusion(df)
        .build()
        .await;

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = rt.load_components() => {}
    }

    dataset_ready_check(&rt, "SELECT * FROM decimal LIMIT 1").await;

    for (query, expected_plan, validate_result) in decimal_queries() {
        run_query_and_check_results(&mut rt, query, &expected_plan, validate_result)
            .await
            .expect("query to succeed");
    }

    // Clean up files
    let dir_path = "./.spice";
    if std::path::Path::new(dir_path).exists() {
        std::fs::remove_dir_all(dir_path).expect("Failed to remove directory");
    }

    Ok(())
}
