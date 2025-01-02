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

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::ArrayRef;
use arrow::datatypes::DecimalType;
use arrow::{
    array::{Decimal128Array, RecordBatch},
    datatypes::{DataType, Decimal128Type},
};
use runtime::{status, Runtime};
use spicepod::component::dataset::{
    acceleration::{Acceleration, Mode},
    Dataset,
};

use crate::{
    get_test_datafusion, init_tracing, run_query_and_check_results,
    run_query_and_check_results_with_plan_checks,
    utils::{runtime_ready_check, test_request_context},
    PlanCheckFn, ValidateFn,
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

enum CheckFunction {
    ValidateFullPlan(String),
    ValidateSubPlan(Vec<(&'static str, PlanCheckFn)>),
}

type QueryTests<'a> = Vec<(&'a str, CheckFunction, Option<Box<ValidateFn>>)>;

#[derive(Debug, Copy, Clone)]
enum DecimalQuery {
    Federated,
    NonFederated,
}

fn decimal_queries(snapshot_name: &str, query_type: DecimalQuery) -> QueryTests<'static> {
    let expected_plan: CheckFunction = match query_type {
        DecimalQuery::Federated => CheckFunction::ValidateSubPlan(vec![(
            "VirtualExecutionPlan",
            Box::new(|plan| {
                plan.contains("sql=SELECT sum(\"decimal\".small_decimal), sum(\"decimal\".medium_decimal), sum(\"decimal\".large_decimal), sum(\"decimal\".precise_decimal) FROM \"decimal\" rewritten_sql=SELECT sum(`decimal`.`small_decimal`), sum(`decimal`.`medium_decimal`), sum(`decimal`.`large_decimal`), sum(`decimal`.`precise_decimal`) FROM `decimal`")
            }),
        )]),
        DecimalQuery::NonFederated => {
            CheckFunction::ValidateFullPlan(format!("{snapshot_name}_non_federated"))
        }
    };
    vec![
    ("SELECT SUM(small_decimal), SUM(medium_decimal), SUM(large_decimal), SUM(precise_decimal) FROM decimal", expected_plan, Some(Box::new(
        |results: Vec<RecordBatch>| {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].num_columns(), 4);
            assert_eq!(results[0].num_rows(), 1);
            assert_eq!(downcast_decimal_array(results[0].column(0)).value(0).to_string(), "22381");
            let schema = results[0].schema();

            // small_decimal
            let DataType::Decimal128(precision, scale) = schema.field(0).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = downcast_decimal_array(results[0].column(0));
            assert_eq!(Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale), "223.81");

            // medium_decimal
            let DataType::Decimal128(precision, scale) = schema.field(1).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = downcast_decimal_array(results[0].column(1));
            assert_eq!(Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale), "186109.5051");

            // large_decimal
            let DataType::Decimal128(precision, scale) = schema.field(2).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = downcast_decimal_array(results[0].column(2));
            assert_eq!(Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale), "10866582.506250");

            // precise_decimal
            let DataType::Decimal128(precision, scale) = schema.field(3).data_type() else {
                panic!("Expected decimal type");
            };
            let decimal_array = downcast_decimal_array(results[0].column(3));
            assert_eq!(Decimal128Type::format_decimal(decimal_array.value(0), *precision, *scale), "-1.7443152324");
        }
    )))]
}

fn downcast_decimal_array(array: &ArrayRef) -> &Decimal128Array {
    match array.as_any().downcast_ref::<Decimal128Array>() {
        Some(array) => array,
        None => panic!("Expected decimal array"),
    }
}

#[tokio::test]
async fn test_sqlite_decimal_memory() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("test_sqlite_decimal_memory")
                .with_dataset(make_sqlite_decimal_dataset(Mode::Memory))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let mut rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .with_runtime_status(status)
                .build()
                .await;

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            for (query, check_function, validate_result) in
                decimal_queries("test_sqlite_decimal_memory", DecimalQuery::NonFederated)
            {
                match check_function {
                    CheckFunction::ValidateFullPlan(snapshot_name) => {
                        run_query_and_check_results(
                            &mut rt,
                            &snapshot_name,
                            query,
                            true,
                            validate_result,
                        )
                        .await
                    }
                    CheckFunction::ValidateSubPlan(plan_checks) => {
                        run_query_and_check_results_with_plan_checks(
                            &mut rt,
                            query,
                            plan_checks,
                            validate_result,
                        )
                        .await
                    }
                }
                .expect("query to succeed");
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_sqlite_decimal_file() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("test_sqlite_decimal_file")
                .with_dataset(make_sqlite_decimal_dataset(Mode::File))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let mut rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .with_runtime_status(status)
                .build()
                .await;

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            for (query, check_function, validate_result) in
                decimal_queries("test_sqlite_decimal_file", DecimalQuery::Federated)
            {
                match check_function {
                    CheckFunction::ValidateFullPlan(snapshot_name) => {
                        run_query_and_check_results(
                            &mut rt,
                            &snapshot_name,
                            query,
                            true,
                            validate_result,
                        )
                        .await
                    }
                    CheckFunction::ValidateSubPlan(plan_checks) => {
                        run_query_and_check_results_with_plan_checks(
                            &mut rt,
                            query,
                            plan_checks,
                            validate_result,
                        )
                        .await
                    }
                }
                .expect("query to succeed");
            }

            // Clean up files
            let dir_path = "./.spice";
            if std::path::Path::new(dir_path).exists() {
                std::fs::remove_dir_all(dir_path).expect("Failed to remove directory");
            }

            Ok(())
        })
        .await
}
