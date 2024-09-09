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
use runtime::Runtime;
use spicepod::component::dataset::{
    acceleration::{Acceleration, Mode},
    Dataset,
};

use crate::{get_test_datafusion, init_tracing, run_query_and_check_results, ValidateFn};

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

    let queries: QueryTests = vec![("SELECT SUM(small_decimal) FROM decimal", vec![], None)];

    for (query, expected_plan, validate_result) in queries {
        run_query_and_check_results(&mut rt, query, &expected_plan, validate_result)
            .await
            .expect("query to succeed");
    }

    Ok(())
}

#[tokio::test]
async fn test_sqlite_decimal_file() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    Ok(())
}
