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

use crate::results::BenchmarkResultsBuilder;
use spicepod::component::{
    dataset::{acceleration::Mode, Dataset},
    params::Params,
};

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    engine: Option<String>,
    mode: Option<Mode>,
) -> Result<(), String> {
    let test_queries = get_test_queries();

    let bench_name = match mode {
        Some(mode) => format!("s3_{}_{}", engine.unwrap_or_default(), mode).to_lowercase(),
        None => "s3".to_string(),
    };

    let mut errors = Vec::new();

    for (query_name, query) in test_queries {
        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            bench_name.as_str(),
            query_name,
            query,
        )
        .await
        {
            errors.push(format!("Query {query_name} failed with error: {e}"));
        }
    }

    if !errors.is_empty() {
        tracing::error!("There are failed queries:\n{}", errors.join("\n"));
    }

    Ok(())
}

pub fn build_app(app_builder: AppBuilder) -> AppBuilder {
    app_builder
        .with_dataset(make_dataset(
            "spiceai-demo-datasets/tpch/customer/",
            "customer",
        ))
        .with_dataset(make_dataset(
            "spiceai-demo-datasets/tpch/lineitem/",
            "lineitem",
        ))
        .with_dataset(make_dataset("spiceai-demo-datasets/tpch/part/", "part"))
        .with_dataset(make_dataset(
            "spiceai-demo-datasets/tpch/partsupp/",
            "partsupp",
        ))
        .with_dataset(make_dataset("spiceai-demo-datasets/tpch/orders/", "orders"))
        .with_dataset(make_dataset("spiceai-demo-datasets/tpch/nation/", "nation"))
        .with_dataset(make_dataset("spiceai-demo-datasets/tpch/region/", "region"))
        .with_dataset(make_dataset(
            "spiceai-demo-datasets/tpch/supplier/",
            "supplier",
        ))
}

fn make_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("s3://{path}"), name.to_string());
    dataset.params = Some(Params::from_string_map(
        vec![("file_format".to_string(), "parquet".to_string())]
            .into_iter()
            .collect(),
    ));
    dataset
}

fn get_test_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("tpch_q1", include_str!("../queries/tpch_q1.sql")),
        ("tpch_q2", include_str!("../queries/tpch_q2.sql")),
        ("tpch_q3", include_str!("../queries/tpch_q3.sql")),
        ("tpch_q4", include_str!("../queries/tpch_q4.sql")),
        ("tpch_q5", include_str!("../queries/tpch_q5.sql")),
        ("tpch_q6", include_str!("../queries/tpch_q6.sql")),
        ("tpch_q7", include_str!("../queries/tpch_q7.sql")),
        ("tpch_q8", include_str!("../queries/tpch_q8.sql")),
        ("tpch_q9", include_str!("../queries/tpch_q9.sql")),
        ("tpch_q10", include_str!("../queries/tpch_q10.sql")),
        ("tpch_q11", include_str!("../queries/tpch_q11.sql")),
        ("tpch_q12", include_str!("../queries/tpch_q12.sql")),
        ("tpch_q13", include_str!("../queries/tpch_q13.sql")),
        ("tpch_q14", include_str!("../queries/tpch_q14.sql")),
        // tpch_q15 has a view creation which we don't support by design
        ("tpch_q16", include_str!("../queries/tpch_q16.sql")),
        ("tpch_q17", include_str!("../queries/tpch_q17.sql")),
        ("tpch_q18", include_str!("../queries/tpch_q18.sql")),
        ("tpch_q19", include_str!("../queries/tpch_q19.sql")),
        ("tpch_q20", include_str!("../queries/tpch_q20.sql")),
        ("tpch_q21", include_str!("../queries/tpch_q21.sql")),
        ("tpch_q22", include_str!("../queries/tpch_q22.sql")),
        (
            "tpch_simple_q1",
            include_str!("../queries/tpch_simple_q1.sql"),
        ),
        (
            "tpch_simple_q2",
            include_str!("../queries/tpch_simple_q2.sql"),
        ),
        (
            "tpch_simple_q3",
            include_str!("../queries/tpch_simple_q3.sql"),
        ),
        (
            "tpch_simple_q4",
            include_str!("../queries/tpch_simple_q4.sql"),
        ),
        (
            "tpch_simple_q5",
            include_str!("../queries/tpch_simple_q5.sql"),
        ),
        (
            "tpch_simple_q6",
            include_str!("../queries/tpch_simple_q6.sql"),
        ),
        (
            "tpch_simple_q7",
            include_str!("../queries/tpch_simple_q7.sql"),
        ),
    ]
}
