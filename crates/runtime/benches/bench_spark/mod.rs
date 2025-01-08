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

use app::AppBuilder;
use runtime::Runtime;
use spicepod::component::dataset::Dataset;
use test_framework::queries::{get_tpch_test_queries, QueryOverrides};

use crate::results::BenchmarkResultsBuilder;

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
) -> Result<(), String> {
    let test_queries = get_tpch_test_queries(Some(QueryOverrides::Spark));

    let mut errors = Vec::new();
    for (query_name, query) in test_queries {
        let verify_query_results = query_name.starts_with("tpch_q");
        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            "spark",
            query_name,
            query,
            verify_query_results,
        )
        .await
        {
            errors.push(format!("Query {query_name} failed with error: {e}"));
        };
    }

    if !errors.is_empty() {
        tracing::error!("There are failed queries:\n{}", errors.join("\n"));
    }

    Ok(())
}

pub fn build_app(app_builder: AppBuilder) -> AppBuilder {
    app_builder
        .with_dataset(make_spark_dataset(
            "spiceai_sandbox.tpch.customer",
            "customer",
        ))
        .with_dataset(make_spark_dataset(
            "spiceai_sandbox.tpch.lineitem",
            "lineitem",
        ))
        .with_dataset(make_spark_dataset("spiceai_sandbox.tpch.part", "part"))
        .with_dataset(make_spark_dataset(
            "spiceai_sandbox.tpch.partsupp",
            "partsupp",
        ))
        .with_dataset(make_spark_dataset("spiceai_sandbox.tpch.orders", "orders"))
        .with_dataset(make_spark_dataset("spiceai_sandbox.tpch.nation", "nation"))
        .with_dataset(make_spark_dataset("spiceai_sandbox.tpch.region", "region"))
        .with_dataset(make_spark_dataset(
            "spiceai_sandbox.tpch.supplier",
            "supplier",
        ))
}

fn make_spark_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("spark:{path}"), name.to_string())
}
