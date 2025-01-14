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

use crate::results::BenchmarkResultsBuilder;
use spicepod::component::{dataset::Dataset, params::Params};
use test_framework::queries::{get_tpch_test_queries, QueryOverrides};

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
) -> Result<(), String> {
    let test_queries = get_tpch_test_queries(Some(QueryOverrides::ODBCAthena));
    let mut errors = Vec::new();

    for (query_name, query) in test_queries {
        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            "odbc-athena",
            query_name,
            query,
            false,
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
        .with_dataset(make_dataset("tpch.customer", "customer"))
        .with_dataset(make_dataset("tpch.lineitem", "lineitem"))
        .with_dataset(make_dataset("tpch.part", "part"))
        .with_dataset(make_dataset("tpch.partsupp", "partsupp"))
        .with_dataset(make_dataset("tpch.orders", "orders"))
        .with_dataset(make_dataset("tpch.nation", "nation"))
        .with_dataset(make_dataset("tpch.region", "region"))
        .with_dataset(make_dataset("tpch.supplier", "supplier"))
}

fn make_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("odbc:{path}"), name.to_string());
    let connection_string = "Driver={Amazon Athena ODBC Driver};Catalog=AwsDataCatalog;AwsRegion=us-east-2;Schema=tpch;Workgroup=primary;S3OutputLocation=s3://aws-athena-query-results-211125479522-us-east-2/;AuthenticationType=IAM Credentials;UID=${ env:AWS_ACCESS_KEY_ID };PWD=${ env:AWS_SECRET_ACCESS_KEY };".to_string();

    dataset.params = Some(Params::from_string_map(
        vec![("odbc_connection_string".to_string(), connection_string)]
            .into_iter()
            .collect(),
    ));
    dataset
}
