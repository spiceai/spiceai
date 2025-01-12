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

use crate::{
    results::BenchmarkResultsBuilder,
    utils::{get_branch_name, get_commit_sha, init_tracing, runtime_ready_check},
};
use app::{App, AppBuilder};
use datafusion::{prelude::SessionContext, sql::TableReference};
use runtime::{
    datafusion::DataFusion,
    dataupdate::DataUpdate,
    status::{self, RuntimeStatus},
    Runtime,
};
use spicepod::component::{
    dataset::{
        acceleration::{Acceleration, IndexType, ZeroResultsAction},
        replication::Replication,
        Dataset, Mode,
    },
    runtime::ResultsCache,
};
use std::{collections::HashMap, sync::Arc};
/// The number of times to run each query in the benchmark.
const ITERATIONS: i32 = 5;

/// Gets a test `DataFusion` to make test results reproducible across all machines.
///
/// 1) Sets the number of `target_partitions` to 4, by default its the number of CPU cores available.
fn get_test_datafusion(status: Arc<RuntimeStatus>) -> Arc<DataFusion> {
    let mut df = DataFusion::builder(status).build();

    // Set the target partitions to 3 to make RepartitionExec show consistent partitioning across machines with different CPU counts.
    let mut new_state = df.ctx.state();
    new_state
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = 4;
    let new_ctx = SessionContext::new_with_state(new_state);

    // Replace the old context with the modified one
    df.ctx = new_ctx.into();
    Arc::new(df)
}

pub(crate) async fn setup_benchmark(
    upload_results_dataset: Option<&String>,
    connector: &str,
    acceleration: Option<Acceleration>,
    bench_name: &str,
) -> Result<(BenchmarkResultsBuilder, Runtime), String> {
    init_tracing(None);

    let app = build_app(upload_results_dataset, connector, acceleration, bench_name)?;

    let status = status::RuntimeStatus::new();
    let rt = Runtime::builder()
        .with_app(app)
        .with_datafusion(get_test_datafusion(Arc::clone(&status)))
        .with_runtime_status(status)
        .build()
        .await;

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(20 * 60)) => { // Databricks can take awhile to start up
            panic!("Timed out waiting for datasets to load in setup_benchmark()");
        }
        () = rt.load_components() => {}
    }

    let wait_time = match bench_name {
        "clickbench" => std::time::Duration::from_secs(3 * 60 * 60),
        _ => std::time::Duration::from_secs(10 * 60),
    };

    runtime_ready_check(&rt, wait_time).await;

    let benchmark_results =
        BenchmarkResultsBuilder::new(get_commit_sha(), get_branch_name(), ITERATIONS);

    Ok((benchmark_results, rt))
}

pub(crate) async fn write_benchmark_results(
    benchmark_results: DataUpdate,
    rt: &Runtime,
) -> Result<(), String> {
    rt.datafusion()
        .write_data(
            &TableReference::parse_str("oss_benchmarks"),
            benchmark_results,
        )
        .await
        .map_err(|e| e.to_string())
}

fn build_app(
    upload_results_dataset: Option<&String>,
    connector: &str,
    acceleration: Option<Acceleration>,
    bench_name: &str,
) -> Result<App, String> {
    let mut app_builder =
        AppBuilder::new("runtime_benchmark_test").with_results_cache(ResultsCache {
            enabled: false,
            cache_max_size: None,
            item_ttl: None,
            eviction_policy: None,
        });

    app_builder = match connector {
        "spice.ai" => crate::bench_spicecloud::build_app(app_builder, bench_name),
        // Run both S3, ABFS and any other object store benchmarks
        "s3" | "abfs" | "file" => {
            // SQLite acceleration does not support default TPC-DS source scale so we use a smaller scale
            if bench_name == "tpcds"
                && acceleration
                    .as_ref()
                    .is_some_and(|a| a.engine == Some("sqlite".to_string()))
            {
                crate::bench_object_store::build_app(
                    connector,
                    app_builder,
                    "tpcds_sf0_01",
                    acceleration.clone(),
                )
            } else {
                crate::bench_object_store::build_app(
                    connector,
                    app_builder,
                    bench_name,
                    acceleration.clone(),
                )
            }
        }
        #[cfg(feature = "spark")]
        "spark" => crate::bench_spark::build_app(app_builder, bench_name),
        #[cfg(feature = "postgres")]
        "postgres" => crate::bench_postgres::build_app(app_builder, bench_name),
        #[cfg(feature = "mysql")]
        "mysql" => crate::bench_mysql::build_app(app_builder, bench_name),
        #[cfg(feature = "duckdb")]
        "duckdb" => crate::bench_duckdb::build_app(app_builder, bench_name),
        #[cfg(feature = "odbc")]
        "odbc-databricks" => crate::bench_odbc_databricks::build_app(app_builder, bench_name),
        #[cfg(feature = "odbc")]
        "odbc-athena" => Ok(crate::bench_odbc_athena::build_app(app_builder)),
        #[cfg(feature = "delta_lake")]
        "delta_lake" => crate::bench_delta::build_app(app_builder, bench_name),
        #[cfg(feature = "mssql")]
        "mssql" => crate::bench_mssql::build_app(app_builder, bench_name),
        #[cfg(feature = "dremio")]
        "dremio" => crate::bench_dremio::build_app(app_builder, bench_name),
        _ => Err(format!("Unknown connector: {connector}")),
    }?;

    if let Some(upload_results_dataset) = upload_results_dataset {
        app_builder = app_builder.with_dataset(make_spiceai_rw_dataset(
            upload_results_dataset,
            "oss_benchmarks",
        ));
    }

    let mut app = app_builder.build();

    if let Some(accel) = acceleration {
        app.datasets.iter_mut().for_each(|ds| {
            let mut accel = accel.clone();
            let indexes = get_accelerator_indexes(accel.engine.clone(), &ds.name, bench_name);
            accel.refresh_sql = get_accelerator_refresh_sql(&accel, &ds.name, bench_name);
            if let Some(indexes) = indexes {
                accel.indexes = indexes;
            }
            if ds.name != "oss_benchmarks" {
                ds.acceleration = Some(accel);
            }
        });
    }

    Ok(app)
}

fn get_accelerator_refresh_sql(
    acceleration: &Acceleration,
    dataset: &str,
    bench_name: &str,
) -> Option<String> {
    match (
        acceleration.engine.as_deref(),
        &acceleration.on_zero_results,
        bench_name,
    ) {
        (Some("sqlite" | "postgres"), &ZeroResultsAction::ReturnEmpty, "clickbench") => {
            // SQLite has troubles loading the whole ClickBench set with indexes enabled
            // remove this refresh SQL when we support index creation after table load.
            //
            // Postgres also can't load the full dataset within the 3 hour time limit
            Some(format!("SELECT * FROM {dataset} LIMIT 10000000"))
        }
        (_, &ZeroResultsAction::UseSource, _) => Some(format!("SELECT * FROM {dataset} LIMIT 0")),
        _ => None,
    }
}

#[allow(clippy::too_many_lines)]
fn get_accelerator_indexes(
    engine: Option<String>,
    dataset: &str,
    bench_name: &str,
) -> Option<HashMap<String, IndexType>> {
    if let Some(engine) = engine {
        match engine.as_str() {
            "sqlite" => match bench_name {
                "tpch" => match dataset {
                    "orders" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("o_orderdate".to_string(), IndexType::Enabled);
                        indexes.insert("o_orderkey".to_string(), IndexType::Enabled);
                        indexes.insert("o_custkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "lineitem" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("l_orderkey".to_string(), IndexType::Enabled);
                        indexes.insert("l_suppkey".to_string(), IndexType::Enabled);
                        indexes.insert("l_discount".to_string(), IndexType::Enabled);
                        indexes.insert("l_shipdate".to_string(), IndexType::Enabled);
                        indexes.insert("l_partkey".to_string(), IndexType::Enabled);
                        indexes.insert("l_quantity".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "partsupp" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ps_suppkey".to_string(), IndexType::Enabled);
                        indexes.insert("ps_partkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "part" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("p_partkey".to_string(), IndexType::Enabled);
                        indexes.insert("p_brand".to_string(), IndexType::Enabled);
                        indexes.insert("p_container".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "nation" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("n_nationkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "supplier" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("s_suppkey".to_string(), IndexType::Enabled);
                        indexes.insert("s_nationkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "customer" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("c_phone".to_string(), IndexType::Enabled);
                        indexes.insert("c_acctbal".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    _ => None,
                },
                "tpcds" => match dataset {
                    "catalog_sales" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("cs_ship_customer_sk".to_string(), IndexType::Enabled);
                        indexes.insert("cs_sold_date_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "customer_address" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ca_county".to_string(), IndexType::Enabled);
                        indexes.insert(
                            "(ca_address_sk, ca_country, ca_state)".to_string(),
                            IndexType::Enabled,
                        );
                        Some(indexes)
                    }
                    "customer_demographics" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert(
                            "(cd_demo_sk, cd_marital_status, cd_education_status)".to_string(),
                            IndexType::Enabled,
                        );
                        Some(indexes)
                    }
                    "date_dim" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("(d_year, d_date_sk)".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "household_demographics" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes
                            .insert("(hd_demo_sk, hd_dep_count)".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "store_sales" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ss_customer_sk".to_string(), IndexType::Enabled);
                        indexes.insert("ss_sold_date_sk".to_string(), IndexType::Enabled);
                        indexes.insert(
                            "(ss_store_sk, ss_sold_date_sk, ss_sales_price, ss_net_profit)"
                                .to_string(),
                            IndexType::Enabled,
                        );
                        Some(indexes)
                    }
                    "web_sales" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ws_bill_customer_sk".to_string(), IndexType::Enabled);
                        indexes.insert("ws_sold_date_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    _ => None,
                },
                "clickbench" => match dataset {
                    "hits" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes
                            .insert("(ClientIP, SearchEngineID)".to_string(), IndexType::Enabled);
                        indexes.insert("(ClientIP, WatchID)".to_string(), IndexType::Enabled);
                        indexes.insert(
                            "(MobilePhone, MobilePhoneModel)".to_string(),
                            IndexType::Enabled,
                        );
                        indexes.insert(
                            "(SearchEngineID, SearchPhrase)".to_string(),
                            IndexType::Enabled,
                        );
                        indexes.insert("(SearchPhrase, UserID)".to_string(), IndexType::Enabled);
                        indexes.insert("AdvEngineID".to_string(), IndexType::Enabled);
                        indexes.insert("MobilePhoneModel".to_string(), IndexType::Enabled);
                        indexes.insert("SearchPhrase".to_string(), IndexType::Enabled);
                        indexes.insert("UserID".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    _ => None,
                },
                _ => None,
            },
            "postgres" => match bench_name {
                "tpch" => match dataset {
                    "partsupp" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ps_partkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "part" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("p_partkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "lineitem" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("l_partkey".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    _ => None,
                },
                "tpcds" => match dataset {
                    "store_sales" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("ss_item_sk".to_string(), IndexType::Enabled);
                        indexes.insert("ss_store_sk".to_string(), IndexType::Enabled);
                        indexes.insert("ss_customer_sk".to_string(), IndexType::Enabled);
                        indexes.insert("ss_ticket_number".to_string(), IndexType::Enabled);
                        indexes.insert("ss_hdemo_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "store" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("s_store_name".to_string(), IndexType::Enabled);
                        indexes.insert("s_zip".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "item" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("i_item_sk".to_string(), IndexType::Enabled);
                        indexes.insert("i_manufact_id".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "customer" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("c_current_addr_sk".to_string(), IndexType::Enabled);
                        indexes.insert("c_customer_sk".to_string(), IndexType::Enabled);
                        indexes.insert("c_current_hdemo_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "catalog_sales" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("cs_sold_date_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "date_dim" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("d_year".to_string(), IndexType::Enabled);
                        indexes.insert("d_date".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    "customer_demographics" => {
                        let mut indexes: HashMap<String, IndexType> = HashMap::new();
                        indexes.insert("cd_demo_sk".to_string(), IndexType::Enabled);
                        Some(indexes)
                    }
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    } else {
        None
    }
}

fn make_spiceai_rw_dataset(path: &str, name: &str) -> Dataset {
    let mut ds = Dataset::new(format!("spice.ai:{path}"), name.to_string());
    ds.mode = Mode::ReadWrite;
    ds.replication = Some(Replication { enabled: true });
    ds
}

#[macro_export]
macro_rules! generate_tpcds_queries {
    ( $( $i:literal ),* ) => {
        vec![
            $(
                (
                    concat!("tpcds_q", stringify!($i)),
                    include_str!(concat!("../queries/tpcds/q", stringify!($i), ".sql"))
                )
            ),*
        ]
    }
}

#[macro_export]
macro_rules! generate_tpch_queries {
    ( $( $i:tt ),* ) => {
        vec![
            $(
                (
                    concat!("tpch_", stringify!($i)),
                    include_str!(concat!("../queries/tpch/", stringify!($i), ".sql"))
                )
            ),*
        ]
    }
}
