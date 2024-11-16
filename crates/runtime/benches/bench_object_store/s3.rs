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

use spicepod::component::{dataset::Dataset, params::Params};

#[allow(clippy::too_many_lines)]
pub fn build_app(app_builder: AppBuilder, bench_name: &str) -> Result<AppBuilder, String> {
    match bench_name {
        "tpch" => Ok(app_builder
            .with_dataset(make_dataset(
                "spiceai-demo-datasets/tpch/customer/",
                "customer",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-demo-datasets/tpch/lineitem/",
                "lineitem",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-demo-datasets/tpch/part/",
                "part",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-demo-datasets/tpch/partsupp/",
                "partsupp",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-demo-datasets/tpch/orders/",
                "orders",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-demo-datasets/tpch/nation/",
                "nation",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-demo-datasets/tpch/region/",
                "region",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-demo-datasets/tpch/supplier/",
                "supplier",
                bench_name,
            ))),
        "tpcds" => Ok(app_builder
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/call_center/",
                "call_center",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/catalog_page/",
                "catalog_page",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/catalog_sales/",
                "catalog_sales",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/catalog_returns/",
                "catalog_returns",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/income_band/",
                "income_band",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/inventory/",
                "inventory",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/store_sales/",
                "store_sales",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/store_returns/",
                "store_returns",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/web_sales/",
                "web_sales",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/web_returns/",
                "web_returns",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/customer/",
                "customer",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/customer_address/",
                "customer_address",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/customer_demographics/",
                "customer_demographics",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/date_dim/",
                "date_dim",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/household_demographics/",
                "household_demographics",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/item/",
                "item",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/promotion/",
                "promotion",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/reason/",
                "reason",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/ship_mode/",
                "ship_mode",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/store/",
                "store",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/time_dim/",
                "time_dim",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/warehouse/",
                "warehouse",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/web_page/",
                "web_page",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/web_site/",
                "web_site",
                bench_name,
            ))),
        "tpcds_sf0_01" => Ok(app_builder
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/call_center.parquet",
                "call_center",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/catalog_page.parquet",
                "catalog_page",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/catalog_sales.parquet",
                "catalog_sales",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/catalog_returns.parquet",
                "catalog_returns",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/income_band.parquet",
                "income_band",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/inventory.parquet",
                "inventory",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/store_sales.parquet",
                "store_sales",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/store_returns.parquet",
                "store_returns",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/web_sales.parquet",
                "web_sales",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/web_returns.parquet",
                "web_returns",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/customer.parquet",
                "customer",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/customer_address.parquet",
                "customer_address",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/customer_demographics.parquet",
                "customer_demographics",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/date_dim.parquet",
                "date_dim",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/household_demographics.parquet",
                "household_demographics",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/item.parquet",
                "item",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/promotion.parquet",
                "promotion",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/reason.parquet",
                "reason",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/ship_mode.parquet",
                "ship_mode",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/store.parquet",
                "store",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/time_dim.parquet",
                "time_dim",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/warehouse.parquet",
                "warehouse",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/web_page.parquet",
                "web_page",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds_sf0_01/web_site.parquet",
                "web_site",
                bench_name,
            ))),

        "clickbench" => Ok(app_builder.with_dataset(make_dataset(
            "benchmarks/clickbench/hits/",
            "hits",
            bench_name,
        ))),
        _ => Err("Only tpcds or tpch benchmark suites are supported".to_string()),
    }
}

fn make_dataset(path: &str, name: &str, bench_name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("s3://{path}"), name.to_string());

    let params: Vec<(String, String)> = match bench_name {
        "clickbench" => vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "3h".to_string()),
            ("allow_http".to_string(), "true".to_string()),
            ("s3_auth".to_string(), "key".to_string()),
            (
                "s3_endpoint".to_string(),
                std::env::var("CLICKBENCH_S3_ENDPOINT").unwrap_or_default(),
            ),
            (
                "s3_key".to_string(),
                std::env::var("CLICKBENCH_S3_KEY").unwrap_or_default(),
            ),
            (
                "s3_secret".to_string(),
                std::env::var("CLICKBENCH_S3_SECRET").unwrap_or_default(),
            ),
        ],
        _ => vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "2m".to_string()),
        ],
    };

    dataset.params = Some(Params::from_string_map(params.into_iter().collect()));
    dataset
}
