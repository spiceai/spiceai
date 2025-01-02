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

use spicepod::component::{dataset::Dataset, params::Params};

#[allow(clippy::too_many_lines)]
pub fn build_app(app_builder: AppBuilder, bench_name: &str) -> Result<AppBuilder, String> {
    match bench_name {
        "tpch" => Ok(app_builder
            .with_dataset(make_dataset("benchmarks/tpch_sf1/customer/", "customer"))
            .with_dataset(make_dataset("benchmarks/tpch_sf1/lineitem/", "lineitem"))
            .with_dataset(make_dataset("benchmarks/tpch_sf1/part/", "part"))
            .with_dataset(make_dataset("benchmarks/tpch_sf1/partsupp/", "partsupp"))
            .with_dataset(make_dataset("benchmarks/tpch_sf1/orders/", "orders"))
            .with_dataset(make_dataset("benchmarks/tpch_sf1/nation/", "nation"))
            .with_dataset(make_dataset("benchmarks/tpch_sf1/region/", "region"))
            .with_dataset(make_dataset("benchmarks/tpch_sf1/supplier/", "supplier"))),
        "tpcds" => Ok(app_builder
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf1/call_center/",
                "call_center",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf1/catalog_page/",
                "catalog_page",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf1/catalog_sales/",
                "catalog_sales",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf1/catalog_returns/",
                "catalog_returns",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf1/income_band/",
                "income_band",
            ))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/inventory/", "inventory"))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf1/store_sales/",
                "store_sales",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf1/store_returns/",
                "store_returns",
            ))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/web_sales/", "web_sales"))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf1/web_returns/",
                "web_returns",
            ))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/customer/", "customer"))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf1/customer_address/",
                "customer_address",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf1/customer_demographics/",
                "customer_demographics",
            ))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/date_dim/", "date_dim"))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf1/household_demographics/",
                "household_demographics",
            ))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/item/", "item"))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/promotion/", "promotion"))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/reason/", "reason"))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/ship_mode/", "ship_mode"))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/store/", "store"))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/time_dim/", "time_dim"))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/warehouse/", "warehouse"))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/web_page/", "web_page"))
            .with_dataset(make_dataset("benchmarks/tpcds_sf1/web_site/", "web_site"))),
        "tpcds_sf0_01" => Ok(app_builder
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/call_center.parquet",
                "call_center",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/catalog_page.parquet",
                "catalog_page",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/catalog_sales.parquet",
                "catalog_sales",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/catalog_returns.parquet",
                "catalog_returns",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/income_band.parquet",
                "income_band",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/inventory.parquet",
                "inventory",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/store_sales.parquet",
                "store_sales",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/store_returns.parquet",
                "store_returns",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/web_sales.parquet",
                "web_sales",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/web_returns.parquet",
                "web_returns",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/customer.parquet",
                "customer",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/customer_address.parquet",
                "customer_address",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/customer_demographics.parquet",
                "customer_demographics",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/date_dim.parquet",
                "date_dim",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/household_demographics.parquet",
                "household_demographics",
            ))
            .with_dataset(make_dataset("benchmarks/tpcds_sf0_01/item.parquet", "item"))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/promotion.parquet",
                "promotion",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/reason.parquet",
                "reason",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/ship_mode.parquet",
                "ship_mode",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/store.parquet",
                "store",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/time_dim.parquet",
                "time_dim",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/warehouse.parquet",
                "warehouse",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/web_page.parquet",
                "web_page",
            ))
            .with_dataset(make_dataset(
                "benchmarks/tpcds_sf0_01/web_site.parquet",
                "web_site",
            ))),

        "clickbench" => {
            Ok(app_builder.with_dataset(make_dataset("benchmarks/clickbench/hits/", "hits")))
        }
        _ => Err("Only tpcds or tpch benchmark suites are supported".to_string()),
    }
}

fn make_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("s3://{path}"), name.to_string());

    let params: Vec<(String, String)> = vec![
        ("file_format".to_string(), "parquet".to_string()),
        ("client_timeout".to_string(), "3h".to_string()),
        ("allow_http".to_string(), "true".to_string()),
        ("s3_auth".to_string(), "key".to_string()),
        (
            "s3_endpoint".to_string(),
            std::env::var("S3_ENDPOINT").unwrap_or_default(),
        ),
        (
            "s3_key".to_string(),
            std::env::var("S3_KEY").unwrap_or_default(),
        ),
        (
            "s3_secret".to_string(),
            std::env::var("S3_SECRET").unwrap_or_default(),
        ),
    ];

    dataset.params = Some(Params::from_string_map(params.into_iter().collect()));
    dataset
}
