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

use spicepod::component::dataset::Dataset;

#[allow(clippy::too_many_lines)]
pub fn build_app(app_builder: AppBuilder, bench_name: &str) -> Result<AppBuilder, String> {
    match bench_name {
        "tpch" => Ok(app_builder
            .with_dataset(make_dataset("customer.parquet", "customer", bench_name))
            .with_dataset(make_dataset("lineitem.parquet", "lineitem", bench_name))
            .with_dataset(make_dataset("orders.parquet", "orders", bench_name))
            .with_dataset(make_dataset("part.parquet", "part", bench_name))
            .with_dataset(make_dataset("partsupp.parquet", "partsupp", bench_name))
            .with_dataset(make_dataset("region.parquet", "region", bench_name))
            .with_dataset(make_dataset("nation.parquet", "nation", bench_name))
            .with_dataset(make_dataset("supplier.parquet", "supplier", bench_name))),
        "tpcds" => Ok(app_builder
            .with_dataset(make_dataset(
                "call_center.parquet",
                "call_center",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "catalog_page.parquet",
                "catalog_page",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "catalog_returns.parquet",
                "catalog_returns",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "catalog_sales.parquet",
                "catalog_sales",
                bench_name,
            ))
            .with_dataset(make_dataset("customer.parquet", "customer", bench_name))
            .with_dataset(make_dataset(
                "customer_address.parquet",
                "customer_address",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "customer_demographics.parquet",
                "customer_demographics",
                bench_name,
            ))
            .with_dataset(make_dataset("date_dim.parquet", "date_dim", bench_name))
            .with_dataset(make_dataset(
                "household_demographics.parquet",
                "household_demographics",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "income_band.parquet",
                "income_band",
                bench_name,
            ))
            .with_dataset(make_dataset("inventory.parquet", "inventory", bench_name))
            .with_dataset(make_dataset("item.parquet", "item", bench_name))
            .with_dataset(make_dataset("promotion.parquet", "promotion", bench_name))
            .with_dataset(make_dataset("reason.parquet", "reason", bench_name))
            .with_dataset(make_dataset("ship_mode.parquet", "ship_mode", bench_name))
            .with_dataset(make_dataset("store.parquet", "store", bench_name))
            .with_dataset(make_dataset(
                "store_returns.parquet",
                "store_returns",
                bench_name,
            ))
            .with_dataset(make_dataset(
                "store_sales.parquet",
                "store_sales",
                bench_name,
            ))
            .with_dataset(make_dataset("time_dim.parquet", "time_dim", bench_name))
            .with_dataset(make_dataset("warehouse.parquet", "warehouse", bench_name))
            .with_dataset(make_dataset("web_page.parquet", "web_page", bench_name))
            .with_dataset(make_dataset(
                "web_returns.parquet",
                "web_returns",
                bench_name,
            ))
            .with_dataset(make_dataset("web_sales.parquet", "web_sales", bench_name))
            .with_dataset(make_dataset("web_site.parquet", "web_site", bench_name))),
        _ => Err(
            "Only tpch and tpcds benchmark suites are supported for the file connector".to_string(),
        ),
    }
}

fn make_dataset(path: &str, name: &str, _bench_name: &str) -> Dataset {
    Dataset::new(format!("file:./{path}"), name.to_string())
}
