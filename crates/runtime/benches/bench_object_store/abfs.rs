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

pub fn build_app(app_builder: AppBuilder, bench_name: &str) -> AppBuilder {
    match bench_name {
        "tpch" => app_builder
            .with_dataset(make_dataset("tpch/customer/", "customer"))
            .with_dataset(make_dataset("tpch/lineitem/", "lineitem"))
            .with_dataset(make_dataset("tpch/part/", "part"))
            .with_dataset(make_dataset("tpch/partsupp/", "partsupp"))
            .with_dataset(make_dataset("tpch/orders/", "orders"))
            .with_dataset(make_dataset("tpch/nation/", "nation"))
            .with_dataset(make_dataset("tpch/region/", "region"))
            .with_dataset(make_dataset("tpch/supplier/", "supplier")),
        "tpcds" => app_builder
            .with_dataset(make_dataset("tpcds/call_center/", "call_center"))
            .with_dataset(make_dataset("tpcds/catalog_page/", "catalog_page"))
            .with_dataset(make_dataset("tpcds/catalog_sales/", "catalog_sales"))
            .with_dataset(make_dataset("tpcds/catalog_returns/", "catalog_returns"))
            .with_dataset(make_dataset("tpcds/income_band/", "income_band"))
            .with_dataset(make_dataset("tpcds/inventory/", "inventory"))
            .with_dataset(make_dataset("tpcds/store_sales/", "store_sales"))
            .with_dataset(make_dataset("tpcds/store_returns/", "store_returns"))
            .with_dataset(make_dataset("tpcds/web_sales/", "web_sales"))
            .with_dataset(make_dataset("tpcds/web_returns/", "web_returns"))
            .with_dataset(make_dataset("tpcds/customer/", "customer"))
            .with_dataset(make_dataset("tpcds/customer_address/", "customer_address"))
            .with_dataset(make_dataset(
                "tpcds/customer_demographics/",
                "customer_demographics",
            ))
            .with_dataset(make_dataset("tpcds/date_dim/", "date_dim"))
            .with_dataset(make_dataset(
                "tpcds/household_demographics/",
                "household_demographics",
            ))
            .with_dataset(make_dataset("tpcds/item/", "item"))
            .with_dataset(make_dataset("tpcds/promotion/", "promotion"))
            .with_dataset(make_dataset("tpcds/reason/", "reason"))
            .with_dataset(make_dataset("tpcds/ship_mode/", "ship_mode"))
            .with_dataset(make_dataset("tpcds/store/", "store"))
            .with_dataset(make_dataset("tpcds/time_dim/", "time_dim"))
            .with_dataset(make_dataset("tpcds/warehouse/", "warehouse"))
            .with_dataset(make_dataset("tpcds/web_page/", "web_page"))
            .with_dataset(make_dataset("tpcds/web_site/", "web_site")),
        _ => panic!("Only tpcds or tpch benchmark suites are supported"),
    }
}

fn make_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("abfs://data/{path}"), name.to_string());
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            (
                "abfs_account".to_string(),
                "spiceaidemodatasets".to_string(),
            ),
            ("abfs_skip_signature".to_string(), "true".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}
