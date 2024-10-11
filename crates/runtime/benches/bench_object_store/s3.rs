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
            ))),
        "tpcds" => Ok(app_builder
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/call_center/",
                "call_center",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/catalog_page/",
                "catalog_page",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/catalog_sales/",
                "catalog_sales",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/catalog_returns/",
                "catalog_returns",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/income_band/",
                "income_band",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/inventory/",
                "inventory",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/store_sales/",
                "store_sales",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/store_returns/",
                "store_returns",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/web_sales/",
                "web_sales",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/web_returns/",
                "web_returns",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/customer/",
                "customer",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/customer_address/",
                "customer_address",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/customer_demographics/",
                "customer_demographics",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/date_dim/",
                "date_dim",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/household_demographics/",
                "household_demographics",
            ))
            .with_dataset(make_dataset("spiceai-public-datasets/tpcds/item/", "item"))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/promotion/",
                "promotion",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/reason/",
                "reason",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/ship_mode/",
                "ship_mode",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/store/",
                "store",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/time_dim/",
                "time_dim",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/warehouse/",
                "warehouse",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/web_page/",
                "web_page",
            ))
            .with_dataset(make_dataset(
                "spiceai-public-datasets/tpcds/web_site/",
                "web_site",
            ))),
        _ => Err("Only tpcds or tpch benchmark suites are supported".to_string()),
    }
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
