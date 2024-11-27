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

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch};
use datafusion::assert_batches_eq;
use runtime::{status, Runtime};
use spicepod::component::dataset::Dataset;

use crate::{
    get_test_datafusion, init_tracing, run_query_and_check_results, utils::test_request_context,
    ValidateFn,
};

#[cfg(feature = "mysql")]
mod mysql;

fn make_spiceai_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("spice.ai:{path}"), name.to_string())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn spiceai_integration_test_single_source_federation_push_down() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);

    test_request_context().scope(async {
        let app = AppBuilder::new("basic_federation_push_down")
            .with_dataset(make_spiceai_dataset("eth.recent_blocks", "blocks"))
            .with_dataset(make_spiceai_dataset("eth.blocks", "full_blocks"))
            .with_dataset(make_spiceai_dataset("eth.recent_transactions", "tx"))
            .with_dataset(make_spiceai_dataset("eth.recent_logs", "eth.logs"))
            .build();

        let status = status::RuntimeStatus::new();
        let df = get_test_datafusion(Arc::clone(&status));

        let mut rt = Runtime::builder()
            .with_app(app)
            .with_datafusion(df)
            .with_runtime_status(status)
            .build()
            .await;

        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                panic!("Timeout waiting for components to load");
            }
            () = rt.load_components() => {}
        }

        let has_one_int_val = |result_batches: Vec<RecordBatch>| {
            for batch in result_batches {
                assert_eq!(batch.num_columns(), 1);
                assert_eq!(batch.num_rows(), 1);
                let result_arr = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64Array");
                assert_eq!(result_arr.len(), 1);
                assert_ne!(result_arr.value(0), 0);
            }
        };

        let queries: QueryTests = vec![
            (
                "SELECT MAX(number) as max_num FROM blocks",
                "select_max_num",
                Some(Box::new(has_one_int_val)),
            ),
            (
                "SELECT number FROM blocks WHERE number = (SELECT MAX(number) FROM blocks)",
                "select_number_subquery",
                Some(Box::new(has_one_int_val)),
            ),
            (
                "SELECT number, hash FROM full_blocks WHERE number BETWEEN 1000 AND 2000 ORDER BY number DESC LIMIT 10",
                "select_number_filter",
                Some(Box::new(|plan_results| {
                    let expected_results = [
                        "+--------+--------------------------------------------------------------------+",
                        "| number | hash                                                               |",
                        "+--------+--------------------------------------------------------------------+",
                        "| 2000   | 0x73b20034e531f385a59401bbda9a225be12b2fd42d7c21e4c3d11b3d7be34244 |",
                        "| 1999   | 0x1af2b6c4d0eb975784441b0fdae7c99d603b1afcf03b18b0be2e8fd1190ae52c |",
                        "| 1998   | 0x317063b3e2d39995ef86384feaa4502f8413286fea86587d31ec35778d2da7cd |",
                        "| 1997   | 0xb83cf3e25014973d6073de708a499b25e9fb447e60057332783e3ee2a43567cf |",
                        "| 1996   | 0x9be7b34b99c125b392f2f9f71c221d167dec2e1a22a79d9e507bc832ce098337 |",
                        "| 1995   | 0xdfd07b4875096ad5fa2ebe330b7d18c57e85bfe7d65fd5b545191bc0950a132e |",
                        "| 1994   | 0x6fd9761d6e15cc4bc41b7c28880c46e28e468eb07553ba5510e87bac3002b259 |",
                        "| 1993   | 0x1074de28cd4fbf430ce2c161d8ce4b27d234ec81b4e742ccc9808681a0502de4 |",
                        "| 1992   | 0xd6bd3d330458bdb644d6d58c7544b98d1632cb71787f4ac904d6c730367e5af8 |",
                        "| 1991   | 0x17b6a9ff7ffdcd02cccb96d2e5ea9c5e73ae0c1de85f19a335a1660421b2c3b7 |",
                        "+--------+--------------------------------------------------------------------+",
                    ];
                    assert_batches_eq!(expected_results, &plan_results);
                })),
            ),
            (
                "SELECT AVG(gas_used) AS avg_gas_used, transaction_count FROM full_blocks WHERE number BETWEEN 100000 AND 200000 GROUP BY transaction_count ORDER BY transaction_count DESC LIMIT 10",
                "select_avg_gas_used",
                Some(Box::new(|plan_results| {
                    let expected_results = [
                        "+--------------+-------------------+",
                        "| avg_gas_used | transaction_count |",
                        "+--------------+-------------------+",
                        "| 3129000.0    | 149               |",
                        "| 3108000.0    | 148               |",
                        "| 3117600.0    | 147               |",
                        "| 3100500.0    | 146               |",
                        "| 3045000.0    | 145               |",
                        "| 3034227.0    | 143               |",
                        "| 2982000.0    | 142               |",
                        "| 2898000.0    | 138               |",
                        "| 3047268.0    | 137               |",
                        "| 2835000.0    | 135               |",
                        "+--------------+-------------------+",
                    ];
                    assert_batches_eq!(expected_results, &plan_results);
                })),
            ),
            (
                "SELECT SUM(tx.receipt_gas_used) AS total_gas_used, blocks.number FROM blocks JOIN tx ON blocks.number = tx.block_number GROUP BY blocks.number ORDER BY blocks.number DESC LIMIT 10",
                "select_sum_tx_receipt_gas_used",
                Some(Box::new(|result_batches| {
                    // Results change over time, but it looks like:
                    // let expected_results = [
                    //     "+----------------+----------+",
                    //     "| total_gas_used | number   |",
                    //     "+----------------+----------+",
                    //     "| 14965044       | 19912051 |",
                    //     "| 19412656       | 19912049 |",
                    //     "| 12304986       | 19912047 |",
                    //     "| 19661381       | 19912046 |",
                    //     "| 10828931       | 19912045 |",
                    //     "| 21121895       | 19912044 |",
                    //     "| 29982938       | 19912043 |",
                    //     "| 10630719       | 19912042 |",
                    //     "| 29988818       | 19912041 |",
                    //     "| 9310052        | 19912040 |",
                    //     "+----------------+----------+",
                    // ];

                    for batch in result_batches {
                        assert_eq!(batch.num_columns(), 2);
                        assert_eq!(batch.num_rows(), 10);
                    }
                })),
            ),
            (
                "SELECT *
                FROM eth.logs
                ORDER BY block_number DESC 
                LIMIT 10",
                "select_eth_logs",
                Some(Box::new(|result_batches| {
                    for batch in result_batches {
                        assert_eq!(batch.num_columns(), 9, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                    }
                })),
            ),
            (
                "SELECT number FROM blocks 
                UNION ALL
                SELECT tx.block_number as number FROM tx
                ORDER BY number DESC LIMIT 10",
                "select_union_all",
                Some(Box::new(|result_batches| {
                    for batch in result_batches {
                        assert_eq!(batch.num_columns(), 1, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                    }
                })),
            ),
            (
                "SELECT MAX(blocks_number) FROM (SELECT b1.number as blocks_number from blocks b1)",
                "select_max_blocks_number",
                Some(Box::new(has_one_int_val)),
            ),
        ];

        for (query, snapshot_suffix, validate_result) in queries {
            run_query_and_check_results(
                &mut rt,
                &format!(
                    "spiceai_integration_test_single_source_federation_push_down_{snapshot_suffix}"
                ),
                query,
                true,
                validate_result,
            )
            .await?;
        }

        Ok(())
    }).await
}
