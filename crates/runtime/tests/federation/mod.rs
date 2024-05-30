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
use runtime::Runtime;
use spicepod::component::{dataset::Dataset, secrets::SpiceSecretStore};

use crate::{
    init_tracing, modify_runtime_datafusion_options, run_query_and_check_results, ValidateFn,
};

#[cfg(feature = "mysql")]
mod mysql;

fn make_spiceai_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("spiceai:{path}"), name.to_string())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn single_source_federation_push_down() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, Vec<&'a str>, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(None);
    let app = AppBuilder::new("basic_federation_push_down")
        .with_secret_store(SpiceSecretStore::File)
        .with_dataset(make_spiceai_dataset("eth.recent_blocks", "blocks"))
        .with_dataset(make_spiceai_dataset("eth.blocks", "full_blocks"))
        .with_dataset(make_spiceai_dataset("eth.recent_transactions", "tx"))
        .with_dataset(make_spiceai_dataset("eth.recent_logs", "eth.logs"))
        .build();

    let rt = Runtime::new("spice".to_string(), Some(app), Arc::new(vec![])).await;

    rt.load_secrets().await;
    rt.load_datasets().await;

    let mut rt = modify_runtime_datafusion_options(rt);

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
            vec![
                "+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                 |",
                "+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                            |",
                "|               |  Projection: MAX(eth.recent_blocks.number) AS max_num                                                                                                                                |",
                "|               |   Aggregate: groupBy=[[]], aggr=[[MAX(eth.recent_blocks.number)]]                                                                                                                    |",
                "|               |     TableScan: eth.recent_blocks                                                                                                                                                     |",
                "| physical_plan | SchemaCastScanExec                                                                                                                                                                   |",
                "|               |   RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                                               |",
                "|               |     VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT MAX(\"eth\".\"recent_blocks\".\"number\") AS \"max_num\" FROM \"eth\".\"recent_blocks\" |",
                "|               |                                                                                                                                                                                      |",
                "+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
            Some(Box::new(has_one_int_val)),
        ),
        (
            "SELECT number FROM blocks WHERE number = (SELECT MAX(number) FROM blocks)",
            vec![
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                |",
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                                           |",
                "|               |  Projection: eth.recent_blocks.number                                                                                                                                                                                                                                               |",
                "|               |   Filter: eth.recent_blocks.number = (<subquery>)                                                                                                                                                                                                                                   |",
                "|               |     Subquery:                                                                                                                                                                                                                                                                       |",
                "|               |       Projection: MAX(eth.recent_blocks.number)                                                                                                                                                                                                                                     |",
                "|               |         Aggregate: groupBy=[[]], aggr=[[MAX(eth.recent_blocks.number)]]                                                                                                                                                                                                             |",
                "|               |           TableScan: eth.recent_blocks                                                                                                                                                                                                                                              |",
                "|               |     TableScan: eth.recent_blocks                                                                                                                                                                                                                                                    |",
                "| physical_plan | SchemaCastScanExec                                                                                                                                                                                                                                                                  |",
                "|               |   RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                                                                                                                                              |",
                "|               |     VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT \"eth\".\"recent_blocks\".\"number\" FROM \"eth\".\"recent_blocks\" WHERE (\"eth\".\"recent_blocks\".\"number\" = (SELECT MAX(\"eth\".\"recent_blocks\".\"number\") FROM \"eth\".\"recent_blocks\")) |",
                "|               |                                                                                                                                                                                                                                                                                     |",
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
            Some(Box::new(has_one_int_val)),
        ),
        (
            "SELECT number, hash FROM full_blocks WHERE number BETWEEN 1000 AND 2000 ORDER BY number DESC LIMIT 10",
            vec![
                "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                         |",
                "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                                                    |",
                "|               |  Limit: skip=0, fetch=10                                                                                                                                                                                                                                                                     |",
                "|               |   Sort: eth.blocks.number DESC NULLS FIRST                                                                                                                                                                                                                                                   |",
                "|               |     Projection: eth.blocks.number, eth.blocks.hash                                                                                                                                                                                                                                           |",
                "|               |       Filter: eth.blocks.number BETWEEN Int64(1000) AND Int64(2000)                                                                                                                                                                                                                          |",
                "|               |         TableScan: eth.blocks                                                                                                                                                                                                                                                                |",
                "| physical_plan | SchemaCastScanExec                                                                                                                                                                                                                                                                           |",
                "|               |   RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                                                                                                                                                       |",
                "|               |     VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT \"eth\".\"blocks\".\"number\", \"eth\".\"blocks\".\"hash\" FROM \"eth\".\"blocks\" WHERE (\"eth\".\"blocks\".\"number\" BETWEEN 1000 AND 2000) ORDER BY \"eth\".\"blocks\".\"number\" DESC NULLS FIRST LIMIT 10 |",
                "|               |                                                                                                                                                                                                                                                                                              |",
                "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
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
            vec![
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                                                          |",
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                                                                                                                                                                     |",
                "|               |  Limit: skip=0, fetch=10                                                                                                                                                                                                                                                                                                                                                                                      |",
                "|               |   Sort: eth.blocks.transaction_count DESC NULLS FIRST                                                                                                                                                                                                                                                                                                                                                         |",
                "|               |     Projection: AVG(eth.blocks.gas_used) AS avg_gas_used, eth.blocks.transaction_count                                                                                                                                                                                                                                                                                                                        |",
                "|               |       Aggregate: groupBy=[[eth.blocks.transaction_count]], aggr=[[AVG(CAST(eth.blocks.gas_used AS Float64))]]                                                                                                                                                                                                                                                                                                 |",
                "|               |         Filter: eth.blocks.number BETWEEN Int64(100000) AND Int64(200000)                                                                                                                                                                                                                                                                                                                                     |",
                "|               |           TableScan: eth.blocks                                                                                                                                                                                                                                                                                                                                                                               |",
                "| physical_plan | SchemaCastScanExec                                                                                                                                                                                                                                                                                                                                                                                            |",
                "|               |   RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                                                                                                                                                                                                                                                                        |",
                "|               |     VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT AVG(CAST(\"eth\".\"blocks\".\"gas_used\" AS DOUBLE)) AS \"avg_gas_used\", \"eth\".\"blocks\".\"transaction_count\" FROM \"eth\".\"blocks\" WHERE (\"eth\".\"blocks\".\"number\" BETWEEN 100000 AND 200000) GROUP BY \"eth\".\"blocks\".\"transaction_count\" ORDER BY \"eth\".\"blocks\".\"transaction_count\" DESC NULLS FIRST LIMIT 10 |",
                "|               |                                                                                                                                                                                                                                                                                                                                                                                                               |",
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
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
            vec![
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |",
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                                                                                                                                                                                                                               |",
                "|               |  Limit: skip=0, fetch=10                                                                                                                                                                                                                                                                                                                                                                                                                                                |",
                "|               |   Sort: eth.recent_blocks.number DESC NULLS FIRST                                                                                                                                                                                                                                                                                                                                                                                                                       |",
                "|               |     Projection: SUM(eth.recent_transactions.receipt_gas_used) AS total_gas_used, eth.recent_blocks.number                                                                                                                                                                                                                                                                                                                                                               |",
                "|               |       Aggregate: groupBy=[[eth.recent_blocks.number]], aggr=[[SUM(eth.recent_transactions.receipt_gas_used)]]                                                                                                                                                                                                                                                                                                                                                           |",
                "|               |         Inner Join:  Filter: eth.recent_blocks.number = eth.recent_transactions.block_number                                                                                                                                                                                                                                                                                                                                                                            |",
                "|               |           TableScan: eth.recent_blocks                                                                                                                                                                                                                                                                                                                                                                                                                                  |",
                "|               |           TableScan: eth.recent_transactions                                                                                                                                                                                                                                                                                                                                                                                                                            |",
                "| physical_plan | SchemaCastScanExec                                                                                                                                                                                                                                                                                                                                                                                                                                                      |",
                "|               |   RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                                                                                                                                                                                                                                                                                                                                  |",
                "|               |     VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT SUM(\"eth\".\"recent_transactions\".\"receipt_gas_used\") AS \"total_gas_used\", \"eth\".\"recent_blocks\".\"number\" FROM \"eth\".\"recent_blocks\" JOIN \"eth\".\"recent_transactions\" ON (\"eth\".\"recent_blocks\".\"number\" = \"eth\".\"recent_transactions\".\"block_number\") GROUP BY \"eth\".\"recent_blocks\".\"number\" ORDER BY \"eth\".\"recent_blocks\".\"number\" DESC NULLS FIRST LIMIT 10 |",
                "|               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |",
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
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
            vec![
                "+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |",
                "+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |",
                "|               |  Limit: skip=0, fetch=10                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |",
                "|               |   Sort: eth.recent_logs.block_number DESC NULLS FIRST                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |",
                "|               |     Projection: eth.recent_logs.log_index, eth.recent_logs.transaction_hash, eth.recent_logs.transaction_index, eth.recent_logs.address, eth.recent_logs.data, eth.recent_logs.topics, eth.recent_logs.block_timestamp, eth.recent_logs.block_hash, eth.recent_logs.block_number                                                                                                                                                                                                                                               |",
                "|               |       TableScan: eth.recent_logs                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |",
                "| physical_plan | SchemaCastScanExec                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |",
                "|               |   RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                                                                                                                                                                                                                                                                                                                                                                                         |",
                "|               |     VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT \"eth\".\"recent_logs\".\"log_index\", \"eth\".\"recent_logs\".\"transaction_hash\", \"eth\".\"recent_logs\".\"transaction_index\", \"eth\".\"recent_logs\".\"address\", \"eth\".\"recent_logs\".\"data\", \"eth\".\"recent_logs\".\"topics\", \"eth\".\"recent_logs\".\"block_timestamp\", \"eth\".\"recent_logs\".\"block_hash\", \"eth\".\"recent_logs\".\"block_number\" FROM \"eth\".\"recent_logs\" ORDER BY \"eth\".\"recent_logs\".\"block_number\" DESC NULLS FIRST LIMIT 10 |",
                "|               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |",
                "+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
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
            vec![
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                                                    |",
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                                                                               |",
                "|               |  Limit: skip=0, fetch=10                                                                                                                                                                                                                                                                                                |",
                "|               |   Sort: number DESC NULLS FIRST                                                                                                                                                                                                                                                                                         |",
                "|               |     Union                                                                                                                                                                                                                                                                                                               |",
                "|               |       Projection: eth.recent_blocks.number                                                                                                                                                                                                                                                                              |",
                "|               |         TableScan: eth.recent_blocks                                                                                                                                                                                                                                                                                    |",
                "|               |       Projection: eth.recent_transactions.block_number AS number                                                                                                                                                                                                                                                        |",
                "|               |         TableScan: eth.recent_transactions                                                                                                                                                                                                                                                                              |",
                "| physical_plan | SchemaCastScanExec                                                                                                                                                                                                                                                                                                      |",
                "|               |   RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                                                                                                                                                                                  |",
                "|               |     VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT \"eth\".\"recent_blocks\".\"number\" FROM \"eth\".\"recent_blocks\" UNION ALL SELECT \"eth\".\"recent_transactions\".\"block_number\" AS \"number\" FROM \"eth\".\"recent_transactions\" ORDER BY \"number\" DESC NULLS FIRST LIMIT 10 |",
                "|               |                                                                                                                                                                                                                                                                                                                         |",
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
            Some(Box::new(|result_batches| {
                for batch in result_batches {
                    assert_eq!(batch.num_columns(), 1, "num_cols: {}", batch.num_columns());
                    assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                }
            })),
        )
    ];

    for (query, expected_plan, validate_result) in queries {
        run_query_and_check_results(&mut rt, query, &expected_plan, validate_result).await?;
    }

    Ok(())
}
