use std::sync::Arc;

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch};
use datafusion::assert_batches_eq;
use runtime::{datafusion::DataFusion, Runtime};
use spicepod::component::{dataset::Dataset, secrets::SpiceSecretStore};
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

fn init_tracing(default_level: Option<&str>) {
    let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
        (_, Some(log)) => EnvFilter::new(log),
        (Some(level), None) => EnvFilter::new(level),
        _ => EnvFilter::new(
            "runtime=TRACE,datafusion-federation=TRACE,datafusion-federation-sql=TRACE",
        ),
    };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_ansi(true)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

fn make_spiceai_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("spiceai:{path}"), name.to_string())
}

type ValidateFn = dyn FnOnce(Vec<RecordBatch>);

async fn run_query_and_check_results<F>(
    rt: &mut Runtime,
    query: &str,
    expected_plan: &[&str],
    validate_result: Option<F>,
) -> Result<(), String>
where
    F: FnOnce(Vec<RecordBatch>),
{
    let df = &mut *rt.df.write().await;

    // Check the plan
    let plan_results = df
        .ctx
        .sql(&format!("EXPLAIN {query}"))
        .await
        .map_err(|e| format!("query `{query}` to plan: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("query `{query}` to results: {e}"))?;

    assert_batches_eq!(expected_plan, &plan_results);

    // Check the result
    if let Some(validate_result) = validate_result {
        let result_batches = df
            .ctx
            .sql(query)
            .await
            .map_err(|e| format!("query `{query}` to plan: {e}"))?
            .collect()
            .await
            .map_err(|e| format!("query `{query}` to results: {e}"))?;

        validate_result(result_batches);
    }

    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn single_source_federation_push_down() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, Vec<&'a str>, Option<Box<ValidateFn>>)>;
    init_tracing(None);
    let app = AppBuilder::new("basic_federation_push_down")
        .with_secret_store(SpiceSecretStore::File)
        .with_dataset(make_spiceai_dataset("eth.recent_blocks", "blocks"))
        .with_dataset(make_spiceai_dataset("eth.blocks", "full_blocks"))
        .with_dataset(make_spiceai_dataset("eth.recent_transactions", "tx"))
        .build();

    let df = Arc::new(RwLock::new(DataFusion::new()));

    let mut rt = Runtime::new(Some(app), df).await;

    rt.load_secrets().await;
    rt.load_datasets().await;

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
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                            |",
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                       |",
                "|               |  Projection: MAX(blocks.number) AS max_num                                                                                                                                      |",
                "|               |   Aggregate: groupBy=[[]], aggr=[[MAX(blocks.number)]]                                                                                                                          |",
                "|               |     SubqueryAlias: blocks                                                                                                                                                       |",
                "|               |       TableScan: eth.recent_blocks                                                                                                                                              |",
                "| physical_plan | VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT MAX(\"blocks\".\"number\") AS \"max_num\" FROM \"eth\".\"recent_blocks\" AS \"blocks\" |",
                "|               |                                                                                                                                                                                 |",
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
            Some(Box::new(has_one_int_val)),
        ),
        (
            "SELECT number FROM blocks WHERE number = (SELECT MAX(number) FROM blocks)",
            vec![
                "+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                             |",
                "+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                        |",
                "|               |  Projection: blocks.number                                                                                                                                                                                                                                       |",
                "|               |   Filter: blocks.number = (<subquery>)                                                                                                                                                                                                                           |",
                "|               |     Subquery:                                                                                                                                                                                                                                                    |",
                "|               |       Projection: MAX(blocks.number)                                                                                                                                                                                                                             |",
                "|               |         Aggregate: groupBy=[[]], aggr=[[MAX(blocks.number)]]                                                                                                                                                                                                     |",
                "|               |           SubqueryAlias: blocks                                                                                                                                                                                                                                  |",
                "|               |             TableScan: eth.recent_blocks                                                                                                                                                                                                                         |",
                "|               |     SubqueryAlias: blocks                                                                                                                                                                                                                                        |",
                "|               |       TableScan: eth.recent_blocks                                                                                                                                                                                                                               |",
                "| physical_plan | VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT \"blocks\".\"number\" FROM \"eth\".\"recent_blocks\" AS \"blocks\" WHERE (\"blocks\".\"number\" = (SELECT MAX(\"blocks\".\"number\") FROM \"eth\".\"recent_blocks\" AS \"blocks\")) |",
                "|               |                                                                                                                                                                                                                                                                  |",
                "+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
            Some(Box::new(has_one_int_val)),
        ),
        (
            "SELECT number, hash FROM full_blocks WHERE number BETWEEN 1000 AND 2000 ORDER BY number DESC LIMIT 10",
            vec![
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                                  |",
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                                                             |",
                "|               |  Limit: skip=0, fetch=10                                                                                                                                                                                                                                                                              |",
                "|               |   Sort: full_blocks.number DESC NULLS FIRST                                                                                                                                                                                                                                                           |",
                "|               |     Projection: full_blocks.number, full_blocks.hash                                                                                                                                                                                                                                                  |",
                "|               |       Filter: full_blocks.number BETWEEN Int64(1000) AND Int64(2000)                                                                                                                                                                                                                                  |",
                "|               |         SubqueryAlias: full_blocks                                                                                                                                                                                                                                                                    |",
                "|               |           TableScan: eth.blocks                                                                                                                                                                                                                                                                       |",
                "| physical_plan | VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT \"full_blocks\".\"number\", \"full_blocks\".\"hash\" FROM \"eth\".\"blocks\" AS \"full_blocks\" WHERE (\"full_blocks\".\"number\" BETWEEN 1000 AND 2000) ORDER BY \"full_blocks\".\"number\" DESC NULLS FIRST LIMIT 10 |",
                "|               |                                                                                                                                                                                                                                                                                                       |",
                "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
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
                "+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                                                                  |",
                "+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                                                                                                                                                                             |",
                "|               |  Limit: skip=0, fetch=10                                                                                                                                                                                                                                                                                                                                                                                              |",
                "|               |   Sort: full_blocks.transaction_count DESC NULLS FIRST                                                                                                                                                                                                                                                                                                                                                                |",
                "|               |     Projection: AVG(full_blocks.gas_used) AS avg_gas_used, full_blocks.transaction_count                                                                                                                                                                                                                                                                                                                              |",
                "|               |       Aggregate: groupBy=[[full_blocks.transaction_count]], aggr=[[AVG(CAST(full_blocks.gas_used AS Float64))]]                                                                                                                                                                                                                                                                                                       |",
                "|               |         Filter: full_blocks.number BETWEEN Int64(100000) AND Int64(200000)                                                                                                                                                                                                                                                                                                                                            |",
                "|               |           SubqueryAlias: full_blocks                                                                                                                                                                                                                                                                                                                                                                                  |",
                "|               |             TableScan: eth.blocks                                                                                                                                                                                                                                                                                                                                                                                     |",
                "| physical_plan | VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT AVG(CAST(\"full_blocks\".\"gas_used\" AS DOUBLE)) AS \"avg_gas_used\", \"full_blocks\".\"transaction_count\" FROM \"eth\".\"blocks\" AS \"full_blocks\" WHERE (\"full_blocks\".\"number\" BETWEEN 100000 AND 200000) GROUP BY \"full_blocks\".\"transaction_count\" ORDER BY \"full_blocks\".\"transaction_count\" DESC NULLS FIRST LIMIT 10 |",
                "|               |                                                                                                                                                                                                                                                                                                                                                                                                                       |",
                "+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
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
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                                  |",
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                                                                                                                                             |",
                "|               |  Limit: skip=0, fetch=10                                                                                                                                                                                                                                                                                                                                                              |",
                "|               |   Sort: blocks.number DESC NULLS FIRST                                                                                                                                                                                                                                                                                                                                                |",
                "|               |     Projection: SUM(tx.receipt_gas_used) AS total_gas_used, blocks.number                                                                                                                                                                                                                                                                                                             |",
                "|               |       Aggregate: groupBy=[[blocks.number]], aggr=[[SUM(tx.receipt_gas_used)]]                                                                                                                                                                                                                                                                                                         |",
                "|               |         Inner Join:  Filter: blocks.number = tx.block_number                                                                                                                                                                                                                                                                                                                          |",
                "|               |           SubqueryAlias: blocks                                                                                                                                                                                                                                                                                                                                                       |",
                "|               |             TableScan: eth.recent_blocks                                                                                                                                                                                                                                                                                                                                              |",
                "|               |           SubqueryAlias: tx                                                                                                                                                                                                                                                                                                                                                           |",
                "|               |             TableScan: eth.recent_transactions                                                                                                                                                                                                                                                                                                                                        |",
                "| physical_plan | VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT SUM(\"tx\".\"receipt_gas_used\") AS \"total_gas_used\", \"blocks\".\"number\" FROM \"eth\".\"recent_blocks\" AS \"blocks\" JOIN \"eth\".\"recent_transactions\" AS \"tx\" ON (\"blocks\".\"number\" = \"tx\".\"block_number\") GROUP BY \"blocks\".\"number\" ORDER BY \"blocks\".\"number\" DESC NULLS FIRST LIMIT 10 |",
                "|               |                                                                                                                                                                                                                                                                                                                                                                                       |",
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
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
    ];

    for (query, expected_plan, validate_result) in queries {
        run_query_and_check_results(&mut rt, query, &expected_plan, validate_result).await?;
    }

    Ok(())
}
