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
    tracing::subscriber::set_global_default(subscriber).expect("tracing subscriber set");
}

fn make_spiceai_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("spiceai:{path}"), name.to_string())
}

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
        .expect("to collect results");

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
            .expect("to collect results");

        validate_result(result_batches);
    }

    Ok(())
}

#[tokio::test]
async fn basic_federation_push_down() -> Result<(), String> {
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

    let queries = vec![
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
            Some(has_one_int_val),
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
            Some(has_one_int_val),
        ),
    ];

    for (query, expected_plan, validate_result) in queries {
        run_query_and_check_results(&mut rt, query, &expected_plan, validate_result).await?;
    }

    Ok(())
}
