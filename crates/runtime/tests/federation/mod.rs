use std::sync::Arc;

use app::AppBuilder;
use arrow::array::Int64Array;
use datafusion::assert_batches_eq;
use runtime::{datafusion::DataFusion, Runtime};
use spicepod::component::{
    dataset::Dataset,
    secrets::{Secrets, SpiceSecretStore},
};
use tokio::sync::RwLock;

fn make_spiceai_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("spiceai:{path}"), name.to_string())
}

#[tokio::test]
async fn basic_federation_push_down() -> Result<(), String> {
    let eth_blocks_ds = make_spiceai_dataset("eth.recent_blocks", "blocks");

    let app = AppBuilder::new("basic_federation_push_down")
        .with_secret(Secrets {
            store: SpiceSecretStore::File,
        })
        .with_dataset(eth_blocks_ds)
        .build();

    let df = Arc::new(RwLock::new(DataFusion::new()));

    let rt = Runtime::new(Some(app), df).await;

    rt.load_secrets().await;
    rt.load_datasets().await;

    let df = &mut *rt.df.write().await;

    let results = df
        .ctx
        .sql("EXPLAIN SELECT MAX(number) as max_num FROM blocks")
        .await
        .expect("EXPLAIN query to plan")
        .collect()
        .await
        .expect("to collect results");

    #[rustfmt::skip]
    let expected = 
    [
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
    ];
    assert_batches_eq!(expected, &results);

    let batches = df
        .ctx
        .sql("SELECT MAX(number) as max_num FROM blocks")
        .await
        .expect("query to plan")
        .collect()
        .await
        .expect("to collect results");

    for batch in batches {
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 1);
        let max_num_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert_eq!(max_num_arr.len(), 1);
        assert_ne!(max_num_arr.value(0), 0);
    }

    Ok(())
}
