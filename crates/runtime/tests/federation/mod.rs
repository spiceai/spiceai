use std::sync::Arc;

use app::AppBuilder;
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
    let eth_tx_ds = make_spiceai_dataset("eth.recent_transactions", "tx");

    let app = AppBuilder::new("basic_federation_push_down")
        .with_secret(Secrets {
            store: SpiceSecretStore::File,
        })
        .with_dataset(eth_blocks_ds)
        .with_dataset(eth_tx_ds)
        .build();

    let df = Arc::new(RwLock::new(DataFusion::new()));

    let rt = Runtime::new(Some(app), df).await;

    rt.load_secrets().await;
    rt.load_datasets().await;

    let df = &mut *rt.df.write().await;

    let results = df
        .ctx
        .sql("EXPLAIN SELECT MAX(number) as max_num FROM blocks UNION ALL SELECT MAX(block_number) as max_num FROM tx")
        .await
        .expect("EXPLAIN query to plan")
        .collect()
        .await
        .expect("to collect results");

    #[rustfmt::skip]
    let expected = 
    [
        "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        "| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                                                                             |",
        "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        "| logical_plan  | TableScan: blocks projection=[number, hash, parent_hash, nonce, sha3_uncles, logs_bloom, transactions_root, state_root, receipts_root, miner, difficulty, total_difficulty, size, extra_data, gas_limit, gas_used, timestamp, transaction_count, base_fee_per_gas, withdrawals_root, withdrawal_count, parent_beacon_block_root, blob_gas_used, excess_blob_gas]                                                                 |",
        "| physical_plan | FlightExec sql=SELECT \"number\", \"hash\", \"parent_hash\", \"nonce\", \"sha3_uncles\", \"logs_bloom\", \"transactions_root\", \"state_root\", \"receipts_root\", \"miner\", \"difficulty\", \"total_difficulty\", \"size\", \"extra_data\", \"gas_limit\", \"gas_used\", \"timestamp\", \"transaction_count\", \"base_fee_per_gas\", \"withdrawals_root\", \"withdrawal_count\", \"parent_beacon_block_root\", \"blob_gas_used\", \"excess_blob_gas\" FROM eth.recent_blocks   |",
        "|               |                                                                                                                                                                                                                                                                                                                                                                                                                                  |",
        "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}
