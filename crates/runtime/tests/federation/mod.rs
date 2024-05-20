use spicepod::component::dataset::Dataset;

fn make_spiceai_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("spiceai:{}", path), name.to_string())
}

#[test]
fn basic_federation_push_down() -> Result<(), String> {
    let eth_blocks_ds = make_spiceai_dataset("eth.recent_blocks", "blocks");
    let eth_tx_ds = make_spiceai_dataset("eth.recent_transactions", "tx");
}
