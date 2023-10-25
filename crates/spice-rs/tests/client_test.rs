#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use spice_rs::Client;
    use std::env;
    use std::path::Path;

    async fn new_client() -> Client {
        dotenv::from_path(Path::new(".env.local")).ok();
        let api_key = env::var("API_KEY").expect("API_KEY not found");
        let spice_client = Client::new(&api_key).await;
        return spice_client;
    }

    #[tokio::test]
    async fn test_new_client() {
        new_client().await;
    }

    #[tokio::test]
    async fn test_query() {
        let mut spice_client = new_client().await;
        match spice_client.query(
            "SELECT number, \"timestamp\", base_fee_per_gas, base_fee_per_gas / 1e9 AS base_fee_per_gas_gwei FROM eth.recent_blocks limit 10",
            ).await {
                Ok(mut flight_data_stream) => {
                      // Read back RecordBatches
                    while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => {
                            assert_eq!(batch.num_columns(), 4);
                            assert_eq!(batch.num_rows(), 10);
                        },
                        Err(e) => {
                            assert!(false, "Error: {}", e)
                        },
                    };
                    }
                }
                Err(e) => {
                    assert!(false, "Error: {}", e);
                }
            };
    }

    #[tokio::test]
    async fn test_query_streaming() {
        let mut spice_client = new_client().await;
        match spice_client.query(
            "SELECT number, \"timestamp\", base_fee_per_gas, base_fee_per_gas / 1e9 AS base_fee_per_gas_gwei FROM eth.blocks limit 2000",
            ).await {
                Ok(mut flight_data_stream) => {
                      // Read back RecordBatches
                    let mut num_batches = 0;
                    let mut total_rows = 0;
                    while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => {
                            num_batches = num_batches + 1;
                            total_rows = total_rows + batch.num_rows();
                        },
                        Err(e) => {
                            assert!(false, "Error: {}", e)
                        },
                    };
                    }
                    assert_eq!(total_rows, 2000);
                    assert_ne!(num_batches, 1);
                }
                Err(e) => {
                    assert!(false, "Error: {}", e);
                }
            };
    }
}
