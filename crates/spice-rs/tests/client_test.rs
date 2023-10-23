#[cfg(test)]
mod tests {
    use spice_rs::Client;
    use spice_rs::*;
    use std::env;
    use std::path::Path;

    async fn new_client() -> Client {
        dotenv::from_path(Path::new(".env.local")).ok();
        let api_key = env::var("API_KEY").expect("API_KEY not found");
        let result = new_spice_client(api_key).await;
        return result.expect("Failed to new spice client");
    }

    #[tokio::test]
    async fn test_new_client() {
        new_client().await;
    }

    #[tokio::test]
    async fn test_query() {
        let mut spice_client = new_client().await;
        match spice_client.query(
            "SELECT number, \"timestamp\", base_fee_per_gas, base_fee_per_gas / 1e9 AS base_fee_per_gas_gwei FROM eth.recent_blocks limit 3".to_string(),
            None).await {
                Ok(r) => {
                    println!("{:?}", r);
                }
                Err(e) => {
                    println!("{:?}",e);
                    assert!(false, "Error: {}", e);
                }
            };
    }

    #[tokio::test]
    async fn test_fire_query() {
        let mut spice_client = new_client().await;
        match spice_client.firecache_query(
            "SELECT number, \"timestamp\", base_fee_per_gas, base_fee_per_gas / 1e9 AS base_fee_per_gas_gwei FROM eth.recent_blocks limit 3".to_string(),
            None).await {
                Ok(r) => {
                    println!("{:?}", r);
                }
                Err(e) => {                    
                    println!("{:?}",e);
                    assert!(false, "Error: {}", e);
                }
            };
    }
}
