#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use spiceai::*;
    use std::env;
    use std::ops::Sub;
    use std::path::Path;

    #[tokio::test]
    async fn test_readme() {
        // NOTE: If you're changing the code below, make sure you update the README.md.
        dotenv::from_path(Path::new(".env.local")).ok();
        let api_key = env::var("API_KEY").expect("API_KEY not found");

        let mut client = Client::new(&api_key).await.unwrap();
        let data = client
            .query("SELECT * FROM eth.recent_blocks LIMIT 10;")
            .await;
        if data.is_err() {
            panic!("failed to query: {:#?}", data.expect_err(""))
        }
        let supported_pairs = client.get_supported_pairs().await;
        if supported_pairs.is_err() {
            panic!(
                "failed to get supported pairs: {:#?}",
                supported_pairs.expect_err("")
            )
        }
        let price_data = client.get_prices(&["BTC-USDC"]).await;
        if price_data.is_err() {
            panic!("failed to get prices: {:#?}", price_data.expect_err(""))
        }
        let historical_price_data = client
            .get_historical_prices(&["BTC-USDC"], Option::None, Option::None, Option::None)
            .await;
        if historical_price_data.is_err() {
            panic!(
                "failed to get prices: {:#?}",
                historical_price_data.expect_err("")
            )
        }
        let now = Utc::now();
        let start = now.sub(Duration::seconds(3600));

        let historical_price_data = client
            .get_historical_prices(&["BTC-USDC"], Some(start), Some(now), Option::None)
            .await;
        if historical_price_data.is_err() {
            panic!(
                "failed to get prices: {:#?}",
                historical_price_data.expect_err("")
            )
        }
    }
}
