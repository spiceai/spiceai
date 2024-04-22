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
    }
}
