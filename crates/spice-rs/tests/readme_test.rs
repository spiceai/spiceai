#[cfg(test)]
mod tests {
    use spiceai::*;
    use std::env;
    use std::path::Path;

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_readme_new() {
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

    #[tokio::test]
    async fn test_readme_builder() {
        // NOTE: If you're changing the code below, make sure you update the README.md.
        dotenv::from_path(Path::new(".env.local")).ok();
        let api_key = env::var("API_KEY").expect("API_KEY not found");

        let mut client = ClientBuilder::new()
            .api_key(&api_key)
            .use_spiceai_cloud()
            .build()
            .await
            .unwrap();

        let data = client
            .query("SELECT * FROM eth.recent_blocks LIMIT 10;")
            .await;
        if data.is_err() {
            panic!("failed to query: {:#?}", data.expect_err(""))
        }
    }

    #[tokio::test]
    async fn test_readme_builder_local() {
        // NOTE: If you're changing the code below, make sure you update the README.md.
        let mut client = ClientBuilder::new().build().await.unwrap();

        let data = client.query("select * from taxi_trips limit 3;").await;
        if data.is_err() {
            panic!("failed to query: {:#?}", data.expect_err(""))
        }
    }
}
