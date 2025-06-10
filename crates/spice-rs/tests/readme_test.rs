#[cfg(test)]
mod tests {
    use spiceai::*;
    use std::env;
    use std::path::Path;

    #[tokio::test]
    #[allow(deprecated)]
    #[ignore]
    async fn test_readme_new() {
        // NOTE: If you're changing the code below, make sure you update the README.md.
        dotenv::from_path(Path::new(".env.local")).ok();
        let api_key = env::var("API_KEY").expect("API_KEY not found");

        let client = Client::new(&api_key)
            .await
            .expect("SpiceClient should be created");
        let data = client.query("SELECT * FROM taxi_trips LIMIT 10;").await;
        assert!(
            data.is_ok(),
            "failed to query: {:#?}",
            data.expect_err("should be an error")
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_readme_builder() {
        // NOTE: If you're changing the code below, make sure you update the README.md.
        dotenv::from_path(Path::new(".env.local")).ok();
        let api_key = env::var("API_KEY").expect("API_KEY not found");

        let client = ClientBuilder::new()
            .api_key(&api_key)
            .use_spiceai_cloud()
            .build()
            .await
            .expect("SpiceClient should be created");

        let data = client.query("SELECT * FROM taxi_trips LIMIT 10;").await;
        assert!(
            data.is_ok(),
            "failed to query: {:#?}",
            data.expect_err("should be an error")
        );
    }

    #[tokio::test]
    async fn test_readme_builder_local() {
        // NOTE: If you're changing the code below, make sure you update the README.md.
        let client = ClientBuilder::new()
            .build()
            .await
            .expect("SpiceClient should be created");

        let data = client.query("select * from taxi_trips limit 3;").await;
        assert!(
            data.is_ok(),
            "failed to query: {:#?}",
            data.expect_err("should be an error")
        );
    }
}
