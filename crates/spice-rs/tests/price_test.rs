#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use spiceai::Client;
    use std::env;
    use std::path::Path;

    async fn new_client() -> Client {
        dotenv::from_path(Path::new(".env.local")).ok();
        let api_key = env::var("API_KEY").expect("API_KEY not found");
        Client::new(&api_key)
            .await
            .expect("Failed to create client")
    }
}
