
use reqwest::{Client as HttpClient, Response, Error};

pub struct Client {
    base_url: String,
    http_client: HttpClient,
}

impl Client {
    pub fn new(base_url: &str) -> Self {
        Client {
            base_url: base_url.to_string(),
            http_client: HttpClient::new(),
        }
    }

    // Method to perform a GET request
    pub async fn get(&self, path: &str) -> Result<Response, Error> {
        let url = format!("{}/{}", self.base_url, path);
        self.http_client.get(&url).send().await
    }
    
    // Further methods for POST, PUT, DELETE can be added here
}
