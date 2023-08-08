
#[cfg(test)]
mod tests {
    use <crate_name>::*;

    #[test]
    fn test_client_creation() {
        let client = Client::new("http://localhost");
        assert_eq!(client.base_url, "http://localhost");
    }
    
    // Further tests for the client module
}
