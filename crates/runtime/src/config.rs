use std::net::SocketAddr;

#[derive(Debug, Clone, clap::Parser)]
pub struct Config {
    /// Configure runtime HTTP address.
    #[arg(
        long = "http",
        value_name = "BIND_ADDRESS",
        default_value = "127.0.0.1:3000",
        action
    )]
    pub http_bind_address: SocketAddr,
    /// Configure runtime Flight address.
    #[arg(
        long = "flight",
        value_name = "FLIGHT_BIND_ADDRESS",
        default_value = "127.0.0.1:50051",
        action
    )]
    pub flight_bind_address: SocketAddr,
    // Configure runtime API key.
    // #[arg(long = "api_key", value_name = "API_KEY", action)]
    // pub api_key: String,
}
