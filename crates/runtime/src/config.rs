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
}
