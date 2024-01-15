use std::net::SocketAddr;

#[derive(Debug, Clone, clap::Parser)]
pub struct Config {
    #[clap(long = "http-bind", default_value = "127.0.0.1:3000", action)]
    pub http_bind_address: SocketAddr,
}
