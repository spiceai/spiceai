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

    /// Configure runtime OpenTelemetry address.
    #[arg(
        long = "open_telemetry",
        value_name = "OPEN_TELEMETRY_BIND_ADDRESS",
        default_value = "127.0.0.1:50052",
        action
    )]
    pub open_telemetry_bind_address: SocketAddr,

    /// Configure remote dataset retry attempts.
    #[arg(
        long = "remote_dataset_retries",
        value_name = "REMOTE_DATASET_RETRIES",
        default_value = "5",
        action
    )]
    pub remote_dataset_retries: u32,
}
