use datafusion::common::{config_namespace, extensions_options};
use datafusion::config::ConfigExtension;

config_namespace! {
    pub struct SpiceClusterExecutionConfig {
        pub file_group_size_bytes: u64, default = 256_000_000
        pub file_scan_expand_stages: usize, default = 200
    }
}

extensions_options! {
    pub struct SpiceClusterConfig {
        pub execution: SpiceClusterExecutionConfig, default = SpiceClusterExecutionConfig::default()
    }
}

impl ConfigExtension for SpiceClusterConfig {
    const PREFIX: &'static str = "spice";
}
