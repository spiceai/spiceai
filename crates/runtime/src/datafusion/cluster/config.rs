use datafusion::common::{config_namespace, extensions_options};
use datafusion::config::ConfigExtension;

config_namespace! {
    pub struct SpiceClusterExecutionConfig {
        /// Target size for file groups. Should be larger than the default row-group size e.g. for formats like Parquet.
        pub file_group_size_bytes: u64, default = 128_000_000
        pub file_scan_expand_max_stages: Option<usize>, default = None
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
