use datafusion::common::{config_namespace, extensions_options};
use datafusion::config::ConfigExtension;

config_namespace! {
    pub struct SpiceClusterExecutionConfig {
        /// Target size for file groups. Should be larger than the default row-group size e.g. for formats like Parquet.
        pub file_group_size_bytes: u64, default = 128_000_000
        /// A fixed upper bound for the number of maximum stages to emit
        pub file_scan_expand_max_stages: Option<usize>, default = None
        /// If a LIMIT is decorated onto a query and is gteq this value, a `RepartitionExec` will be decorated on top of it
        /// to optimize for any transforms expressed by projections above it
        pub file_scan_min_repartition_limit: u64, default = 100_000
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
