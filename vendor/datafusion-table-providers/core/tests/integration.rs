use rand::Rng;

#[cfg(feature = "adbc")]
mod adbc;
mod arrow_record_batch_gen;
#[cfg(feature = "clickhouse")]
mod clickhouse;
mod docker;
#[cfg(all(feature = "duckdb", feature = "federation"))]
mod duckdb;
#[cfg(feature = "flight")]
mod flight;
#[cfg(feature = "mongodb")]
mod mongodb;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(any(feature = "sqlite", all(feature = "duckdb", feature = "federation")))]
mod roundtrip;
#[cfg(feature = "sqlite")]
mod sqlite;

fn container_registry() -> String {
    std::env::var("CONTAINER_REGISTRY")
        .unwrap_or_else(|_| "public.ecr.aws/docker/library".to_string())
}

fn get_random_port() -> usize {
    rand::rng().random_range(15432..65535)
}
