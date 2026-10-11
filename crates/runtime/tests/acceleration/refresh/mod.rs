// Cayenne refreshes from a PostgreSQL source, so it needs the connector and
// not the PostgreSQL-accelerator tests `postgres-accel` selects.
#[cfg(feature = "postgres")]
pub(crate) mod common;
#[cfg(all(not(windows), feature = "postgres"))]
mod refresh_cayenne;
#[cfg(all(feature = "duckdb", feature = "postgres-accel"))]
mod refresh_duckdb;
#[cfg(feature = "postgres-accel")]
mod refresh_modes;
#[cfg(feature = "postgres-accel")]
mod refresh_postgres;
#[cfg(all(feature = "sqlite", feature = "postgres-accel"))]
mod refresh_sqlite;
