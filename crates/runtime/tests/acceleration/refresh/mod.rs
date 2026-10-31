// Cayenne refreshes from a PostgreSQL source, so it needs the connector and
// not the PostgreSQL-accelerator tests `postgres-accel` selects.
#[cfg(feature = "postgres")]
pub(crate) mod common;
#[cfg(feature = "postgres")]
mod refresh_cayenne;
#[cfg(all(feature = "duckdb", feature = "postgres-accel"))]
mod refresh_duckdb;
// Append and full row comparisons. `postgres` is the connector feature the integration
// workflow already enables, so those tests are in that lane. The `PostgreSQL` accelerator
// variants in the module stay behind `postgres-accel`, which that workflow does not enable.
#[cfg(feature = "postgres")]
mod refresh_modes;
#[cfg(feature = "postgres-accel")]
mod refresh_postgres;
#[cfg(all(feature = "sqlite", feature = "postgres-accel"))]
mod refresh_sqlite;
