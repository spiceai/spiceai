// Cayenne refreshes from a PostgreSQL source, so it needs the connector.
#[cfg(feature = "postgres")]
pub(crate) mod common;
#[cfg(feature = "postgres")]
mod refresh_cayenne;
// Append and full row comparisons. `postgres` is the connector feature the integration
// workflow already enables, so those tests are in that lane.
#[cfg(feature = "postgres")]
mod refresh_modes;
