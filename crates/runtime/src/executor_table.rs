/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! `executor_table('endpoint', 'table')` — a table-valued function that fetches
//! a single peer executor's partition of a table over Flight SQL.
//!
//! This is the primitive behind distributed broadcast joins on the
//! distributed-acceleration (Cayenne catalog) path. Each executor physically
//! holds only its assigned partition of a table, so a plain `SELECT * FROM t`
//! issued to one executor returns just that executor's slice. A small dimension
//! table can therefore be reconstructed in full on ANY node by UNION-ing
//! `executor_table(eᵢ, 't')` over the live executors:
//!
//! ```sql
//! SELECT * FROM executor_table('https://e0:50052', 'spicebench.bench.nation')
//! UNION ALL
//! SELECT * FROM executor_table('https://e1:50052', 'spicebench.bench.nation')
//! ...
//! ```
//!
//! The broadcast-join rewrite rule pushes a per-executor join SQL down to each
//! fact-partition executor, where the small dimension is gathered via this
//! UNION and the join runs locally — instead of shipping the whole fact table
//! up to the scheduler. The schema is resolved from the LOCAL catalog (every
//! node shares the table definition; only the data is partitioned), so no
//! remote schema round-trip is needed, and the wrapped [`FlightSQLTable`]
//! pushes the dimension's own filters/projection into the peer `SELECT` for
//! free.

use std::sync::{Arc, Weak};

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_schema::SchemaRef;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use flight_client::cookie::{CookieService, CookieStore};
use tonic::transport::{ClientTlsConfig, Endpoint};

use cayenne::catalog_provider::CayenneSchemaProvider;
use data_components::flightsql::{FlightSQLTable, FlightSqlClient};

use crate::datafusion::DataFusion;

/// UDTF name for `executor_table`.
pub const EXECUTOR_TABLE_UDTF_NAME: &str = "executor_table";

const MAX_FLIGHT_MESSAGE_SIZE: usize = 100 * 1024 * 1024;

/// `executor_table('endpoint', 'table')` table function.
pub struct ExecutorTableFunc {
    df: Weak<DataFusion>,
}

impl std::fmt::Debug for ExecutorTableFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorTableFunc").finish()
    }
}

impl ExecutorTableFunc {
    #[must_use]
    pub fn new(df: Weak<DataFusion>) -> Self {
        Self { df }
    }
}

fn literal_string(arg: Option<&Expr>, what: &str) -> DataFusionResult<String> {
    match arg {
        Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => Ok(s.clone()),
        Some(Expr::Literal(ScalarValue::Utf8View(Some(s)), _)) => Ok(s.clone()),
        other => Err(DataFusionError::Plan(format!(
            "executor_table: expected a string literal for the {what}, got {other:?}"
        ))),
    }
}

/// Build a Flight SQL client to a peer executor's endpoint, applying the node's
/// cluster mTLS config. Mirrors `cluster::service::create_executor_flight_client`.
fn build_peer_client(
    endpoint: &str,
    client_tls_config: Option<ClientTlsConfig>,
) -> DataFusionResult<FlightSqlClient> {
    let address = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    let mut channel = Endpoint::from_shared(address)
        .map_err(|e| DataFusionError::Execution(format!("executor_table: bad endpoint: {e}")))?;
    if let Some(tls_config) = client_tls_config {
        channel = channel
            .tls_config(tls_config)
            .map_err(|e| DataFusionError::Execution(format!("executor_table: tls: {e}")))?;
    }
    let channel = CookieService::new(channel.connect_lazy(), Arc::new(CookieStore::new()));
    Ok(FlightSqlServiceClient::new_from_inner(
        FlightServiceClient::new(channel)
            .max_encoding_message_size(MAX_FLIGHT_MESSAGE_SIZE)
            .max_decoding_message_size(MAX_FLIGHT_MESSAGE_SIZE),
    ))
}

impl TableFunctionImpl for ExecutorTableFunc {
    fn call(&self, args: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let endpoint = literal_string(args.first(), "executor endpoint")?;
        let table_str = literal_string(args.get(1), "table name")?;
        let table_ref = TableReference::parse_str(&table_str);

        let df = self.df.upgrade().ok_or_else(|| {
            DataFusionError::Plan("executor_table: DataFusion instance has been dropped".to_string())
        })?;

        // Resolve the schema SYNCHRONOUSLY from the LOCAL catalog. `get_table_sync`
        // only sees `SpiceSchemaProvider` tables (not external catalogs like
        // Cayenne), and the generic `SchemaProvider::table()` is async — so
        // resolve the catalog/schema (both sync) and read the Cayenne schema
        // provider's in-memory table cache directly via `table_sync`.
        let cat_name = table_ref.catalog().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "executor_table: table '{table_str}' must be catalog-qualified"
            ))
        })?;
        let sch_name = table_ref.schema().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "executor_table: table '{table_str}' must be schema-qualified"
            ))
        })?;
        let catalog = df.ctx.catalog(cat_name).ok_or_else(|| {
            DataFusionError::Plan(format!("executor_table: catalog '{cat_name}' not found"))
        })?;
        let schema_provider = catalog.schema(sch_name).ok_or_else(|| {
            DataFusionError::Plan(format!("executor_table: schema '{sch_name}' not found"))
        })?;
        let cayenne_schema = schema_provider
            .as_any()
            .downcast_ref::<CayenneSchemaProvider>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "executor_table: schema '{cat_name}.{sch_name}' is not a Cayenne catalog schema"
                ))
            })?;
        let provider = cayenne_schema.table_sync(table_ref.table()).ok_or_else(|| {
            DataFusionError::Plan(format!("executor_table: table '{table_str}' not found locally"))
        })?;
        let schema: SchemaRef = provider.schema();

        let client = build_peer_client(&endpoint, df.cluster_config.client_tls_config())?;
        let cookie_store = Arc::new(CookieStore::new());

        Ok(Arc::new(FlightSQLTable::create_with_schema(
            EXECUTOR_TABLE_UDTF_NAME,
            &endpoint,
            client,
            table_ref,
            schema,
            cookie_store,
        )))
    }
}
