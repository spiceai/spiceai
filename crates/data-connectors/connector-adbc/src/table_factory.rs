/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The ADBC table factory, with the federation policy attached to its type.
//!
//! This module exists for the privacy boundary: [`AdbcTableFactoryWithPolicy`]'s
//! field is private to it, so [`AdbcTableFactoryWithPolicy::new`] is the only way
//! to obtain one anywhere else in the crate.

use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::optimizer::OptimizerRule;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_table_providers::adbc::AdbcTableFactory;
use datafusion_table_providers::sql::db_connection_pool::adbcpool::ADBCPool;
use datafusion_table_providers::util::supported_functions::FunctionSupport;
use runtime_datafusion::dialect::new_bigquery_dialect;
use runtime_datafusion::function_support::deny_spice_functions_for_bigquery_table_providers;
use runtime_datafusion::optimizer_rule::RegexpMatchNullCheckRewrite;
use runtime_udfs_api::deny_spice_functions_for_table_providers;

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// The `BigQuery` driver, as ADBC spells it in the `driver` parameter.
const BIGQUERY_DRIVER: &str = "bigquery";

/// The unparser dialect for a driver.
///
/// This and [`function_support_for_driver`] are deliberately adjacent and keyed
/// off the same name: a dialect that rewrites a Spice function into native SQL
/// is only safe when the matching policy — the one that lets that function
/// federate, and refuses the call shapes the dialect cannot render — is
/// installed with it. Splitting them is how they come apart.
pub(crate) fn dialect_for_driver(driver_name: &str) -> Option<Arc<dyn Dialect + Send + Sync>> {
    match driver_name {
        BIGQUERY_DRIVER => Some(new_bigquery_dialect()),
        _ => None,
    }
}

/// The federation function-support policy for a driver. Defaults to denying
/// every Spice function, which is correct for a driver whose dialect rewrites
/// none of them.
fn function_support_for_driver(driver_name: &str) -> FunctionSupport {
    match driver_name {
        BIGQUERY_DRIVER => deny_spice_functions_for_bigquery_table_providers(),
        _ => deny_spice_functions_for_table_providers(),
    }
}

fn pre_federation_optimizer_rules_for_driver(
    driver_name: &str,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    match driver_name {
        BIGQUERY_DRIVER => vec![Arc::new(RegexpMatchNullCheckRewrite::new())],
        _ => vec![],
    }
}

/// An [`AdbcTableFactory`] that carries Spice's federation policy by
/// construction.
///
/// `AdbcTableFactory::new` defaults to `federation_enabled: true` and
/// `function_support: None`, and both defaults are silent:
///
/// * without `with_function_support`, federation unparses every Spice-only UDF
///   — the JSON functions, the embedding and distance UDFs, and every
///   user-registered function — into the SQL sent to the remote database, which
///   has no such function and answers with an unknown-function error;
/// * without `with_federation_enabled`, `query_federation: disabled` is parsed,
///   validated, and then ignored, so the documented way to turn federation off
///   does nothing.
///
/// Both calls have been dropped once already, by a refactor that moved the
/// construction and compiled without them
/// ([#10703](https://github.com/spiceai/spiceai/issues/10703)). Requiring this
/// type wherever a factory is held is what stops that recurring: the policy is
/// now applied in one function that cannot be bypassed, so dropping it is an
/// edit to that function rather than an omission a move can make invisible.
///
/// The scope is this connector. The ADBC **catalog** connector builds an
/// `AdbcTableFactory` directly and carries no policy at all —
/// [#13664](https://github.com/spiceai/spiceai/issues/13664).
pub(crate) struct AdbcTableFactoryWithPolicy<D>
where
    D: adbc_core::Database + Send + 'static,
    D::ConnectionType: adbc_core::Connection + Send + Sync,
    <D::ConnectionType as adbc_core::Connection>::StatementType:
        datafusion_table_providers::sql::db_connection_pool::dbconnection::adbcconn::CancellableStatement,
{
    factory: AdbcTableFactory<D>,
}

impl<D> AdbcTableFactoryWithPolicy<D>
where
    D: adbc_core::Database + Send + 'static,
    D::ConnectionType: adbc_core::Connection + Send + Sync,
    <D::ConnectionType as adbc_core::Connection>::StatementType:
        datafusion_table_providers::sql::db_connection_pool::dbconnection::adbcconn::CancellableStatement,
{
    /// Builds the factory with the driver's function-support policy installed
    /// and the `query_federation` setting applied.
    pub(crate) fn new(pool: Arc<ADBCPool<D>>, federation_enabled: bool, driver_name: &str) -> Self {
        Self {
            factory: AdbcTableFactory::new(pool)
                .with_federation_enabled(federation_enabled)
                .with_function_support(function_support_for_driver(driver_name))
                .with_pre_federation_optimizer_rules(pre_federation_optimizer_rules_for_driver(
                    driver_name,
                )),
        }
    }

    pub(crate) async fn table_provider(
        &self,
        table_reference: TableReference,
        dialect: Option<Arc<dyn Dialect + Send + Sync>>,
    ) -> Result<Arc<dyn TableProvider + 'static>, BoxedError> {
        self.factory.table_provider(table_reference, dialect).await
    }

    pub(crate) async fn read_write_table_provider(
        &self,
        table_reference: TableReference,
        dialect: Option<Arc<dyn Dialect + Send + Sync>>,
    ) -> Result<Arc<dyn TableProvider + 'static>, BoxedError> {
        self.factory
            .read_write_table_provider(table_reference, dialect)
            .await
    }
}
