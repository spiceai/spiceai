// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Driver, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::ManagedDriver;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::{
    adbc::AdbcTableFactory, sql::db_connection_pool::adbcpool::ADBCPool,
};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let mut driver =
        ManagedDriver::load_from_name("duckdb", None, AdbcVersion::V110, LOAD_FLAG_DEFAULT, None)
            .unwrap();
    let db = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            "core/examples/duckdb_example.db".into(),
        )])
        .unwrap();

    let adbc_pool = Arc::new(ADBCPool::new(db, None).expect("Failed to create ADBC pool"));

    let table_factory = AdbcTableFactory::new(adbc_pool.clone());

    let ctx = SessionContext::new();

    ctx.register_table(
        "companies_v2",
        table_factory
            .table_provider(TableReference::bare("companies"), None)
            .await
            .expect("Failed to register table provider"),
    )
    .expect("Failed to register table");

    let df = ctx
        .sql("SELECT * FROM datafusion.public.companies_v2")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");
}
