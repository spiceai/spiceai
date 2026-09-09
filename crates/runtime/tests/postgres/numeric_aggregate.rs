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

//! Regression test for a pushed-down aggregate over a `PostgreSQL` `bigint`
//! column whose result has more than 18 integer digits (#13785).
//!
//! `PostgreSQL` answers `avg(bigint)` and `sum(bigint)` as an unconstrained
//! `numeric` named after the function (`avg`, `sum`), while the plan expects
//! `avg(hits.UserID)` as `Float64` and `sum(hits.UserID)` as `Int64`. The
//! connector used to read every undeclared `numeric` as `Decimal128(38, 20)`
//! first, which leaves 18 integer digits, so a 19-digit average failed the
//! whole query. The fork now reads such a column straight into the type the
//! plan committed to; this drives the query through the runtime so the real
//! unparsed SQL and the real row conversion are what is exercised.

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::array::{Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use secrecy::ExposeSecret;
use spicepod::{component::dataset::Dataset, param::Params};

use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{self, get_pg_params},
    utils::{register_test_connectors, run_query, runtime_ready_check, test_request_context},
};

fn postgres_dataset(port: usize, table: &str, name: &str) -> Dataset {
    let mut ds = Dataset::new(format!("postgres:{table}"), name.to_string());
    ds.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    ds
}

#[tokio::test]
async fn test_postgres_bigint_aggregate_with_nineteen_integer_digits() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let pool = common::get_postgres_connection_pool(port, None).await?;
            let db_conn = pool
                .connect_direct()
                .await
                .expect("connection can be established");
            db_conn
                .conn
                .execute("CREATE TABLE hits (\"UserID\" bigint);", &[])
                .await
                .expect("table is created");
            // `PostgreSQL` keeps 16 significant digits through the division, so
            // the average of these 19-digit values is itself a 19-digit integer:
            // more than the 18 integer digits `Decimal128(38, 20)` leaves.
            db_conn
                .conn
                .execute(
                    "INSERT INTO hits VALUES (2528953029789715792), (2528953029789715793), (2528953029789715796);",
                    &[],
                )
                .await
                .expect("rows are inserted");
            let text_rows = db_conn
                .conn
                .query(
                    "SELECT avg(\"UserID\")::text, sum(\"UserID\")::text FROM hits",
                    &[],
                )
                .await
                .expect("query executes");
            let avg_text: String = text_rows[0].get(0);
            let sum_text: String = text_rows[0].get(1);
            assert_eq!(avg_text, "2528953029789715794");

            register_test_connectors().await;
            let app = AppBuilder::new("postgres_numeric_aggregate_test")
                .with_dataset(postgres_dataset(port, "hits", "hits"))
                .build();
            configure_test_datafusion();
            let rt = Arc::new(runtime::Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(2)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let batches = run_query(&rt, "SELECT AVG(\"UserID\"), SUM(\"UserID\") FROM hits")
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "a pushed-down aggregate over a bigint column must read into the plan's \
                         Float64/Int64 rather than overflow Decimal128(38, 20) (#13785): {e}"
                    )
                })?;
            let batch = batches
                .into_iter()
                .find(|b| b.num_rows() > 0)
                .ok_or_else(|| anyhow::anyhow!("the aggregate returned no row"))?;
            assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
            assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);

            let avg = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64 column")
                .value(0);
            assert_eq!(
                avg,
                avg_text.parse::<f64>().expect("parses"),
                "the nearest f64 to what PostgreSQL computed ({avg_text})"
            );
            let sum = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column")
                .value(0);
            assert_eq!(sum.to_string(), sum_text);

            running_container.remove().await?;
            Ok(())
        })
        .await
}
