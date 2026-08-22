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

//! Regression test for a Postgres `NUMERIC` decimal-scale mismatch on a
//! pushed-down aggregate.
//!
//! Federation pushes `AVG`/division on a `NUMERIC` column down to Postgres as
//! a single query. The caller's schema commits to a scale ahead of execution
//! (e.g. DataFusion's own decimal type-coercion for `AVG` widens the source
//! column's scale by a fixed few digits), but Postgres computes the actual
//! average with its own, wider `NUMERIC` scale. `rows_to_arrow` used to treat
//! that as a hard error (`NumericScaleTooWide` / #13349) instead of rounding
//! to the declared scale, which broke every benchmark query dividing a
//! `NUMERIC` column against a live Postgres source (`tpch_q1`'s `avg_qty`,
//! `tpch_q8`'s `mkt_share`).
//!
//! This drives the exact conversion function the query engine calls
//! (`rows_to_arrow`) with rows a real Postgres server returned for `AVG`, so
//! the "more decimal places than declared" scale mismatch is genuine —
//! Postgres computes it, this test doesn't synthesize it — rather than going
//! through the full federation optimizer just to reach the same rows.

use std::sync::Arc;

use arrow::array::Decimal128Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_table_providers::sql::arrow_sql_gen::postgres::rows_to_arrow;

use crate::postgres::common;
use crate::{init_tracing, utils::test_request_context};

#[tokio::test]
async fn test_postgres_avg_rounds_to_declared_scale_instead_of_erroring()
-> Result<(), anyhow::Error> {
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
                .execute("CREATE TABLE numeric_avg_test (qty NUMERIC(15,2));", &[])
                .await
                .expect("table is created");
            // 4 / 3 = 1.3333... : Postgres's NUMERIC average carries far more
            // than 6 fractional digits for this value.
            db_conn
                .conn
                .execute(
                    "INSERT INTO numeric_avg_test (qty) VALUES (1.00), (1.00), (2.00);",
                    &[],
                )
                .await
                .expect("inserted data");

            // Mirrors what DataFusion's own decimal type-coercion computes for
            // `AVG` on a `NUMERIC(15,2)` column ahead of pushing the query
            // down: `Decimal128(38, 6)`, a fixed few digits wider than the
            // source column's own scale of 2 -- narrower than what Postgres
            // will actually compute.
            let declared_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                "avg_qty",
                DataType::Decimal128(38, 6),
                true,
            )]));

            let rows = db_conn
                .conn
                .query("SELECT AVG(qty) AS avg_qty FROM numeric_avg_test", &[])
                .await
                .expect("query executes");

            // Postgres's own rounding at the declared scale is the ground
            // truth this test cross-checks against, rather than hand-deriving
            // the expected rounded value.
            let rounded_rows = db_conn
                .conn
                .query(
                    "SELECT ROUND(AVG(qty), 6) AS avg_qty FROM numeric_avg_test",
                    &[],
                )
                .await
                .expect("query executes");
            let expected_batch = rows_to_arrow(&rounded_rows, &Some(Arc::clone(&declared_schema)))
                .expect(
                    "a value Postgres already rounded to 6 places fits Decimal128(38, 6) exactly",
                );
            let expected_coefficient = expected_batch.columns()[0]
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("array can be cast")
                .value(0);

            let batch = rows_to_arrow(&rows, &Some(declared_schema)).expect(
                "a value with more decimal places than the declared scale should be rounded, \
                 not refused (regression test for #13349)",
            );
            let actual_coefficient = batch.columns()[0]
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("array can be cast")
                .value(0);

            assert_eq!(
                expected_coefficient, actual_coefficient,
                "AVG(qty) rounded to the declared scale should match Postgres's own ROUND()"
            );

            running_container.remove().await?;

            Ok(())
        })
        .await
}
