// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::prelude::SessionContext;
use datafusion_table_providers::flight::sql::{FlightSqlDriver, QUERY};
use datafusion_table_providers::flight::FlightTableFactory;
use std::collections::HashMap;
use std::sync::Arc;

/// Prerequisites:
/// ```
/// $ brew install roapi
/// $ roapi -t taxi=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
/// ```
#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let ctx = SessionContext::new();
    let flight_sql = FlightTableFactory::new(Arc::new(FlightSqlDriver::new()));
    let table = flight_sql
        .open_table(
            "http://localhost:32010",
            HashMap::from([(QUERY.into(), "SELECT * FROM taxi".into())]),
        )
        .await?;
    ctx.register_table("trip_data", Arc::new(table))?;
    ctx.sql("select * from trip_data limit 10")
        .await?
        .show()
        .await?;

    // The same table created using DDL
    ctx.state_ref()
        .write()
        .table_factories_mut()
        .insert("FLIGHT_SQL".into(), Arc::new(flight_sql));
    let _ = ctx
        .sql(
            r#"
            CREATE EXTERNAL TABLE trip_data2 STORED AS FLIGHT_SQL
            LOCATION 'http://localhost:32010'
            OPTIONS (
                'flight.sql.query' 'SELECT * FROM taxi'
            )
        "#,
        )
        .await?;

    let df = ctx
        .sql(
            r#"
            SELECT "VendorID", COUNT(*), SUM(passenger_count), SUM(total_amount)
            FROM trip_data2
            GROUP BY "VendorID"
            ORDER BY COUNT(*) DESC
        "#,
        )
        .await?;
    df.clone().explain(false, false)?.show().await?;
    df.show().await
}
