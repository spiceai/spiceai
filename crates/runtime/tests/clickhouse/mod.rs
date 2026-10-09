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

//! The `ClickHouse` connector against a real server: every column type it maps, read
//! federated and accelerated, compared with the rows the fixture inserted.

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::array::RecordBatch;
use arrow::util::display::FormatOptions;
use bollard::secret::HealthConfig;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::Acceleration, component::dataset::Dataset, param::Params as DatasetParams,
};

use crate::docker::{ContainerRunnerBuilder, RunningContainer};
use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};

const CLICKHOUSE_IMAGE: &str = "docker.io/clickhouse/clickhouse-server:26.3";
const CLICKHOUSE_PASSWORD: &str = "integration-test-pw";
const CLICKHOUSE_TCP_PORT: u16 = 9000;

/// One column per mapped type, with NULLs, empty values and each type's boundaries.
/// The first `map` repeats a key: `ClickHouse` keeps both entries in order, and a decoder that
/// collected the entries by key would lose one.
/// `dt64_tokyo` carries an explicit timezone: the connector reads every timestamp as a
/// UTC instant, so its rows show the Tokyo wall-clock time minus nine hours.
const FIXTURE: &str = "
CREATE TABLE types (
    id UInt32,
    lc LowCardinality(String),
    lc_nullable LowCardinality(Nullable(String)),
    dt64_3 DateTime64(3),
    dt64_tokyo DateTime64(9, 'Asia/Tokyo'),
    dt_utc DateTime('UTC'),
    arr Array(Nullable(String)),
    map Map(String, Int32),
    tuple Tuple(Int32, String),
    named_tuple Tuple(id Int64, label Nullable(String)),
    enum8 Enum8('a' = 1, 'b' = 2),
    enum16 Enum16('x' = -32768, 'y' = 32767),
    u128 UInt128,
    i128 Int128,
    ipv4 IPv4,
    ipv6 IPv6,
    nested Array(Tuple(id Int32, tags Map(String, Array(Nullable(Int32)))))
) ENGINE = MergeTree ORDER BY id;
INSERT INTO types VALUES
    (1, 'x', 'a', '2024-04-24 12:34:56.789', '2024-01-01 09:00:00.123456789', '2024-04-24 12:34:56',
     ['p', NULL], {'b': 2, 'a': 1, 'b': 3}, (1, 'one'), (10, 'ten'), 'a', 'x',
     340282366920938463463374607431768211455, -170141183460469231731687303715884105728, '127.0.0.1', '2001:db8::1',
     [(1, {'a': [1, NULL]}), (2, {})]),
    (2, '', NULL, '1969-12-31 23:59:59.999', '1970-01-01 09:00:00', '1970-01-01 00:00:00',
     [], {}, (-2, ''), (-1, NULL), 'b', 'y',
     0, 170141183460469231731687303715884105727, '0.0.0.0', '::',
     []),
    (3, 'x', 'a', '2100-01-01 00:00:00.001', '2262-04-12 08:47:16.854775807', '2106-02-07 06:28:15',
     [NULL], {'k': -7}, (2147483647, 'max'), (0, ''), 'a', 'x',
     12345678901234567890123, -1, '255.255.255.255', '::ffff:1.2.3.4',
     [(3, {'x': [], 'y': [7]})]);
CREATE TABLE datetime64_past_2262 (id UInt32, at DateTime64(8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO datetime64_past_2262 VALUES (1, '2299-12-31 23:59:59.99999999');
";

fn make_clickhouse_dataset(table: &str, name: &str, port: u16, accelerated: bool) -> Dataset {
    let mut dataset = Dataset::new(format!("clickhouse:{table}"), name.to_string());
    let params = HashMap::from([
        ("clickhouse_host".to_string(), "localhost".to_string()),
        ("clickhouse_tcp_port".to_string(), port.to_string()),
        ("clickhouse_db".to_string(), "default".to_string()),
        ("clickhouse_user".to_string(), "default".to_string()),
        (
            "clickhouse_pass".to_string(),
            CLICKHOUSE_PASSWORD.to_string(),
        ),
        ("clickhouse_secure".to_string(), "false".to_string()),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    if accelerated {
        dataset.acceleration = Some(Acceleration::default());
    }
    dataset
}

async fn start_clickhouse_docker_container() -> Result<RunningContainer, anyhow::Error> {
    ContainerRunnerBuilder::new("runtime-integration-test-clickhouse")
        .image(CLICKHOUSE_IMAGE.to_string())
        .publish_port(CLICKHOUSE_TCP_PORT)
        .add_env_var("CLICKHOUSE_PASSWORD", CLICKHOUSE_PASSWORD)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                format!("clickhouse-client --password {CLICKHOUSE_PASSWORD} --query 'SELECT 1'"),
            ]),
            interval: Some(1_000_000_000), // 1s
            timeout: Some(5_000_000_000),  // 5s
            retries: Some(120),
            start_period: Some(30_000_000_000), // 30s
            start_interval: None,
        })
        .build()?
        .run(Some(Duration::from_mins(3)))
        .await
}

async fn query(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, String> {
    rt.datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| e.to_string())?
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn clickhouse_reads_every_mapped_type_federated_and_accelerated() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let container = start_clickhouse_docker_container()
                .await
                .map_err(|e| format!("start ClickHouse: {e}"))?;
            container
                .exec([
                    "clickhouse-client",
                    "--password",
                    CLICKHOUSE_PASSWORD,
                    "-n",
                    "--query",
                    FIXTURE,
                ])
                .await
                .map_err(|e| format!("load the fixture: {e}"))?;
            let port = container
                .host_port(CLICKHOUSE_TCP_PORT)
                .map_err(|e| e.to_string())?;

            let app = AppBuilder::new("clickhouse_types")
                .with_dataset(make_clickhouse_dataset("types", "types", port, false))
                .with_dataset(make_clickhouse_dataset(
                    "types",
                    "types_accelerated",
                    port,
                    true,
                ))
                .with_dataset(make_clickhouse_dataset(
                    "datetime64_past_2262",
                    "datetime64_past_2262",
                    port,
                    false,
                ))
                .build();
            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(2)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = Arc::new(rt.clone()).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // NULL is spelled out so it cannot be mistaken for an empty string or list.
            let with_nulls = FormatOptions::default().with_null("NULL");
            let federated = query(&rt, "SELECT * FROM types ORDER BY id").await?;
            let schema = federated
                .first()
                .ok_or("the federated query returned no batch")?
                .schema()
                .fields()
                .iter()
                .map(|field| format!("{}: {}", field.name(), field.data_type()))
                .collect::<Vec<_>>()
                .join("\n");
            insta::assert_snapshot!("clickhouse_types_schema", schema);
            let federated = arrow::util::pretty::pretty_format_batches_with_options(&federated, &with_nulls)
                .map_err(|e| e.to_string())?
                .to_string();
            insta::assert_snapshot!("clickhouse_types_rows", federated);

            // The accelerated copy was loaded through the same conversion and is read
            // back from the accelerator, so it must hold exactly the federated rows.
            let accelerated = query(&rt, "SELECT * FROM types_accelerated ORDER BY id").await?;
            let accelerated = arrow::util::pretty::pretty_format_batches_with_options(&accelerated, &with_nulls)
                .map_err(|e| e.to_string())?
                .to_string();
            assert_eq!(accelerated, federated);

            let past_2262 = query(&rt, "SELECT * FROM datetime64_past_2262")
                .await
                .expect_err("a DateTime64(8) value after 2262-04-11 has no Arrow timestamp");
            assert!(
                past_2262.contains(
                    "The DateTime64(8, 'UTC') value 2299-12-31 23:59:59.999999990 is outside the range an Arrow timestamp of its precision can hold (nanosecond timestamps end at 2262-04-11)"
                ),
                "{past_2262}"
            );

            container.remove().await.map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
}
