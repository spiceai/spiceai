/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::LazyLock;

use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter},
};

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("flight"));

pub(crate) static HANDSHAKE_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("handshake_requests").init());

pub(crate) static HANDSHAKE_REQUEST_DURATION_MS: LazyLock<Histogram<f64>> =
    LazyLock::new(|| METER.f64_histogram("handshake_request_duration_ms").init());

pub(crate) static LIST_FLIGHTS_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("list_flights_requests").init());

pub(crate) static GET_FLIGHT_INFO_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("get_flight_info_requests").init());

pub(crate) static GET_SCHEMA_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("get_schema_requests").init());

pub(crate) static DO_GET_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("do_get_requests").init());

pub(crate) static DO_GET_SIMPLE_DURATION_MS: LazyLock<Histogram<f64>> =
    LazyLock::new(|| METER.f64_histogram("do_get_simple_duration_ms").init());

pub(crate) static DO_PUT_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("do_put_requests").init());

pub(crate) static DO_PUT_DURATION_MS: LazyLock<Histogram<f64>> =
    LazyLock::new(|| METER.f64_histogram("do_put_duration_ms").init());

pub(crate) static DO_EXCHANGE_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("do_exchange_requests").init());

pub(crate) static DO_EXCHANGE_DATA_UPDATES_SENT: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("do_exchange_data_updates_sent").init());

pub(crate) static DO_ACTION_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("do_action_requests").init());

pub(crate) static LIST_ACTIONS_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("list_actions_requests").init());

pub(crate) static LIST_ACTIONS_DURATION_MS: LazyLock<Histogram<f64>> =
    LazyLock::new(|| METER.f64_histogram("list_actions_duration_ms").init());

pub(crate) static DO_ACTION_DURATION_MS: LazyLock<Histogram<f64>> =
    LazyLock::new(|| METER.f64_histogram("do_action_duration_ms").init());

pub(crate) mod flightsql {
    use super::*;

    pub(crate) static DO_GET_GET_CATALOGS_DURATION_MS: LazyLock<Histogram<f64>> =
        LazyLock::new(|| {
            METER
                .f64_histogram("do_get_get_catalogs_duration_ms")
                .init()
        });

    pub(crate) static DO_GET_GET_PRIMARY_KEYS_DURATION_MS: LazyLock<Histogram<f64>> =
        LazyLock::new(|| {
            METER
                .f64_histogram("do_get_get_primary_keys_duration_ms")
                .init()
        });

    pub(crate) static DO_GET_GET_SCHEMAS_DURATION_MS: LazyLock<Histogram<f64>> =
        LazyLock::new(|| METER.f64_histogram("do_get_get_schemas_duration_ms").init());

    pub(crate) static DO_GET_GET_SQL_INFO_DURATION_MS: LazyLock<Histogram<f64>> =
        LazyLock::new(|| {
            METER
                .f64_histogram("do_get_get_sql_info_duration_ms")
                .init()
        });

    pub(crate) static DO_GET_TABLE_TYPES_DURATION_MS: LazyLock<Histogram<f64>> =
        LazyLock::new(|| METER.f64_histogram("do_get_table_types_duration_ms").init());

    pub(crate) static DO_GET_GET_TABLES_DURATION_MS: LazyLock<Histogram<f64>> =
        LazyLock::new(|| METER.f64_histogram("do_get_get_tables_duration_ms").init());

    pub(crate) static DO_GET_PREPARED_STATEMENT_QUERY_DURATION_MS: LazyLock<Histogram<f64>> =
        LazyLock::new(|| {
            METER
                .f64_histogram("do_get_prepared_statement_query_duration_ms")
                .init()
        });

    pub(crate) static DO_GET_STATEMENT_QUERY_DURATION_MS: LazyLock<Histogram<f64>> =
        LazyLock::new(|| {
            METER
                .f64_histogram("do_get_statement_query_duration_ms")
                .init()
        });
}
