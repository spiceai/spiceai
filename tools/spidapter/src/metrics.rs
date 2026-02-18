/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use test_framework::opentelemetry::metrics::{Gauge, Histogram};
use test_framework::telemetry::meter;

pub static ITERATIONS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("iterations")
        .with_description("Number of query iterations.")
        .with_unit("iterations")
        .build()
});

pub static QUERY_STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("query_status")
        .with_description("Query pass status.")
        .with_unit("status")
        .build()
});

pub static ROW_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("row_count")
        .with_description("Number of rows returned from the query.")
        .with_unit("rows")
        .build()
});

pub static ACCELERATION_SIZE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("acceleration_size_bytes")
        .with_description("Size of acceleration data on disk.")
        .with_unit("bytes")
        .build()
});

pub static READY_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("ready_duration_ms")
        .with_description("Duration until the spicepod is ready.")
        .with_unit("ms")
        .build()
});

pub static HEALTH_LATENCY: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    meter()
        .f64_histogram("health_latency_ms")
        .with_description("Latency of /health and /v1/ready probes.")
        .with_unit("ms")
        .build()
});

pub static MEDIAN_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("median_duration_ms")
        .with_description("Median duration of the query.")
        .with_unit("ms")
        .build()
});

pub static MIN_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("min_duration_ms")
        .with_description("Minimum duration of the query.")
        .with_unit("ms")
        .build()
});

pub static MAX_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("max_duration_ms")
        .with_description("Maximum duration of the query.")
        .with_unit("ms")
        .build()
});

pub static P90_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("p90_duration_ms")
        .with_description("90th percentile duration of the query.")
        .with_unit("ms")
        .build()
});

pub static P95_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("p95_duration_ms")
        .with_description("95th percentile duration of the query.")
        .with_unit("ms")
        .build()
});

pub static P99_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("p99_duration_ms")
        .with_description("99th percentile duration of the query.")
        .with_unit("ms")
        .build()
});

pub static TEST_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("test_duration_ms")
        .with_description("The entire duration of the test.")
        .with_unit("ms")
        .build()
});

pub static VECTOR_INDEX_CREATION_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("vector_index_creation_duration_ms")
        .with_description("Duration of vector search index (embeddings) creation.")
        .with_unit("ms")
        .build()
});

pub static SEARCH_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("search_duration_ms")
        .with_description("Total duration to process all search queries.")
        .with_unit("ms")
        .build()
});

pub static SEARCH_RPS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("search_rps")
        .with_description("Search queries per second.")
        .with_unit("rps")
        .build()
});

pub static SEARCH_P95_RESPONSE_TIME: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("search_p95_time_ms")
        .with_description("95th percentile response time for search queries.")
        .with_unit("ms")
        .build()
});

pub static PEAK_MEMORY_USAGE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("peak_memory_usage_mb")
        .with_description("The maximum observed memory usage during the test.")
        .with_unit("mb")
        .build()
});

pub static MEDIAN_MEMORY_USAGE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("median_memory_usage_mb")
        .with_description("The median observed memory usage during the test.")
        .with_unit("mb")
        .build()
});

pub static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("status")
        .with_description("Test execution status.")
        .with_unit("status")
        .build()
});

pub static SCORE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("score")
        .with_description("Test score.")
        .with_unit("score")
        .build()
});

// Text to Sql specific metrics
// Aggregate Text to Sql specific metrics (run-level)
pub static TEXT_TO_SQL_EXACT_MATCH_RATE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("text_to_sql_exact_match_rate")
        .with_description(
            "The rate at which a text-to-SQL operation correctly outputs an exact match",
        )
        .with_unit("ratio")
        .build()
});
pub static TEXT_TO_SQL_ERROR_RATE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("text_to_sql_error_rate")
        .with_description("The rate at which a text-to-SQL operation returns an error externally")
        .with_unit("ratio")
        .build()
});
pub static TEXT_TO_SQL_MEAN_SQL_QUERY_COUNT: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("text_to_sql_mean_sql_query_count")
        .with_description("Mean number of sql_query operations per text-to-SQL request")
        .with_unit("queries")
        .build()
});
pub static TEXT_TO_SQL_MEAN_LLM_INPUT_TOKENS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("text_to_sql_mean_llm_input_tokens")
        .with_description("Mean LLM input tokens per text-to-SQL request")
        .with_unit("tokens")
        .build()
});
pub static TEXT_TO_SQL_MEAN_LLM_OUTPUT_TOKENS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("text_to_sql_mean_llm_output_tokens")
        .with_description("Mean LLM output tokens per text-to-SQL request")
        .with_unit("tokens")
        .build()
});

// Individual Text to Sql specific metrics (operation-level)
pub static TEXT_TO_SQL_LATENCY: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("text_to_sql_latency_ms")
        .with_description("Client-side text-to-SQL HTTP duration")
        .with_unit("ms")
        .build()
});

pub static TEXT_TO_SQL_SQL_DURATION: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("text_to_sql_sql_duration_ms")
        .with_description("Summation of sql_query operation durations within text-to-SQL operation")
        .with_unit("ms")
        .build()
});

pub static TEXT_TO_SQL_SQL_QUERY_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("text_to_sql_sql_query_count")
        .with_description("Number of sql_query operations within text-to-SQL operation")
        .with_unit("queries")
        .build()
});

pub static TEXT_TO_SQL_LLM_DURATION: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("text_to_sql_llm_duration_ms")
        .with_description(
            "Summation of ai_completion operation durations within text-to-SQL operation",
        )
        .with_unit("ms")
        .build()
});

pub static TEXT_TO_SQL_LLM_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("text_to_sql_llm_count")
        .with_description("Number of ai_completion operations within text-to-SQL operation")
        .with_unit("completions")
        .build()
});

pub static TEXT_TO_SQL_LLM_INPUT_TOKENS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("text_to_sql_llm_input_tokens")
        .with_description("Summation of input tokens used across all ai_completion operations within text-to-SQL operation")
        .with_unit("tokens")
        .build()
});

pub static TEXT_TO_SQL_LLM_OUTPUT_TOKENS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("text_to_sql_llm_output_tokens")
        .with_description("Summation of output tokens used across all ai_completion operations within text-to-SQL operation")
        .with_unit("tokens")
        .build()
});

pub static TEXT_TO_SQL_EXACT_MATCH: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("text_to_sql_exact_match")
        .with_description("the produced SQL matches the expected (string equality)")
        .with_unit("1")
        .build()
});

pub static TEXT_TO_SQL_EXACT_LOGICAL_PLAN_MATCH: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("text_to_sql_exact_logical_plan_match")
        .with_description("the produced logical plan matches that derived from the expected SQL")
        .with_unit("1")
        .build()
});

pub static TEXT_TO_SQL_ERROR: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("text_to_sql_error")
        .with_description("the text-to-SQL operation returned an HTTP error")
        .with_unit("1")
        .build()
});

pub static TEXT_TO_SQL_CORRECT_TABLES: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("text_to_sql_correct_tables")
        .with_description("Jaccard similarity of tables scanned in the expected and produced SQL")
        .with_unit("1")
        .build()
});

pub static TEXT_TO_SQL_CORRECT_TABLE_PROJECTIONS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("text_to_sql_correct_table_projections")
        .with_description("Jaccard similarity of the table-qualified column names requested for each table from expected and produced SQL")
        .with_unit("1")
        .build()
});

pub static TEXT_TO_SQL_CORRECT_OUTPUT_SCHEMA: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("text_to_sql_correct_output_schema")
        .with_description("Jaccard similarity of the fields from the output SQL schema from expected and produced SQL")
        .with_unit("1")
        .build()
});

// Spiced runtime metrics (scraped from /metrics endpoint)

pub static SPICED_QUERY_COUNT: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("spiced_query_count")
        .with_description("Total number of queries executed by spiced.")
        .with_unit("queries")
        .build()
});

#[expect(dead_code)]
pub static SPICED_QUERY_DURATION_AVG: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("spiced_query_duration_avg_ms")
        .with_description("Average query duration from spiced metrics.")
        .with_unit("ms")
        .build()
});

pub static SPICED_CACHE_HIT_RATE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("spiced_cache_hit_rate")
        .with_description("Cache hit rate from spiced metrics.")
        .with_unit("ratio")
        .build()
});

pub static SPICED_ACTIVE_CONNECTIONS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("spiced_active_connections")
        .with_description("Peak active connections during test.")
        .with_unit("connections")
        .build()
});

// Streaming ingestion benchmark metrics

pub static INGESTION_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("ingestion_duration_ms")
        .with_description(
            "Duration from Spice ready until all markers detected (CDC ingestion time).",
        )
        .with_unit("ms")
        .build()
});

pub static STREAM_LAG: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("stream_lag_ms")
        .with_description("Duration from marker insertion until marker detected (CDC stream lag).")
        .with_unit("ms")
        .build()
});

#[expect(dead_code)]
pub static DATA_INSERTION_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("data_insertion_duration_ms")
        .with_description("Duration to insert all data into DynamoDB.")
        .with_unit("ms")
        .build()
});

pub static RECORD_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("record_count")
        .with_description("Number of records generated and inserted.")
        .with_unit("records")
        .build()
});

pub static RECORDS_PER_SECOND: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("records_per_second")
        .with_description("Ingestion throughput in records per second.")
        .with_unit("records/s")
        .build()
});

pub static DYNAMODB_TRANSIENT_ERRORS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("dynamodb_transient_errors_total")
        .with_description("Total transient errors during DynamoDB streaming ingestion.")
        .with_unit("errors")
        .build()
});

pub static LIVENESS_FAILURES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("liveness_failures")
        .with_description("Total health check failures during streaming ingestion.")
        .with_unit("failures")
        .build()
});

pub static LIVENESS_MAX_LATENCY: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("liveness_max_latency_ms")
        .with_description("Maximum health check latency during streaming ingestion.")
        .with_unit("ms")
        .build()
});

// Query liveness metrics
pub static QUERY_LIVENESS_TOTAL: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("query_liveness_total")
        .with_description("Total query liveness checks executed during streaming ingestion.")
        .with_unit("queries")
        .build()
});

pub static QUERY_LIVENESS_FAILURES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("query_liveness_failures")
        .with_description("Failed query liveness checks during streaming ingestion.")
        .with_unit("failures")
        .build()
});

pub static QUERY_LIVENESS_SUCCESS_RATE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("query_liveness_success_rate")
        .with_description("Query liveness success rate during streaming ingestion.")
        .with_unit("%")
        .build()
});

pub static QUERY_LIVENESS_AVG_LATENCY: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("query_liveness_avg_latency_ms")
        .with_description("Average query liveness latency during streaming ingestion.")
        .with_unit("ms")
        .build()
});

pub static QUERY_LIVENESS_MAX_LATENCY: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("query_liveness_max_latency_ms")
        .with_description("Maximum query liveness latency during streaming ingestion.")
        .with_unit("ms")
        .build()
});

pub static QUERY_LIVENESS_P90_LATENCY: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("query_liveness_p90_latency_ms")
        .with_description("P90 query liveness latency during streaming ingestion.")
        .with_unit("ms")
        .build()
});

pub static QUERY_LIVENESS_P95_LATENCY: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("query_liveness_p95_latency_ms")
        .with_description("P95 query liveness latency during streaming ingestion.")
        .with_unit("ms")
        .build()
});

pub static QUERY_LIVENESS_P99_LATENCY: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("query_liveness_p99_latency_ms")
        .with_description("P99 query liveness latency during streaming ingestion.")
        .with_unit("ms")
        .build()
});
