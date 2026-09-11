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

//! Real listing-table scan vs in-place Parquet overwrite.
//!
//! Regression test for <https://github.com/spiceai/spiceai/issues/13793>. A
//! listed S3 object that is replaced while a scan is reading it must not be
//! decoded as a mixture of two generations (decoder error, panic, or silently
//! wrong rows).

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::ensure;
use app::AppBuilder;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use bollard::secret::HealthConfig;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use futures::StreamExt;
use runtime::Runtime;
use runtime::accelerated::refresh_task::refresh_error_reason_from_message;
use runtime_metrics::acceleration::{
    REFRESH_ERROR_REASON_OBJECT_GENERATION_CHANGED, REFRESH_ERROR_REASON_PARQUET_DECODE,
};
use spicepod::{
    acceleration::{Acceleration, RefreshMode},
    component::{
        caching::SQLResultsCacheConfig,
        dataset::{Dataset, TimeFormat},
    },
    param::Params,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::{
    configure_test_datafusion,
    docker::{ContainerRunnerBuilder, RunningContainer, wait_for_tcp_port},
    init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

const MINIO_HOST_PORT: u16 = 19_137;
const PROXY_PORT: u16 = 19_138;
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const BUCKET: &str = "overwrite-race";
const OBJECT_KEY: &str = "listing/data.parquet";
const ROWS: i64 = 20_000;
const ROW_GROUP_SIZE: usize = 2_000;
/// Unix seconds for row `id=0`. Later rows are `EVENT_TIME_EPOCH + id` so
/// `refresh_mode: append` has a strictly increasing `time_column`.
const EVENT_TIME_EPOCH: i64 = 1_700_000_000;

fn listing_dataset(endpoint: &str, bucket: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("s3://{bucket}/listing/"), "overwrite_race");
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("s3_endpoint".to_string(), endpoint.to_string()),
            ("s3_region".to_string(), "us-east-1".to_string()),
            ("s3_auth".to_string(), "key".to_string()),
            ("s3_key".to_string(), ACCESS_KEY.to_string()),
            ("s3_secret".to_string(), SECRET_KEY.to_string()),
            ("s3_url_style".to_string(), "path".to_string()),
            ("allow_http".to_string(), "true".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

fn snappy_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(ROW_GROUP_SIZE))
        .set_dictionary_enabled(false)
        .build()
}

/// Payload for row `i` of generation `seed`.
///
/// Every generation pads with `seed` extra `x` characters so a mixed scan of
/// two generations cannot share either generation's `sum(char_length(payload))`.
/// Putting `{seed}` in the payload changes content but almost never length, so a
/// replaced row group would otherwise keep the listed generation's sum.
fn generation_payload(seed: i64, i: i64) -> String {
    let pad_len = usize::try_from(seed).expect("generation seed is non-negative");
    format!(
        "pay-{seed}-{i:08}-{}-{}",
        (i * 17 + seed * 31) % 1_000_000,
        "x".repeat(pad_len)
    )
}

fn generation_table(seed: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..ROWS).collect();
    let payload: Vec<String> = (0..ROWS).map(|i| generation_payload(seed, i)).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .expect("generation batch")
}

fn generation_table_with_event_time(seed: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..ROWS).collect();
    let event_times: Vec<i64> = (0..ROWS).map(|i| EVENT_TIME_EPOCH + i).collect();
    let payload: Vec<String> = (0..ROWS).map(|i| generation_payload(seed, i)).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(event_times)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .expect("append generation batch")
}

fn write_snappy_parquet(batch: &RecordBatch) -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(snappy_properties()))
                .expect("parquet writer");
        writer.write(batch).expect("write batch");
        writer.close().expect("close writer");
    }
    buffer
}

fn expected_payload_char_len(seed: i64) -> i64 {
    (0..ROWS)
        .map(|i| {
            i64::try_from(generation_payload(seed, i).chars().count())
                .expect("payload char length fits i64")
        })
        .sum()
}

#[test]
fn a_mixed_row_group_does_not_share_either_generation_payload_sum() {
    let sum_a = expected_payload_char_len(1);
    let sum_b = expected_payload_char_len(2);
    assert_ne!(
        sum_a, sum_b,
        "generations must differ in payload-length sum"
    );
    let group = i64::try_from(ROW_GROUP_SIZE).expect("row group size fits i64");
    let mixed: i64 = (0..ROWS)
        .map(|i| {
            let seed = if (group..2 * group).contains(&i) {
                2
            } else {
                1
            };
            i64::try_from(generation_payload(seed, i).chars().count()).expect("fits i64")
        })
        .sum();
    assert_ne!(
        mixed, sum_a,
        "replacing one row group with generation B must not keep generation A's payload-length sum"
    );
    assert_ne!(
        mixed, sum_b,
        "replacing one row group with generation B must not equal generation B's payload-length sum"
    );
}

#[test]
fn http_response_status_reads_412_from_the_status_line() {
    assert_eq!(
        http_response_status(b"HTTP/1.1 412 Precondition Failed\r\n"),
        Some(412)
    );
    assert_eq!(
        http_response_status(b"GET /listing/data.parquet HTTP/1.1\r\n"),
        None
    );
    assert_eq!(http_response_status(b"HTTP/1.1 200 OK\r\n"), Some(200));
}

/// Schema inference GETs the listed object with a Range footer read and no pin.
/// The refresh scan is the first GET that carries `If-Match` or `versionId`.
#[test]
fn generation_pin_is_only_if_match_or_version_id() {
    let inference = b"GET /overwrite-race/listing/data.parquet HTTP/1.1\r\n\
Host: 127.0.0.1\r\n\
Range: bytes=-8\r\n\r\n";
    assert!(
        is_object_get_request(inference),
        "inference still GETs the listed object"
    );
    assert!(
        !is_generation_pinned_object_get(inference),
        "an unpinned inference GET must not trigger the overwrite wait"
    );

    let if_match = b"GET /overwrite-race/listing/data.parquet HTTP/1.1\r\n\
Host: 127.0.0.1\r\n\
If-Match: \"etag-a\"\r\n\
Range: bytes=0-8191\r\n\r\n";
    assert!(
        is_generation_pinned_object_get(if_match),
        "a scan GET with If-Match is the refresh pin"
    );

    let versioned = b"GET /overwrite-race/listing/data.parquet?versionId=v1 HTTP/1.1\r\n\
Host: 127.0.0.1\r\n\
Range: bytes=0-8191\r\n\r\n";
    assert!(
        is_generation_pinned_object_get(versioned),
        "a scan GET with versionId is the refresh pin"
    );

    let if_none_match = b"GET /overwrite-race/listing/data.parquet HTTP/1.1\r\n\
If-None-Match: \"etag-a\"\r\n\r\n";
    assert!(
        !is_generation_pinned_object_get(if_none_match),
        "If-None-Match is not a generation pin"
    );

    let other_key = b"GET /overwrite-race/listing/other.parquet HTTP/1.1\r\n\
If-Match: \"etag-a\"\r\n\r\n";
    assert!(
        !is_generation_pinned_object_get(other_key),
        "a different object is not the listed key"
    );
}

fn s3_client(endpoint: &str) -> aws_sdk_s3::Client {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

async fn ensure_bucket_and_object(
    endpoint: &str,
    bucket: &str,
    body: Vec<u8>,
    versioned: bool,
) -> Result<(), anyhow::Error> {
    let s3 = s3_client(endpoint);
    if let Err(err) = s3.create_bucket().bucket(bucket).send().await {
        let text = err.to_string();
        if !text.contains("BucketAlreadyOwnedByYou") && !text.contains("BucketAlreadyExists") {
            return Err(anyhow::anyhow!("create bucket: {err}"));
        }
    }
    if versioned {
        s3.put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await?;
    }
    s3.put_object()
        .bucket(bucket)
        .key(OBJECT_KEY)
        .body(ByteStream::from(body))
        .send()
        .await?;
    Ok(())
}

async fn overwrite_object(
    endpoint: &str,
    bucket: &str,
    body: Vec<u8>,
) -> Result<(), anyhow::Error> {
    s3_client(endpoint)
        .put_object()
        .bucket(bucket)
        .key(OBJECT_KEY)
        .body(ByteStream::from(body))
        .send()
        .await?;
    Ok(())
}

async fn start_minio() -> Result<RunningContainer<'static>, anyhow::Error> {
    let container = ContainerRunnerBuilder::new("spice_test_minio_parquet_overwrite")
        .image("minio/minio:latest".to_string())
        .add_port_binding(9000, MINIO_HOST_PORT)
        .add_env_var("MINIO_ROOT_USER", ACCESS_KEY)
        .add_env_var("MINIO_ROOT_PASSWORD", SECRET_KEY)
        .command(["server", "/data", "--console-address", ":9001"])
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "curl -f http://127.0.0.1:9000/minio/health/live || exit 1".to_string(),
            ]),
            interval: Some(500_000_000),
            timeout: Some(1_000_000_000),
            retries: Some(20),
            start_period: Some(2_000_000_000),
            start_interval: None,
        })
        .build()?
        .run(Some(Duration::from_mins(2)))
        .await?;
    wait_for_tcp_port("127.0.0.1", MINIO_HOST_PORT, Duration::from_mins(1)).await?;
    Ok(container)
}

/// Delay object GETs after the first one so a replacement can land mid-scan.
///
/// Schema/statistics inference GETs the object without a generation pin.
/// The refresh scan pins via `If-Match` or `versionId`. Overwrite waits must
/// use the pinned GET, or they fire during inference and miss the 412 path.
struct MixProxy {
    seen_first_get: Arc<AtomicBool>,
    seen_first_pinned_get: Arc<AtomicBool>,
    unpinned_object_gets: Arc<AtomicU64>,
    pinned_object_gets: Arc<AtomicU64>,
    precondition_failures: Arc<AtomicU64>,
}

impl MixProxy {
    fn start() -> Self {
        let seen_first_get = Arc::new(AtomicBool::new(false));
        let seen_first_pinned_get = Arc::new(AtomicBool::new(false));
        let unpinned_object_gets = Arc::new(AtomicU64::new(0));
        let pinned_object_gets = Arc::new(AtomicU64::new(0));
        let precondition_failures = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&seen_first_get);
        let seen_pinned = Arc::clone(&seen_first_pinned_get);
        let unpinned = Arc::clone(&unpinned_object_gets);
        let pinned = Arc::clone(&pinned_object_gets);
        let preconditions = Arc::clone(&precondition_failures);
        tokio::spawn(async move {
            let listener = TcpListener::bind(("127.0.0.1", PROXY_PORT))
                .await
                .expect("bind delay proxy");
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    continue;
                };
                let seen = Arc::clone(&seen);
                let seen_pinned = Arc::clone(&seen_pinned);
                let unpinned = Arc::clone(&unpinned);
                let pinned = Arc::clone(&pinned);
                let preconditions = Arc::clone(&preconditions);
                tokio::spawn(async move {
                    if let Err(err) = proxy_connection(
                        client,
                        &seen,
                        &seen_pinned,
                        &unpinned,
                        &pinned,
                        &preconditions,
                    )
                    .await
                    {
                        tracing::debug!("overwrite-race proxy connection ended: {err}");
                    }
                });
            }
        });
        Self {
            seen_first_get,
            seen_first_pinned_get,
            unpinned_object_gets,
            pinned_object_gets,
            precondition_failures,
        }
    }

    fn reset(&self) {
        self.seen_first_get.store(false, Ordering::SeqCst);
        self.seen_first_pinned_get.store(false, Ordering::SeqCst);
        self.unpinned_object_gets.store(0, Ordering::SeqCst);
        self.pinned_object_gets.store(0, Ordering::SeqCst);
        self.precondition_failures.store(0, Ordering::SeqCst);
    }

    fn precondition_failures(&self) -> u64 {
        self.precondition_failures.load(Ordering::SeqCst)
    }

    fn unpinned_object_gets(&self) -> u64 {
        self.unpinned_object_gets.load(Ordering::SeqCst)
    }

    async fn wait_for_first_pinned_object_get(&self) -> Result<(), anyhow::Error> {
        let started = std::time::Instant::now();
        while !self.seen_first_pinned_get.load(Ordering::SeqCst) {
            ensure!(
                started.elapsed() < Duration::from_secs(15),
                "timed out waiting for the first generation-pinned object GET (If-Match or versionId)"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

fn http_response_status(headers: &[u8]) -> Option<u16> {
    let first = headers.split(|&b| b == b'\n').next()?;
    let line = std::str::from_utf8(first).ok()?.trim();
    let mut parts = line.split_whitespace();
    let version = parts.next()?;
    if !version.starts_with("HTTP/") {
        return None;
    }
    parts.next()?.parse().ok()
}

fn request_line(headers: &[u8]) -> &str {
    headers
        .split(|&b| b == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .map_or("", str::trim)
}

fn http_header_present(headers: &str, name: &str) -> bool {
    headers.lines().any(|line| {
        line.split_once(':')
            .is_some_and(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
    })
}

fn is_object_get_request(headers: &[u8]) -> bool {
    let line = request_line(headers);
    line.starts_with("GET ") && line.contains(OBJECT_KEY)
}

/// A listing-table scan pin: `If-Match` on the listed `ETag`, or `versionId` on
/// a versioned bucket. Schema inference GETs the same key without either.
fn is_generation_pinned_object_get(headers: &[u8]) -> bool {
    if !is_object_get_request(headers) {
        return false;
    }
    let line = request_line(headers);
    let text = String::from_utf8_lossy(headers);
    http_header_present(&text, "if-match") || line.contains("versionId=")
}

async fn proxy_connection(
    mut client: TcpStream,
    seen_first_get: &AtomicBool,
    seen_first_pinned_get: &AtomicBool,
    unpinned_object_gets: &AtomicU64,
    pinned_object_gets: &AtomicU64,
    precondition_failures: &AtomicU64,
) -> Result<(), anyhow::Error> {
    loop {
        let Some((headers, body)) = read_http_message(&mut client, true).await? else {
            return Ok(());
        };
        let line = request_line(&headers);
        let is_head = line.starts_with("HEAD ");
        let is_object_get = is_object_get_request(&headers);
        let is_pinned_get = is_generation_pinned_object_get(&headers);
        let delay_later_gets = is_object_get && seen_first_get.load(Ordering::SeqCst);
        if delay_later_gets {
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        let mut upstream = TcpStream::connect(("127.0.0.1", MINIO_HOST_PORT)).await?;
        upstream.write_all(&headers).await?;
        upstream.write_all(&body).await?;
        let Some((resp_headers, resp_body)) = read_http_message(&mut upstream, !is_head).await?
        else {
            return Ok(());
        };
        if http_response_status(&resp_headers) == Some(412) {
            precondition_failures.fetch_add(1, Ordering::SeqCst);
        }
        client.write_all(&resp_headers).await?;
        client.write_all(&resp_body).await?;
        if is_object_get {
            if is_pinned_get {
                pinned_object_gets.fetch_add(1, Ordering::SeqCst);
                seen_first_pinned_get.store(true, Ordering::SeqCst);
            } else {
                unpinned_object_gets.fetch_add(1, Ordering::SeqCst);
            }
            seen_first_get.store(true, Ordering::SeqCst);
        }
    }
}

async fn read_http_message(
    stream: &mut TcpStream,
    expect_body: bool,
) -> Result<Option<(Vec<u8>, Vec<u8>)>, anyhow::Error> {
    let mut buf = Vec::new();
    let mut tmp = [0_u8; 4096];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return if buf.is_empty() {
                Ok(None)
            } else {
                Err(anyhow::anyhow!("truncated HTTP headers"))
            };
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let header_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header terminator present")
        + 4;
    let headers = buf[..header_end].to_vec();
    let leftover = buf[header_end..].to_vec();
    if !expect_body || headers.starts_with(b"HEAD ") {
        return Ok(Some((headers, leftover)));
    }
    let header_text = String::from_utf8_lossy(&headers);
    if header_text
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut body = leftover;
        while !body.windows(5).any(|w| w == b"0\r\n\r\n") {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
        return Ok(Some((headers, body)));
    }
    let length = header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    let mut body = leftover;
    while body.len() < length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    Ok(Some((headers, body)))
}

async fn scan_i64_query(rt: &Runtime, sql: &str) -> Result<i64, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| e.to_string())?;
    let mut total = 0_i64;
    let mut data = result.data;
    while let Some(batch) = data.next().await.transpose().map_err(|e| e.to_string())? {
        total += sum_i64_column(batch.column(0))?;
    }
    Ok(total)
}

async fn scan_payload_char_len(rt: &Runtime) -> Result<i64, String> {
    scan_i64_query(
        rt,
        "SELECT SUM(char_length(payload)) AS total FROM overwrite_race",
    )
    .await
}

fn sum_i64_column(column: &arrow::array::ArrayRef) -> Result<i64, String> {
    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return Ok(array.value(0));
    }
    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return Ok(i64::from(array.value(0)));
    }
    if let Some(array) = column.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        return i64::try_from(array.value(0)).map_err(|e| e.to_string());
    }
    if let Some(array) = column
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
    {
        return i64::try_from(array.value(0)).map_err(|e| e.to_string());
    }
    Err(format!("unexpected sum type: {}", column.data_type()))
}

fn is_mixed_generation_failure(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    e.contains("invalid page header")
        || e.contains("snappy:")
        || e.contains("corrupt input")
        || e.contains("failed to fill whole buffer")
        || e.contains("range length must match")
        || e.contains("buffer length")
        || e.contains("does not match length")
        || e.contains("driver task ended unexpectedly")
        || e.contains("parquet argument error")
        || e.contains("unexpected struct field")
}

async fn run_runtime_with_dataset(dataset: Dataset) -> Result<Arc<Runtime>, anyhow::Error> {
    configure_test_datafusion();
    let rt = Arc::new(
        Runtime::builder()
            .with_app_opt(Some(Arc::new(
                AppBuilder::new("s3_parquet_overwrite")
                    .with_sql_cache(SQLResultsCacheConfig {
                        enabled: false,
                        ..SQLResultsCacheConfig::default()
                    })
                    .with_dataset(dataset)
                    .build(),
            )))
            .build()
            .await,
    );
    let cloned = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(1)) => {
            return Err(anyhow::anyhow!("timed out loading listing-table dataset"));
        }
        () = cloned.load_components() => {}
    }
    runtime_ready_check(rt.as_ref()).await;
    Ok(rt)
}

async fn run_runtime(endpoint: &str, bucket: &str) -> Result<Arc<Runtime>, anyhow::Error> {
    run_runtime_with_dataset(listing_dataset(endpoint, bucket)).await
}

struct GenerationPair {
    bytes_a: Vec<u8>,
    bytes_b: Vec<u8>,
    sum_a: i64,
    sum_b: i64,
}

async fn race_listing_scan(
    mix: &MixProxy,
    proxy: &str,
    minio: &str,
    bucket: &str,
    generations: &GenerationPair,
    versioned: bool,
) -> Result<(), anyhow::Error> {
    ensure_bucket_and_object(minio, bucket, generations.bytes_a.clone(), versioned).await?;
    mix.reset();
    let rt = run_runtime(proxy, bucket).await?;
    mix.reset();

    let control = scan_payload_char_len(&rt)
        .await
        .map_err(|e| anyhow::anyhow!("control scan of generation A failed: {e}"))?;
    ensure!(
        control == generations.sum_a,
        "control scan must return generation A ({}), got {control}",
        generations.sum_a
    );
    drop(rt);

    mix.reset();
    let rt = run_runtime(proxy, bucket).await?;
    mix.reset();
    let query = tokio::spawn({
        let rt = Arc::clone(&rt);
        async move { scan_payload_char_len(rt.as_ref()).await }
    });
    mix.wait_for_first_pinned_object_get().await?;
    overwrite_object(minio, bucket, generations.bytes_b.clone()).await?;
    let raced = query
        .await
        .map_err(|e| anyhow::anyhow!("scan task join: {e}"))?;

    match raced {
        Ok(total) => {
            ensure!(
                total == generations.sum_a,
                "scan of a replaced object returned {total}, not the listed generation ({})",
                generations.sum_a
            );
        }
        Err(err) => {
            ensure!(
                !is_mixed_generation_failure(&err),
                "replacing a listed Parquet object mid-scan must not decode as corruption or panic: {err}"
            );
            ensure!(
                !versioned,
                "a versioned listing must keep reading the listed generation, not fail: {err}"
            );
            ensure!(
                refresh_error_reason_from_message(&err)
                    == REFRESH_ERROR_REASON_OBJECT_GENERATION_CHANGED,
                "an unversioned overwrite must fail as a generation change (412 / If-Match), not decoder corruption: {err}"
            );
        }
    }
    Ok(())
}

fn accelerated_listing_dataset(endpoint: &str, bucket: &str) -> Dataset {
    let mut dataset = listing_dataset(endpoint, bucket);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        refresh_retry_enabled: true,
        refresh_retry_max_attempts: Some(8),
        ..Acceleration::default()
    });
    dataset
}

/// Production shape from [#13793](https://github.com/spiceai/spiceai/issues/13793):
/// accelerated listing table, `refresh_mode: append`, and a valid `time_column`.
fn accelerated_append_listing_dataset(endpoint: &str, bucket: &str) -> Dataset {
    let mut dataset = listing_dataset(endpoint, bucket);
    dataset.time_column = Some("event_time".to_string());
    dataset.time_format = Some(TimeFormat::UnixSeconds);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        refresh_mode: Some(RefreshMode::Append),
        refresh_retry_enabled: true,
        refresh_retry_max_attempts: Some(8),
        ..Acceleration::default()
    });
    dataset
}

#[test]
fn accelerated_append_fixture_sets_append_and_a_time_column() {
    let dataset = accelerated_append_listing_dataset("http://127.0.0.1:1", "bucket");
    assert_eq!(dataset.time_column.as_deref(), Some("event_time"));
    assert_eq!(dataset.time_format, Some(TimeFormat::UnixSeconds));
    let acceleration = dataset
        .acceleration
        .as_ref()
        .expect("append fixture enables acceleration");
    assert_eq!(acceleration.refresh_mode, Some(RefreshMode::Append));
}

async fn corrupt_parquet_is_not_classified_as_generation_change(
    endpoint: &str,
    minio: &str,
    bucket: &str,
    mut bytes: Vec<u8>,
) -> Result<(), anyhow::Error> {
    ensure!(bytes.len() > 256, "need a real parquet body to corrupt");
    let start = bytes.len() / 3;
    for byte in bytes.iter_mut().skip(start).take(64) {
        *byte ^= 0xff;
    }
    ensure_bucket_and_object(minio, bucket, bytes, false).await?;
    configure_test_datafusion();
    let rt = Arc::new(
        Runtime::builder()
            .with_app_opt(Some(Arc::new(
                AppBuilder::new("s3_parquet_overwrite")
                    .with_sql_cache(SQLResultsCacheConfig {
                        enabled: false,
                        ..SQLResultsCacheConfig::default()
                    })
                    .with_dataset(listing_dataset(endpoint, bucket))
                    .build(),
            )))
            .build()
            .await,
    );
    let cloned = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_secs(30)) => {
            return Err(anyhow::anyhow!("timed out loading corrupt Parquet dataset"));
        }
        () = cloned.load_components() => {}
    }
    let err = match scan_payload_char_len(rt.as_ref()).await {
        Ok(total) => {
            return Err(anyhow::anyhow!(
                "corrupt Parquet must fail the scan, got total={total}"
            ));
        }
        Err(e) => e,
    };
    let reason = refresh_error_reason_from_message(&err);
    ensure!(
        reason == REFRESH_ERROR_REASON_PARQUET_DECODE,
        "genuine corrupt Parquet must be reason=parquet_decode, got reason={reason} from {err}"
    );
    ensure!(
        reason != REFRESH_ERROR_REASON_OBJECT_GENERATION_CHANGED,
        "genuine corrupt Parquet must not be filterable as a generation change: {err}"
    );
    Ok(())
}

async fn accelerated_refresh_retries_a_replaced_object(
    mix: &MixProxy,
    proxy: &str,
    minio: &str,
    bucket: &str,
    generations: &GenerationPair,
) -> Result<(), anyhow::Error> {
    ensure_bucket_and_object(minio, bucket, generations.bytes_a.clone(), false).await?;
    mix.reset();
    let load = tokio::spawn({
        let proxy = proxy.to_string();
        let bucket = bucket.to_string();
        async move { run_runtime_with_dataset(accelerated_listing_dataset(&proxy, &bucket)).await }
    });
    // Inference GETs the object first without a pin. Overwrite only after the
    // refresh scan sends If-Match, or the 412 path is never exercised.
    mix.wait_for_first_pinned_object_get().await?;
    ensure!(
        mix.unpinned_object_gets() >= 1,
        "schema inference must GET '{OBJECT_KEY}' without If-Match/versionId before the refresh scan pins it; \
         otherwise waiting for any object GET would overwrite during inference"
    );
    overwrite_object(minio, bucket, generations.bytes_b.clone()).await?;
    let rt = load
        .await
        .map_err(|e| anyhow::anyhow!("accelerated load join: {e}"))?
        .map_err(|e| anyhow::anyhow!("accelerated refresh did not recover after overwrite: {e}"))?;
    let total = scan_payload_char_len(rt.as_ref())
        .await
        .map_err(|e| anyhow::anyhow!("accelerated scan after overwrite retry: {e}"))?;
    ensure!(
        mix.precondition_failures() >= 1,
        "accelerated refresh must observe a 412 from the replaced object before succeeding, saw {}",
        mix.precondition_failures()
    );
    ensure!(
        total == generations.sum_a || total == generations.sum_b,
        "retry after a replaced object must return one generation ({} or {}), got {total}",
        generations.sum_a,
        generations.sum_b
    );
    Ok(())
}

/// Same overwrite race as the full-refresh arm, on the production append path
/// (`RefreshTask::run` → `get_incremental_append_update`). A 412 mid-stream
/// plus retry must not leave a partial write or a second copy of the same ids.
async fn accelerated_append_refresh_retries_without_partial_or_duplicate_rows(
    mix: &MixProxy,
    proxy: &str,
    minio: &str,
    bucket: &str,
    generations: &GenerationPair,
) -> Result<(), anyhow::Error> {
    ensure_bucket_and_object(minio, bucket, generations.bytes_a.clone(), false).await?;
    mix.reset();
    let load = tokio::spawn({
        let proxy = proxy.to_string();
        let bucket = bucket.to_string();
        async move {
            run_runtime_with_dataset(accelerated_append_listing_dataset(&proxy, &bucket)).await
        }
    });
    mix.wait_for_first_pinned_object_get().await?;
    ensure!(
        mix.unpinned_object_gets() >= 1,
        "schema inference must GET '{OBJECT_KEY}' without If-Match/versionId before the append refresh scan pins it; \
         otherwise waiting for any object GET would overwrite during inference"
    );
    overwrite_object(minio, bucket, generations.bytes_b.clone()).await?;
    let rt = load
        .await
        .map_err(|e| anyhow::anyhow!("append load join: {e}"))?
        .map_err(|e| anyhow::anyhow!("append refresh did not recover after overwrite: {e}"))?;
    ensure!(
        mix.precondition_failures() >= 1,
        "append refresh must observe a 412 from the replaced object before succeeding, saw {}",
        mix.precondition_failures()
    );
    let count = scan_i64_query(rt.as_ref(), "SELECT COUNT(*) FROM overwrite_race")
        .await
        .map_err(|e| anyhow::anyhow!("append row count after overwrite retry: {e}"))?;
    let distinct = scan_i64_query(rt.as_ref(), "SELECT COUNT(DISTINCT id) FROM overwrite_race")
        .await
        .map_err(|e| anyhow::anyhow!("append distinct id count after overwrite retry: {e}"))?;
    let total = scan_payload_char_len(rt.as_ref())
        .await
        .map_err(|e| anyhow::anyhow!("append payload sum after overwrite retry: {e}"))?;
    ensure!(
        count == ROWS,
        "append 412 retry must not leave a partial or extra write: expected {ROWS} rows, got {count}"
    );
    ensure!(
        distinct == ROWS,
        "append 412 retry must not duplicate ids: COUNT(*)={count} COUNT(DISTINCT id)={distinct}"
    );
    ensure!(
        total == generations.sum_a || total == generations.sum_b,
        "append 412 retry must return one generation ({} or {}), got payload sum {total}",
        generations.sum_a,
        generations.sum_b
    );
    Ok(())
}

/// Control: a listing-table scan of one unchanged `Snappy` object returns that
/// generation's payload length. Mutation: replacing the object after the
/// footer is read must not surface as Parquet corruption, a worker panic, or
/// the replacement's rows. Covers both unversioned (`ETag` / `If-Match`) and
/// versioned (version id) buckets.
#[tokio::test]
async fn listing_table_scan_does_not_decode_a_replaced_object() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let container = start_minio().await?;
            let minio = format!("http://127.0.0.1:{MINIO_HOST_PORT}");
            let proxy = format!("http://127.0.0.1:{PROXY_PORT}");
            let mix = MixProxy::start();
            wait_for_tcp_port("127.0.0.1", PROXY_PORT, Duration::from_secs(5)).await?;

            let gen_a = generation_table(1);
            let gen_b = generation_table(2);
            let generations = GenerationPair {
                bytes_a: write_snappy_parquet(&gen_a),
                bytes_b: write_snappy_parquet(&gen_b),
                sum_a: expected_payload_char_len(1),
                sum_b: expected_payload_char_len(2),
            };
            let append_generations = GenerationPair {
                bytes_a: write_snappy_parquet(&generation_table_with_event_time(1)),
                bytes_b: write_snappy_parquet(&generation_table_with_event_time(2)),
                sum_a: generations.sum_a,
                sum_b: generations.sum_b,
            };
            ensure!(
                !generations.bytes_a.is_empty(),
                "generation A parquet is empty"
            );
            ensure!(
                generations.bytes_a != generations.bytes_b,
                "generations A and B are identical"
            );

            let result = async {
                race_listing_scan(&mix, &proxy, &minio, BUCKET, &generations, false).await?;
                race_listing_scan(
                    &mix,
                    &proxy,
                    &minio,
                    "overwrite-race-versioned",
                    &generations,
                    true,
                )
                .await?;
                corrupt_parquet_is_not_classified_as_generation_change(
                    &proxy,
                    &minio,
                    "overwrite-race-corrupt",
                    generations.bytes_a.clone(),
                )
                .await?;
                accelerated_refresh_retries_a_replaced_object(
                    &mix,
                    &proxy,
                    &minio,
                    "overwrite-race-accel",
                    &generations,
                )
                .await?;
                accelerated_append_refresh_retries_without_partial_or_duplicate_rows(
                    &mix,
                    &proxy,
                    &minio,
                    "overwrite-race-accel-append",
                    &append_generations,
                )
                .await?;
                Ok::<(), anyhow::Error>(())
            }
            .await;

            container.remove().await?;
            result
        })
        .await
}
