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

//! A catalog decides where each of its tables is stored, so a table whose storage
//! location is on the Spice host itself must not be registered: reading it would
//! serve the host's local files as rows to anyone who can query the catalog.
//!
//! Each test loads a real runtime with a catalog served by [`MockCatalog`], which
//! answers the Unity Catalog API and also serves a directory as the S3 bucket
//! `bucket`, so a table stored in object storage can still be loaded end to end.

use std::{
    collections::HashMap,
    fmt::Write as _,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{Method, Response, StatusCode, header},
};
use datafusion::parquet::arrow::ArrowWriter;
use spicepod::{component::catalog::Catalog, param::Params};

use crate::Runtime;

const LOCAL_ROW: &str = "LOCAL_FILE_ROW";
const OBJECT_STORE_ROW: &str = "OBJECT_STORE_ROW";

/// Writes a Delta table with one `value` row at `dir`.
fn write_delta_table(dir: &Path, value: &str) {
    std::fs::create_dir_all(dir.join("_delta_log")).expect("create _delta_log");
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![value]))],
    )
    .expect("batch builds");
    let data_file = dir.join("part-0.parquet");
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&data_file).expect("create data file"),
        schema,
        None,
    )
    .expect("parquet writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close parquet file");
    let size = std::fs::metadata(&data_file).expect("data file").len();

    let commit = [
        serde_json::json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        serde_json::json!({"metaData": {
            "id": "00000000-0000-0000-0000-000000000000",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"string","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 0
        }}),
        serde_json::json!({"add": {
            "path": "part-0.parquet",
            "partitionValues": {},
            "size": size,
            "modificationTime": 0,
            "dataChange": true
        }}),
    ]
    .iter()
    .map(serde_json::Value::to_string)
    .collect::<Vec<_>>()
    .join("\n");
    std::fs::write(dir.join("_delta_log/00000000000000000000.json"), commit).expect("write commit");
}

fn uc_table(name: &str, storage_location: &str) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "catalog_name": "cat",
        "schema_name": "sch",
        "table_type": "EXTERNAL",
        "data_source_format": "DELTA",
        "storage_location": storage_location,
        "table_id": format!("{name}-id"),
    })
}

struct MockState {
    tables: Vec<serde_json::Value>,
    bucket: PathBuf,
    credential_requests: AtomicUsize,
}

/// A Unity Catalog API with the one schema `cat.sch`, plus an S3 endpoint serving
/// the bucket `bucket` from a local directory.
///
/// It denies every credential-vending request, because a vended S3 store always
/// connects to AWS. A vended catalog therefore reads its tables with the static
/// storage credentials it falls back to.
struct MockCatalog {
    endpoint: String,
    state: Arc<MockState>,
}

impl MockCatalog {
    async fn start(tables: Vec<serde_json::Value>, bucket: PathBuf) -> Self {
        let state = Arc::new(MockState {
            tables,
            bucket,
            credential_requests: AtomicUsize::new(0),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener binds");
        let endpoint = format!("http://{}", listener.local_addr().expect("local address"));
        let router = Router::new()
            .fallback(handle)
            .with_state(Arc::clone(&state));
        tokio::spawn(axum::serve(listener, router).into_future());
        Self { endpoint, state }
    }

    fn credential_requests(&self) -> usize {
        self.state.credential_requests.load(Ordering::SeqCst)
    }
}

fn json_response(body: &serde_json::Value) -> Response<Body> {
    Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("response builds")
}

fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from(
            "<Error><Code>NoSuchKey</Code><Message>not found</Message></Error>",
        ))
        .expect("response builds")
}

async fn handle(State(state): State<Arc<MockState>>, request: Request) -> Response<Body> {
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or_default().to_string();
    let method = request.method().clone();
    match (method.clone(), path.as_str()) {
        (Method::GET, "/api/2.1/unity-catalog/schemas") => {
            json_response(&serde_json::json!({"schemas": [{"name": "sch", "catalog_name": "cat"}]}))
        }
        (Method::GET, "/api/2.1/unity-catalog/tables") => {
            json_response(&serde_json::json!({ "tables": state.tables }))
        }
        (Method::POST, "/api/2.1/unity-catalog/temporary-table-credentials") => {
            state.credential_requests.fetch_add(1, Ordering::SeqCst);
            Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Body::from("PERMISSION_DENIED"))
                .expect("response builds")
        }
        (Method::GET, "/bucket") => list_objects(&state.bucket, &query),
        (Method::GET | Method::HEAD, _) => match path.strip_prefix("/bucket/") {
            Some(key) => get_object(
                &state.bucket,
                key,
                method == Method::HEAD,
                request.headers().get(header::RANGE),
            ),
            None => not_found(),
        },
        _ => not_found(),
    }
}

fn bucket_keys(root: &Path, dir: &Path, keys: &mut Vec<(String, u64)>) {
    for entry in std::fs::read_dir(dir).expect("read bucket directory") {
        let entry = entry.expect("bucket entry");
        let path = entry.path();
        if path.is_dir() {
            bucket_keys(root, &path, keys);
        } else {
            let key = path
                .strip_prefix(root)
                .expect("key inside the bucket")
                .to_string_lossy()
                .replace('\\', "/");
            keys.push((key, entry.metadata().expect("object metadata").len()));
        }
    }
}

/// `ListObjectsV2`, honouring `prefix` and `start-after`.
fn list_objects(root: &Path, query: &str) -> Response<Body> {
    let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    let prefix = params.get("prefix").map_or("", String::as_str);
    let start_after = params.get("start-after").map_or("", String::as_str);
    let mut keys = Vec::new();
    bucket_keys(root, root, &mut keys);
    keys.retain(|(key, _)| key.starts_with(prefix) && key.as_str() > start_after);
    keys.sort();
    let mut contents = String::new();
    for (key, size) in &keys {
        write!(
            contents,
            "<Contents><Key>{key}</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>\"etag\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
        )
        .expect("writing to a String succeeds");
    }
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>bucket</Name><Prefix>{prefix}</Prefix><KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>",
        keys.len()
    );
    Response::builder()
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(body))
        .expect("response builds")
}

/// `GetObject`/`HeadObject`, honouring a single `Range: bytes=…` header.
fn get_object(
    root: &Path,
    key: &str,
    head: bool,
    range: Option<&header::HeaderValue>,
) -> Response<Body> {
    let Ok(bytes) = std::fs::read(root.join(key)) else {
        return not_found();
    };
    let total = bytes.len();
    let requested = range
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("bytes="))
        .and_then(|spec| spec.split_once('-'))
        .map(
            |(start, end)| match (start.parse::<usize>(), end.parse::<usize>()) {
                (Ok(start), Ok(end)) => (start, end.min(total - 1)),
                (Ok(start), Err(_)) => (start, total - 1),
                (Err(_), Ok(suffix)) => (total.saturating_sub(suffix), total - 1),
                (Err(_), Err(_)) => (0, total - 1),
            },
        );
    let mut response = Response::builder()
        .header(header::ETAG, "\"etag\"")
        .header(header::LAST_MODIFIED, "Mon, 01 Jan 2024 00:00:00 GMT");
    let body = match requested {
        Some((start, end)) => {
            response = response.status(StatusCode::PARTIAL_CONTENT).header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{total}"),
            );
            bytes[start..=end].to_vec()
        }
        None => bytes,
    };
    response = response.header(header::CONTENT_LENGTH, body.len());
    response
        .body(if head {
            Body::empty()
        } else {
            Body::from(body)
        })
        .expect("response builds")
}

/// Loads a runtime whose only component is `catalog`.
async fn load_catalog(catalog: Catalog) -> Arc<Runtime> {
    let app = app::AppBuilder::new("catalog_storage_location")
        .with_catalog(catalog)
        .build();
    let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::time::timeout(
        Duration::from_secs(60),
        Arc::clone(&runtime).load_components(),
    )
    .await
    .expect("the catalog must finish loading");
    runtime
}

/// The tables registered in schema `sch` of `catalog`, sorted.
fn registered_tables(runtime: &Runtime, catalog: &str) -> Vec<String> {
    let schema = runtime
        .datafusion()
        .ctx
        .catalog(catalog)
        .unwrap_or_else(|| panic!("catalog '{catalog}' must be registered"))
        .schema("sch")
        .unwrap_or_else(|| panic!("schema '{catalog}.sch' must be registered"));
    let mut tables = schema.table_names();
    tables.sort();
    tables
}

/// The `value` column of `SELECT value FROM {table}`, or the error text.
async fn select_values(runtime: &Runtime, table: &str) -> Result<Vec<String>, String> {
    let batches = runtime
        .datafusion()
        .ctx
        .sql(&format!("SELECT value FROM {table}"))
        .await
        .map_err(|e| e.to_string())?
        .collect()
        .await
        .map_err(|e| e.to_string())?;
    Ok(batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("value is a string column");
            values
                .iter()
                .map(|value| value.unwrap_or_default().to_string())
                .collect::<Vec<_>>()
        })
        .collect())
}

fn catalog(from: String, params: &[(&str, &str)]) -> Catalog {
    let mut catalog = Catalog::new(from, "uc".to_string());
    catalog.params = Some(Params::from_string_map(
        params
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
    ));
    catalog
}

struct Fixture {
    _dir: tempfile::TempDir,
    local_location: String,
    bucket: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let local = dir.path().join("local_delta");
        write_delta_table(&local, LOCAL_ROW);
        let bucket = dir.path().join("bucket");
        write_delta_table(&bucket.join("tables/remote"), OBJECT_STORE_ROW);
        let local_location = url::Url::from_directory_path(&local)
            .expect("absolute path")
            .to_string();
        Self {
            _dir: dir,
            local_location,
            bucket,
        }
    }
}

const NOT_FOUND: &str = "table 'uc.sch.local_table' not found";

/// A Unity Catalog catalog over `mock` with static S3 credentials for its bucket,
/// plus `extra` params.
fn unity_catalog(mock: &MockCatalog, extra: &[(&str, &str)]) -> Catalog {
    let mut params = vec![
        ("unity_catalog_aws_endpoint", mock.endpoint.as_str()),
        ("unity_catalog_aws_allow_http", "true"),
        ("unity_catalog_aws_region", "us-east-1"),
        ("unity_catalog_aws_access_key_id", "AKIASTATIC"),
        ("unity_catalog_aws_secret_access_key", "static"),
    ];
    params.extend_from_slice(extra);
    catalog(
        format!(
            "unity_catalog:{}/api/2.1/unity-catalog/catalogs/cat",
            mock.endpoint
        ),
        &params,
    )
}

async fn start_mock(fixture: &Fixture) -> MockCatalog {
    MockCatalog::start(
        vec![
            uc_table("local_table", &fixture.local_location),
            uc_table("remote_table", "s3://bucket/tables/remote/"),
        ],
        fixture.bucket.clone(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn unity_catalog_refuses_a_local_storage_location() {
    let fixture = Fixture::new();
    let mock = start_mock(&fixture).await;
    let runtime = load_catalog(unity_catalog(&mock, &[])).await;

    assert_eq!(registered_tables(&runtime, "uc"), vec!["remote_table"]);
    assert_eq!(
        select_values(&runtime, "uc.sch.remote_table").await,
        Ok(vec![OBJECT_STORE_ROW.to_string()]),
        "a table stored in object storage must still load"
    );
    let error = select_values(&runtime, "uc.sch.local_table")
        .await
        .expect_err("a table stored on the Spice host must not be queryable");
    assert!(error.contains(NOT_FOUND), "unexpected error: {error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn unity_catalog_with_credential_vending_refuses_a_local_storage_location() {
    let fixture = Fixture::new();
    let mock = start_mock(&fixture).await;
    let runtime = load_catalog(unity_catalog(
        &mock,
        &[("unity_catalog_credential_vending", "enabled")],
    ))
    .await;

    assert_eq!(registered_tables(&runtime, "uc"), vec!["remote_table"]);
    assert_eq!(
        mock.credential_requests(),
        1,
        "credentials must be requested for the object storage table, and not for the local table"
    );
    assert_eq!(
        select_values(&runtime, "uc.sch.remote_table").await,
        Ok(vec![OBJECT_STORE_ROW.to_string()]),
        "a table stored in object storage must still load"
    );
    let error = select_values(&runtime, "uc.sch.local_table")
        .await
        .expect_err("a table stored on the Spice host must not be queryable");
    assert!(error.contains(NOT_FOUND), "unexpected error: {error}");
}

#[cfg(feature = "databricks")]
#[tokio::test(flavor = "multi_thread")]
async fn databricks_delta_lake_mode_refuses_a_local_storage_location() {
    let fixture = Fixture::new();
    let mock = MockCatalog::start(
        vec![uc_table("local_table", &fixture.local_location)],
        fixture.bucket.clone(),
    )
    .await;
    let runtime = load_catalog(catalog(
        "databricks:cat".to_string(),
        &[
            ("databricks_endpoint", mock.endpoint.as_str()),
            ("databricks_token", "token"),
            ("mode", "delta_lake"),
        ],
    ))
    .await;

    assert_eq!(registered_tables(&runtime, "uc"), Vec::<String>::new());
    let error = select_values(&runtime, "uc.sch.local_table")
        .await
        .expect_err("a table stored on the Spice host must not be queryable");
    assert!(error.contains(NOT_FOUND), "unexpected error: {error}");
}
