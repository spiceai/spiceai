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

//! Basic authentication for files read over HTTP, configured the documented
//! way: the user name in `from` and `http_password` from a secret.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode, header},
    response::Response,
};
use base64::Engine;
use datafusion::parquet::arrow::ArrowWriter;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime_api_types::v1::ComponentStatus;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use tokio::net::TcpListener;
use url::Url;

use crate::configure_test_datafusion;
use crate::utils::{require_process_per_test, test_request_context, wait_until_true};

const USER: &str = "csvuser";
const PASSWORD_ENV: &str = "HTTP_BASIC_AUTH_TEST_PASSWORD";
const ITEMS_CSV: &str = "id,name\n1,alpha\n2,beta\n3,gamma\n";

type SeenAuthorization = Arc<Mutex<Vec<Option<String>>>>;

#[derive(Clone)]
struct Server {
    expected: String,
    files: Arc<HashMap<&'static str, Vec<u8>>>,
    seen: SeenAuthorization,
}

fn items_parquet() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
        ],
    )
    .expect("valid batch");
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    bytes
}

/// The bytes a `Range: bytes=...` header asks for, with their `Content-Range`.
fn byte_range(headers: &HeaderMap, len: usize) -> Option<(usize, usize, String)> {
    let range = headers
        .get(header::RANGE)?
        .to_str()
        .ok()?
        .strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    let (start, end) = if start.is_empty() {
        (len.saturating_sub(end.parse().ok()?), len - 1)
    } else {
        let start: usize = start.parse().ok()?;
        let end = if end.is_empty() {
            len - 1
        } else {
            end.parse::<usize>().ok()?.min(len - 1)
        };
        (start, end)
    };
    Some((start, end, format!("bytes {start}-{end}/{len}")))
}

async fn serve_file(State(server): State<Server>, request: Request<Body>) -> Response<Body> {
    let authorization = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    let authorized = authorization.as_deref() == Some(server.expected.as_str());
    server
        .seen
        .lock()
        .expect("the request log is not poisoned")
        .push(authorization);
    let response = Response::builder().header(header::ACCEPT_RANGES, "bytes");
    let Some(file) = server
        .files
        .get(request.uri().path())
        .filter(|_| authorized)
    else {
        let status = if authorized {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::UNAUTHORIZED
        };
        return response
            .status(status)
            .body(Body::empty())
            .expect("response");
    };
    match byte_range(request.headers(), file.len()) {
        Some((start, end, content_range)) => response
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_RANGE, content_range)
            .body(Body::from(file[start..=end].to_vec())),
        None => response
            .status(StatusCode::OK)
            .body(Body::from(file.clone())),
    }
    .expect("response")
}

/// Serves `/items.csv` and `/items.parquet` to requests whose Basic credentials
/// are `csvuser` and `accepted`, answers 401 to every other request, and records
/// each request's `Authorization` header.
async fn basic_auth_server(accepted: &str) -> (SocketAddr, SeenAuthorization) {
    let server = Server {
        expected: format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{accepted}"))
        ),
        files: Arc::new(HashMap::from([
            ("/items.csv", ITEMS_CSV.as_bytes().to_vec()),
            ("/items.parquet", items_parquet()),
        ])),
        seen: Arc::new(Mutex::new(Vec::new())),
    };
    let seen = Arc::clone(&server.seen);
    let app = Router::new()
        .fallback(axum::routing::any(serve_file))
        .with_state(server);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the server binds a loopback port");
    let addr = listener.local_addr().expect("the server has an address");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, seen)
}

fn sets_password_env(password: &str) {
    require_process_per_test("the test sets an environment variable");
    // SAFETY: the test runs in a process of its own, before the runtime starts
    // any thread that reads the environment.
    unsafe { std::env::set_var(PASSWORD_ENV, password) };
}

/// The documented pattern: the user name in `from`, the password from a secret.
fn http_dataset(addr: SocketAddr, file: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("http://{USER}@{addr}/{file}"), name);
    dataset.params = Some(Params::from_string_map(HashMap::from([(
        "http_password".to_string(),
        format!("${{ env:{PASSWORD_ENV} }}"),
    )])));
    dataset
}

async fn start(app: app::App) -> Arc<Runtime> {
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::spawn(Arc::clone(&rt).load_components());
    rt
}

async fn row_count(rt: &Runtime, sql: &str) -> Result<usize, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| e.to_string())?;
    let batches: Vec<_> = result.data.try_collect().await.map_err(|e| e.to_string())?;
    Ok(batches.iter().map(RecordBatch::num_rows).sum())
}

fn dataset_error(rt: &Runtime, name: &str) -> Option<String> {
    match rt
        .status()
        .get_dataset_statuses()
        .get(&datafusion::common::TableReference::bare(name))
    {
        Some(ComponentStatus::Error(message)) => message.clone(),
        _ => None,
    }
}

#[tokio::test]
async fn http_password_from_a_secret_authenticates_file_reads() -> Result<(), anyhow::Error> {
    sets_password_env("S3cr3tAcceptedPass");
    test_request_context()
        .scope(async {
            let (addr, seen) = basic_auth_server("S3cr3tAcceptedPass").await;
            let rt = start(
                AppBuilder::new("http_basic_auth")
                    .with_dataset(http_dataset(addr, "items.csv", "auth_csv"))
                    .with_dataset(http_dataset(addr, "items.parquet", "auth_parquet"))
                    .build(),
            )
            .await;

            let loaded = wait_until_true(Duration::from_secs(30), || async {
                row_count(&rt, "SELECT * FROM auth_csv").await == Ok(3)
                    && row_count(&rt, "SELECT * FROM auth_parquet").await == Ok(3)
            })
            .await;
            let expected = format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD
                    .encode(format!("{USER}:S3cr3tAcceptedPass"))
            );
            let seen = seen.lock().expect("not poisoned").clone();
            assert!(
                loaded,
                "auth_csv and auth_parquet should read 3 rows each; errors: {:?} {:?}; requests: {seen:?}",
                dataset_error(&rt, "auth_csv"),
                dataset_error(&rt, "auth_parquet")
            );
            assert!(
                seen.iter()
                    .all(|header| header.as_deref() == Some(expected.as_str())),
                "every request should carry the credentials: {seen:?}"
            );
            rt.shutdown().await;
            Ok(())
        })
        .await
}

#[tokio::test]
async fn refused_http_password_is_not_shown_in_status_or_logs() -> Result<(), anyhow::Error> {
    sets_password_env("S3cr3tRefusedPass");
    let log_dir = tempfile::tempdir()?;
    let log_path = log_dir.path().join("spiced.log");
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .with_writer(Mutex::new(std::fs::File::create(&log_path)?))
        .finish();
    let _tracing = tracing::subscriber::set_default(subscriber);

    test_request_context()
        .scope(async {
            let (addr, seen) = basic_auth_server("another-password").await;
            let rt = start(
                AppBuilder::new("http_basic_auth")
                    .with_dataset(http_dataset(addr, "items.csv", "auth_csv"))
                    .build(),
            )
            .await;

            let failed = wait_until_true(Duration::from_secs(30), || async {
                dataset_error(&rt, "auth_csv").is_some()
                    && std::fs::read_to_string(&log_path).is_ok_and(|logs| {
                        logs.lines()
                            .any(|line| line.contains(" WARN ") && line.contains("auth_csv"))
                    })
            })
            .await;
            let error = dataset_error(&rt, "auth_csv");
            let logs = std::fs::read_to_string(&log_path)?;
            rt.shutdown().await;

            assert!(
                failed,
                "auth_csv should fail with a WARN line; status: {error:?}"
            );
            let error = error.unwrap_or_default();
            assert!(
                error.contains("401"),
                "the stored error names the refusal: {error}"
            );
            assert!(
                !error.contains("S3cr3tRefusedPass"),
                "stored error: {error}"
            );
            assert!(
                !logs.contains("S3cr3tRefusedPass"),
                "log lines with the password: {:?}",
                logs.lines()
                    .filter(|line| line.contains("S3cr3tRefusedPass"))
                    .collect::<Vec<_>>()
            );
            assert!(
                !seen.lock().expect("not poisoned").is_empty(),
                "the server should have been asked for the file"
            );
            Ok(())
        })
        .await
}

#[tokio::test]
async fn url_table_on_another_origin_gets_no_stored_credential() -> Result<(), anyhow::Error> {
    sets_password_env("S3cr3tAcceptedPass");
    // SAFETY: as in `sets_password_env`.
    unsafe { std::env::set_var("SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS", "true") };
    test_request_context()
        .scope(async {
            let (dataset_addr, _) = basic_auth_server("S3cr3tAcceptedPass").await;
            // Accepts the same credentials, so a request that carried them would succeed.
            let (other_addr, other_seen) = basic_auth_server("S3cr3tAcceptedPass").await;
            let app = AppBuilder::new("http_basic_auth")
                .with_dataset(http_dataset(dataset_addr, "items.csv", "auth_csv"))
                .with_runtime_params(HashMap::from([(
                    "url_tables".to_string(),
                    "enabled".to_string(),
                )]))
                .build();
            let rt = start(app).await;
            assert!(
                wait_until_true(Duration::from_secs(30), || async {
                    row_count(&rt, "SELECT * FROM auth_csv").await == Ok(3)
                })
                .await,
                "auth_csv should load first; status: {:?}",
                dataset_error(&rt, "auth_csv")
            );

            // The token the dataset's URL carries: the same credentials return it.
            let token = runtime_object_store::http_auth::register(
                &Url::parse(&format!("http://{dataset_addr}/items.csv"))?,
                USER,
                secrecy::SecretString::from("S3cr3tAcceptedPass"),
            );
            let url_table = row_count(
                &rt,
                &format!("SELECT * FROM 'http://{other_addr}/items.csv#basic_auth={token}'"),
            )
            .await;
            let other_requests = other_seen.lock().expect("not poisoned").clone();
            rt.shutdown().await;

            assert!(
                url_table.is_err(),
                "the URL table must not read the file: {url_table:?}"
            );
            assert!(
                !other_requests.is_empty(),
                "the URL table should reach the other server"
            );
            assert!(
                other_requests.iter().all(Option::is_none),
                "the URL table sent credentials to another origin: {other_requests:?}"
            );
            Ok(())
        })
        .await
}
