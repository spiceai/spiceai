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

//! A dataset placed in an `s3://` `runtime.state.location` keeps writing,
//! checkpointing and scanning while the session credentials its store signs
//! with expire and are replaced.
//!
//! The dataset is opened through the accelerator with `runtime.params.cayenne_vnext`
//! enabled and `s3_auth: iam_role`, so the state location's store is the one the
//! runtime builds, signing through the AWS SDK credential chain. The chain reaches
//! a container credentials endpoint (`AWS_CONTAINER_CREDENTIALS_FULL_URI`) served
//! by the test, which hands out STS `AssumeRole` session credentials from `RustFS`.
//!
//! `RustFS` issues session credentials for at least 15 minutes. To make a set
//! really stop working at a short expiry, each set is assumed by a `RustFS` user of
//! its own, and the user is removed when the set expires, which `RustFS` enforces
//! on the session credentials at once. A store that kept signing with an expired
//! set would get `403 InvalidAccessKeyId`; the control test shows it does.
//!
//! Needs `RustFS`: runs when `CAYENNE_TEST_DATA_STORE=rustfs`, with
//! `CAYENNE_TEST_RUSTFS_ENDPOINT`, `CAYENNE_TEST_RUSTFS_BUCKET`,
//! `CAYENNE_TEST_RUSTFS_ACCESS_KEY` and `CAYENNE_TEST_RUSTFS_SECRET_KEY` naming a
//! server, a bucket and its root credentials, as the `RustFS` lane of
//! `.github/workflows/cayenne_object_native.yml` sets them. Skipped otherwise.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use accelerator_cayenne::CayenneAccelerator;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, SignableBody, SignableRequest, SigningSettings, sign,
};
use cayenne::CayenneTableProvider;
use data_accelerator_api::{AccelerationSource, AcceleratorEngineRegistry};
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionConfig, SessionContext};
use runtime_acceleration::{
    Engine,
    acceleration::{Acceleration, Mode, RefreshMode},
    testing::TestAccelerationSource,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// How long each set of session credentials lives. The SDK's identity cache
/// replaces a set 5 to 10 seconds before its expiry, so a set is never used in
/// its last 5 seconds.
const LIFETIME: Duration = Duration::from_secs(15);

/// How many sets must expire, while the dataset is in use, after the store
/// first signs with one. The SDK checks the chain once when it loads, with a
/// set the store never signs with, so one more set than this must expire.
const MIN_EXPIRED: usize = 3;

/// The longest either test may run.
const DEADLINE: Duration = Duration::from_secs(150);

/// Rows each write appends.
const ROWS_PER_WRITE: i64 = 500;

/// Writes between checkpoints.
const WRITES_PER_CHECKPOINT: usize = 3;

/// Inline row cap: above what the writes between two checkpoints add, so each
/// checkpoint is what moves them into a data file.
const INLINE_MAX_ROWS: i64 = 4096;

/// Inline byte caps, far above the 16 bytes a row takes. An append dataset
/// otherwise writes every append straight to a data file.
const INLINE_MAX_BYTES: usize = 4 << 20;

/// The `RustFS` server and bucket the `CAYENNE_TEST_RUSTFS_*` variables name, or
/// `None` when another store is selected.
#[derive(Clone)]
struct RustFs {
    endpoint: String,
    bucket: String,
    access_key: String,
    secret_key: String,
}

fn rustfs() -> Option<RustFs> {
    if std::env::var("CAYENNE_TEST_DATA_STORE").as_deref() != Ok("rustfs") {
        eprintln!("skipped: set CAYENNE_TEST_DATA_STORE=rustfs and CAYENNE_TEST_RUSTFS_* to run");
        return None;
    }
    let var = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| {
                panic!(
                    "CAYENNE_TEST_DATA_STORE is 'rustfs', but {name} is not set. Start RustFS, create a bucket, and set the CAYENNE_TEST_RUSTFS_* variables; .github/workflows/cayenne_object_native.yml shows how"
                )
            })
    };
    Some(RustFs {
        endpoint: var("CAYENNE_TEST_RUSTFS_ENDPOINT")
            .trim_end_matches('/')
            .to_string(),
        bucket: var("CAYENNE_TEST_RUSTFS_BUCKET"),
        access_key: var("CAYENNE_TEST_RUSTFS_ACCESS_KEY"),
        secret_key: var("CAYENNE_TEST_RUSTFS_SECRET_KEY"),
    })
}

/// A SigV4-signed request to `RustFS` with `key`, `secret` and an optional
/// session token, returning the response body, or an error naming the status
/// and body.
async fn signed(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    service: &str,
    (key, secret, token): (&str, &str, Option<&str>),
    body: Option<(&str, String)>,
) -> Result<String, String> {
    let (content_type, body) = body.unwrap_or(("application/json", String::new()));
    let headers = [("content-type", content_type)];
    let identity =
        Credentials::new(key, secret, token.map(ToString::to_string), None, "rustfs").into();
    let mut settings = SigningSettings::default();
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    let params = aws_sigv4::sign::v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name(service)
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .map_err(|error| format!("signing parameters: {error}"))?
        .into();
    let signable = SignableRequest::new(
        method.as_str(),
        url,
        headers.iter().copied(),
        SignableBody::Bytes(body.as_bytes()),
    )
    .map_err(|error| format!("signable request: {error}"))?;
    let (instructions, _) = sign(signable, &params)
        .map_err(|error| format!("sign: {error}"))?
        .into_parts();
    let mut request = client
        .request(method.clone(), url)
        .header("content-type", content_type)
        .body(body);
    for (name, value) in instructions.headers() {
        request = request.header(name, value);
    }
    let response = request
        .send()
        .await
        .map_err(|error| format!("{method} {url}: {error}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| format!("{method} {url}: read the body: {error}"))?;
    if status.is_success() {
        Ok(text)
    } else {
        Err(format!("{method} {url}: HTTP {status}: {text}"))
    }
}

/// The text between `<tag>` and `</tag>` in an STS XML response.
fn xml_field<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = start + xml[start..].find(&format!("</{tag}>"))?;
    Some(&xml[start..end])
}

/// One set of session credentials, the `RustFS` user that assumed it, and the
/// moment it expires.
#[derive(Clone)]
struct SessionCredentials {
    access_key: String,
    secret_key: String,
    token: String,
    user: String,
    expires: SystemTime,
}

/// Issues session credentials that stop working when they expire.
struct Issuer {
    rustfs: RustFs,
    client: reqwest::Client,
    issued: AtomicUsize,
    expired: Arc<AtomicUsize>,
    /// The set issued last.
    last: parking_lot::Mutex<Option<SessionCredentials>>,
}

impl Issuer {
    fn new(rustfs: RustFs) -> Arc<Self> {
        Arc::new(Self {
            rustfs,
            client: reqwest::Client::new(),
            issued: AtomicUsize::new(0),
            expired: Arc::new(AtomicUsize::new(0)),
            last: parking_lot::Mutex::new(None),
        })
    }

    fn root(&self) -> (&str, &str, Option<&str>) {
        (&self.rustfs.access_key, &self.rustfs.secret_key, None)
    }

    /// A new set from STS `AssumeRole`, assumed by a new user that is removed,
    /// ending the set, [`LIFETIME`] from now.
    async fn issue(self: &Arc<Self>) -> Result<SessionCredentials, String> {
        let n = self.issued.fetch_add(1, Ordering::SeqCst);
        let id = uuid::Uuid::now_v7().simple().to_string();
        let user = format!("session{n:03}{}", &id[id.len() - 6..]);
        let user_secret = format!("{id}-secret");
        let admin = format!("{}/rustfs/admin/v3", self.rustfs.endpoint);
        signed(
            &self.client,
            reqwest::Method::PUT,
            &format!("{admin}/add-user?accessKey={user}"),
            "s3",
            self.root(),
            Some((
                "application/json",
                format!(r#"{{"secretKey":"{user_secret}","status":"enabled"}}"#),
            )),
        )
        .await?;
        signed(
            &self.client,
            reqwest::Method::PUT,
            &format!(
                "{admin}/set-user-or-group-policy?policyName=readwrite&userOrGroup={user}&isGroup=false"
            ),
            "s3",
            self.root(),
            None,
        )
        .await?;
        let expires = SystemTime::now() + LIFETIME;
        let response = signed(
            &self.client,
            reqwest::Method::POST,
            &format!("{}/", self.rustfs.endpoint),
            "sts",
            (&user, &user_secret, None),
            Some((
                "application/x-www-form-urlencoded",
                "Action=AssumeRole&Version=2011-06-15&RoleArn=arn:aws:iam::000000000000:role/cayenne-test&RoleSessionName=cayenne-state-location&DurationSeconds=900".to_string(),
            )),
        )
        .await?;
        let field = |tag: &str| {
            xml_field(&response, tag)
                .map(ToString::to_string)
                .ok_or_else(|| format!("AssumeRole returned no {tag}: {response}"))
        };
        let credentials = SessionCredentials {
            access_key: field("AccessKeyId")?,
            secret_key: field("SecretAccessKey")?,
            token: field("SessionToken")?,
            user,
            expires,
        };
        self.expire_at(&credentials);
        *self.last.lock() = Some(credentials.clone());
        Ok(credentials)
    }

    /// Removes the set's user when the set expires.
    fn expire_at(self: &Arc<Self>, credentials: &SessionCredentials) {
        let issuer = Arc::clone(self);
        let user = credentials.user.clone();
        let wait = credentials
            .expires
            .duration_since(SystemTime::now())
            .unwrap_or_default();
        tokio::spawn(async move {
            // The expiry itself is under test: the set must stop working then.
            tokio::time::sleep(wait).await;
            let removed = signed(
                &issuer.client,
                reqwest::Method::DELETE,
                &format!(
                    "{}/rustfs/admin/v3/remove-user?accessKey={user}",
                    issuer.rustfs.endpoint
                ),
                "s3",
                issuer.root(),
                None,
            )
            .await;
            match removed {
                Ok(_) => {
                    issuer.expired.fetch_add(1, Ordering::SeqCst);
                }
                Err(error) => eprintln!("could not expire the credentials of {user}: {error}"),
            }
        });
    }
}

/// What the credentials endpoint hands out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refresh {
    /// A new set on every request, reported with its real expiry, so the SDK
    /// replaces each set before it expires.
    Rotating,
    /// One set on every request, reported as valid for an hour, so the SDK never
    /// replaces it, though it expires after [`LIFETIME`] like any other.
    Never,
}

/// The container credentials JSON for `credentials`, reported to expire at `expires`.
fn credentials_json(credentials: &SessionCredentials, expires: SystemTime) -> String {
    let expiration = chrono::DateTime::<chrono::Utc>::from(expires)
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    serde_json::json!({
        "AccessKeyId": credentials.access_key,
        "SecretAccessKey": credentials.secret_key,
        "Token": credentials.token,
        "Expiration": expiration,
    })
    .to_string()
}

/// Serves container credentials on a loopback port, and returns its address.
async fn serve_credentials(issuer: Arc<Issuer>, refresh: Refresh) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the credentials endpoint");
    let address = listener.local_addr().expect("credentials endpoint address");
    let first: Arc<tokio::sync::OnceCell<SessionCredentials>> =
        Arc::new(tokio::sync::OnceCell::new());
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let issuer = Arc::clone(&issuer);
            let first = Arc::clone(&first);
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut buffer).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buffer[..read]),
                    }
                }
                let answer = match refresh {
                    Refresh::Rotating => issuer
                        .issue()
                        .await
                        .map(|credentials| credentials_json(&credentials, credentials.expires)),
                    Refresh::Never => {
                        first
                            .get_or_try_init(|| issuer.issue())
                            .await
                            .map(|credentials| {
                                credentials_json(
                                    credentials,
                                    SystemTime::now() + Duration::from_secs(3600),
                                )
                            })
                    }
                };
                let (status, body) = match answer {
                    Ok(body) => ("200 OK", body),
                    Err(error) => {
                        eprintln!("the credentials endpoint could not issue credentials: {error}");
                        ("500 Internal Server Error", String::new())
                    }
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    address
}

/// Points the AWS SDK credential chain at the endpoint at `address`, and at
/// nothing else the environment or a profile could supply.
fn use_only_the_credentials_endpoint(address: SocketAddr, directory: &std::path::Path) {
    let missing = directory.join("no-aws-profile");
    // SAFETY: the test runs in a process of its own (`require_process_per_test`)
    // and changes the environment before it starts any task that reads it.
    unsafe {
        for name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_ROLE_ARN",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
            "AWS_ENDPOINT",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3",
        ] {
            std::env::remove_var(name);
        }
        std::env::set_var("AWS_CONFIG_FILE", &missing);
        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &missing);
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
        std::env::set_var(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            format!("http://{address}/credentials"),
        );
    }
}

fn require_process_per_test() {
    assert!(
        std::env::var("NEXTEST_EXECUTION_MODE").as_deref() == Ok("process-per-test"),
        "this test points the AWS SDK credential chain at a test endpoint through environment variables, so it must run in a process of its own: run it with `cargo nextest run -p accelerator-cayenne --test integration`"
    );
}

/// A session configured like `spiced`'s: Cayenne tags its table schema with
/// `spice.accelerator`, which `DataFusion`'s physical aggregate schema check rejects
/// (apache/datafusion#12733), so `spiced` skips that check.
fn session() -> SessionContext {
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .skip_physical_aggregate_schema_check = true;
    SessionContext::new_with_config(config)
}

/// Opens a new file-mode `orders` dataset placed in an `s3://` state location in
/// `rustfs`'s bucket, signing with the AWS SDK credential chain.
async fn open_placed_dataset(
    rustfs: &RustFs,
    directory: &std::path::Path,
) -> (SessionContext, Arc<dyn TableProvider>) {
    let mut app = app::AppBuilder::new("test")
        .with_runtime_params(
            [("cayenne_vnext".to_string(), "enabled".to_string())]
                .into_iter()
                .collect(),
        )
        .build();
    app.runtime.state = Some(spicepod::component::runtime::RuntimeState {
        location: format!(
            "s3://{}/cayenne-test/{}/state/",
            rustfs.bucket,
            uuid::Uuid::now_v7().simple()
        ),
        params: Some(spicepod::param::Params::from_string_map(HashMap::from([
            ("s3_endpoint".to_string(), rustfs.endpoint.clone()),
            ("s3_region".to_string(), "us-east-1".to_string()),
            ("s3_auth".to_string(), "iam_role".to_string()),
        ]))),
    });
    let source = TestAccelerationSource::new("orders")
        .with_app(app)
        .with_acceleration(Acceleration {
            enabled: true,
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Append),
            params: [
                (
                    "cayenne_metadata_dir".to_string(),
                    directory.join("metadata").to_string_lossy().into_owned(),
                ),
                // Writes commit their rows inline in the metastore, and each
                // checkpoint moves them into a data file in the location.
                (
                    "cayenne_inline_max_rows".to_string(),
                    INLINE_MAX_ROWS.to_string(),
                ),
                (
                    "cayenne_inline_max_bytes".to_string(),
                    INLINE_MAX_BYTES.to_string(),
                ),
                (
                    "cayenne_inline_max_buffer_bytes".to_string(),
                    INLINE_MAX_BYTES.to_string(),
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        });
    let registry = AcceleratorEngineRegistry::new();
    registry
        .register_accelerator_engine(Engine::Cayenne, Arc::new(CayenneAccelerator::new()))
        .await;
    let acceleration = source.acceleration().expect("acceleration").clone();
    let ctx = session();
    let table = registry
        .create_accelerator_table(
            TableReference::bare("orders"),
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ])),
            None,
            &acceleration,
            source.secrets(),
            Some(&source),
            Arc::new(ctx.clone()),
        )
        .await
        .unwrap_or_else(|error| panic!("create the acceleration: {error}"));
    ctx.register_table("orders", Arc::clone(&table))
        .expect("register");
    (ctx, table)
}

/// Every row of `orders`, in id order.
async fn rows(ctx: &SessionContext) -> Result<Vec<(i64, i64)>, String> {
    let batches = ctx
        .sql("SELECT id, v FROM orders ORDER BY id")
        .await
        .map_err(|error| format!("plan the scan: {error}"))?
        .collect()
        .await
        .map_err(|error| format!("scan: {error}"))?;
    let mut rows = Vec::new();
    for batch in &batches {
        let column = |index: usize| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column")
        };
        rows.extend(
            column(0)
                .values()
                .iter()
                .copied()
                .zip(column(1).values().iter().copied()),
        );
    }
    Ok(rows)
}

/// What a workload run did before it stopped.
#[derive(Debug)]
struct Run {
    writes: usize,
    checkpoints_after_an_expiry: usize,
    expired: usize,
}

/// Appends [`ROWS_PER_WRITE`] rows a second, checkpoints the inlined rows into a
/// data file every [`WRITES_PER_CHECKPOINT`] writes, and after each step requires exactly the rows
/// written so far, until `done` holds or a step fails.
async fn workload(
    ctx: &SessionContext,
    table: &Arc<dyn TableProvider>,
    issuer: &Issuer,
    done: impl Fn(&Run) -> bool,
) -> Result<Run, (Run, String)> {
    let cayenne = spice_table::find_concrete::<CayenneTableProvider>(
        table.as_ref(),
        spice_table::LayerWalk::Write,
    )
    .expect("the placed dataset is a Cayenne table");
    let started = Instant::now();
    let mut run = Run {
        writes: 0,
        checkpoints_after_an_expiry: 0,
        expired: 0,
    };
    let mut written = 0_i64;
    while started.elapsed() < DEADLINE {
        let first = written + 1;
        let last = written + ROWS_PER_WRITE;
        let step = async {
            ctx.sql(&format!(
                "INSERT INTO orders SELECT value AS id, value * 10 AS v FROM generate_series({first}, {last})"
            ))
            .await
            .map_err(|error| format!("plan write {}: {error}", run.writes + 1))?
            .collect()
            .await
            .map_err(|error| format!("write {}: {error}", run.writes + 1))?;
            let checkpointed = if (run.writes + 1).is_multiple_of(WRITES_PER_CHECKPOINT) {
                let flushed = cayenne.checkpoint_inlined_data().await.map_err(|error| {
                    format!("checkpoint after write {}: {error}", run.writes + 1)
                })?;
                let expected =
                    ROWS_PER_WRITE * i64::try_from(WRITES_PER_CHECKPOINT).expect("writes");
                if i64::try_from(flushed).ok() != Some(expected) {
                    return Err(format!(
                        "the checkpoint after write {} moved {flushed} inlined rows into a data file, not {expected}",
                        run.writes + 1
                    ));
                }
                true
            } else {
                false
            };
            let expected: Vec<(i64, i64)> = (1..=last).map(|id| (id, id * 10)).collect();
            let seen = rows(ctx).await?;
            if seen != expected {
                return Err(format!(
                    "after write {}: expected ids 1..={last}, saw {} rows (first {:?}, last {:?})",
                    run.writes + 1,
                    seen.len(),
                    seen.first(),
                    seen.last()
                ));
            }
            Ok(checkpointed)
        };
        let expired_before = issuer.expired.load(Ordering::SeqCst);
        match step.await {
            Ok(checkpointed) => {
                written = last;
                run.writes += 1;
                // The first two sets expire together: the one the SDK checks the
                // chain with and the first the store signs with.
                if checkpointed && expired_before >= 2 {
                    run.checkpoints_after_an_expiry += 1;
                }
                run.expired = issuer.expired.load(Ordering::SeqCst);
            }
            Err(error) => {
                run.expired = issuer.expired.load(Ordering::SeqCst);
                return Err((run, error));
            }
        }
        if done(&run) {
            return Ok(run);
        }
        // Spreads the steps over several credential lifetimes.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err((run, format!("the run passed its {DEADLINE:?} deadline")))
}

async fn start(refresh: Refresh) -> Option<(Arc<Issuer>, tempfile::TempDir)> {
    let rustfs = rustfs()?;
    require_process_per_test();
    let directory = tempfile::tempdir().expect("temp dir");
    let issuer = Issuer::new(rustfs);
    let address = serve_credentials(Arc::clone(&issuer), refresh).await;
    use_only_the_credentials_endpoint(address, directory.path());
    Some((issuer, directory))
}

/// Writes, checkpoints and scans of a placed dataset keep returning exactly the
/// rows written while [`MIN_EXPIRED`] sets of session credentials expire after
/// the store starts signing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_placed_dataset_keeps_working_across_session_credential_replacements() {
    let Some((issuer, directory)) = start(Refresh::Rotating).await else {
        return;
    };
    let (ctx, table) = open_placed_dataset(&issuer.rustfs, directory.path()).await;
    let run = workload(&ctx, &table, &issuer, |run| {
        run.expired > MIN_EXPIRED && run.checkpoints_after_an_expiry > 0
    })
    .await
    .unwrap_or_else(|(run, error)| {
        panic!(
            "{error}. {run:?}, {} set(s) of credentials issued",
            issuer.issued.load(Ordering::SeqCst)
        )
    });
    eprintln!(
        "{run:?}: {} rows written and scanned over {} sets of session credentials",
        i64::try_from(run.writes).expect("writes") * ROWS_PER_WRITE,
        issuer.issued.load(Ordering::SeqCst)
    );
}

/// The log lines at `WARN` and above that every thread writes, for a test running
/// in a process of its own.
#[derive(Clone, Default)]
struct Warnings(Arc<parking_lot::Mutex<Vec<u8>>>);

impl std::io::Write for Warnings {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Warnings {
    fn capture() -> Self {
        let warnings = Self::default();
        let writer = warnings.clone();
        tracing_subscriber::fmt()
            .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .try_init()
            .expect("no other subscriber in the test's process");
        warnings
    }
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock()).into_owned()
    }
}

/// How `object_store` reports S3's `403` for the expired set.
const REJECTED: &str = "The operation lacked the necessary privileges to complete";

/// Control: a store whose credentials are never replaced fails once they expire,
/// so the test above passes only because the store replaces them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_a_dataset_whose_credentials_are_never_replaced_fails_once_they_expire() {
    let Some((issuer, directory)) = start(Refresh::Never).await else {
        return;
    };
    let warnings = Warnings::capture();
    let (ctx, table) = open_placed_dataset(&issuer.rustfs, directory.path()).await;
    let outcome = workload(&ctx, &table, &issuer, |_| false).await;
    let Err((run, error)) = outcome else {
        panic!("the workload cannot finish without an error");
    };
    eprintln!("{run:?} ended with: {error}");
    assert_eq!(
        issuer.issued.load(Ordering::SeqCst),
        1,
        "the endpoint issued one set and handed out only that one"
    );
    assert!(
        run.writes > 0,
        "the dataset worked before its credentials expired: {error}"
    );
    assert_eq!(
        run.expired, 1,
        "the step failed after the set expired: {error}"
    );
    // The step fails on the first request signed with the expired set, and says
    // why. A commit published in the background can meet it first: its outcome
    // is unknown, its warning names the rejection, and the step is refused
    // until the dataset reads its state back.
    let rejected = |text: &str| text.contains(REJECTED) && text.contains("InvalidAccessKeyId");
    if error.contains(
        "Dataset 'orders' (cayenne) is reading its state back after a change whose outcome is unknown",
    ) {
        let warnings = warnings.text();
        assert!(
            warnings.lines().any(|line| line.contains(
                "Dataset 'orders' (cayenne) could not confirm whether its last change was saved"
            ) && rejected(line)),
            "the unconfirmed commit that stopped the step failed on the expired set: {warnings}"
        );
    } else {
        assert!(
            rejected(&error),
            "the step failed on the expired set, not for another reason: {error}"
        );
    }
    // The store's only set is the one RustFS now refuses.
    let set = issuer
        .last
        .lock()
        .clone()
        .expect("the set the endpoint handed out");
    let refused = signed(
        &issuer.client,
        reqwest::Method::GET,
        &format!(
            "{}/{}?list-type=2&max-keys=1",
            issuer.rustfs.endpoint, issuer.rustfs.bucket
        ),
        "s3",
        (&set.access_key, &set.secret_key, Some(&set.token)),
        None,
    )
    .await
    .expect_err("RustFS refuses the expired set");
    assert!(
        refused.contains("InvalidAccessKeyId"),
        "RustFS refuses the expired set as unknown: {refused}"
    );
}
