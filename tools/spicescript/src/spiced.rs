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

//! One `spiced` process: its ports, working directory and log stay the same
//! across restarts, so a restarted instance finds its own acceleration files
//! and replication slots again.

use secrecy::SecretString;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use arrow::array::RecordBatch;
use flight_client::{Credentials, FlightClient};
use futures::TryStreamExt;
use nix::{sys::signal::Signal, unistd::Pid};
use tokio::process::{Child, Command};

use crate::docker::free_port;

const ERROR_GRACE: Duration = Duration::from_secs(20);

pub struct Spiced {
    pub binary: PathBuf,
    pub dir: PathBuf,
    env: BTreeMap<String, String>,
    http: u16,
    flight: u16,
    metrics: u16,
    otel: u16,
    child: Option<Child>,
    flight_client: Option<FlightClient>,
    /// Sends the API key, if the runtime requires one, on every request.
    http_client: reqwest::Client,
    /// Sends no key unless asked, for a step that chooses its own.
    bare_client: reqwest::Client,
    api_key: Option<String>,
    /// The root certificate the runtime's TLS certificate chains to, when it
    /// serves TLS.
    tls_ca: Option<PathBuf>,
    /// Arguments passed to `spiced` after the endpoint flags.
    extra_args: Vec<String>,
    /// Flight SQL clients kept by session name, for statements that share one.
    sessions: std::collections::HashMap<
        String,
        arrow_flight::sql::client::FlightSqlServiceClient<tonic::transport::Channel>,
    >,
}

/// Which API key a request presents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key {
    /// The script's key (`defaults.api-key`), or none if it has none.
    Default,
    None,
    Given(String),
}

impl Spiced {
    pub fn new(binary: PathBuf, dir: PathBuf, env: BTreeMap<String, String>) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            binary,
            dir,
            env,
            http: free_port()?,
            flight: free_port()?,
            metrics: free_port()?,
            otel: free_port()?,
            child: None,
            flight_client: None,
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()?,
            bare_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()?,
            api_key: None,
            tls_ca: None,
            extra_args: Vec::new(),
            sessions: std::collections::HashMap::new(),
        })
    }

    fn scheme(&self) -> &'static str {
        if self.tls_ca.is_some() {
            "https"
        } else {
            "http"
        }
    }

    fn client_builder(&self) -> Result<reqwest::ClientBuilder> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(60));
        if let Some(ca) = &self.tls_ca {
            let pem =
                std::fs::read(ca).with_context(|| format!("Failed to read {}", ca.display()))?;
            builder = builder.tls_certs_only([reqwest::Certificate::from_pem(&pem)?]);
        }
        Ok(builder)
    }

    /// Passes `args` to `spiced` on every start.
    pub fn set_extra_args(&mut self, args: Vec<String>) {
        self.extra_args = args;
    }

    /// Connects over TLS, trusting `ca`, from now on.
    pub fn set_tls(&mut self, ca: Option<PathBuf>) -> Result<()> {
        self.tls_ca = ca;
        self.bare_client = self.client_builder()?.build()?;
        let key = self.api_key.clone();
        self.set_api_key(key)
    }

    /// Presents `key` on every request this client makes from now on.
    pub fn set_api_key(&mut self, key: Option<String>) -> Result<()> {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(key) = &key {
            headers.insert(
                "x-api-key",
                key.parse()
                    .context("the API key is not a valid header value")?,
            );
        }
        self.http_client = self.client_builder()?.default_headers(headers).build()?;
        self.api_key = key;
        self.flight_client = None;
        Ok(())
    }

    async fn connect_flight(&self, key: Option<&str>) -> Result<FlightClient> {
        let credentials = match key {
            Some(key) => Credentials::new("spicescript", SecretString::from(key.to_string())),
            None => Credentials::anonymous(),
        };
        FlightClient::try_new(
            Arc::from(format!("{}://127.0.0.1:{}", self.scheme(), self.flight)),
            credentials,
            None,
            self.tls_ca.as_deref(),
        )
        .await
        .context("Failed to connect to spiced over Flight")
    }

    /// The runtime's HTTP endpoint, as the `spice` CLI takes it.
    pub fn http_endpoint(&self) -> String {
        format!("{}://127.0.0.1:{}", self.scheme(), self.http)
    }

    pub fn log_path(&self) -> PathBuf {
        self.dir.join("spiced.log")
    }

    pub fn is_running(&mut self) -> bool {
        self.child
            .as_mut()
            .is_some_and(|c| matches!(c.try_wait(), Ok(None)))
    }

    /// Replaces the spicepod file, for a running `spiced` that watches it.
    pub fn write_spicepod(&self, spicepod_yaml: &str) -> Result<()> {
        // Written beside it and renamed over it, so a watcher never reads half.
        let tmp = self.dir.join(".spicepod.yaml.tmp");
        std::fs::write(&tmp, spicepod_yaml)?;
        std::fs::rename(&tmp, self.dir.join("spicepod.yaml"))?;
        Ok(())
    }

    /// Writes the spicepod and starts `spiced`; does not wait for readiness.
    pub fn start(&mut self, spicepod_yaml: &str) -> Result<()> {
        if self.is_running() {
            bail!("spiced is already running");
        }
        std::fs::write(self.dir.join("spicepod.yaml"), spicepod_yaml)?;
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path())?;
        let child = Command::new(&self.binary)
            .current_dir(&self.dir)
            .arg("--telemetry-enabled=false")
            .args(["--http", &format!("127.0.0.1:{}", self.http)])
            .args(["--flight", &format!("127.0.0.1:{}", self.flight)])
            .args(["--metrics", &format!("127.0.0.1:{}", self.metrics)])
            .args(["--open_telemetry", &format!("127.0.0.1:{}", self.otel)])
            .args(&self.extra_args)
            .envs(&self.env)
            .stdout(log.try_clone()?)
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("Failed to start {}", self.binary.display()))?;
        // Lets a later run stop this process if this runner is killed.
        if let Some(pid) = child.id() {
            std::fs::write(
                self.dir.join(crate::reap::PID_FILE),
                format!("{pid} {}\n", self.http),
            )?;
            crate::cleanup::track_process(pid, self.http);
        }
        self.child = Some(child);
        self.flight_client = None;
        Ok(())
    }

    /// Waits until a scheduler has at least `count` executors ready to take work
    /// (or, with `joined_only`, registered with it), whether or not its own
    /// components are ready yet: the verbose `/v1/ready` reports the executors
    /// on a line of their own, `executors: <ready>/<registered> ready (…)`.
    pub async fn wait_executors(
        &mut self,
        count: usize,
        timeout: Duration,
        joined_only: bool,
    ) -> Result<()> {
        let url = format!(
            "{}://127.0.0.1:{}/v1/ready?min_ready_executors={count}&verbose=true",
            self.scheme(),
            self.http
        );
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            ensure!(
                self.is_running(),
                "spiced exited; see {}",
                self.log_path().display()
            );
            let body = match self.http_client.get(&url).send().await {
                Ok(r) => r.text().await.unwrap_or_default(),
                Err(e) => e.to_string(),
            };
            let joined = body
                .lines()
                .filter_map(|l| l.split("executors: ").nth(1))
                .filter_map(|rest| {
                    let (ready, rest) = rest.split_once('/')?;
                    let registered = rest.split(|c: char| !c.is_ascii_digit()).next()?;
                    if joined_only { registered } else { ready }
                        .trim()
                        .parse::<usize>()
                        .ok()
                })
                .any(|n| n >= count);
            if joined {
                return Ok(());
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "no executor {} after {timeout:?}: {}",
                if joined_only { "registered" } else { "ready" },
                body.split_whitespace().collect::<Vec<_>>().join(" ")
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    pub async fn wait_ready(&mut self, timeout: Duration) -> Result<()> {
        let url = format!("{}://127.0.0.1:{}/v1/ready", self.scheme(), self.http);
        let deadline = tokio::time::Instant::now() + timeout;
        let mut erroring_since: Option<tokio::time::Instant> = None;
        loop {
            if !self.is_running() {
                bail!(
                    "spiced exited before it was ready; see {}",
                    self.log_path().display()
                );
            }
            if let Ok(r) = self.http_client.get(&url).send().await
                && r.status().is_success()
            {
                return self.wait_flight(deadline, timeout).await;
            }
            // A dataset can report `Error` while it retries a transient
            // failure, so only an error that persists ends the wait early.
            let status = self.dataset_status().await.unwrap_or_default();
            let erroring = status.contains(r#""status":"Error""#);
            if erroring {
                erroring_since.get_or_insert_with(tokio::time::Instant::now);
            } else {
                erroring_since = None;
            }
            // Whichever limit ends the wait, a dataset in Error is reported
            // the same way, so the same failure always reads the same.
            let error_persisted = erroring_since.is_some_and(|since| since.elapsed() > ERROR_GRACE);
            let timed_out = tokio::time::Instant::now() > deadline;
            if erroring && (error_persisted || timed_out) {
                bail!(
                    "spiced not ready: a dataset is in Error: {status}; see {}",
                    self.log_path().display()
                );
            }
            if timed_out {
                bail!(
                    "spiced not ready after {timeout:?}; dataset status: {status}; see {}",
                    self.log_path().display()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// A dataset's entry from `/v1/datasets?status=true`.
    pub async fn dataset_info(&self, dataset: &str) -> Result<serde_json::Value> {
        let url = format!(
            "{}://127.0.0.1:{}/v1/datasets?status=true",
            self.scheme(),
            self.http
        );
        let all: serde_json::Value = self.http_client.get(url).send().await?.json().await?;
        all.as_array()
            .and_then(|list| {
                list.iter()
                    .find(|d| d.get("name").and_then(serde_json::Value::as_str) == Some(dataset))
            })
            .cloned()
            .with_context(|| format!("spiced has no dataset '{dataset}'"))
    }

    /// The Prometheus exposition from `/metrics`.
    pub async fn metrics(&self) -> Result<String> {
        let url = format!("{}://127.0.0.1:{}/metrics", self.scheme(), self.metrics);
        Ok(self.http_client.get(url).send().await?.text().await?)
    }

    /// Waits until the Flight port accepts connections. `spiced` answers HTTP
    /// (`/health`, and `/v1/ready`) up to ~0.1 s before its Flight listener
    /// is bound, and queries go over Flight.
    async fn wait_flight(&self, deadline: tokio::time::Instant, timeout: Duration) -> Result<()> {
        while tokio::net::TcpStream::connect(("127.0.0.1", self.flight))
            .await
            .is_err()
        {
            if tokio::time::Instant::now() > deadline {
                bail!(
                    "spiced answered HTTP but Flight did not accept connections on port {} within {timeout:?}; see {}",
                    self.flight,
                    self.log_path().display()
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    /// Waits until `spiced` answers `/health` and accepts Flight connections,
    /// without waiting for datasets.
    pub async fn wait_http(&mut self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        self.wait_health(timeout).await?;
        self.wait_flight(deadline, timeout).await
    }

    /// Waits until `spiced` answers `/health`. A cluster executor serves no
    /// client Flight, so this is all it offers to wait on.
    pub async fn wait_health(&mut self, timeout: Duration) -> Result<()> {
        let url = format!("{}://127.0.0.1:{}/health", self.scheme(), self.http);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if !self.is_running() {
                bail!("spiced exited; see {}", self.log_path().display());
            }
            if let Ok(r) = self.http_client.get(&url).send().await
                && r.status().is_success()
            {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                bail!("spiced did not answer /health within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Whether `/v1/ready` answers ready.
    pub async fn ready(&self) -> bool {
        let url = format!("{}://127.0.0.1:{}/v1/ready", self.scheme(), self.http);
        matches!(self.http_client.get(url).send().await, Ok(r) if r.status().is_success())
    }

    async fn dataset_status(&self) -> Result<String> {
        let url = format!(
            "{}://127.0.0.1:{}/v1/datasets?status=true",
            self.scheme(),
            self.http
        );
        Ok(self.http_client.get(url).send().await?.text().await?)
    }

    /// SIGTERM, then SIGKILL if `spiced` has not exited within 30 seconds.
    /// Asks `spiced` to shut down (SIGTERM) and waits for it. Fails if it does
    /// not exit within `timeout` (it is then killed) or exits unsuccessfully,
    /// since either is a shutdown that was not graceful.
    pub async fn stop(&mut self, timeout: Duration) -> Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        if let Some(pid) = child.id() {
            let _ = nix::sys::signal::kill(Pid::from_raw(pid.cast_signed()), Signal::SIGTERM);
            crate::cleanup::untrack_process(pid);
        }
        let started = std::time::Instant::now();
        let waited = tokio::time::timeout(timeout, child.wait()).await;
        let outcome = match waited {
            Ok(Ok(status)) if status.success() => Ok(()),
            Ok(Ok(status)) => Err(anyhow::anyhow!(
                "spiced exited with {status} on SIGTERM after {:.1}s; see {}",
                started.elapsed().as_secs_f64(),
                self.log_path().display()
            )),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => {
                child.kill().await?;
                Err(anyhow::anyhow!(
                    "spiced did not exit within {timeout:?} of SIGTERM and was killed; see {}",
                    self.log_path().display()
                ))
            }
        };
        self.child = None;
        self.flight_client = None;
        outcome
    }

    /// Kills `spiced` without letting it shut down. With `freeze_first` it is
    /// sent SIGSTOP first, so it is frozen at one point before it dies.
    pub async fn kill(&mut self, freeze_first: bool) -> Result<()> {
        let Some(child) = self.child.as_mut() else {
            bail!("spiced is not running");
        };
        if let Some(pid) = child.id() {
            crate::cleanup::untrack_process(pid);
            if freeze_first {
                nix::sys::signal::kill(Pid::from_raw(pid.cast_signed()), Signal::SIGSTOP)?;
            }
        }
        child.kill().await?;
        self.child = None;
        self.flight_client = None;
        Ok(())
    }

    async fn flight(&mut self) -> Result<&FlightClient> {
        if self.flight_client.is_none() {
            let client = self.connect_flight(self.api_key.as_deref()).await?;
            self.flight_client = Some(client);
        }
        self.flight_client
            .as_ref()
            .context("flight client was just created")
    }

    pub async fn query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let client = self.flight().await?;
        let stream = client.query(sql).await?;
        Ok(stream.try_collect().await?)
    }

    /// Runs a query presenting `key` on a connection of its own.
    /// Writes `rows` (values as text, `null` for NULL) to `dataset` with a
    /// Flight `DoPut`, each column cast to the dataset's type.
    pub async fn flight_put(
        &self,
        dataset: &str,
        rows: &[Vec<serde_json::Value>],
        key: &Key,
    ) -> Result<()> {
        use arrow::array::{ArrayRef, StringArray};
        let key = match key {
            Key::Default => self.api_key.clone(),
            Key::None => None,
            Key::Given(k) => Some(k.clone()),
        };
        let mut client = self.connect_flight(key.as_deref()).await?;
        let schema = client
            .get_schema(vec![dataset.to_string()])
            .await
            .with_context(|| format!("Failed to read the schema of '{dataset}'"))?;
        let mut columns: Vec<ArrayRef> = Vec::new();
        for (i, field) in schema.fields().iter().enumerate() {
            let texts: Vec<Option<String>> = rows
                .iter()
                .map(|row| match row.get(i) {
                    None | Some(serde_json::Value::Null) => None,
                    Some(serde_json::Value::String(s)) => Some(s.clone()),
                    Some(other) => Some(other.to_string()),
                })
                .collect();
            let raw: ArrayRef = Arc::new(StringArray::from(texts));
            columns.push(
                arrow::compute::cast(&raw, field.data_type()).with_context(|| {
                    format!(
                        "column {} does not cast to {}",
                        field.name(),
                        field.data_type()
                    )
                })?,
            );
        }
        let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
        client.publish(dataset, vec![batch]).await?;
        Ok(())
    }

    /// Runs a statement over Flight SQL `ExecuteUpdate`, returning the count
    /// of rows it reports affected.
    pub async fn flight_update(&self, sql: &str, key: &Key) -> Result<i64> {
        use arrow_flight::sql::client::FlightSqlServiceClient;
        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", self.flight))?
                .connect()
                .await
                .context("Failed to connect to spiced over Flight")?;
        let mut client = FlightSqlServiceClient::new(channel);
        let key = match key {
            Key::Default => self.api_key.clone(),
            Key::None => None,
            Key::Given(k) => Some(k.clone()),
        };
        if let Some(key) = key {
            client.handshake("spicescript", &key).await?;
        }
        Ok(client.execute_update(sql.to_string(), None).await?)
    }

    /// Runs `sql` as a Flight SQL prepared statement with `parameters` bound,
    /// each cast to the type the server's parameter schema gives it.
    pub async fn query_prepared(
        &mut self,
        sql: &str,
        parameters: &[serde_json::Value],
        session: Option<&str>,
        key: &Key,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::array::{ArrayRef, StringArray};
        use arrow_flight::sql::client::FlightSqlServiceClient;
        use futures::TryStreamExt;
        let mut client = if let Some(client) = session.and_then(|s| self.sessions.get(s)) {
            client.clone()
        } else {
            let channel = tonic::transport::Endpoint::from_shared(format!(
                "http://127.0.0.1:{}",
                self.flight
            ))?
            .connect()
            .await
            .context("Failed to connect to spiced over Flight")?;
            let mut client = FlightSqlServiceClient::new(channel);
            let key = match key {
                Key::Default => self.api_key.clone(),
                Key::None => None,
                Key::Given(k) => Some(k.clone()),
            };
            if let Some(key) = key {
                client.handshake("spicescript", &key).await?;
            }
            if let Some(session) = session {
                self.sessions.insert(session.to_string(), client.clone());
            }
            client
        };
        let mut statement = client.prepare(sql.to_string(), None).await?;
        if !parameters.is_empty() {
            let declared = statement.parameter_schema()?.clone();
            let mut fields = Vec::new();
            let mut columns: Vec<ArrayRef> = Vec::new();
            for (i, value) in parameters.iter().enumerate() {
                let text = match value {
                    serde_json::Value::Null => None,
                    serde_json::Value::String(s) => Some(s.clone()),
                    other => Some(other.to_string()),
                };
                // A type the server could not infer (`Null`) comes from the value.
                let from_value = match value {
                    serde_json::Value::Number(n) if n.is_i64() => arrow::datatypes::DataType::Int64,
                    serde_json::Value::Number(_) => arrow::datatypes::DataType::Float64,
                    serde_json::Value::Bool(_) => arrow::datatypes::DataType::Boolean,
                    serde_json::Value::Null => arrow::datatypes::DataType::Null,
                    _ => arrow::datatypes::DataType::Utf8,
                };
                let target = match declared.fields().get(i).map(|f| f.data_type().clone()) {
                    Some(arrow::datatypes::DataType::Null) | None => from_value,
                    Some(declared) => declared,
                };
                let column: ArrayRef = if text.is_none() {
                    arrow::array::new_null_array(&target, 1)
                } else {
                    let raw: ArrayRef = Arc::new(StringArray::from(vec![text]));
                    arrow::compute::cast(&raw, &target).with_context(|| {
                        format!("parameter {} ({value}) does not cast to {target}", i + 1)
                    })?
                };
                let name = declared
                    .fields()
                    .get(i)
                    .map_or_else(|| format!("${}", i + 1), |f| f.name().clone());
                fields.push(arrow::datatypes::Field::new(name, target, true));
                columns.push(column);
            }
            let batch =
                RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)?;
            statement.set_parameters(batch)?;
        }
        let info = statement.execute().await?;
        let mut batches = Vec::new();
        for endpoint in info.endpoint {
            let Some(ticket) = endpoint.ticket else {
                continue;
            };
            let stream = client.do_get(ticket).await?;
            batches.extend(stream.try_collect::<Vec<_>>().await?);
        }
        let _ = statement.close().await;
        Ok(batches)
    }

    pub async fn query_as(&mut self, sql: &str, key: &Key) -> Result<Vec<RecordBatch>> {
        let key = match key {
            Key::Default => return self.query(sql).await,
            Key::None => None,
            Key::Given(k) => Some(k.as_str()),
        };
        let client = self.connect_flight(key).await?;
        let stream = client.query(sql).await?;
        Ok(stream.try_collect().await?)
    }

    /// Sends one HTTP request to the runtime; returns the status, the headers
    /// (names lowercased) and the body.
    pub async fn http(
        &self,
        method: &str,
        path: &str,
        headers: &[(String, String)],
        body: Option<&str>,
        key: &Key,
    ) -> Result<(u16, Vec<(String, String)>, String)> {
        let url = format!("{}://127.0.0.1:{}{path}", self.scheme(), self.http);
        let method = reqwest::Method::from_bytes(method.as_bytes())
            .with_context(|| format!("'{method}' is not an HTTP method"))?;
        let mut request = match key {
            Key::Default => self.http_client.request(method, url),
            Key::None => self.bare_client.request(method, url),
            Key::Given(k) => self.bare_client.request(method, url).header("x-api-key", k),
        };
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if let Some(body) = body {
            request = request.body(body.to_string());
        }
        let response = request.send().await?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_ascii_lowercase(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect();
        let body = response
            .text()
            .await
            .context("connection lost while reading the response body")?;
        Ok((status, headers, body))
    }

    /// Runs a statement through `/v1/sql`, which unlike a Flight ticket accepts
    /// a `BEGIN; …; COMMIT;` transaction body. Returns the response body.
    pub async fn exec(&self, sql: &str) -> Result<String> {
        let url = format!("{}://127.0.0.1:{}/v1/sql", self.scheme(), self.http);
        let response = self
            .http_client
            .post(url)
            .header("Content-Type", "text/plain")
            .body(sql.to_string())
            .send()
            .await?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("connection lost while reading the response body")?;
        if !status.is_success() {
            bail!("{status}: {body}");
        }
        Ok(body)
    }

    /// Asks `spiced` to start a refresh, with `refresh_sql` in place of the
    /// dataset's own when given; it returns before the refresh runs.
    pub async fn refresh(&self, dataset: &str, refresh_sql: Option<&str>) -> Result<()> {
        let url = format!(
            "{}://127.0.0.1:{}/v1/datasets/{dataset}/acceleration/refresh",
            self.scheme(),
            self.http
        );
        let mut request = self.http_client.post(url);
        if let Some(sql) = refresh_sql {
            request = request.json(&serde_json::json!({ "refresh_sql": sql }));
        }
        let response = request.send().await?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("connection lost while reading the response body")?;
        if !status.is_success() {
            bail!("refresh of '{dataset}' returned {status}: {body}");
        }
        Ok(())
    }
}
