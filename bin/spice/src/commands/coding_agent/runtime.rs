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

//! The runtime a coding agent talks to, and who stops it.
//!
//! A coding agent needs a runtime for as long as it runs, and a user may run
//! several agents at once against the same endpoint. So a runtime that some
//! `spice <agent>` session started belongs to all of the sessions using it,
//! and the last of them to exit stops it. A runtime nobody here started — one
//! the user runs with `spice run` — is only ever used, never stopped.
//!
//! The bookkeeping is two advisory file locks per endpoint under
//! `~/.spice/run/`:
//!
//! - `<endpoint>.sessions.lock`: every session holds it shared while its agent
//!   runs, so a session that can take it exclusively knows it is the last one.
//! - `<endpoint>.start.lock`: held exclusively while a session decides whether
//!   to start a runtime and while an exiting session decides whether it is the
//!   last one, so a session joining and a session leaving cannot both conclude
//!   they are alone.
//!
//! The operating system releases both when a process dies, however it dies,
//! so a crashed session never blocks the next one. `<endpoint>.json` records
//! the runtime a session started (its PID and executable), which is what
//! separates a runtime to stop from one to leave alone; `<endpoint>.log` is
//! that runtime's output, since the agent owns the terminal.

use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::context::RuntimeContext;
use crate::error::{
    CodingAgentNoSpicepodSnafu, CodingAgentOtherSpicepodSnafu, CodingAgentRuntimeElsewhereSnafu,
    CodingAgentRuntimeLockSnafu, CodingAgentRuntimeStartSnafu, CodingAgentRuntimeStateSnafu,
    InterruptedSnafu, Result,
};

/// How long a started runtime has to start answering `/health`. It normally
/// does within a second; the bound only exists so a runtime that never binds
/// its port is reported rather than waited on forever.
const HEALTH_BUDGET: Duration = Duration::from_secs(120);

/// How long a runtime is given to shut down cleanly before it is killed.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);

const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// How much of the runtime's log an error quotes.
const LOG_TAIL_BYTES: u64 = 4096;

/// A runtime some session started, as recorded for the sessions that follow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StartedRuntime {
    pub pid: u32,
    pub executable: PathBuf,
    /// The endpoint it was started at: what another spelling of its address
    /// that does not reach it is told to use.
    #[serde(default)]
    pub endpoint: String,
}

/// The files that coordinate the sessions using one endpoint.
#[derive(Debug, Clone)]
pub(crate) struct SessionFiles {
    pub sessions_lock: PathBuf,
    pub start_lock: PathBuf,
    pub state: PathBuf,
    pub log: PathBuf,
}

impl SessionFiles {
    /// The files for `endpoint`, under `run_dir`.
    #[must_use]
    pub fn for_endpoint(run_dir: &Path, endpoint: &str) -> Self {
        let key = endpoint_key(endpoint);
        Self {
            sessions_lock: run_dir.join(format!("{key}.sessions.lock")),
            start_lock: run_dir.join(format!("{key}.start.lock")),
            state: run_dir.join(format!("{key}.json")),
            log: run_dir.join(format!("{key}.log")),
        }
    }
}

/// A file name for the address an endpoint names: its host and port.
///
/// Every name for this machine — `localhost`, `127.0.0.1`, `[::1]`,
/// `0.0.0.0` — is one host, `localhost`, so sessions that spell one runtime's
/// address differently coordinate as the sessions of one runtime.
fn endpoint_key(endpoint: &str) -> String {
    let address = match reqwest::Url::parse(endpoint) {
        Ok(url) => match (url.host_str(), url.port_or_known_default()) {
            (Some(host), Some(port)) => {
                let host = if is_this_machine(host) {
                    "localhost"
                } else {
                    host
                };
                format!("{host}:{port}")
            }
            _ => endpoint.to_string(),
        },
        Err(_) => endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .trim_end_matches('/')
            .to_string(),
    };
    address
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Whether `host`, as a parsed URL spells it (lowercase, IPv6 in brackets),
/// names this machine: a loopback or unspecified address, or `localhost` and
/// its subdomains.
fn is_this_machine(host: &str) -> bool {
    match host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
    {
        Ok(ip) => ip.is_loopback() || ip.is_unspecified(),
        Err(_) => host == "localhost" || host.ends_with(".localhost"),
    }
}

/// Run `step` unless Ctrl-C comes first, which nothing has to be undone for.
async fn interruptible<T>(step: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::select! {
        result = step => result,
        _ = tokio::signal::ctrl_c() => InterruptedSnafu.fail(),
    }
}

/// This session's claim on the runtime at one endpoint. Releasing it stops the
/// runtime when this is the last session using a runtime a session started.
#[derive(Debug)]
pub(crate) struct RuntimeLease {
    files: SessionFiles,
    sessions: File,
}

impl RuntimeLease {
    /// Join the sessions using the runtime at the context's endpoint, starting
    /// the runtime for the Spicepod in the current directory if none is serving.
    /// `name` and `command` are the product and `spice` command the session
    /// runs, for what it reports.
    ///
    /// A runtime already serving at the default endpoint must serve the
    /// Spicepod in the current directory, if there is one: models from another
    /// project's would answer instead. At an endpoint the user named, the
    /// runtime is used whichever Spicepod it serves, and that is said.
    ///
    /// Ctrl-C ends it with [`Error::Interrupted`](crate::error::Error::Interrupted),
    /// having stopped a runtime it was starting, so it must not be cancelled by
    /// dropping it instead.
    ///
    /// # Errors
    ///
    /// Returns an error when the coordination files cannot be used, when a
    /// runtime has to be started and there is no Spicepod to start it for or it
    /// fails to start serving, when the runtime serving at the default endpoint
    /// serves another Spicepod, or when another session's runtime holds the
    /// address but does not answer at this endpoint.
    pub async fn acquire(
        ctx: &RuntimeContext,
        name: &'static str,
        command: &'static str,
    ) -> Result<Self> {
        let run_dir = ctx.spice_runtime_dir().join("run");
        std::fs::create_dir_all(&run_dir).context(CodingAgentRuntimeLockSnafu {
            path: run_dir.clone(),
        })?;
        let files = SessionFiles::for_endpoint(&run_dir, ctx.http_endpoint());

        let start = open_lock(&files.start_lock)?;
        interruptible(lock_exclusive_reporting_wait(
            &start,
            &files.start_lock,
            ctx.http_endpoint(),
        ))
        .await?;

        // Whether another session is using the runtime, asked while this one
        // holds the start lock, which every session joining or leaving takes.
        let sessions = open_lock(&files.sessions_lock)?;
        let alone = match sessions.try_lock() {
            Ok(()) => {
                sessions.unlock().context(CodingAgentRuntimeLockSnafu {
                    path: files.sessions_lock.clone(),
                })?;
                true
            }
            Err(std::fs::TryLockError::WouldBlock) => false,
            Err(std::fs::TryLockError::Error(source)) => {
                return Err(source).context(CodingAgentRuntimeLockSnafu {
                    path: files.sessions_lock.clone(),
                });
            }
        };
        interruptible(lock_shared(&sessions, &files.sessions_lock)).await?;

        // On an error the locks are released on drop, and nothing outlives
        // this session: a runtime that fails to start is stopped.
        prepare_runtime(ctx, &files, alone, name, command).await?;
        drop(start);

        Ok(Self { files, sessions })
    }

    /// [`Self::release`], reporting a runtime that could not be stopped: how a
    /// session ends once what it started has exited.
    pub async fn finish(self, endpoint: &str) {
        match self.release().await {
            Ok(true) => tracing::debug!("Stopped the Spice runtime at {endpoint}"),
            Ok(false) => {}
            Err(error) => tracing::warn!(
                "Failed to stop the Spice runtime at {endpoint}, so it may still be running. Stop it manually if so. Cause: {error}"
            ),
        }
    }

    /// Leave the sessions using this runtime, stopping it when this was the
    /// last one and a session started it.
    ///
    /// Returns whether the runtime was stopped.
    ///
    /// # Errors
    ///
    /// Returns an error when the coordination files cannot be used.
    pub async fn release(self) -> Result<bool> {
        let Self { files, sessions } = self;
        // Taking the start lock first serializes this check against a session
        // that is joining, which holds it until its shared lease is taken.
        let start = open_lock(&files.start_lock)?;
        lock_exclusive(&start, &files.start_lock).await?;
        drop(sessions);

        let sessions = open_lock(&files.sessions_lock)?;
        let last = match sessions.try_lock() {
            Ok(()) => true,
            Err(std::fs::TryLockError::WouldBlock) => false,
            Err(std::fs::TryLockError::Error(source)) => {
                return Err(source).context(CodingAgentRuntimeLockSnafu {
                    path: files.sessions_lock.clone(),
                });
            }
        };
        if !last {
            return Ok(false);
        }

        let Some(started) = read_state(&files.state)? else {
            return Ok(false);
        };
        let stopped = if process::is_runtime(&started) {
            process::stop(started.pid, SHUTDOWN_BUDGET).await;
            true
        } else {
            false
        };
        remove_state(&files.state)?;
        drop(sessions);
        drop(start);
        Ok(stopped)
    }
}

/// Use the runtime already serving at the context's endpoint, or start one.
///
/// `alone` is whether no other session is using the runtime at this address.
async fn prepare_runtime(
    ctx: &RuntimeContext,
    files: &SessionFiles,
    alone: bool,
    name: &'static str,
    command: &'static str,
) -> Result<()> {
    let recorded = read_state(&files.state)?;
    if is_healthy(ctx).await {
        if let Some(started) = &recorded
            && !process::is_runtime(started)
        {
            // The session that recorded it crashed, and what answers now is a
            // runtime nobody here started.
            remove_state(&files.state)?;
        }
        return check_served_spicepod(ctx, name, command).await;
    }

    if let Some(started) = recorded {
        if process::is_runtime(&started) {
            // Alive but not answering here: another spelling of this address
            // reaches it, and the sessions using it through that one need it.
            snafu::ensure!(
                alone,
                CodingAgentRuntimeElsewhereSnafu {
                    endpoint: ctx.http_endpoint().to_string(),
                    started_at: started.endpoint.clone(),
                }
            );
            // Recorded by a session that no longer exists, so nobody is
            // waiting on it any more.
            process::stop(started.pid, SHUTDOWN_BUDGET).await;
        }
        remove_state(&files.state)?;
    }

    // A runtime started without a Spicepod serves nothing, and every request
    // to it fails with `App not initialized`.
    snafu::ensure!(
        crate::manifest::existing_spicepod_path(ctx.app_dir()).is_some(),
        CodingAgentNoSpicepodSnafu {
            agent: name,
            command,
            dir: ctx.app_dir().clone(),
            endpoint: ctx.http_endpoint().to_string(),
        }
    );
    start_runtime(ctx, files).await
}

/// Which Spicepod the runtime already serving at an endpoint serves, against
/// the one in the current directory.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ServedSpicepod {
    /// The runtime does not say.
    Unknown,
    /// The Spicepod it serves: this directory's, or the only one there is.
    Named(String),
    /// Another Spicepod than the one in this directory.
    Other { served: String, local: String },
}

/// `served` names the Spicepods the runtime reports, and is empty when it
/// reports none.
fn compare_spicepods(local: Option<&str>, served: &[String]) -> ServedSpicepod {
    let Some(first) = served.first() else {
        return ServedSpicepod::Unknown;
    };
    match local {
        None => ServedSpicepod::Named(first.clone()),
        Some(local) if served.iter().any(|name| name == local) => {
            ServedSpicepod::Named(local.to_string())
        }
        Some(local) => ServedSpicepod::Other {
            served: first.clone(),
            local: local.to_string(),
        },
    }
}

/// Say which Spicepod the runtime serves, and refuse one serving another
/// project's Spicepod at the default endpoint.
async fn check_served_spicepod(
    ctx: &RuntimeContext,
    name: &'static str,
    command: &'static str,
) -> Result<()> {
    let endpoint = ctx.http_endpoint();
    let served = served_spicepods(ctx).await;
    let local = local_spicepod_name(ctx.app_dir());
    match compare_spicepods(local.as_deref(), &served) {
        ServedSpicepod::Unknown => {
            tracing::info!("Using the Spice runtime already serving at {endpoint}");
        }
        ServedSpicepod::Named(spicepod) => tracing::info!(
            "Using the Spice runtime already serving the Spicepod '{spicepod}' at {endpoint}"
        ),
        ServedSpicepod::Other { served, local } if ctx.http_endpoint_chosen() => tracing::info!(
            "Using the Spice runtime already serving the Spicepod '{served}' at {endpoint}, not the Spicepod '{local}' in this directory, since --http-endpoint names it"
        ),
        ServedSpicepod::Other { served, local } => {
            return CodingAgentOtherSpicepodSnafu {
                agent: name,
                command,
                endpoint: endpoint.to_string(),
                served,
                local,
            }
            .fail();
        }
    }
    Ok(())
}

/// The names of the Spicepods the runtime reports: none when it does not
/// answer with them.
async fn served_spicepods(ctx: &RuntimeContext) -> Vec<String> {
    let Ok(response) = ctx.get("/v1/spicepods").await else {
        return Vec::new();
    };
    if !response.status().is_success() {
        return Vec::new();
    }
    response
        .json::<Vec<crate::commands::pods::SpicepodStatus>>()
        .await
        .map(|spicepods| {
            spicepods
                .into_iter()
                .filter_map(|spicepod| spicepod.name)
                .collect()
        })
        .unwrap_or_default()
}

/// The name of the Spicepod in `dir`, when there is one that names itself.
fn local_spicepod_name(dir: &Path) -> Option<String> {
    let path = crate::manifest::existing_spicepod_path(dir)?;
    let spicepod = crate::manifest::read_spicepod_value(&path).ok()?;
    spicepod.get("name")?.as_str().map(str::to_string)
}

/// The address `spiced` listens for Flight on unless told otherwise.
const DEFAULT_FLIGHT_ADDRESS: &str = "127.0.0.1:50051";

/// The arguments that give a runtime started here a Flight address it can
/// bind: none while `default` is free, else a free port. A runtime that cannot
/// bind its Flight address does not start, and another runtime — such as the
/// one serving another project's Spicepod, beside which `--http-endpoint`
/// starts this one — holds the default. Coding agents use only the HTTP
/// endpoint.
fn flight_args(default: &str) -> Vec<String> {
    if std::net::TcpListener::bind(default).is_ok() {
        return Vec::new();
    }
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map_or_else(
            |_| Vec::new(),
            |address| vec!["--flight".to_string(), address.to_string()],
        )
}

async fn start_runtime(ctx: &RuntimeContext, files: &SessionFiles) -> Result<()> {
    ctx.ensure_local_runtime_supported()?;
    let resolved = interruptible(crate::runtime_launcher::resolve_or_install(ctx)).await?;
    let flight = flight_args(DEFAULT_FLIGHT_ADDRESS);
    if let [_, address] = flight.as_slice() {
        tracing::info!(
            "Another process listens on the Spice runtime's default Flight address {DEFAULT_FLIGHT_ADDRESS}, so the runtime started for this session listens for Flight on {address}"
        );
    }
    let mut command = ctx.get_run_cmd(&resolved, &flight, None)?;

    let log = File::create(&files.log).context(CodingAgentRuntimeLockSnafu {
        path: files.log.clone(),
    })?;
    let log_err = log.try_clone().context(CodingAgentRuntimeLockSnafu {
        path: files.log.clone(),
    })?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    // Its own process group, so the Ctrl-C an agent's user presses reaches the
    // agent rather than taking the runtime down under it.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        command.process_group(0);
    }

    tracing::info!(
        "Starting the Spice runtime at {} for the Spicepod in '{}' (log: {})",
        ctx.http_endpoint(),
        ctx.app_dir().display(),
        files.log.display()
    );
    let mut child = command.spawn().map_err(|source| {
        CodingAgentRuntimeStartSnafu {
            endpoint: ctx.http_endpoint().to_string(),
            log: files.log.clone(),
            detail: format!("the runtime could not be launched: {source}"),
        }
        .build()
    })?;
    let started = StartedRuntime {
        pid: child.id(),
        executable: resolved.path().to_path_buf(),
        endpoint: ctx.http_endpoint().to_string(),
    };
    write_state(&files.state, &started)?;

    let serving = tokio::select! {
        serving = wait_until_serving(ctx, &mut child) => serving,
        _ = tokio::signal::ctrl_c() => Serving::Interrupted,
    };
    match serving {
        Serving::Yes => {
            // The child is deliberately not waited on: it outlives this
            // session whenever another one is still using it.
            drop(child);
            Ok(())
        }
        Serving::Exited(status) => {
            remove_state(&files.state)?;
            Err(CodingAgentRuntimeStartSnafu {
                endpoint: ctx.http_endpoint().to_string(),
                log: files.log.clone(),
                detail: format!(
                    "the runtime exited ({status}) before it started serving. Its last output:\n{}",
                    log_tail(&files.log)
                ),
            }
            .build())
        }
        Serving::TimedOut => {
            process::stop(started.pid, SHUTDOWN_BUDGET).await;
            remove_state(&files.state)?;
            Err(CodingAgentRuntimeStartSnafu {
                endpoint: ctx.http_endpoint().to_string(),
                log: files.log.clone(),
                detail: format!(
                    "the runtime did not answer `/health` within {}s, so it was stopped. Its last output:\n{}",
                    HEALTH_BUDGET.as_secs(),
                    log_tail(&files.log)
                ),
            }
            .build())
        }
        Serving::Interrupted => {
            // No agent will use it, and no other session can have joined it:
            // they wait on the start lock this session holds.
            tracing::info!("Stopping the Spice runtime it was starting...");
            process::stop(started.pid, SHUTDOWN_BUDGET).await;
            remove_state(&files.state)?;
            InterruptedSnafu.fail()
        }
    }
}

/// How a runtime that was launched came out of starting.
enum Serving {
    /// It answers `/health`.
    Yes,
    /// It exited first.
    Exited(std::process::ExitStatus),
    /// It did not answer within [`HEALTH_BUDGET`].
    TimedOut,
    /// Ctrl-C came first.
    Interrupted,
}

/// Wait for the launched runtime to answer `/health`, to exit, or to run out of
/// [`HEALTH_BUDGET`].
async fn wait_until_serving(ctx: &RuntimeContext, child: &mut std::process::Child) -> Serving {
    let deadline = Instant::now() + HEALTH_BUDGET;
    loop {
        if is_healthy(ctx).await {
            return Serving::Yes;
        }
        if let Ok(Some(status)) = child.try_wait() {
            return Serving::Exited(status);
        }
        if Instant::now() >= deadline {
            return Serving::TimedOut;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Whether a runtime answers `/health` at the context's endpoint.
async fn is_healthy(ctx: &RuntimeContext) -> bool {
    let url = format!("{}/health", ctx.http_endpoint());
    let request = ctx.http_client().get(&url).timeout(Duration::from_secs(2));
    matches!(request.send().await, Ok(response) if response.status().is_success())
}

/// The end of the runtime's log, for an error that has to explain a failed start.
fn log_tail(path: &Path) -> String {
    let Ok(mut file) = File::open(path) else {
        return String::from("(the log could not be read)");
    };
    let len = file.metadata().map_or(0, |m| m.len());
    let _ = file.seek(SeekFrom::Start(len.saturating_sub(LOG_TAIL_BYTES)));
    let mut bytes = Vec::new();
    let _ = file.read_to_end(&mut bytes);
    let text = String::from_utf8_lossy(&bytes);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        String::from("(the runtime wrote nothing)")
    } else {
        trimmed.to_string()
    }
}

fn open_lock(path: &Path) -> Result<File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .context(CodingAgentRuntimeLockSnafu {
            path: path.to_path_buf(),
        })
}

/// Take `file` exclusively, saying so when another session is holding it.
async fn lock_exclusive_reporting_wait(file: &File, path: &Path, endpoint: &str) -> Result<()> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => {
            tracing::info!(
                "Waiting for another `spice` session to finish starting or stopping the Spice runtime at {endpoint}..."
            );
            lock_exclusive(file, path).await
        }
        Err(std::fs::TryLockError::Error(source)) => {
            Err(source).context(CodingAgentRuntimeLockSnafu {
                path: path.to_path_buf(),
            })
        }
    }
}

async fn lock_exclusive(file: &File, path: &Path) -> Result<()> {
    let handle = file.try_clone().context(CodingAgentRuntimeLockSnafu {
        path: path.to_path_buf(),
    })?;
    blocking_lock(move || handle.lock(), path).await
}

async fn lock_shared(file: &File, path: &Path) -> Result<()> {
    let handle = file.try_clone().context(CodingAgentRuntimeLockSnafu {
        path: path.to_path_buf(),
    })?;
    blocking_lock(move || handle.lock_shared(), path).await
}

/// Wait for a lock off the async runtime: another session may hold it for as
/// long as a runtime takes to start.
async fn blocking_lock<F>(lock: F, path: &Path) -> Result<()>
where
    F: FnOnce() -> std::io::Result<()> + Send + 'static,
{
    match tokio::task::spawn_blocking(lock).await {
        Ok(result) => result.context(CodingAgentRuntimeLockSnafu {
            path: path.to_path_buf(),
        }),
        Err(join) => Err(std::io::Error::other(join)).context(CodingAgentRuntimeLockSnafu {
            path: path.to_path_buf(),
        }),
    }
}

fn read_state(path: &Path) -> Result<Option<StartedRuntime>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(source).context(CodingAgentRuntimeStateSnafu {
            path: path.to_path_buf(),
        }),
    }
}

fn write_state(path: &Path, started: &StartedRuntime) -> Result<()> {
    let bytes = serde_json::to_vec(started)
        .map_err(std::io::Error::other)
        .context(CodingAgentRuntimeStateSnafu {
            path: path.to_path_buf(),
        })?;
    std::fs::write(path, bytes).context(CodingAgentRuntimeStateSnafu {
        path: path.to_path_buf(),
    })
}

fn remove_state(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(source).context(CodingAgentRuntimeStateSnafu {
            path: path.to_path_buf(),
        }),
    }
}

/// Identifying and stopping a recorded runtime process.
mod process {
    use std::time::Duration;
    #[cfg(unix)]
    use std::time::Instant;

    #[cfg(unix)]
    use super::POLL_INTERVAL;
    use super::StartedRuntime;

    /// Whether `started` still names a running runtime: the PID is alive and
    /// its executable is the one recorded, so a PID the system has since
    /// reused for something else is never signalled.
    #[cfg(unix)]
    pub(super) fn is_runtime(started: &StartedRuntime) -> bool {
        if !is_alive(started.pid) {
            return false;
        }
        let Ok(output) = std::process::Command::new("ps")
            .args(["-o", "command=", "-p", &started.pid.to_string()])
            .output()
        else {
            return false;
        };
        let command = String::from_utf8_lossy(&output.stdout);
        command_runs_executable(command.trim(), &started.executable)
    }

    #[cfg(not(unix))]
    pub(super) fn is_runtime(_started: &StartedRuntime) -> bool {
        false
    }

    /// Whether a `ps -o command=` line — the process's arguments, starting with
    /// how it was invoked — ran `executable`. The runtime is always started by
    /// its absolute path, so a process the system later gave the same PID would
    /// have to have been started from that exact file to be taken for it.
    #[cfg(any(unix, test))]
    pub(super) fn command_runs_executable(command: &str, executable: &std::path::Path) -> bool {
        let Some(path) = executable.to_str().filter(|path| !path.is_empty()) else {
            return false;
        };
        let invoked = |line: &str| line == path || line.starts_with(&format!("{path} "));
        // Directly, or as the script an interpreter was given first.
        invoked(command)
            || command
                .split_once(' ')
                .is_some_and(|(_, rest)| invoked(rest))
    }

    /// Whether `pid` is still running.
    ///
    /// A process that has exited but not been reaped is not running, yet still
    /// answers a signal check. When this process is its parent — the session
    /// that started the runtime — it is reaped here; otherwise `ps` reports it
    /// in state `Z` until its own parent reaps it.
    #[cfg(unix)]
    fn is_alive(pid: u32) -> bool {
        use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};

        let Ok(raw) = i32::try_from(pid) else {
            return false;
        };
        let target = nix::unistd::Pid::from_raw(raw);
        match waitpid(target, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => return true,
            // It exited, and is reaped now.
            Ok(_) => return false,
            // Not a child of this process.
            Err(_) => {}
        }
        if nix::sys::signal::kill(target, None).is_err() {
            return false;
        }
        let Ok(output) = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
        else {
            return true;
        };
        !String::from_utf8_lossy(&output.stdout)
            .trim_start()
            .starts_with('Z')
    }

    /// Ask the runtime to shut down, and kill it if it has not within `budget`.
    #[cfg(unix)]
    pub(super) async fn stop(pid: u32, budget: Duration) {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;

        let Ok(raw) = i32::try_from(pid) else {
            return;
        };
        let target = Pid::from_raw(raw);
        if kill(target, Signal::SIGTERM).is_err() {
            return;
        }
        let deadline = Instant::now() + budget;
        while is_alive(pid) {
            if Instant::now() >= deadline {
                tracing::warn!(
                    "The Spice runtime (PID {pid}) did not shut down within {}s, so it was killed.",
                    budget.as_secs()
                );
                let _ = kill(target, Signal::SIGKILL);
                return;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    #[cfg(not(unix))]
    pub(super) async fn stop(_pid: u32, _budget: Duration) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_runtime_is_compared_with_the_spicepod_in_this_directory() {
        let served = |names: &[&str]| names.iter().map(ToString::to_string).collect::<Vec<_>>();
        assert_eq!(
            compare_spicepods(Some("b"), &served(&[])),
            ServedSpicepod::Unknown
        );
        assert_eq!(
            compare_spicepods(Some("b"), &served(&["a", "b"])),
            ServedSpicepod::Named("b".to_string()),
            "this directory's Spicepod, served beside a dependency"
        );
        assert_eq!(
            compare_spicepods(None, &served(&["a"])),
            ServedSpicepod::Named("a".to_string())
        );
        assert_eq!(
            compare_spicepods(Some("b"), &served(&["a"])),
            ServedSpicepod::Other {
                served: "a".to_string(),
                local: "b".to_string()
            }
        );
    }

    /// A runtime started beside another listens for Flight on a port of its
    /// own, since one that cannot bind its Flight address does not start.
    #[test]
    fn a_runtime_started_beside_another_gets_a_flight_port_of_its_own() {
        let held = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
        let held_address = held.local_addr().expect("its address").to_string();
        let args = flight_args(&held_address);
        let [flag, address] = args.as_slice() else {
            panic!("expected `--flight <address>`, got {args:?}");
        };
        assert_eq!(flag, "--flight");
        assert_ne!(address, &held_address);
        assert!(
            std::net::TcpListener::bind(address.as_str()).is_ok(),
            "the address given is free: {address}"
        );
    }

    #[test]
    fn a_second_runtime_is_suggested_on_the_next_port() {
        use crate::error::next_port;
        assert_eq!(next_port("http://127.0.0.1:8090"), "http://127.0.0.1:8091");
        assert_eq!(next_port("http://localhost:9000/"), "http://localhost:9001");
        assert_eq!(next_port("not a url"), "http://127.0.0.1:8091");
    }

    #[test]
    fn the_refusals_name_the_fix() {
        let other = CodingAgentOtherSpicepodSnafu {
            agent: "Claude Code",
            command: "claude",
            endpoint: "http://127.0.0.1:8090",
            served: "proj-a",
            local: "proj-b",
        }
        .build();
        assert_eq!(
            other.to_string(),
            "Failed to start Claude Code: the Spice runtime at http://127.0.0.1:8090 serves the Spicepod 'proj-a', not 'proj-b' in this directory. Stop that runtime, or start one for this Spicepod on another port with `spice --http-endpoint http://127.0.0.1:8091 claude`. To use the running one anyway, pass `--http-endpoint http://127.0.0.1:8090`. See: https://spiceai.org/docs/cli"
        );
        let none = CodingAgentNoSpicepodSnafu {
            agent: "Codex CLI",
            command: "codex",
            dir: PathBuf::from("/src/app"),
            endpoint: "http://127.0.0.1:8090",
        }
        .build();
        assert_eq!(
            none.to_string(),
            "Failed to start the Spice runtime for Codex CLI: there is no spicepod.yaml in '/src/app', and no Spice runtime answers at http://127.0.0.1:8090. Run `spice codex` in your Spicepod's directory, start a runtime there with `spice run`, or create a Spicepod here with `spice init` and add a model with `spice model add my-model --from openai:gpt-4o-mini`. See: https://spiceai.org/docs/cli"
        );
    }

    #[test]
    fn every_spelling_of_one_address_gets_the_same_files() {
        let files =
            SessionFiles::for_endpoint(Path::new("/home/u/.spice/run"), "http://127.0.0.1:8090");
        assert_eq!(
            files.sessions_lock,
            PathBuf::from("/home/u/.spice/run/localhost_8090.sessions.lock")
        );
        assert_eq!(
            files.start_lock,
            PathBuf::from("/home/u/.spice/run/localhost_8090.start.lock")
        );
        assert_eq!(
            files.state,
            PathBuf::from("/home/u/.spice/run/localhost_8090.json")
        );
        assert_eq!(
            files.log,
            PathBuf::from("/home/u/.spice/run/localhost_8090.log")
        );
        for spelling in [
            "http://localhost:8090",
            "http://LocalHost:8090/",
            "http://[::1]:8090",
            "http://0.0.0.0:8090",
            "http://127.0.0.2:8090",
            "http://spice.localhost:8090",
            "https://localhost:8090",
        ] {
            assert_eq!(endpoint_key(spelling), "localhost_8090", "{spelling}");
        }
        // Another port, or another machine, is another runtime.
        assert_eq!(endpoint_key("http://localhost:9000"), "localhost_9000");
        assert_eq!(
            endpoint_key("http://Spice.Internal:8090"),
            "spice.internal_8090"
        );
        assert_eq!(endpoint_key("http://10.0.0.5:8090"), "10.0.0.5_8090");
        assert_eq!(endpoint_key("http://[fd00::1]:8090"), "_fd00__1__8090");
        // A port left out is the scheme's.
        assert_eq!(endpoint_key("http://localhost"), "localhost_80");
        // An endpoint that is not a URL is still a file name.
        assert_eq!(endpoint_key("not a url/"), "not_a_url");
    }

    #[test]
    fn a_reused_pid_is_not_taken_for_the_runtime() {
        let spiced = Path::new("/home/u/.spice/bin/spiced");
        assert!(process::command_runs_executable(
            "/home/u/.spice/bin/spiced --pods-watcher-enabled --http 127.0.0.1:8090",
            spiced
        ));
        // A runtime started through an interpreter, as a script with a shebang is.
        assert!(process::command_runs_executable(
            "/usr/bin/python3 /home/u/.spice/bin/spiced --http 127.0.0.1:8090",
            spiced
        ));
        // A different program, or the same name from another directory, is not it.
        assert!(!process::command_runs_executable(
            "/usr/bin/python3 -m http.server",
            spiced
        ));
        assert!(!process::command_runs_executable(
            "/opt/other/spiced --http 127.0.0.1:8090",
            spiced
        ));
        // Not the program's path when it appears only as a later argument.
        assert!(!process::command_runs_executable(
            "/usr/bin/vim notes --file /home/u/.spice/bin/spiced",
            spiced
        ));
        assert!(!process::command_runs_executable("", spiced));
        // A home directory with a space in it.
        assert!(process::command_runs_executable(
            "/Users/A B/.spice/bin/spiced --http 127.0.0.1:8090",
            Path::new("/Users/A B/.spice/bin/spiced")
        ));
    }

    #[test]
    fn recorded_runtimes_round_trip_and_garbage_reads_as_none() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("state.json");
        assert_eq!(read_state(&path).expect("absent state"), None);
        let started = StartedRuntime {
            pid: 42,
            executable: PathBuf::from("/bin/spiced"),
            endpoint: String::from("http://127.0.0.1:8090"),
        };
        write_state(&path, &started).expect("write state");
        assert_eq!(read_state(&path).expect("read state"), Some(started));
        std::fs::write(&path, b"not json").expect("overwrite state");
        assert_eq!(read_state(&path).expect("unreadable state"), None);
        remove_state(&path).expect("remove state");
        remove_state(&path).expect("removing absent state is not an error");
    }
}
