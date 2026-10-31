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

//! `spice orca`: Orca, with every coding agent it starts on the Spice runtime.
//!
//! [Orca](https://github.com/stablyai/orca) runs coding agents side by side in
//! git worktrees, starting each one through a per-agent command. `spice orca`
//! runs Orca on a profile of its own, under `~/.spice/orca`, in which each agent
//! `spice` supports runs as `spice <agent>`: every agent session Orca starts
//! there gets that command's model check, context window and credential
//! handling. The user's own Orca profile is never read or changed.
//!
//! Orca gives other programs no way to set an agent's command, and its
//! settings store has its own locking and migrations, so `spice` never writes
//! that store. A new profile instead starts as a legacy settings file holding
//! the commands, which Orca imports into its store on its first start. Every
//! run then reads the commands back once Orca answers, and warns about any
//! agent that does not run through Spice.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::Value;
use snafu::ResultExt;

use super::agent::{Agent, Launch, shell_quote};
use super::runtime::RuntimeLease;
use super::{CodingAgentArgs, find_program, is_executable, models, process};
use crate::context::RuntimeContext;
use crate::error::{
    CodingAgentCloudUnsupportedSnafu, CodingAgentFileSnafu, CodingAgentNotInstalledSnafu,
    InterruptedSnafu, InvalidUsageSnafu, Result,
};

/// The name the command is reported under.
const ORCA: &str = "Orca";

/// The variable Orca's `orca` command reads its profile's directory from.
const USER_DATA_ENV: &str = "ORCA_USER_DATA_PATH";

/// How long Orca has to answer before its agent commands go unchecked. The
/// desktop app answers once its window has loaded.
const ANSWER_BUDGET: Duration = Duration::from_mins(3);

/// How long one request to Orca may take.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// How long adding the project to Orca may take.
const PROJECT_TIMEOUT: Duration = Duration::from_mins(1);

const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Where Orca's `orca` command is inside its macOS app.
const MACOS_SCRIPT: &str = "Orca.app/Contents/Resources/bin/orca";

/// How long an Orca that has exited gets to start again — it restarts itself
/// to install an update — before the session counts it as quit.
const RESTART_GRACE: Duration = Duration::from_secs(5);

/// The one-line description of `spice orca`.
#[must_use]
pub fn about() -> String {
    "Run Orca with every coding agent it starts on the Spice runtime".to_string()
}

/// The full help text of `spice orca`, shown by `spice help orca`.
#[must_use]
pub fn long_about() -> String {
    let agents = Agent::ALL
        .iter()
        .map(|agent| agent.display_name())
        .collect::<Vec<_>>();
    format!(
        "Run Orca with every coding agent it starts on the Spice runtime.

Orca runs coding agents side by side in git worktrees. `spice orca` runs it on an Orca \
profile of its own, under ~/.spice/orca, in which {} each run as \
`spice <agent>`: every agent session Orca starts there uses the Spice runtime, on any \
model in your Spicepod. Your own Orca profile is not changed.

Every argument after `orca` is passed to Orca's own `orca` command unchanged, run \
against that profile. With none, `spice orca` runs `orca open`, which opens the Orca \
app. Options for spice itself, such as --http-endpoint or --api-key, go before `orca`.

spice uses the runtime already serving at the HTTP endpoint (http://127.0.0.1:8090, or \
--http-endpoint), or starts one for the Spicepod in the current directory, and keeps it \
while the Orca it opened runs. The current git repository is added to Orca's projects. \
A runtime that `spice orca` started is stopped once Orca has quit and no coding agent \
is using it.

Examples:
  spice orca                    # open Orca
  spice orca serve              # a headless Orca server
  spice orca --help             # Orca's own help
  spice --http-endpoint http://127.0.0.1:9000 orca

See: https://spiceai.org/docs/cli",
        and_list(&agents)
    )
}

/// `items` as an English list: `a, b and c`.
fn and_list(items: &[&str]) -> String {
    match items {
        [] => String::new(),
        [only] => (*only).to_string(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

/// Orca's id for `agent`, as its settings name it.
const fn orca_id(agent: Agent) -> &'static str {
    match agent {
        Agent::Antigravity => "antigravity",
        Agent::ClaudeCode => "claude",
        Agent::Codex => "codex",
        Agent::CopilotCli => "copilot",
        Agent::Cursor => "cursor",
        Agent::Grok => "grok",
        Agent::OpenCode => "opencode",
        Agent::Pi => "pi",
    }
}

/// Run Orca on the Spice profile, and return the exit code to report.
///
/// # Errors
///
/// Returns an error when Orca is not installed, the runtime cannot be started
/// or has no model, or the profile cannot be written. Orca's own failure is
/// not an error: its exit code is returned.
pub async fn execute(ctx: &RuntimeContext, args: &CodingAgentArgs) -> Result<i32> {
    snafu::ensure!(
        !ctx.is_cloud(),
        CodingAgentCloudUnsupportedSnafu { command: "orca" }
    );
    snafu::ensure!(
        cfg!(unix),
        InvalidUsageSnafu {
            message: "`spice orca` runs on macOS and Linux.".to_string()
        }
    );
    let home = dirs::home_dir();
    let path = std::env::var_os("PATH").unwrap_or_default();
    let Some(orca) = find_orca(&path, home.as_deref(), cfg!(target_os = "macos")) else {
        return not_installed();
    };
    let profile = Profile::new(ctx.spice_runtime_dir());

    let lease = RuntimeLease::acquire(ctx, ORCA, "orca").await?;
    let result = run(ctx, &orca, &profile, &args.args).await;
    lease.finish(ctx.http_endpoint()).await;
    result
}

fn not_installed() -> Result<i32> {
    let instructions = if cfg!(target_os = "macos") {
        "Install it with `brew install --cask stablyai/orca/orca`, or download it from https://onorca.dev/download."
    } else {
        "Download it from https://onorca.dev/download, and register its `orca-ide` command in Orca's Settings, under General."
    };
    CodingAgentNotInstalledSnafu {
        agent: ORCA,
        binary: if cfg!(target_os = "macos") {
            "orca"
        } else {
            "orca-ide"
        }
        .to_string(),
        instructions,
        command: "orca",
        docs: "https://www.onorca.dev/docs/install",
    }
    .fail()
}

async fn run(
    ctx: &RuntimeContext,
    orca: &Path,
    profile: &Profile,
    args: &[OsString],
) -> Result<i32> {
    // Every agent Orca starts waits on the runtime's models anyway; waiting
    // here reports a model that fails to load before Orca opens, not in the
    // terminal of the first agent.
    tokio::select! {
        ready = models::wait_until_ready(ctx) => { ready?; }
        _ = tokio::signal::ctrl_c() => return InterruptedSnafu.fail(),
    }
    profile.prepare(&spice_executable()?, ctx.http_endpoint(), ctx.api_key())?;

    let command = orca_command(args);
    let first = command.first().and_then(|arg| arg.to_str());
    let opens = first == Some("open");
    // The commands that start Orca: the app, or a headless server.
    let follow_up = (opens || first == Some("serve")).then(|| {
        tokio::spawn(follow_up(
            orca.to_path_buf(),
            profile.clone(),
            ctx.app_dir().clone(),
        ))
    });

    if follow_up.is_some() {
        tracing::info!(
            "Starting Orca with its coding agents on the Spice runtime at {}",
            ctx.http_endpoint()
        );
    }
    let launch = Launch {
        args: Vec::new(),
        env: vec![(USER_DATA_ENV, profile.user_data().into_os_string())],
        env_remove: Vec::new(),
        files: Vec::new(),
        prepare: None,
    };
    let code = process::run(ORCA, orca, &launch, &command)
        .await
        .map(process::exit_code);

    // `orca open` returns once the app's window is up; the session lasts as
    // long as the app does.
    if matches!(code, Ok(0)) && opens && profile.is_running().await {
        tracing::info!(
            "Orca is open, and its coding agents use the Spice runtime at {} while it runs. Quit Orca to end this session.",
            ctx.http_endpoint()
        );
        tokio::select! {
            () = profile.wait_until_quit() => {}
            () = session_ended() => tracing::info!(
                "Ended this session while Orca is still open. Its coding agents still use the Spice runtime, which stops once none is running; the next agent Orca starts then starts a runtime for the Spicepod in its own workspace."
            ),
        }
    }
    if let Some(task) = follow_up {
        task.abort();
    }
    code
}

/// The arguments for Orca's `orca` command: the user's, or `open`.
fn orca_command(args: &[OsString]) -> Vec<OsString> {
    if args.is_empty() {
        vec![OsString::from("open")]
    } else {
        args.to_vec()
    }
}

/// Resolves when the user ends the session: Ctrl-C, or a request to stop.
async fn session_ended() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let (Ok(mut terminate), Ok(mut hangup)) = (
            signal(SignalKind::terminate()),
            signal(SignalKind::hangup()),
        ) else {
            let _ = tokio::signal::ctrl_c().await;
            return;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
            _ = hangup.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Once Orca answers: warn about any agent that does not run through Spice,
/// and add the git repository containing `dir` to Orca's projects.
async fn follow_up(orca: PathBuf, profile: Profile, dir: PathBuf) {
    let Some(settings) = profile.wait_for_settings().await else {
        tracing::debug!(
            "Orca did not answer within {}s, so its agent commands were not checked",
            ANSWER_BUDGET.as_secs()
        );
        return;
    };
    for warning in command_warnings(&profile, &settings) {
        tracing::warn!("{warning}");
    }
    if let Some(project) = git_root(&dir).await {
        add_project(&orca, &profile, &project).await;
    }
}

/// What to tell the user about each agent Orca does not run through Spice,
/// given Orca's settings.
fn command_warnings(profile: &Profile, settings: &Value) -> Vec<String> {
    let commands = settings.get("agentCmdOverrides").and_then(Value::as_object);
    let wrong: Vec<(Agent, Option<&str>)> = Agent::ALL
        .iter()
        .filter_map(|&agent| {
            let actual = commands
                .and_then(|commands| commands.get(orca_id(agent)))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|command| !command.is_empty());
            (actual != Some(profile.agent_command(agent).as_str())).then_some((agent, actual))
        })
        .collect();
    if wrong.len() == Agent::ALL.len() {
        return vec![format!(
            "Orca does not run its coding agents through Spice on the profile at '{}', so none of its agent sessions use the Spice runtime. Set each agent's command in Orca's Settings, under Agents, to the Spice launcher and the agent's `spice` command, as in `{}` for Claude Code. See: https://spiceai.org/docs/cli",
            profile.user_data().display(),
            profile.agent_command(Agent::ClaudeCode)
        )];
    }
    wrong
        .into_iter()
        .map(|(agent, actual)| {
            let name = agent.display_name();
            let runs = actual.map_or_else(
                || "its default command".to_string(),
                |command| format!("the command '{command}'"),
            );
            format!(
                "Orca runs {name} with {runs}, so {name} sessions in Orca do not use Spice. Set {name}'s command in Orca's Settings, under Agents, to `{}`. See: https://spiceai.org/docs/cli",
                profile.agent_command(agent)
            )
        })
        .collect()
}

/// The top of the git repository containing `dir`, if it is in one.
async fn git_root(dir: &Path) -> Option<PathBuf> {
    let output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "--show-toplevel"])
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .await
        .ok()?;
    let root = String::from_utf8(output.stdout).ok()?;
    let root = root.trim_end_matches(['\r', '\n']);
    (output.status.success() && !root.is_empty()).then(|| PathBuf::from(root))
}

/// Add `project` to Orca's projects, which Orca does nothing for when it is
/// listed already.
async fn add_project(orca: &Path, profile: &Profile, project: &Path) {
    let run = tokio::process::Command::new(orca)
        .args(["repo", "add", "--path"])
        .arg(project)
        .arg("--json")
        .env(USER_DATA_ENV, profile.user_data())
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output();
    let failure = match tokio::time::timeout(PROJECT_TIMEOUT, run).await {
        Ok(Ok(output)) if output.status.success() => {
            tracing::debug!("Added '{}' to Orca's projects", project.display());
            return;
        }
        Ok(Ok(output)) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let detail = [stderr.trim(), stdout.trim()]
                .into_iter()
                .find(|text| !text.is_empty())
                .unwrap_or("no output")
                .replace('\n', " ");
            format!("`orca repo add` exited with {}: {detail}", output.status)
        }
        Ok(Err(error)) => error.to_string(),
        Err(_) => format!(
            "`orca repo add` did not finish within {}s",
            PROJECT_TIMEOUT.as_secs()
        ),
    };
    tracing::warn!(
        "Failed to add '{}' to Orca's projects, so Orca does not list it; add it in Orca to work on it there. Cause: {failure}",
        project.display()
    );
}

/// Find Orca's `orca` command.
///
/// On macOS it is a script inside the app, `Orca.app/Contents/Resources/bin/orca`,
/// which is on `PATH` once registered from Orca's settings. An `orca` on `PATH`
/// that resolves anywhere else is another program — Plotly's image exporter
/// has the name too — so it is passed over for the app's own copy. On Linux
/// the command is `orca-ide`: the GNOME screen reader owns `orca`.
fn find_orca(path: &OsStr, home: Option<&Path>, macos: bool) -> Option<PathBuf> {
    if !macos {
        return find_program(path, &["orca-ide"], &[".local/bin/orca-ide"], home);
    }
    let on_path = std::env::split_paths(path)
        .map(|dir| dir.join("orca"))
        .find(|candidate| {
            is_executable(candidate)
                && std::fs::canonicalize(candidate).is_ok_and(|real| real.ends_with(MACOS_SCRIPT))
        });
    on_path.or_else(|| {
        let applications = [
            Some(PathBuf::from("/Applications")),
            home.map(|home| home.join("Applications")),
        ];
        applications
            .into_iter()
            .flatten()
            .map(|dir| dir.join(MACOS_SCRIPT))
            .find(|candidate| is_executable(candidate))
    })
}

/// This `spice`, by a path that keeps working across upgrades: its entry on
/// `PATH` when that is the same file, since a package manager's real path
/// often names the version.
fn spice_executable() -> Result<PathBuf> {
    let current = std::env::current_exe()
        .map_err(|source| crate::error::Error::RuntimeExecution { source })?;
    let real = std::fs::canonicalize(&current).unwrap_or_else(|_| current.clone());
    let path = std::env::var_os("PATH").unwrap_or_default();
    let on_path = std::env::split_paths(&path)
        .map(|dir| dir.join("spice"))
        .find(|candidate| std::fs::canonicalize(candidate).is_ok_and(|other| other == real));
    Ok(on_path.unwrap_or(current))
}

/// The Orca profile `spice orca` owns, and the launcher Orca runs its agents
/// through.
#[derive(Debug, Clone)]
struct Profile {
    root: PathBuf,
}

/// The legacy settings file a new profile starts as. Orca reads only the keys
/// it finds and fills in the rest.
#[derive(Serialize)]
struct Seed {
    settings: SeedSettings,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SeedSettings {
    /// Sorted, so the file reads the same however serde orders maps.
    agent_cmd_overrides: BTreeMap<&'static str, String>,
}

impl Profile {
    /// The profile under the `.spice` directory `spice_dir`.
    fn new(spice_dir: &Path) -> Self {
        Self {
            root: spice_dir.join("orca"),
        }
    }

    /// Orca's directory for the profile, which `orca` reads from
    /// `ORCA_USER_DATA_PATH`.
    fn user_data(&self) -> PathBuf {
        self.root.join("profile")
    }

    fn launcher(&self) -> PathBuf {
        self.root.join("bin").join("spice-agent")
    }

    fn runtime_metadata(&self) -> PathBuf {
        self.user_data().join("orca-runtime.json")
    }

    /// The command Orca runs `agent` with: the launcher, then the agent's
    /// `spice` command, to which Orca appends its own arguments. It is two
    /// words on purpose: Orca runs a one-word command through its chat
    /// protocols too, which a `spice` session does not serve.
    fn agent_command(&self, agent: Agent) -> String {
        format!(
            "{} {}",
            shell_quote(&self.launcher().to_string_lossy()),
            agent.command()
        )
    }

    /// Whether Orca has never run on the profile, nor has it been seeded.
    fn is_new(&self) -> bool {
        let user_data = self.user_data();
        !["orca-data.json", "orca-profile-index.json", "profiles"]
            .iter()
            .any(|name| user_data.join(name).exists())
    }

    fn seed(&self) -> Result<String> {
        let seed = Seed {
            settings: SeedSettings {
                agent_cmd_overrides: Agent::ALL
                    .iter()
                    .map(|&agent| (orca_id(agent), self.agent_command(agent)))
                    .collect(),
            },
        };
        serde_json::to_string(&seed)
            .map_err(std::io::Error::other)
            .context(CodingAgentFileSnafu {
                path: self.user_data().join("orca-data.json"),
            })
    }

    /// Rewrite the launcher for this session's runtime, and create the profile
    /// on the first run.
    ///
    /// A profile Orca has opened is never seeded again: Orca keeps the file it
    /// imported, and refuses to start when that file has changed since.
    fn prepare(&self, spice: &Path, endpoint: &str, api_key: Option<&str>) -> Result<()> {
        let launcher = self.launcher();
        for path in [&self.root, &self.user_data(), &launcher] {
            utf8(path)?;
        }
        let spice = utf8(spice)?;
        create_private_dir(&self.root)?;
        create_private_dir(launcher.parent().unwrap_or(&self.root))?;
        write_atomically(&launcher, &launcher_script(spice, endpoint, api_key), 0o700)?;
        if self.is_new() {
            create_private_dir(&self.user_data())?;
            let seed = self.user_data().join("orca-data.json");
            write_new(&seed, &self.seed()?, 0o600)?;
            tracing::info!(
                "Created an Orca profile for Spice at '{}', with every coding agent running through Spice",
                self.root.display()
            );
        }
        Ok(())
    }

    /// Whether an Orca runs on this profile: the PID its runtime recorded is
    /// still an Orca started on this profile.
    async fn is_running(&self) -> bool {
        // A file of a few hundred bytes, which Orca rewrites only as it starts.
        let pid = std::fs::read(self.runtime_metadata())
            .ok()
            .and_then(|metadata| serde_json::from_slice::<Value>(&metadata).ok())
            .and_then(|metadata| u32::try_from(metadata.get("pid")?.as_u64()?).ok());
        match pid {
            Some(pid) => is_orca_on(pid, &self.user_data()).await,
            None => false,
        }
    }

    /// Wait until no Orca runs on this profile.
    async fn wait_until_quit(&self) {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            if !self.is_running().await {
                tokio::time::sleep(RESTART_GRACE).await;
                if !self.is_running().await {
                    return;
                }
            }
        }
    }

    /// Orca's settings, once the Orca on this profile answers.
    async fn wait_for_settings(&self) -> Option<Value> {
        let deadline = Instant::now() + ANSWER_BUDGET;
        while Instant::now() < deadline {
            let metadata = self.runtime_metadata();
            let answer = tokio::task::spawn_blocking(move || read_settings(&metadata)).await;
            if let Ok(Some(settings)) = answer {
                return Some(settings);
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        None
    }
}

/// Whether `pid` is an Orca process started on the profile at `user_data`:
/// Orca starts its app and its server with `--user-data-dir=<directory>`, so
/// a PID the system has since given to another program does not match.
async fn is_orca_on(pid: u32, user_data: &Path) -> bool {
    #[cfg(unix)]
    {
        let Ok(raw) = i32::try_from(pid) else {
            return false;
        };
        if nix::sys::signal::kill(nix::unistd::Pid::from_raw(raw), None).is_err() {
            return false;
        }
    }
    // `-ww`: the whole command line, however long, since the profile is last.
    let Ok(output) = tokio::process::Command::new("ps")
        .args(["-ww", "-o", "command=", "-p", &pid.to_string()])
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .await
    else {
        return false;
    };
    String::from_utf8_lossy(&output.stdout)
        .contains(&format!("--user-data-dir={}", user_data.display()))
}

/// Ask the Orca whose runtime `metadata` describes for its settings, giving
/// it [`REQUEST_TIMEOUT`] to answer.
///
/// Orca's runtime listens on a Unix socket for one JSON request per line,
/// carrying the token it recorded beside the socket, and answers with JSON
/// lines, interleaved with keepalives, which do not extend the time it has.
fn read_settings(metadata: &Path) -> Option<Value> {
    use std::io::BufRead as _;

    let metadata: Value = serde_json::from_slice(&std::fs::read(metadata).ok()?).ok()?;
    let socket = metadata
        .get("transports")?
        .as_array()?
        .iter()
        .find(|transport| transport.get("kind").and_then(Value::as_str) == Some("unix"))?
        .get("endpoint")?
        .as_str()?;
    let token = metadata.get("authToken")?.as_str()?;

    #[cfg(unix)]
    {
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let mut stream = std::os::unix::net::UnixStream::connect(socket).ok()?;
        stream.set_read_timeout(Some(REQUEST_TIMEOUT)).ok()?;
        stream.set_write_timeout(Some(REQUEST_TIMEOUT)).ok()?;
        let request = serde_json::json!({
            "id": "spice-orca-settings",
            "authToken": token,
            "method": "settings.get",
            "params": {},
        });
        writeln!(stream, "{request}").ok()?;
        for line in std::io::BufReader::new(stream).lines() {
            let frame: Value = serde_json::from_str(&line.ok()?).ok()?;
            if frame.get("_keepalive").is_some() {
                if Instant::now() >= deadline {
                    return None;
                }
                continue;
            }
            if frame.get("ok").and_then(Value::as_bool) != Some(true) {
                return None;
            }
            return frame.get("result")?.get("settings").cloned();
        }
        None
    }
    #[cfg(not(unix))]
    {
        let _ = (socket, token);
        None
    }
}

/// The launcher Orca runs each agent through: this `spice`, against this
/// session's runtime, with the agent's `spice` command and Orca's arguments
/// after it.
fn launcher_script(spice: &str, endpoint: &str, api_key: Option<&str>) -> String {
    let mut script = String::from(
        "#!/bin/sh\n# Written by `spice orca`, which rewrites it on every run. Orca runs each coding\n# agent as `spice-agent <agent> [ARGS]`, which starts it on the Spice runtime.\n",
    );
    if let Some(api_key) = api_key {
        // In the environment rather than an argument, which other users can list.
        let _ = write!(
            script,
            "SPICE_API_KEY={}\nexport SPICE_API_KEY\n",
            shell_quote(api_key)
        );
    }
    let _ = writeln!(
        script,
        "exec {} --http-endpoint {} \"$@\"",
        shell_quote(spice),
        shell_quote(endpoint)
    );
    script
}

fn utf8(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the path is not valid UTF-8, so Orca cannot be given it as a command",
            )
        })
        .context(CodingAgentFileSnafu {
            path: path.to_path_buf(),
        })
}

fn create_private_dir(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).context(CodingAgentFileSnafu {
        path: dir.to_path_buf(),
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).context(
            CodingAgentFileSnafu {
                path: dir.to_path_buf(),
            },
        )?;
    }
    Ok(())
}

/// Replace `path` with `contents` in one step, since Orca may be running the
/// file while it is rewritten. The staged copy has `mode` from the start, so
/// what it holds is never readable by others even for a moment.
fn write_atomically(path: &Path, contents: &str, mode: u32) -> Result<()> {
    let error = |source: std::io::Error| crate::error::Error::CodingAgentFile {
        path: path.to_path_buf(),
        source,
    };
    let mut builder = tempfile::Builder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        builder.permissions(std::fs::Permissions::from_mode(mode));
    }
    #[cfg(not(unix))]
    let _ = mode;
    let mut staged = builder
        .tempfile_in(path.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(error)?;
    staged.write_all(contents.as_bytes()).map_err(error)?;
    staged.as_file().sync_all().map_err(error)?;
    staged
        .persist(path)
        .map_err(|persist| error(persist.error))?;
    Ok(())
}

/// Write `contents` to `path`, which must not exist yet, with `mode` from the
/// start.
fn write_new(path: &Path, contents: &str, mode: u32) -> Result<()> {
    let write = || -> std::io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(mode);
        }
        #[cfg(not(unix))]
        let _ = mode;
        let mut file = options.open(path)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()
    };
    write().context(CodingAgentFileSnafu {
        path: path.to_path_buf(),
    })
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    fn executable(path: &Path) {
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("create the directory");
        std::fs::write(path, "#!/bin/sh\n").expect("write the executable");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
            .expect("make it executable");
    }

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path)
            .expect("stat the file")
            .permissions()
            .mode()
            & 0o777
    }

    #[test]
    fn a_new_profile_runs_every_agent_through_the_launcher() {
        let profile = Profile::new(Path::new("/home/u/.spice"));
        assert_eq!(
            profile.seed().expect("serialize the seed"),
            concat!(
                r#"{"settings":{"agentCmdOverrides":{"#,
                r#""antigravity":"'/home/u/.spice/orca/bin/spice-agent' agy","#,
                r#""claude":"'/home/u/.spice/orca/bin/spice-agent' claude","#,
                r#""codex":"'/home/u/.spice/orca/bin/spice-agent' codex","#,
                r#""copilot":"'/home/u/.spice/orca/bin/spice-agent' copilot","#,
                r#""cursor":"'/home/u/.spice/orca/bin/spice-agent' cursor","#,
                r#""grok":"'/home/u/.spice/orca/bin/spice-agent' grok","#,
                r#""opencode":"'/home/u/.spice/orca/bin/spice-agent' opencode","#,
                r#""pi":"'/home/u/.spice/orca/bin/spice-agent' pi"}}}"#,
            )
        );
        assert_eq!(
            profile.user_data(),
            PathBuf::from("/home/u/.spice/orca/profile")
        );
    }

    #[test]
    fn the_launcher_runs_this_spice_against_the_sessions_runtime() {
        assert_eq!(
            launcher_script("/opt/spice/bin/spice", "http://127.0.0.1:8090", None),
            "#!/bin/sh\n# Written by `spice orca`, which rewrites it on every run. Orca runs each coding\n# agent as `spice-agent <agent> [ARGS]`, which starts it on the Spice runtime.\nexec '/opt/spice/bin/spice' --http-endpoint 'http://127.0.0.1:8090' \"$@\"\n"
        );
        assert_eq!(
            launcher_script("/opt/spice", "http://127.0.0.1:9000", Some("k'ey")),
            "#!/bin/sh\n# Written by `spice orca`, which rewrites it on every run. Orca runs each coding\n# agent as `spice-agent <agent> [ARGS]`, which starts it on the Spice runtime.\nSPICE_API_KEY='k'\\''ey'\nexport SPICE_API_KEY\nexec '/opt/spice' --http-endpoint 'http://127.0.0.1:9000' \"$@\"\n"
        );
    }

    /// Quoting is checked by the shell that will read it, not by a copy of the
    /// rule: each value comes back from `sh` as one, unchanged word.
    #[test]
    fn a_quoted_value_reaches_the_shell_as_one_unchanged_word() {
        for value in ["/Users/A B/.spice", "it's", "'", "$HOME `id` \"x\" \\n", ""] {
            let output = std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(format!("printf '%s|' {}", shell_quote(value)))
                .output()
                .expect("run sh");
            assert_eq!(
                String::from_utf8(output.stdout).expect("utf-8"),
                format!("{value}|"),
                "{value:?}"
            );
        }
    }

    #[test]
    fn orca_opens_its_app_unless_told_otherwise() {
        assert_eq!(orca_command(&[]), vec![OsString::from("open")]);
        let args = vec![OsString::from("serve"), OsString::from("--port=6768")];
        assert_eq!(orca_command(&args), args);
    }

    #[test]
    fn only_an_agent_whose_command_is_not_spices_is_warned_about() {
        let profile = Profile::new(Path::new("/home/u/.spice"));
        let mut commands: serde_json::Map<String, Value> = Agent::ALL
            .iter()
            .map(|&agent| {
                (
                    orca_id(agent).to_string(),
                    Value::String(profile.agent_command(agent)),
                )
            })
            .collect();
        let settings = |commands: &serde_json::Map<String, Value>| serde_json::json!({ "agentCmdOverrides": commands });
        assert_eq!(
            command_warnings(&profile, &settings(&commands)),
            Vec::<String>::new()
        );

        commands.insert("codex".to_string(), Value::String("codex".to_string()));
        commands.remove("pi");
        assert_eq!(
            command_warnings(&profile, &settings(&commands)),
            vec![
                "Orca runs Codex CLI with the command 'codex', so Codex CLI sessions in Orca do not use Spice. Set Codex CLI's command in Orca's Settings, under Agents, to `'/home/u/.spice/orca/bin/spice-agent' codex`. See: https://spiceai.org/docs/cli".to_string(),
                "Orca runs pi with its default command, so pi sessions in Orca do not use Spice. Set pi's command in Orca's Settings, under Agents, to `'/home/u/.spice/orca/bin/spice-agent' pi`. See: https://spiceai.org/docs/cli".to_string(),
            ]
        );

        assert_eq!(
            command_warnings(&profile, &serde_json::json!({})),
            vec!["Orca does not run its coding agents through Spice on the profile at '/home/u/.spice/orca/profile', so none of its agent sessions use the Spice runtime. Set each agent's command in Orca's Settings, under Agents, to the Spice launcher and the agent's `spice` command, as in `'/home/u/.spice/orca/bin/spice-agent' claude` for Claude Code. See: https://spiceai.org/docs/cli".to_string()]
        );
    }

    #[test]
    fn on_macos_only_the_apps_own_orca_is_used() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let app_script = dir
            .path()
            .join("Applications/Orca.app/Contents/Resources/bin/orca");
        executable(&app_script);
        // Another program named `orca` on PATH, and the app's script linked
        // onto PATH under the same name.
        let other = dir.path().join("other/orca");
        executable(&other);
        let linked = dir.path().join("linked/orca");
        std::fs::create_dir_all(linked.parent().expect("parent")).expect("create the directory");
        std::os::unix::fs::symlink(&app_script, &linked).expect("link the app's script");

        let path = std::env::join_paths([dir.path().join("other"), dir.path().join("linked")])
            .expect("join PATH");
        assert_eq!(find_orca(&path, None, true), Some(linked));

        // With only the other program on PATH, the app is found where it is
        // installed.
        let path = std::env::join_paths([dir.path().join("other")]).expect("join PATH");
        assert_eq!(find_orca(&path, Some(dir.path()), true), Some(app_script));
    }

    #[test]
    fn on_linux_orca_is_orca_ide_never_the_screen_reader() {
        let dir = tempfile::tempdir().expect("create tempdir");
        executable(&dir.path().join("bin/orca"));
        let path = std::env::join_paths([dir.path().join("bin")]).expect("join PATH");
        assert_eq!(find_orca(&path, Some(dir.path()), false), None);

        let registered = dir.path().join(".local/bin/orca-ide");
        executable(&registered);
        assert_eq!(find_orca(&path, Some(dir.path()), false), Some(registered));
    }

    #[test]
    fn the_first_run_seeds_the_profile_and_every_run_rewrites_the_launcher() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let profile = Profile::new(dir.path());
        profile
            .prepare(
                Path::new("/opt/spice"),
                "http://127.0.0.1:8090",
                Some("key"),
            )
            .expect("prepare the profile");
        let seed = profile.user_data().join("orca-data.json");
        assert_eq!(
            std::fs::read_to_string(&seed).expect("the seed was written"),
            profile.seed().expect("serialize the seed")
        );
        assert_eq!(mode(&seed), 0o600);
        assert_eq!(mode(&profile.launcher()), 0o700);
        assert_eq!(mode(&profile.root), 0o700);
        assert_eq!(mode(&profile.user_data()), 0o700);
        assert!(
            std::fs::read_to_string(profile.launcher())
                .expect("the launcher was written")
                .contains("SPICE_API_KEY='key'")
        );

        // Orca has imported the seed: it is never rewritten, and the launcher
        // follows the next session's runtime and key.
        std::fs::write(&seed, "imported").expect("stand in for the imported file");
        profile
            .prepare(Path::new("/opt/spice"), "http://127.0.0.1:9000", None)
            .expect("prepare the profile again");
        assert_eq!(
            std::fs::read_to_string(&seed).expect("read the seed"),
            "imported"
        );
        assert_eq!(
            std::fs::read_to_string(profile.launcher()).expect("read the launcher"),
            launcher_script("/opt/spice", "http://127.0.0.1:9000", None)
        );
    }

    #[test]
    fn a_profile_orca_created_itself_is_not_seeded() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let profile = Profile::new(dir.path());
        std::fs::create_dir_all(profile.user_data().join("profiles/local-default"))
            .expect("stand in for a profile Orca created");
        profile
            .prepare(Path::new("/opt/spice"), "http://127.0.0.1:8090", None)
            .expect("prepare the profile");
        assert!(!profile.user_data().join("orca-data.json").exists());
    }

    #[test]
    fn every_agent_has_an_orca_id_orca_knows() {
        // Orca's ids for these agents, as its `TuiAgent` type spells them.
        let known = [
            "claude",
            "codex",
            "opencode",
            "pi",
            "antigravity",
            "cursor",
            "copilot",
            "grok",
        ];
        for agent in Agent::ALL {
            assert!(known.contains(&orca_id(agent)), "{agent:?}");
        }
    }

    /// An Orca that keeps the connection alive without ever answering is given up on
    /// within the time asked for, rather than holding the thread that asks.
    #[test]
    fn a_runtime_that_only_keeps_alive_is_given_up_on() {
        use std::os::unix::net::UnixListener;

        let dir = tempfile::Builder::new()
            .prefix("orca")
            .tempdir_in("/tmp")
            .expect("a temporary directory");
        let socket = dir.path().join("rt.sock");
        let listener = UnixListener::bind(&socket).expect("a socket");
        std::thread::spawn(move || {
            let Ok((mut connection, _)) = listener.accept() else {
                return;
            };
            while writeln!(connection, r#"{{"_keepalive":true}}"#).is_ok() {
                std::thread::sleep(Duration::from_millis(50));
            }
        });
        let metadata = dir.path().join("orca-runtime.json");
        std::fs::write(
            &metadata,
            serde_json::json!({
                "pid": 1,
                "transports": [{"kind": "unix", "endpoint": socket}],
                "authToken": "t0k",
            })
            .to_string(),
        )
        .expect("write the runtime metadata");

        let (sent, answered) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = sent.send(read_settings(&metadata));
        });
        let settings = answered
            .recv_timeout(Duration::from_secs(10))
            .expect("no answer within 10s: the keepalives kept it reading");
        assert_eq!(settings, None);
    }
}
