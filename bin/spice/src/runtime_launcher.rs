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

//! Starting the runtime in the foreground, from any command that needs to.
//!
//! `spice run` installs the runtime when it is missing, resolves endpoint
//! flags, inherits the terminal so the runtime's own output is the command's
//! output, forwards shutdown signals, and exits with the runtime's status.
//!
//! One launcher is what keeps those identical. Re-invoking the `spice run`
//! subcommand from another command would add a third process to every Ctrl-C
//! and put the CLI's argument parsing between the caller and the runtime.

use std::path::PathBuf;
use std::process::Stdio;

use snafu::{OptionExt, ResultExt, ensure};

use crate::context::{ResolvedSpiced, RuntimeContext};
use crate::error::RuntimeNotInstalledSnafu;
use crate::error::{
    ChildProcessIdSnafu, InvalidArgumentSnafu, Result, RuntimeExecutionSnafu, SignalHandlerSnafu,
};

/// Who tells the operator that this instance is connected to Spice Cloud.
///
/// The runtime is the default reporter, and the only one for a direct `spiced`
/// or `spice run` start: it is the process that knows when the instance is
/// actually serving and Spice Cloud has answered it. A caller that has just
/// completed a connect transaction has already printed that block, so it says
/// so here rather than letting the same two lines arrive twice.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionReport {
    /// The runtime reports it, once both facts are true.
    #[default]
    Runtime,
    /// The caller already reported it; the runtime stays quiet.
    AlreadyReported,
}

/// The environment variable that carries [`ConnectionReport::AlreadyReported`]
/// to the runtime.
///
/// A private handshake between this launcher and the runtime it starts, not a
/// configuration surface: it is never documented for operators, never read from
/// a spicepod, and absent for every start the CLI does not parent.
const CONNECTION_REPORTED_ENV: &str = "SPICE_CONNECTION_REPORTED";
const SPICE_CONFIG_DIR_ENV: &str = "SPICE_CONFIG_DIR";

/// How the runtime should be started.
#[derive(Debug, Default, Clone)]
pub struct RunConfig {
    /// `--endpoint`: routed to the HTTP or Flight endpoint by its scheme.
    pub endpoint: Option<String>,
    /// `--http-endpoint`, the address the runtime binds its HTTP API on.
    pub http_endpoint: Option<String>,
    /// `--flight-endpoint`, the address the runtime binds Flight on.
    pub flight_endpoint: Option<String>,
    /// `--metrics-endpoint`, the address the runtime serves Prometheus on.
    pub metrics_endpoint: Option<String>,
    /// `-v` count, forwarded to the runtime as its own verbosity flag.
    pub verbosity: u8,
    /// Arguments passed through to `spiced` verbatim.
    pub args: Vec<String>,
    /// Who reports the Cloud connection once the runtime is serving.
    pub connection_report: ConnectionReport,
    /// The directory the runtime runs in. It resolves the spicepod *and* the
    /// per-instance `.spice` state, so a caller acting on another instance
    /// directory has to set it rather than assume the CLI was invoked there.
    /// `None` inherits this process's working directory.
    pub working_dir: Option<PathBuf>,
}

/// Start the runtime in the foreground and stay attached to it until it exits.
///
/// Installs the runtime first if this host has none. Returns only when the
/// runtime has exited: a non-zero exit is propagated as this process's own
/// status, so a caller in a script sees what the runtime reported.
///
/// # Errors
///
/// Returns an error when the runtime cannot be installed, when the endpoint
/// flags conflict, or when the child process cannot be started or waited on.
pub async fn run_runtime(ctx: &RuntimeContext, config: &RunConfig) -> Result<()> {
    let status = start_runtime_process(ctx, config).await?;

    if !status.success() {
        // The runtime's status is this command's status: a caller in a script
        // sees what the runtime reported, not that the CLI managed to run it.
        #[cfg(unix)]
        let code = {
            use std::os::unix::process::ExitStatusExt as _;
            status
                .code()
                .or_else(|| status.signal().map(|signal| 128 + signal))
                .unwrap_or(1)
        };
        #[cfg(not(unix))]
        let code = status.code().unwrap_or(1);
        std::process::exit(code);
    }

    Ok(())
}

/// Name the runtime about to be launched, and where it came from.
///
/// Announced at `info` whenever it is anything other than the binary
/// `spice install` manages, because that is the case a user cannot otherwise
/// see: a CLI and a runtime from different builds start without a word, and the
/// symptoms surface much later as unexplained behaviour. The expected default
/// stays at `debug` rather than adding a line to every ordinary `spice run`.
fn report_resolved_runtime(resolved: &ResolvedSpiced) {
    let path = resolved.path().display();
    let source = resolved.source.describe();
    if resolved.source.is_expected_default() {
        tracing::debug!("Using the Spice.ai runtime at '{path}' ({source}).");
    } else {
        tracing::info!("Using the Spice.ai runtime at '{path}' ({source}).");
    }
}

async fn start_runtime_process(
    ctx: &RuntimeContext,
    config: &RunConfig,
) -> Result<std::process::ExitStatus> {
    ctx.ensure_local_runtime_supported()?;

    let resolved = resolve_or_install(ctx).await?;
    report_resolved_runtime(&resolved);

    launch_resolved_runtime(ctx, config, &resolved).await
}

/// The runtime to start, installing one when this host has none.
///
/// Resolution comes before the install, so a `SPICED_PATH` that names nothing
/// is reported rather than answered with a download of the latest release.
async fn resolve_or_install(ctx: &RuntimeContext) -> Result<ResolvedSpiced> {
    if let Some(resolved) = ctx.resolve_spiced()? {
        return Ok(resolved);
    }

    tracing::info!("Spice.ai runtime is not installed. Installing now...");
    crate::commands::install::execute(ctx, &crate::commands::install::InstallArgs::default())
        .await?;
    ctx.resolve_spiced()?.context(RuntimeNotInstalledSnafu)
}

/// Start `resolved` in the foreground and stay attached to it until it exits.
///
/// Returns the status the runtime exited with rather than adopting it, which is
/// what lets a test assert the status at all — [`run_runtime`] adopting it ends
/// the process.
async fn launch_resolved_runtime(
    ctx: &RuntimeContext,
    config: &RunConfig,
    resolved: &ResolvedSpiced,
) -> Result<std::process::ExitStatus> {
    // Route --endpoint to the appropriate endpoint based on scheme
    let (http_endpoint, flight_endpoint) = resolve_endpoint(
        config.endpoint.as_deref(),
        config.http_endpoint.as_deref(),
        config.flight_endpoint.as_deref(),
    )?;

    tracing::info!("Spice.ai runtime starting...");

    let spiced_args = spiced_args(config, flight_endpoint.as_deref());
    let std_cmd = ctx.get_run_cmd(resolved, &spiced_args, http_endpoint.as_deref())?;

    // Convert std::process::Command to tokio::process::Command
    let mut cmd = tokio::process::Command::from(std_cmd);

    // Environment paths are normally resolved relative to the parent's current
    // directory. Preserve that meaning when a caller gives the child a different
    // working directory; otherwise the runtime can enroll against a different
    // `.spice` directory than the CLI just wrote.
    if let Some(config_dir) = std::env::var_os(SPICE_CONFIG_DIR_ENV) {
        cmd.env(
            SPICE_CONFIG_DIR_ENV,
            absolute_from_parent(PathBuf::from(config_dir))?,
        );
    }
    if let Some(dir) = &config.working_dir {
        cmd.current_dir(dir);
    }

    match config.connection_report {
        ConnectionReport::AlreadyReported => {
            cmd.env(CONNECTION_REPORTED_ENV, "1");
        }
        ConnectionReport::Runtime => {
            // This is a private parent-child handshake, not an operator
            // setting. A direct `spice run` must not inherit a stale value and
            // accidentally suppress the runtime's own connection report.
            cmd.env_remove(CONNECTION_REPORTED_ENV);
        }
    }

    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    // Install the handlers before spawning so a signal cannot land before the
    // launcher is ready to forward it.
    let mut child_signals = ChildSignals::new()?;
    let mut child = cmd.spawn().context(RuntimeExecutionSnafu)?;

    run_with_signal_forwarding(&mut child, &mut child_signals).await
}

/// Resolve a path with the same parent-working-directory semantics environment
/// variables have before the child changes directory. An empty path remains
/// empty: `CloudConnectConfig` deliberately treats an explicitly empty
/// `SPICE_CONFIG_DIR` as unset, so the child must retain that meaning after it
/// changes to the configured instance directory.
fn absolute_from_parent(path: PathBuf) -> Result<PathBuf> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Ok(path);
    }
    std::env::current_dir()
        .map(|parent| parent.join(path))
        .context(RuntimeExecutionSnafu)
}

/// The `spiced` argument list a [`RunConfig`] resolves to, apart from the
/// endpoint and defaults [`RuntimeContext::get_run_cmd`] supplies.
///
/// Pass-through arguments come first so a flag the caller repeats explicitly is
/// the one `spiced`'s parser sees last only where that is intended.
fn spiced_args(config: &RunConfig, flight_endpoint: Option<&str>) -> Vec<String> {
    let mut args = config.args.clone();

    if config.verbosity > 0 {
        args.push(format!("-{}", "v".repeat(config.verbosity as usize)));
    }

    if let Some(flight) = flight_endpoint {
        args.push("--flight".to_string());
        args.push(flight.to_string());
    }

    if let Some(metrics) = &config.metrics_endpoint {
        args.push("--metrics".to_string());
        args.push(metrics.clone());
    }

    args
}

/// Signal streams installed before the runtime child is spawned and retained
/// through the final child wait.
struct ChildSignals {
    #[cfg(unix)]
    sigterm: tokio::signal::unix::Signal,
    #[cfg(unix)]
    sigint: tokio::signal::unix::Signal,
}

#[derive(Clone, Copy)]
enum ChildSignal {
    #[cfg(unix)]
    Terminate,
    #[cfg(unix)]
    Interrupt,
    #[cfg(not(unix))]
    Never,
}

impl ChildSignals {
    fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                sigterm: signal(SignalKind::terminate()).context(SignalHandlerSnafu)?,
                sigint: signal(SignalKind::interrupt()).context(SignalHandlerSnafu)?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn recv(&mut self) -> ChildSignal {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.sigterm.recv() => ChildSignal::Terminate,
                _ = self.sigint.recv() => ChildSignal::Interrupt,
            }
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<ChildSignal>().await
        }
    }

    fn forward(child: &tokio::process::Child, signal: ChildSignal) -> Result<()> {
        #[cfg(unix)]
        {
            use nix::sys::signal::{Signal, kill};
            use nix::unistd::Pid;

            let pid = child
                .id()
                .map(|id| Pid::from_raw(id as i32))
                .context(ChildProcessIdSnafu)?;
            let signal = match signal {
                ChildSignal::Terminate => Signal::SIGTERM,
                ChildSignal::Interrupt => Signal::SIGINT,
            };
            tracing::debug!(?signal, "Forwarding signal to runtime child");
            let _ = kill(pid, signal);
        }
        #[cfg(not(unix))]
        {
            let ChildSignal::Never = signal;
            let _ = child;
        }
        Ok(())
    }
}

/// Run the child process and forward signals (SIGTERM, SIGINT) to it.
async fn run_with_signal_forwarding(
    child: &mut tokio::process::Child,
    signals: &mut ChildSignals,
) -> Result<std::process::ExitStatus> {
    tokio::select! {
        status = child.wait() => {
            status.context(RuntimeExecutionSnafu)
        }
        signal = signals.recv() => {
            ChildSignals::forward(child, signal)?;
            child.wait().await.context(RuntimeExecutionSnafu)
        }
    }
}

/// Resolve `--endpoint` into the appropriate HTTP or Flight endpoint based on its URL scheme.
///
/// Returns `(http_endpoint, flight_endpoint)`. If `--endpoint` is provided, it takes precedence
/// over the corresponding specific endpoint flag. An error is returned if `--endpoint` has no
/// recognized scheme or conflicts with an already-specified endpoint.
fn resolve_endpoint(
    endpoint: Option<&str>,
    http_endpoint: Option<&str>,
    flight_endpoint: Option<&str>,
) -> Result<(Option<String>, Option<String>)> {
    let Some(ep) = endpoint else {
        return Ok((
            http_endpoint.map(String::from),
            flight_endpoint.map(String::from),
        ));
    };

    if ep.starts_with("http://") || ep.starts_with("https://") {
        ensure!(
            http_endpoint.is_none(),
            InvalidArgumentSnafu {
                message: "--endpoint with http(s):// scheme cannot be combined with --http-endpoint"
            }
        );
        Ok((Some(ep.to_string()), flight_endpoint.map(String::from)))
    } else if ep.starts_with("grpc://") || ep.starts_with("grpc+tls://") {
        ensure!(
            flight_endpoint.is_none(),
            InvalidArgumentSnafu {
                message: "--endpoint with grpc:// scheme cannot be combined with --flight-endpoint"
            }
        );
        let addr = ep
            .trim_start_matches("grpc+tls://")
            .trim_start_matches("grpc://");
        Ok((http_endpoint.map(String::from), Some(addr.to_string())))
    } else {
        Err(InvalidArgumentSnafu {
            message: format!(
                "Unrecognized scheme in --endpoint '{ep}'. Use http://, https://, grpc://, or grpc+tls://"
            ),
        }
        .build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_routes_by_scheme() {
        assert_eq!(
            resolve_endpoint(Some("http://127.0.0.1:8090"), None, None).expect("http scheme"),
            (Some("http://127.0.0.1:8090".to_string()), None)
        );
        // The Flight address is passed to `spiced` without its scheme.
        assert_eq!(
            resolve_endpoint(Some("grpc://127.0.0.1:50051"), None, None).expect("grpc scheme"),
            (None, Some("127.0.0.1:50051".to_string()))
        );
        assert_eq!(
            resolve_endpoint(Some("grpc+tls://127.0.0.1:50051"), None, None).expect("grpc+tls"),
            (None, Some("127.0.0.1:50051".to_string()))
        );
    }

    #[test]
    fn endpoint_conflicts_and_unknown_schemes_are_refused() {
        resolve_endpoint(Some("http://a:1"), Some("http://b:2"), None)
            .expect_err("two HTTP endpoints must not both be honoured");
        resolve_endpoint(Some("grpc://a:1"), None, Some("b:2"))
            .expect_err("two Flight endpoints must not both be honoured");
        resolve_endpoint(Some("a:1"), None, None).expect_err("a scheme is required");
    }

    #[test]
    fn without_an_endpoint_the_specific_flags_pass_through() {
        assert_eq!(
            resolve_endpoint(None, Some("http://a:1"), Some("b:2")).expect("pass through"),
            (Some("http://a:1".to_string()), Some("b:2".to_string()))
        );
    }

    #[test]
    fn verbosity_metrics_and_flight_reach_the_runtime() {
        let config = RunConfig {
            metrics_endpoint: Some("127.0.0.1:9090".to_string()),
            verbosity: 2,
            args: vec!["--dataset-path".to_string(), "x".to_string()],
            ..RunConfig::default()
        };
        assert_eq!(
            spiced_args(&config, Some("127.0.0.1:50051")),
            vec![
                "--dataset-path",
                "x",
                "-vv",
                "--flight",
                "127.0.0.1:50051",
                "--metrics",
                "127.0.0.1:9090",
            ]
        );
    }

    #[test]
    fn a_quiet_run_adds_no_flags_of_its_own() {
        assert!(spiced_args(&RunConfig::default(), None).is_empty());
    }

    #[test]
    fn an_empty_config_directory_stays_unset_for_the_child() {
        assert_eq!(
            absolute_from_parent(PathBuf::new()).expect("an empty path needs no resolution"),
            PathBuf::new()
        );
    }

    /// The launcher against a stub runtime: what it does with a real child
    /// process, which is the half no argument assertion can cover.
    #[cfg(unix)]
    mod child_process {
        use super::*;
        use std::time::{Duration, Instant};

        /// How long a stub is given to reach the state a test waits for. Only
        /// an upper bound on something that normally happens in milliseconds.
        const READY_BUDGET: Duration = Duration::from_secs(10);
        const POLL_INTERVAL: Duration = Duration::from_millis(20);

        /// Install `script` as a stub `spiced`, and return the context plus the
        /// stub as an already-resolved runtime. The directory is the caller's to
        /// keep alive.
        ///
        /// Handing back the resolution is what keeps these tests hermetic: the
        /// stub only ever occupies the context's managed install directory, and
        /// the ladder ranks a `SPICED_PATH` pin, a `spiced` beside the test
        /// executable, and a `spiced` on `PATH` above it. Any of those on the
        /// machine running the suite would otherwise be launched instead — and
        /// a real runtime does not exit, so the failure is a hang.
        fn context_with_stub_runtime(
            bin_dir: &std::path::Path,
            script: &str,
        ) -> (RuntimeContext, ResolvedSpiced) {
            use std::os::unix::fs::PermissionsExt as _;

            std::fs::create_dir_all(bin_dir).expect("create the stub bin directory");
            let stub = bin_dir.join("spiced");
            std::fs::write(&stub, script).expect("write the stub runtime");
            std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755))
                .expect("make the stub runtime executable");
            let ctx = RuntimeContext::with_bin_dir_for_test(bin_dir.to_path_buf());
            // Built through the production constructor rather than as a struct
            // literal, so the stub is anchored the way every resolved runtime
            // is and these tests launch it the same way `spice run` would.
            let resolved = ResolvedSpiced::at(stub, crate::context::SpicedSource::ManagedInstall)
                .expect("anchor the stub runtime");
            (ctx, resolved)
        }

        /// Wait for the stub to report the state a test needs, rather than
        /// sleeping for a duration that is either flaky or slow.
        async fn wait_for(path: &std::path::Path) {
            let deadline = Instant::now() + READY_BUDGET;
            while !path.exists() {
                assert!(
                    Instant::now() < deadline,
                    "the stub runtime never created {}",
                    path.display()
                );
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }

        #[tokio::test]
        async fn the_runtime_runs_in_the_configured_working_directory() {
            // The runtime resolves both the spicepod and the `.spice` state
            // from its working directory, so this is what makes the runtime
            // it starts the one that directory describes.
            let dir = tempfile::tempdir().expect("create tempdir");
            let instance_dir = dir.path().join("instance");
            std::fs::create_dir_all(&instance_dir).expect("create the instance directory");
            let observed = dir.path().join("cwd");
            let (ctx, resolved) = context_with_stub_runtime(
                &dir.path().join("bin"),
                &format!("#!/bin/sh\npwd > {}\n", observed.display()),
            );

            let status = launch_resolved_runtime(
                &ctx,
                &RunConfig {
                    working_dir: Some(instance_dir.clone()),
                    ..RunConfig::default()
                },
                &resolved,
            )
            .await
            .expect("the stub runtime runs");

            assert!(status.success());
            let reported = std::fs::read_to_string(&observed).expect("the stub reported its cwd");
            assert_eq!(
                std::fs::canonicalize(reported.trim()).expect("canonicalize the reported cwd"),
                std::fs::canonicalize(&instance_dir).expect("canonicalize the instance directory"),
            );
        }

        #[tokio::test]
        async fn a_failing_runtime_reports_its_own_status() {
            // The CLI adopts this as its own exit status, so a script that runs
            // A caller sees what the runtime reported rather than merely that
            // the CLI managed to start it.
            let dir = tempfile::tempdir().expect("create tempdir");
            let (ctx, resolved) =
                context_with_stub_runtime(&dir.path().join("bin"), "#!/bin/sh\nexit 3\n");

            let status = launch_resolved_runtime(&ctx, &RunConfig::default(), &resolved)
                .await
                .expect("a failing runtime is an exit status, not a launcher error");

            assert_eq!(status.code(), Some(3));
        }

        #[tokio::test]
        async fn a_termination_signal_reaches_the_runtime() {
            let status = tokio::process::Command::new(
                std::env::current_exe().expect("resolve the unit-test executable"),
            )
            .args([
                "--ignored",
                "--exact",
                "runtime_launcher::tests::child_process::signal_forwarding_subprocess",
                "--nocapture",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await
            .expect("run the isolated signal-forwarding test");

            assert!(status.success(), "the isolated signal test must pass");
        }

        /// Signals are process-global. Keep the real raise in a test-harness
        /// subprocess selected with `--exact`, where no parallel unit test can
        /// install a listener or receive the broadcast.
        #[tokio::test]
        #[ignore = "run by a_termination_signal_reaches_the_runtime in isolation"]
        async fn signal_forwarding_subprocess() {
            // Ctrl-C and `systemctl stop` reach the CLI, and the runtime is the
            // process that has to shut down cleanly — its identity and desired
            // state are only intact if it gets to do that itself.
            use tokio::signal::unix::{SignalKind, signal};

            // Registered before the signal is raised: this is what makes the
            // default terminate action inapplicable, so a raise cannot kill the
            // test process before the launcher installs its own handler.
            let mut guard = signal(SignalKind::terminate()).expect("register SIGTERM");

            let dir = tempfile::tempdir().expect("create tempdir");
            let started = dir.path().join("started");
            let terminated = dir.path().join("terminated");
            // `sleep &` + `wait` rather than a foreground sleep: a shell runs a
            // trap only between commands, so a foreground sleep would swallow
            // the signal for its whole duration.
            let (ctx, resolved) = context_with_stub_runtime(
                &dir.path().join("bin"),
                &format!(
                    "#!/bin/sh\ntrap 'echo yes > {terminated}; kill \"$idle\" 2>/dev/null; exit 143' TERM\ntouch {started}\nsleep 30 & idle=$!\nwait\n",
                    terminated = terminated.display(),
                    started = started.display(),
                ),
            );

            let launched = tokio::spawn(async move {
                launch_resolved_runtime(&ctx, &RunConfig::default(), &resolved)
                    .await
                    .expect("the stub runtime runs")
            });

            wait_for(&started).await;
            nix::sys::signal::raise(nix::sys::signal::Signal::SIGTERM)
                .expect("raise SIGTERM in this process");
            // Consumed so the test's own listener does not outlive the raise
            // with a pending signal.
            let _ = guard.recv().await;

            let status = tokio::time::timeout(READY_BUDGET, launched)
                .await
                .expect("the launcher must return once the runtime exits")
                .expect("the launcher task must not panic");

            assert_eq!(
                status.code(),
                Some(143),
                "the runtime's own exit status must reach the caller"
            );
            assert!(
                terminated.exists(),
                "the runtime must receive the signal rather than be killed by it"
            );
        }
    }
}
