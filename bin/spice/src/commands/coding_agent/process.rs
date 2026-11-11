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

//! Running a coding agent in the foreground.
//!
//! The agent owns the terminal for its whole run: it reads keys in raw mode,
//! redraws the screen, and decides what Ctrl-C means (most cancel the current
//! turn and exit only on a second press). So this process stays out of the
//! way — it inherits the agent's standard streams, ignores the interrupts the
//! terminal sends to the whole foreground process group, and forwards only
//! the signals that ask it to stop, so the agent can shut down and this
//! process can then stop the runtime.

use std::ffi::OsString;
use std::path::Path;
use std::process::{ExitStatus, Stdio};

use snafu::ResultExt;

use super::agent::{Launch, LaunchFile};
use crate::error::{
    CodingAgentFileSnafu, CodingAgentLaunchSnafu, CodingAgentPrepareSnafu, Result,
    SignalHandlerSnafu,
};

/// Write the files `launch` needs.
///
/// # Errors
///
/// Returns an error when a file or its directory cannot be written.
pub(crate) fn write_files(files: &[LaunchFile]) -> Result<()> {
    for file in files {
        if let Some(parent) = file.path.parent() {
            std::fs::create_dir_all(parent).context(CodingAgentFileSnafu {
                path: parent.to_path_buf(),
            })?;
        }
        std::fs::write(&file.path, &file.contents).context(CodingAgentFileSnafu {
            path: file.path.clone(),
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            // Owner-only either way: these files can carry the runtime key.
            let mode = if file.executable { 0o700 } else { 0o600 };
            std::fs::set_permissions(&file.path, std::fs::Permissions::from_mode(mode)).context(
                CodingAgentFileSnafu {
                    path: file.path.clone(),
                },
            )?;
        }
    }
    Ok(())
}

fn command(program: &Path, launch: &Launch, args: &[OsString]) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(program);
    command.args(args);
    for key in &launch.env_remove {
        command.env_remove(key);
    }
    for (key, value) in &launch.env {
        command.env(key, value);
    }
    command
}

/// Run the agent's preparation step to completion, quietly.
///
/// # Errors
///
/// Returns an error when the step cannot be started or does not succeed; the
/// error quotes its output.
pub(crate) async fn prepare(agent: &str, program: &Path, launch: &Launch) -> Result<()> {
    let Some(args) = &launch.prepare else {
        return Ok(());
    };
    // Ctrl-C while it runs drops this wait, and the step must end with it.
    let output = command(program, launch, args)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .context(CodingAgentLaunchSnafu {
            agent: agent.to_string(),
            program: program.to_path_buf(),
        })?;
    if output.status.success() {
        return Ok(());
    }
    let mut detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if detail.is_empty() {
        detail = String::from_utf8_lossy(&output.stdout).trim().to_string();
    }
    Err(CodingAgentPrepareSnafu {
        agent: agent.to_string(),
        command: format!(
            "{} {}",
            program.display(),
            args.iter()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join(" ")
        ),
        status: output.status.to_string(),
        detail,
    }
    .build())
}

/// Run the agent with the terminal, and return how it exited.
///
/// `user_args` follow the launch's own arguments.
///
/// # Errors
///
/// Returns an error when the agent cannot be started or waited on.
pub(crate) async fn run(
    agent: &str,
    program: &Path,
    launch: &Launch,
    user_args: &[OsString],
) -> Result<ExitStatus> {
    let mut args = launch.args.clone();
    args.extend(user_args.iter().cloned());
    let mut command = command(program, launch, &args);
    command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    // Installed before the spawn, so no signal can arrive between the two and
    // take its default action — terminating this process and orphaning the
    // agent mid-session.
    let mut signals = Signals::new()?;
    let mut child = command.spawn().context(CodingAgentLaunchSnafu {
        agent: agent.to_string(),
        program: program.to_path_buf(),
    })?;

    loop {
        tokio::select! {
            status = child.wait() => {
                return status.context(CodingAgentLaunchSnafu {
                    agent: agent.to_string(),
                    program: program.to_path_buf(),
                });
            }
            signal = signals.recv() => signal.forward(&child),
        }
    }
}

/// The exit code that reports `status` the way a shell would.
#[must_use]
pub(crate) fn exit_code(status: ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        status
            .code()
            .or_else(|| status.signal().map(|signal| 128 + signal))
            .unwrap_or(1)
    }
    #[cfg(not(unix))]
    {
        status.code().unwrap_or(1)
    }
}

/// The signals this process receives while the agent runs.
struct Signals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    quit: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    hangup: tokio::signal::unix::Signal,
}

#[derive(Clone, Copy)]
enum Received {
    /// Sent by the terminal to the whole foreground group, so the agent has
    /// it too; it decides what the key means.
    #[cfg(unix)]
    Ignored,
    #[cfg(unix)]
    Forward(nix::sys::signal::Signal),
}

impl Signals {
    fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt()).context(SignalHandlerSnafu)?,
                quit: signal(SignalKind::quit()).context(SignalHandlerSnafu)?,
                terminate: signal(SignalKind::terminate()).context(SignalHandlerSnafu)?,
                hangup: signal(SignalKind::hangup()).context(SignalHandlerSnafu)?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn recv(&mut self) -> Received {
        #[cfg(unix)]
        {
            use nix::sys::signal::Signal;
            tokio::select! {
                _ = self.interrupt.recv() => Received::Ignored,
                _ = self.quit.recv() => Received::Ignored,
                _ = self.terminate.recv() => Received::Forward(Signal::SIGTERM),
                _ = self.hangup.recv() => Received::Forward(Signal::SIGHUP),
            }
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<Received>().await
        }
    }
}

impl Received {
    fn forward(self, child: &tokio::process::Child) {
        #[cfg(unix)]
        {
            let Self::Forward(signal) = self else {
                return;
            };
            if let Some(pid) = child.id().and_then(|id| i32::try_from(id).ok()) {
                tracing::debug!(?signal, "Forwarding signal to the coding agent");
                let _ = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), signal);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (self, child);
        }
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    fn stub(dir: &Path, script: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let path = dir.join("agent");
        std::fs::write(&path, script).expect("write the stub agent");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("make the stub agent executable");
        path
    }

    /// `HOME` stands in for a vendor credential in the user's shell: every
    /// test process inherits it, so its absence in the child can only be the
    /// removal list at work.
    fn launch() -> Launch {
        Launch {
            args: vec![OsString::from("--model"), OsString::from("qwen")],
            env: vec![("SPICE_TEST_SET", OsString::from("set by spice"))],
            env_remove: vec!["HOME"],
            files: Vec::new(),
            prepare: None,
        }
    }

    #[tokio::test]
    async fn the_agent_gets_the_launch_arguments_before_the_users_and_the_environment() {
        assert!(
            std::env::var_os("HOME").is_some(),
            "the test needs an inherited HOME to show it is removed"
        );
        let dir = tempfile::tempdir().expect("create tempdir");
        let record = dir.path().join("record");
        let program = stub(
            dir.path(),
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > {r}\nprintf 'set=%s home=%s\\n' \"$SPICE_TEST_SET\" \"${{HOME-unset}}\" >> {r}\nexit 7\n",
                r = record.display()
            ),
        );
        let status = run(
            "Stub",
            &program,
            &launch(),
            &[OsString::from("--resume"), OsString::from("a b")],
        )
        .await
        .expect("the stub agent runs");
        assert_eq!(exit_code(status), 7, "the agent's own status is reported");
        assert_eq!(
            std::fs::read_to_string(&record).expect("read the record"),
            "--model\nqwen\n--resume\na b\nset=set by spice home=unset\n"
        );
    }

    #[tokio::test]
    async fn a_failed_preparation_quotes_its_output() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let program = stub(dir.path(), "#!/bin/sh\necho 'Not signed in' >&2\nexit 3\n");
        let mut prepared = launch();
        prepared.prepare = Some(vec![OsString::from("login")]);
        let error = prepare("Stub", &program, &prepared)
            .await
            .expect_err("a failing preparation is an error");
        let message = error.to_string();
        assert!(
            message.starts_with("Failed to prepare Stub: `")
                && message.contains(" login` exited with exit status: 3: Not signed in."),
            "{message}"
        );
    }

    /// Ctrl-C while an agent prepares drops the wait on its preparation step,
    /// which must not leave the step running after the session has ended.
    #[tokio::test]
    async fn a_preparation_whose_wait_is_dropped_is_stopped() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let pid_file = dir.path().join("pid");
        let program = stub(
            dir.path(),
            &format!(
                "#!/bin/sh\necho $$ > '{}'\nexec sleep 30\n",
                pid_file.display()
            ),
        );
        let mut prepared = launch();
        prepared.prepare = Some(vec![OsString::from("login")]);

        let started = async {
            while !std::fs::read_to_string(&pid_file).is_ok_and(|pid| pid.ends_with('\n')) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {
            result = prepare("Stub", &program, &prepared) => {
                panic!("the preparation finished on its own: {result:?}")
            }
            () = started => {}
        }
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .expect("read the PID")
            .trim()
            .parse()
            .expect("a PID");

        let running = || {
            let alive = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok();
            // An exited process its parent has not reaped yet answers the check too.
            let zombie = std::process::Command::new("ps")
                .args(["-o", "stat=", "-p", &pid.to_string()])
                .output()
                .is_ok_and(|output| {
                    String::from_utf8_lossy(&output.stdout)
                        .trim()
                        .starts_with('Z')
                });
            alive && !zombie
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while running() {
            assert!(
                std::time::Instant::now() < deadline,
                "the preparation step {pid} still runs after its wait was dropped"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[test]
    fn files_are_written_private_to_the_owner() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("create tempdir");
        let script = dir.path().join("nested").join("token.sh");
        let config = dir.path().join("nested").join("config.ts");
        write_files(&[
            LaunchFile {
                path: script.clone(),
                contents: "#!/bin/sh\n".to_string(),
                executable: true,
            },
            LaunchFile {
                path: config.clone(),
                contents: "export default 1\n".to_string(),
                executable: false,
            },
        ])
        .expect("write the files");
        let mode = |path: &Path| {
            std::fs::metadata(path)
                .expect("stat the file")
                .permissions()
                .mode()
                & 0o777
        };
        assert_eq!(mode(&script), 0o700);
        assert_eq!(mode(&config), 0o600);
        assert_eq!(
            std::fs::read_to_string(&config).expect("read the file"),
            "export default 1\n"
        );
    }
}
