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

//! `spice claude`, `spice codex`, and the other coding agents: run the agent
//! with the Spice runtime as its model backend.
//!
//! The command makes sure a runtime is serving — the one already running at
//! the endpoint, or one started for the Spicepod in the current directory —
//! waits for its models, then starts the agent pointed at it and hands it the
//! terminal. When the agent exits, the runtime is stopped if a `spice <agent>`
//! session started it and no other one is still using it.

mod agent;
mod models;
mod process;
mod runtime;

use std::ffi::OsString;
use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};

use clap::Args;

pub use agent::Agent;

use crate::context::RuntimeContext;
use crate::error::{
    CodingAgentCloudUnsupportedSnafu, CodingAgentNotInstalledSnafu,
    CodingAgentUnsupportedBuildSnafu, InterruptedSnafu, Result,
};
use runtime::{Origin, RuntimeLease};

/// The arguments of a coding-agent command: all of them are the agent's.
///
/// `spice` takes none of its own here — not even `--help` — so that
/// `spice codex --yolo` is `codex --yolo`. Options for `spice` itself go before
/// the command name, and the agent's own model option chooses the Spice model.
#[derive(Args, Debug, Default)]
pub struct CodingAgentArgs {
    /// Arguments passed to the agent unchanged.
    #[arg(
        value_name = "AGENT_ARGS",
        trailing_var_arg = true,
        allow_hyphen_values = true
    )]
    pub args: Vec<OsString>,
}

/// The one-line description of an agent's command.
#[must_use]
pub fn about(agent: Agent) -> String {
    format!(
        "Run {} with the Spice runtime as its model backend",
        agent.display_name()
    )
}

/// The full help text of an agent's command, shown by `spice help <command>`.
#[must_use]
pub fn long_about(agent: Agent) -> String {
    let name = agent.display_name();
    let command = agent.command();
    let binary = agent.install().binaries[0];
    let model = agent.model_example();
    format!(
        "Run {name} with the Spice runtime as its model backend.

Every argument after `{command}` is passed to {name} unchanged: `spice {command} ARGS` \
runs `{binary} ARGS`, with {name} pointed at the Spice runtime. Options for spice \
itself, such as --http-endpoint or --api-key, go before `{command}`.

spice uses the runtime already serving at the HTTP endpoint (http://127.0.0.1:8090, \
or --http-endpoint), or starts one for the Spicepod in the current directory, and \
waits for its models to load. {name} starts on the model its own `{model}` option \
names, or on the runtime's only model (with several, spice asks which), and every \
model the runtime serves is available to it. {name}'s own configuration files are \
not changed.

A runtime that `spice {command}` started is stopped when the last coding agent using \
it exits; its output is written to ~/.spice/run/. A runtime started any other way, \
such as with `spice run`, is left running.

Examples:
  spice {command}                       # the runtime's only model, or choose one
  spice {command} {model}     # a model listed by `spice models`
  spice {command} --help                # {name}'s own help
  spice --http-endpoint http://127.0.0.1:9000 {command}

See: https://spiceai.org/docs/cli"
    )
}

/// Run `agent` against the Spice runtime, and return the exit code to report.
///
/// # Errors
///
/// Returns an error when the agent is not installed, the runtime cannot be
/// started or reached, or no model can be chosen. An agent that runs and
/// fails is not an error: its exit code is returned.
pub async fn execute(ctx: &RuntimeContext, agent: Agent, args: &CodingAgentArgs) -> Result<i32> {
    snafu::ensure!(
        !ctx.is_cloud(),
        CodingAgentCloudUnsupportedSnafu {
            command: agent.command()
        }
    );
    let program = find_agent(agent)?;

    // Not raced against Ctrl-C here: acquiring handles it, stopping a runtime
    // it was starting, which dropping it midway would leave running.
    let lease = RuntimeLease::acquire(ctx).await?;
    if *lease.origin() == Origin::AlreadyRunning {
        tracing::info!(
            "Using the Spice runtime already serving at {}",
            ctx.http_endpoint()
        );
    }

    let interactive = std::io::stdin().is_terminal() && std::io::stdout().is_terminal();
    let requested = agent::requested_model(agent, &args.args);
    // Interruptible until the agent starts: waiting on a model can take as
    // long as a download. From then on Ctrl-C is the agent's to interpret.
    let prepared = tokio::select! {
        prepared = prepare_agent(ctx, agent, &program, requested, interactive) => prepared,
        _ = tokio::signal::ctrl_c() => InterruptedSnafu.fail(),
    };
    let result = match prepared {
        Ok(prepared) => prepared.run(agent, &program, &args.args).await,
        Err(error) => Err(error),
    };

    let stopped = lease.release().await;
    match stopped {
        Ok(true) => tracing::debug!("Stopped the Spice runtime at {}", ctx.http_endpoint()),
        Ok(false) => {}
        Err(error) => tracing::warn!(
            "Failed to stop the Spice runtime at {}, so it may still be running. Stop it manually if so. Cause: {error}",
            ctx.http_endpoint()
        ),
    }
    result
}

/// An agent ready to start: its model chosen and its files written.
struct Prepared {
    model: String,
    launch: agent::Launch,
    /// Holds the files the agent reads; removed when this is dropped.
    scratch: tempfile::TempDir,
}

/// Choose the model, and write and run whatever the agent needs first.
///
/// `requested` is what the user's own model option named.
async fn prepare_agent(
    ctx: &RuntimeContext,
    agent: Agent,
    program: &Path,
    requested: agent::RequestedModel,
    interactive: bool,
) -> Result<Prepared> {
    let model_given = requested.given();
    let selection = models::select(ctx, agent, &requested, interactive).await?;

    let scratch = tempfile::Builder::new()
        .prefix(&format!("spice-{}-", agent.command()))
        .tempdir()
        .map_err(|source| crate::error::Error::RuntimeExecution { source })?;
    let backend = agent::Backend {
        endpoint: ctx.http_endpoint().trim_end_matches('/'),
        api_key: ctx.api_key(),
        model: &selection.model,
        models: &selection.available,
        native_messages: selection.native_messages,
        model_given,
        context_windows: &selection.context_windows,
    };
    let launch = agent::launch(agent, &backend, scratch.path());
    process::write_files(&launch.files)?;
    process::prepare(agent.display_name(), program, &launch).await?;
    Ok(Prepared {
        model: selection.model,
        launch,
        scratch,
    })
}

impl Prepared {
    /// Run the agent with the terminal until it exits.
    async fn run(self, agent: Agent, program: &Path, user_args: &[OsString]) -> Result<i32> {
        tracing::info!(
            "Starting {} with Spice model '{}'",
            agent.display_name(),
            self.model
        );
        let status = process::run(agent.display_name(), program, &self.launch, user_args).await?;
        // Removed here rather than left to the caller, which exits the process
        // with the agent's code and so would skip the removal.
        drop(self.scratch);
        Ok(process::exit_code(status))
    }
}

/// Find the agent's executable.
fn find_agent(agent: Agent) -> Result<PathBuf> {
    let install = agent.install();
    let home = dirs::home_dir();
    if let Some(found) = find_program(install.binaries, install.home_fallbacks, home.as_deref()) {
        return Ok(found);
    }
    if let Some((binaries, explanation)) = agent.unsupported_build()
        && find_program(binaries, &[], home.as_deref()).is_some()
    {
        return CodingAgentUnsupportedBuildSnafu {
            agent: agent.display_name(),
            explanation,
        }
        .fail();
    }
    CodingAgentNotInstalledSnafu {
        agent: agent.display_name(),
        binary: install.binaries.join("` or `"),
        instructions: install.instructions,
        command: agent.command(),
        docs: install.docs,
    }
    .fail()
}

/// The first of `binaries` on `PATH`, else the first of `home_fallbacks` under
/// `home` that exists.
fn find_program(
    binaries: &[&str],
    home_fallbacks: &[&str],
    home: Option<&Path>,
) -> Option<PathBuf> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let on_path = binaries.iter().find_map(|binary| {
        std::env::split_paths(&path)
            .map(|dir| dir.join(binary))
            .find(|candidate| is_executable(candidate))
    });
    on_path.or_else(|| {
        let home = home?;
        home_fallbacks
            .iter()
            .map(|relative| home.join(relative))
            .find(|candidate| is_executable(candidate))
    })
}

fn is_executable(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        metadata.is_file()
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;

    fn executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt as _;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create the directory");
        }
        std::fs::write(path, "#!/bin/sh\n").expect("write the executable");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
            .expect("make it executable");
    }

    #[test]
    fn a_fresh_install_off_path_is_found_under_home() {
        let home = tempfile::tempdir().expect("create tempdir");
        let installed = home.path().join(".grok/bin/grok");
        executable(&installed);
        assert_eq!(
            find_program(
                &["spice-test-no-such-binary"],
                &[".grok/bin/grok"],
                Some(home.path())
            ),
            Some(installed)
        );
    }

    #[test]
    fn a_file_without_the_execute_bit_is_not_an_install() {
        let home = tempfile::tempdir().expect("create tempdir");
        let path = home.path().join(".local/bin/pi");
        std::fs::create_dir_all(path.parent().expect("parent")).expect("create the directory");
        std::fs::write(&path, "not a program").expect("write the file");
        assert_eq!(
            find_program(
                &["spice-test-no-such-binary"],
                &[".local/bin/pi"],
                Some(home.path())
            ),
            None
        );
    }

    #[test]
    fn the_not_installed_error_says_how_to_install() {
        let error = CodingAgentNotInstalledSnafu {
            agent: "Claude Code",
            binary: "claude",
            instructions: "Install it with `curl -fsSL https://claude.ai/install.sh | bash`.",
            command: "claude",
            docs: "https://code.claude.com/docs/en/setup",
        }
        .build();
        assert_eq!(
            error.to_string(),
            "Failed to start Claude Code: it is not installed (no `claude` on PATH). Install it with `curl -fsSL https://claude.ai/install.sh | bash`. Then rerun `spice claude`. See: https://code.claude.com/docs/en/setup"
        );
    }

    #[test]
    fn every_agent_has_help_naming_its_own_command() {
        for agent in Agent::ALL {
            let help = long_about(agent);
            assert!(
                help.contains(&format!(
                    "spice {} {}",
                    agent.command(),
                    agent.model_example()
                )),
                "{help}"
            );
            assert!(help.starts_with(&format!("Run {} with", agent.display_name())));
        }
    }
}
