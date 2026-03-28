/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Shell completions generation command.

use clap::Args;
use clap_complete::Shell;

/// Arguments for the completions command.
#[derive(Args, Debug)]
pub struct CompletionsArgs {
    /// The shell to generate completions for (detected from $SHELL if omitted)
    #[arg(value_enum)]
    pub shell: Option<Shell>,
}

/// Generate shell completions and write them to stdout.
pub fn execute(args: &CompletionsArgs, cmd: &mut clap::Command) {
    let shell = args.shell.or_else(Shell::from_env).unwrap_or_else(|| {
        eprintln!("Could not detect shell. Please specify one: bash, zsh, fish, elvish, powershell");
        std::process::exit(1);
    });
    clap_complete::generate(shell, cmd, "spice", &mut std::io::stdout());
}
