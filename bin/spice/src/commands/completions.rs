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

use std::path::PathBuf;

use clap::Args;
use clap_complete::Shell;

/// Arguments for the completions command.
#[derive(Args, Debug)]
#[command(
    about = "Generate shell completions for the spice CLI",
    long_about = r#"Generate shell completion scripts for the `spice` CLI.

With no shell argument the shell is detected from the `$SHELL` environment
variable. By default the completion script is written to the standard location
for the chosen shell; use `--stdout` to print it to standard output instead so
you can pipe it into your own configuration.

SUPPORTED SHELLS
  bash, zsh, fish, elvish, powershell

EXAMPLES
  spice completions                 # Detect $SHELL and install
  spice completions zsh             # Install zsh completions to a standard path
  spice completions bash --stdout   # Print bash completions to stdout
  spice completions fish > ~/.config/fish/completions/spice.fish
"#
)]
pub struct CompletionsArgs {
    /// The shell to generate completions for (detected from `$SHELL` if omitted).
    #[arg(value_enum)]
    pub shell: Option<Shell>,

    /// Print completions to stdout instead of writing to a file.
    #[arg(long)]
    pub stdout: bool,
}

/// Generate shell completions, writing to the appropriate file by default.
pub fn execute(args: &CompletionsArgs, cmd: &mut clap::Command) {
    let shell = args.shell.or_else(Shell::from_env).unwrap_or_else(|| {
        eprintln!(
            "Could not detect shell. Please specify one: bash, zsh, fish, elvish, powershell"
        );
        std::process::exit(1);
    });

    if args.stdout {
        clap_complete::generate(shell, cmd, "spice", &mut std::io::stdout());
        return;
    }

    let mut buf = Vec::new();
    clap_complete::generate(shell, cmd, "spice", &mut buf);

    if let Some((path, post_install_msg)) = completion_path(shell) {
        if let Some(parent) = path.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            eprintln!("Failed to create directory {}: {e}", parent.display());
            std::process::exit(1);
        }

        if let Err(e) = std::fs::write(&path, &buf) {
            eprintln!("Failed to write completions to {}: {e}", path.display());
            std::process::exit(1);
        }

        println!("Completions for {shell} written to {}", path.display());
        if let Some(msg) = post_install_msg {
            println!("\n{msg}");
        }
    } else {
        eprintln!("No standard completion directory for {shell}. Printing to stdout.");
        eprintln!("Add the output to your shell profile to enable completions.\n");
        std::io::Write::write_all(&mut std::io::stdout(), &buf).unwrap_or_else(|e| {
            eprintln!("Failed to write to stdout: {e}");
            std::process::exit(1);
        });
    }
}

/// Returns `(file_path, optional_post_install_message)` for the given shell,
/// or `None` when no standard completion directory is known.
fn completion_path(shell: Shell) -> Option<(PathBuf, Option<String>)> {
    match shell {
        Shell::Bash => bash_completion_path(),
        Shell::Zsh => zsh_completion_path(),
        Shell::Fish => fish_completion_path(),
        Shell::Elvish => elvish_completion_path(),
        Shell::PowerShell => powershell_completion_path(),
        _ => None,
    }
}

/// Homebrew prefixes on macOS (Apple Silicon, then Intel).
#[cfg(target_os = "macos")]
const HOMEBREW_PREFIXES: &[&str] = &["/opt/homebrew", "/usr/local"];

fn xdg_data_dir() -> Option<PathBuf> {
    std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|h| h.join(".local/share")))
}

fn xdg_config_dir() -> Option<PathBuf> {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|h| h.join(".config")))
}

/// Returns the first directory from `candidates` that already exists on disk.
#[cfg(target_os = "macos")]
fn first_existing_dir(candidates: impl IntoIterator<Item = PathBuf>) -> Option<PathBuf> {
    candidates.into_iter().find(|p| p.is_dir())
}

fn bash_completion_path() -> Option<(PathBuf, Option<String>)> {
    // macOS: prefer Homebrew bash-completion directories when present.
    #[cfg(target_os = "macos")]
    {
        let candidates = HOMEBREW_PREFIXES
            .iter()
            .map(|p| PathBuf::from(format!("{p}/share/bash-completion/completions")));

        if let Some(dir) = first_existing_dir(candidates) {
            return Some((
                dir.join("spice"),
                Some(
                    "Completions will be loaded automatically in new bash sessions.\n\
                     Requires bash-completion: brew install bash-completion@2"
                        .to_string(),
                ),
            ));
        }
    }

    // Linux / WSL / macOS fallback: XDG user directory.
    let dir = xdg_data_dir()?;
    Some((
        dir.join("bash-completion/completions/spice"),
        Some(
            "Completions will be loaded automatically in new bash sessions.\n\
             If bash-completion is not installed, install it first:\n  \
             apt install bash-completion   # Debian/Ubuntu\n  \
             brew install bash-completion@2 # macOS"
                .to_string(),
        ),
    ))
}

fn zsh_completion_path() -> Option<(PathBuf, Option<String>)> {
    // macOS: prefer Homebrew zsh site-functions when present.
    // These directories are already on fpath by default in Homebrew-managed zsh.
    #[cfg(target_os = "macos")]
    {
        let candidates = HOMEBREW_PREFIXES
            .iter()
            .map(|p| PathBuf::from(format!("{p}/share/zsh/site-functions")));

        if let Some(dir) = first_existing_dir(candidates) {
            return Some((
                dir.join("_spice"),
                Some(
                    "Completions will be loaded automatically in new zsh sessions.\n\
                     If not working, ensure compinit is enabled in your .zshrc:\n  \
                     autoload -Uz compinit && compinit"
                        .to_string(),
                ),
            ));
        }
    }

    // Linux / WSL / macOS fallback: XDG user directory.
    let dir = xdg_data_dir()?;
    let fpath_dir = dir.join("zsh/site-functions");
    Some((
        fpath_dir.join("_spice"),
        Some(format!(
            "To enable, ensure the following is in your .zshrc:\n  \
             fpath=({fpath} $fpath)\n  \
             autoload -Uz compinit && compinit\n\n\
             Then restart your shell or run: source ~/.zshrc",
            fpath = fpath_dir.display(),
        )),
    ))
}

fn fish_completion_path() -> Option<(PathBuf, Option<String>)> {
    let dir = xdg_config_dir()?;
    Some((
        dir.join("fish/completions/spice.fish"),
        Some("Completions will be loaded automatically in new fish sessions.".to_string()),
    ))
}

fn elvish_completion_path() -> Option<(PathBuf, Option<String>)> {
    let dir = xdg_config_dir()?;
    Some((
        dir.join("elvish/lib/spice.elv"),
        Some(
            "To enable, add to your ~/.config/elvish/rc.elv:\n  \
             use spice"
                .to_string(),
        ),
    ))
}

fn powershell_completion_path() -> Option<(PathBuf, Option<String>)> {
    let dir = xdg_config_dir()?;
    let file = dir.join("powershell/completions/spice.ps1");
    let path_display = file.display().to_string();
    Some((
        file,
        Some(format!(
            "To enable, add to your PowerShell profile ($PROFILE):\n  \
             . {path_display}"
        )),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap_complete::Shell;
    use std::sync::Mutex;

    /// Serialize tests that modify process-wide environment variables.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    /// Run `f` with `XDG_DATA_HOME` and `XDG_CONFIG_HOME` pointing at a
    /// temporary directory, then restore the originals.
    fn with_xdg_env<F: FnOnce(&PathBuf)>(f: F) {
        let _guard = ENV_MUTEX.lock().expect("env mutex poisoned");
        let tmp = std::env::temp_dir().join("spice-completion-test");

        let orig_data = std::env::var_os("XDG_DATA_HOME");
        let orig_config = std::env::var_os("XDG_CONFIG_HOME");

        // SAFETY: Serialized by ENV_MUTEX — no other test modifies env concurrently.
        unsafe {
            std::env::set_var("XDG_DATA_HOME", &tmp);
            std::env::set_var("XDG_CONFIG_HOME", &tmp);
        }

        f(&tmp);

        unsafe {
            match orig_data {
                Some(v) => std::env::set_var("XDG_DATA_HOME", v),
                None => std::env::remove_var("XDG_DATA_HOME"),
            }
            match orig_config {
                Some(v) => std::env::set_var("XDG_CONFIG_HOME", v),
                None => std::env::remove_var("XDG_CONFIG_HOME"),
            }
        }
    }

    #[test]
    fn xdg_data_dir_respects_env() {
        with_xdg_env(|tmp| {
            let dir = xdg_data_dir().expect("should return Some when XDG_DATA_HOME is set");
            assert_eq!(dir, *tmp);
        });
    }

    #[test]
    fn xdg_config_dir_respects_env() {
        with_xdg_env(|tmp| {
            let dir = xdg_config_dir().expect("should return Some when XDG_CONFIG_HOME is set");
            assert_eq!(dir, *tmp);
        });
    }

    #[test]
    fn all_shells_return_some_path() {
        with_xdg_env(|_| {
            for shell in [
                Shell::Bash,
                Shell::Zsh,
                Shell::Fish,
                Shell::Elvish,
                Shell::PowerShell,
            ] {
                assert!(
                    completion_path(shell).is_some(),
                    "completion_path({shell}) should return Some"
                );
            }
        });
    }

    #[test]
    fn all_shells_have_post_install_message() {
        with_xdg_env(|_| {
            for shell in [
                Shell::Bash,
                Shell::Zsh,
                Shell::Fish,
                Shell::Elvish,
                Shell::PowerShell,
            ] {
                let (_, msg) = completion_path(shell).expect("completion_path should return Some");
                assert!(msg.is_some(), "{shell} should have a post-install message");
            }
        });
    }

    #[test]
    fn bash_path_contains_expected_components() {
        with_xdg_env(|_| {
            let (path, _) =
                completion_path(Shell::Bash).expect("bash completion path should be Some");
            assert_eq!(
                path.file_name().and_then(|f| f.to_str()),
                Some("spice"),
                "bash completion filename should be 'spice'"
            );
            let path_str = path.to_string_lossy();
            assert!(
                path_str.contains("bash-completion"),
                "bash path should contain 'bash-completion': {path_str}"
            );
        });
    }

    #[test]
    fn zsh_path_contains_expected_components() {
        with_xdg_env(|_| {
            let (path, _) =
                completion_path(Shell::Zsh).expect("zsh completion path should be Some");
            assert_eq!(
                path.file_name().and_then(|f| f.to_str()),
                Some("_spice"),
                "zsh completion filename should be '_spice'"
            );
            let path_str = path.to_string_lossy();
            assert!(
                path_str.contains("site-functions"),
                "zsh path should contain 'site-functions': {path_str}"
            );
        });
    }

    #[test]
    fn fish_path_uses_xdg_config() {
        with_xdg_env(|tmp| {
            let (path, _) =
                completion_path(Shell::Fish).expect("fish completion path should be Some");
            assert_eq!(path, tmp.join("fish/completions/spice.fish"));
        });
    }

    #[test]
    fn elvish_path_uses_xdg_config() {
        with_xdg_env(|tmp| {
            let (path, _) =
                completion_path(Shell::Elvish).expect("elvish completion path should be Some");
            assert_eq!(path, tmp.join("elvish/lib/spice.elv"));
        });
    }

    #[test]
    fn powershell_path_uses_xdg_config() {
        with_xdg_env(|tmp| {
            let (path, _) = completion_path(Shell::PowerShell)
                .expect("powershell completion path should be Some");
            assert_eq!(path, tmp.join("powershell/completions/spice.ps1"));
        });
    }

    /// On non-macOS, bash and zsh must use XDG paths (no Homebrew fallback).
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn bash_and_zsh_use_xdg_on_linux() {
        with_xdg_env(|tmp| {
            let (bash_path, _) = completion_path(Shell::Bash).expect("bash should be Some");
            assert_eq!(bash_path, tmp.join("bash-completion/completions/spice"));

            let (zsh_path, _) = completion_path(Shell::Zsh).expect("zsh should be Some");
            assert_eq!(zsh_path, tmp.join("zsh/site-functions/_spice"));
        });
    }
}
