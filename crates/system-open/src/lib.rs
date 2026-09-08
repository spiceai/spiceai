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

//! Open a path or URL with the operating system's default application.
//!
//! This is a minimal replacement for the external [`open`](https://crates.io/crates/open)
//! crate. Spice only needs "open this URL in the user's browser" for OAuth / feedback
//! flows in the CLI, so this crate exposes a single [`that`] helper and nothing else —
//! no custom-app launchers, no detached-thread helpers, no WSL path rewriting.
//!
//! Platform behavior:
//! - **macOS**: `/usr/bin/open`
//! - **Windows**: `cmd /c start "" <path>`
//! - **other Unix**: try `xdg-open`, then `gio open`, `gnome-open`, `kde-open`

use std::ffi::OsStr;
use std::io;
use std::process::{Command, Stdio};

/// Open `path` (file path or URL) with the OS default application.
///
/// Returns `Ok(())` once a launcher has exited successfully. Launchers are
/// started with stdin/stdout/stderr redirected to null so a GUI opener does not
/// attach to the CLI's terminal. We **wait** for the launcher (via
/// [`Command::status`]) so the child is reaped and cannot become a zombie while
/// the CLI keeps polling (e.g. OAuth device login). Typical openers
/// (`/usr/bin/open`, `xdg-open`) return quickly after handing off to the browser.
/// On Unix desktop environments several launchers are tried in order; the first
/// that exits successfully wins (spawn failure *or* non-zero exit tries the next).
///
/// # Errors
///
/// Returns the last I/O error if every candidate launcher fails to spawn or
/// exits with a non-zero status.
pub fn that(path: impl AsRef<OsStr>) -> io::Result<()> {
    platform::that(path.as_ref())
}

/// Run `cmd` with stdio discarded and wait for it, so the process is reaped.
fn run_null(cmd: &mut Command) -> io::Result<()> {
    let status = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!("launcher exited with {status}")))
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::run_null;
    use std::ffi::OsStr;
    use std::io;
    use std::process::Command;

    pub(super) fn that(path: &OsStr) -> io::Result<()> {
        run_null(Command::new("/usr/bin/open").arg(path))
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
mod platform {
    use super::run_null;
    use std::ffi::OsStr;
    use std::io;
    use std::process::Command;

    pub(super) fn that(path: &OsStr) -> io::Result<()> {
        // Prefer the FreeDesktop standard opener, then fall back through common
        // desktop-environment helpers. First successful run wins (including a
        // zero exit status — non-zero tries the next helper).
        let mut last_err = None;

        for mut cmd in [
            {
                let mut c = Command::new("xdg-open");
                c.arg(path);
                c
            },
            {
                let mut c = Command::new("gio");
                c.arg("open").arg(path);
                c
            },
            {
                let mut c = Command::new("gnome-open");
                c.arg(path);
                c
            },
            {
                let mut c = Command::new("kde-open");
                c.arg(path);
                c
            },
        ] {
            match run_null(&mut cmd) {
                Ok(()) => return Ok(()),
                Err(err) => last_err = Some(err),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "no URL/path opener available")
        }))
    }
}

/// Quote `path` for `cmd /c start` on Windows.
///
/// Wraps the value in double quotes and rejects embedded `"` characters so
/// cmd.exe cannot end the quoted argument early (argument injection).
///
/// Compiled for the Windows launcher and for unit tests on all platforms.
#[cfg(any(test, target_os = "windows"))]
fn quote_for_cmd_start(path: &OsStr) -> io::Result<std::ffi::OsString> {
    if contains_double_quote(path) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "system-open: path must not contain double-quote characters",
        ));
    }
    let mut out = std::ffi::OsString::from("\"");
    out.push(path);
    out.push("\"");
    Ok(out)
}

#[cfg(any(test, target_os = "windows"))]
fn contains_double_quote(path: &OsStr) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        // Check the native UTF-16 form so we do not miss non-UTF8 OsStr values.
        path.encode_wide().any(|c| c == u16::from(b'"'))
    }
    #[cfg(not(windows))]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_bytes().contains(&b'"')
    }
}

#[cfg(target_os = "windows")]
mod platform {
    use super::quote_for_cmd_start;
    use std::ffi::OsStr;
    use std::io;
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};

    // CREATE_NO_WINDOW — avoid a brief console flash when launching via cmd.
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    pub(super) fn that(path: &OsStr) -> io::Result<()> {
        // `start` treats the first quoted argument as a window title, so pass an
        // empty title (`""`) then the path. `raw_arg` preserves the quotes that
        // `start` requires for paths/URLs with special characters.
        let quoted = quote_for_cmd_start(path)?;
        let mut cmd = Command::new("cmd");
        cmd.arg("/c")
            .arg("start")
            .raw_arg("\"\"")
            .raw_arg(quoted)
            .creation_flags(CREATE_NO_WINDOW)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // Wait so the cmd process is reaped (no zombies if the CLI stays up).
        let status = cmd.status()?;
        if status.success() {
            Ok(())
        } else {
            Err(io::Error::other(format!("launcher exited with {status}")))
        }
    }
}

#[cfg(not(any(unix, target_os = "windows")))]
mod platform {
    use std::ffi::OsStr;
    use std::io;

    pub(super) fn that(_path: &OsStr) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "system-open: unsupported platform",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{contains_double_quote, quote_for_cmd_start};
    use std::ffi::OsStr;
    use std::io;

    #[test]
    fn quote_wraps_plain_path() {
        use std::ffi::OsString;
        let quoted = quote_for_cmd_start(OsStr::new("https://example.com/login")).expect("quote");
        assert_eq!(quoted, OsString::from("\"https://example.com/login\""));
    }

    #[test]
    fn quote_wraps_path_with_spaces() {
        use std::ffi::OsString;
        let quoted = quote_for_cmd_start(OsStr::new("C:\\Program Files\\app")).expect("quote");
        assert_eq!(quoted, OsString::from("\"C:\\Program Files\\app\""));
    }

    #[test]
    fn quote_rejects_embedded_double_quote() {
        let err = quote_for_cmd_start(OsStr::new("evil\" & calc")).expect_err("must reject");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(contains_double_quote(OsStr::new("a\"b")));
        assert!(!contains_double_quote(OsStr::new("ab")));
    }
}
