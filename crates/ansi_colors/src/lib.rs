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

//! Simple ANSI color helpers for terminal output.
//!
//! Colors are only applied when the output destination is a terminal and the user
//! hasn't opted out via `NO_COLOR`. The check is lazy and cached once per process
//! per stream (stdout/stderr), so subsequent calls are a plain atomic load.

use std::fmt::{self, Display};
#[cfg(not(test))]
use std::io::IsTerminal;
use std::sync::OnceLock;

static STDOUT_COLORS: OnceLock<bool> = OnceLock::new();
static STDERR_COLORS: OnceLock<bool> = OnceLock::new();

/// Which output stream a [`Painted`] is targeted at. The answer to "should we emit
/// ANSI escapes?" differs for stdout vs stderr (one can be a TTY while the other is
/// a pipe), so callers declare the target when they create a `Painted`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Stdout,
    Stderr,
}

#[cfg(not(test))]
fn check_target(target: Target) -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if std::env::var_os("FORCE_COLOR").is_some() {
        return true;
    }
    match target {
        Target::Stdout => std::io::stdout().is_terminal(),
        Target::Stderr => std::io::stderr().is_terminal(),
    }
}

/// Returns `true` when colored output should be emitted for the given stream.
#[must_use]
pub fn colors_enabled_for(target: Target) -> bool {
    let cell = match target {
        Target::Stdout => &STDOUT_COLORS,
        Target::Stderr => &STDERR_COLORS,
    };

    // Internal tests assert literal escape codes, so under cfg(test) we default to
    // colors enabled. Still respect an explicit override installed via
    // `set_colors_enabled(...)` before the first use, so tests (here or downstream)
    // can opt in to "no color" behavior.
    #[cfg(test)]
    {
        cell.get().copied().unwrap_or(true)
    }
    #[cfg(not(test))]
    {
        *cell.get_or_init(|| check_target(target))
    }
}

/// Convenience — whether stdout gets colour. Equivalent to `colors_enabled_for(Target::Stdout)`.
#[must_use]
pub fn colors_enabled() -> bool {
    colors_enabled_for(Target::Stdout)
}

/// Force-set the color mode for *both* stdout and stderr. Callers should invoke this
/// once at startup (e.g. for tests or when the CLI knows better than the default
/// heuristic). After the first read of the relevant cell this call is a no-op for
/// that stream.
pub fn set_colors_enabled(enabled: bool) {
    let _ = STDOUT_COLORS.set(enabled);
    let _ = STDERR_COLORS.set(enabled);
}

/// ANSI color codes for terminal output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Color {
    /// Black (ANSI 30)
    Black,
    /// Red (ANSI 31)
    Red,
    /// Green (ANSI 32)
    Green,
    /// Yellow (ANSI 33)
    Yellow,
    /// Blue (ANSI 34)
    Blue,
    /// Magenta (ANSI 35)
    Magenta,
    /// Cyan (ANSI 36)
    Cyan,
    /// White (ANSI 37)
    White,
    /// 256-color palette index (ANSI 38;5;n)
    Fixed(u8),
}

impl Color {
    /// Returns the ANSI escape code for this color.
    #[must_use]
    const fn code(self) -> &'static str {
        match self {
            Self::Black => "\x1b[30m",
            Self::Red => "\x1b[31m",
            Self::Green => "\x1b[32m",
            Self::Yellow => "\x1b[33m",
            Self::Blue => "\x1b[34m",
            Self::Magenta => "\x1b[35m",
            Self::Cyan => "\x1b[36m",
            Self::White => "\x1b[37m",
            Self::Fixed(_) => "", // Handled specially in Painted::fmt
        }
    }

    /// Paint the given text with this color for use with `println!` / stdout.
    #[must_use]
    pub fn paint<S: AsRef<str>>(self, text: S) -> Painted<S> {
        Painted {
            color: self,
            text,
            target: Target::Stdout,
        }
    }

    /// Paint the given text with this color for use with `eprintln!` / stderr.
    /// Uses the stderr TTY check so a redirected stderr stays clean even when stdout
    /// is a terminal (and vice versa).
    #[must_use]
    pub fn paint_err<S: AsRef<str>>(self, text: S) -> Painted<S> {
        Painted {
            color: self,
            text,
            target: Target::Stderr,
        }
    }
}

/// A painted (colored) string that implements [`Display`].
pub struct Painted<S> {
    color: Color,
    text: S,
    target: Target,
}

impl<S: AsRef<str>> Display for Painted<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const RESET: &str = "\x1b[0m";

        if !colors_enabled_for(self.target) {
            return write!(f, "{}", self.text.as_ref());
        }

        match self.color {
            Color::Fixed(n) => write!(f, "\x1b[38;5;{n}m{}{RESET}", self.text.as_ref()),
            _ => write!(f, "{}{}{RESET}", self.color.code(), self.text.as_ref()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_color_code_basic_colors() {
        assert_eq!(Color::Black.code(), "\x1b[30m");
        assert_eq!(Color::Red.code(), "\x1b[31m");
        assert_eq!(Color::Green.code(), "\x1b[32m");
        assert_eq!(Color::Yellow.code(), "\x1b[33m");
        assert_eq!(Color::Blue.code(), "\x1b[34m");
        assert_eq!(Color::Magenta.code(), "\x1b[35m");
        assert_eq!(Color::Cyan.code(), "\x1b[36m");
        assert_eq!(Color::White.code(), "\x1b[37m");
    }

    #[test]
    fn test_color_fixed_code_returns_empty() {
        // Fixed colors are handled specially in Display, so code() returns empty
        assert_eq!(Color::Fixed(0).code(), "");
        assert_eq!(Color::Fixed(255).code(), "");
    }

    #[test]
    fn test_painted_display_basic_color() {
        let painted = Color::Red.paint("Error");
        let output = painted.to_string();
        assert_eq!(output, "\x1b[31mError\x1b[0m");
    }

    #[test]
    fn test_paint_err_targets_stderr() {
        // `paint_err` must produce a Painted tagged for the Stderr target so the
        // Display impl consults the stderr TTY check, not stdout's.
        let p = Color::Red.paint_err("Oops");
        assert_eq!(p.target, Target::Stderr);
        // Under cfg(test), `colors_enabled_for` returns true for either target, so
        // the rendered output still contains the escape sequence.
        assert_eq!(p.to_string(), "\x1b[31mOops\x1b[0m");
    }

    #[test]
    fn test_paint_defaults_to_stdout_target() {
        let p = Color::Red.paint("Hello");
        assert_eq!(p.target, Target::Stdout);
    }

    #[test]
    fn test_painted_display_fixed_color() {
        let painted = Color::Fixed(8).paint("Gray text");
        let output = painted.to_string();
        assert_eq!(output, "\x1b[38;5;8mGray text\x1b[0m");
    }

    #[test]
    fn test_painted_display_fixed_color_boundaries() {
        // Test boundary values for 256-color palette
        assert_eq!(
            Color::Fixed(0).paint("text").to_string(),
            "\x1b[38;5;0mtext\x1b[0m"
        );
        assert_eq!(
            Color::Fixed(255).paint("text").to_string(),
            "\x1b[38;5;255mtext\x1b[0m"
        );
    }

    #[test]
    fn test_painted_empty_string() {
        let painted = Color::Green.paint("");
        let output = painted.to_string();
        assert_eq!(output, "\x1b[32m\x1b[0m");
    }

    #[test]
    fn test_painted_with_special_characters() {
        let painted = Color::Blue.paint("Hello\nWorld\t!");
        let output = painted.to_string();
        assert_eq!(output, "\x1b[34mHello\nWorld\t!\x1b[0m");
    }

    #[test]
    fn test_painted_with_unicode() {
        let painted = Color::Cyan.paint("日本語 🎉 émoji");
        let output = painted.to_string();
        assert_eq!(output, "\x1b[36m日本語 🎉 émoji\x1b[0m");
    }

    #[test]
    fn test_painted_with_string_type() {
        let owned = String::from("Owned string");
        let painted = Color::Yellow.paint(owned);
        let output = painted.to_string();
        assert_eq!(output, "\x1b[33mOwned string\x1b[0m");
    }

    #[test]
    fn test_painted_with_str_reference() {
        let reference = "String reference";
        let painted = Color::Magenta.paint(reference);
        let output = painted.to_string();
        assert_eq!(output, "\x1b[35mString reference\x1b[0m");
    }

    #[test]
    fn test_color_equality() {
        assert_eq!(Color::Red, Color::Red);
        assert_ne!(Color::Red, Color::Blue);
        assert_eq!(Color::Fixed(42), Color::Fixed(42));
        assert_ne!(Color::Fixed(42), Color::Fixed(43));
    }

    #[test]
    fn test_color_clone() {
        let color = Color::Fixed(128);
        let cloned = color;
        assert_eq!(color, cloned);
    }

    #[test]
    fn test_color_debug() {
        let output = format!("{:?}", Color::Red);
        assert_eq!(output, "Red");

        let output = format!("{:?}", Color::Fixed(42));
        assert_eq!(output, "Fixed(42)");
    }

    #[test]
    fn test_all_basic_colors_format_correctly() {
        let colors = [
            (Color::Black, "\x1b[30m"),
            (Color::Red, "\x1b[31m"),
            (Color::Green, "\x1b[32m"),
            (Color::Yellow, "\x1b[33m"),
            (Color::Blue, "\x1b[34m"),
            (Color::Magenta, "\x1b[35m"),
            (Color::Cyan, "\x1b[36m"),
            (Color::White, "\x1b[37m"),
        ];

        for (color, expected_code) in colors {
            let painted = color.paint("test");
            let output = painted.to_string();
            assert!(
                output.starts_with(expected_code),
                "Color {color:?} should start with {expected_code}, got {output}"
            );
            assert!(
                output.ends_with("\x1b[0m"),
                "Color {color:?} should end with reset code"
            );
        }
    }

    #[test]
    fn test_paint_preserves_text_content() {
        let original = "The quick brown fox jumps over the lazy dog";
        let painted = Color::Red.paint(original);
        let output = painted.to_string();

        // Strip ANSI codes and verify content
        let stripped = output.replace("\x1b[31m", "").replace("\x1b[0m", "");
        assert_eq!(stripped, original);
    }
}
