/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::future::Future;

use tracing::{Dispatch, instrument::WithSubscriber, subscriber};

fn fmt_subscriber() -> tracing_subscriber::FmtSubscriber {
    // Writes to stdout, the same sink the installed global subscriber uses, so
    // it owes the reader the same answer: escapes on a terminal, plain text in a
    // redirected log. This subscriber carries `spiced`'s startup banner and its
    // fatal-error line, which are the two lines a captured log is most often
    // grepped for.
    tracing_subscriber::FmtSubscriber::builder()
        .with_ansi(ansi_colors::colors_enabled_for(ansi_colors::Target::Stdout))
        .finish()
}

/// Collapses the control characters in `message` so it logs as a single record.
///
/// A message that breaks mid-line is one a log collector cannot group or alert
/// on, and the text a log line embeds is routinely something else's: a
/// `PostgreSQL` connection failure spreads its cause over two lines, and a
/// `DataFusion` error appends `Caused by:` on its own. A formatter that
/// interpolates such a cause emits a multiline event however carefully its own
/// wording is kept to one line, so normalize at the point of interpolation
/// rather than trusting the source.
///
/// Every control character is replaced rather than dropped, so offsets into the
/// logged line still match the original message, and a message with nothing to
/// collapse is borrowed, so the common path does not allocate.
#[must_use]
pub fn single_line(message: &str) -> std::borrow::Cow<'_, str> {
    if message.contains(char::is_control) {
        std::borrow::Cow::Owned(
            message
                .chars()
                .map(|c| if c.is_control() { ' ' } else { c })
                .collect(),
        )
    } else {
        std::borrow::Cow::Borrowed(message)
    }
}

pub fn in_tracing_context<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    subscriber::with_default(fmt_subscriber(), f)
}

/// Async equivalent of [`in_tracing_context`]. Use this when the work that
/// needs a temporary subscriber is async (e.g. it `.await`s I/O). The given
/// future is polled with the temporary `FmtSubscriber` as its dispatcher, so
/// `tracing::*` events emitted from any awaited code reach a subscriber even
/// if the global subscriber has not been installed yet.
pub async fn in_tracing_context_async<F, R>(f: F) -> R
where
    F: Future<Output = R>,
{
    f.with_subscriber(Dispatch::new(fmt_subscriber())).await
}

#[cfg(test)]
mod tests {
    use super::single_line;

    /// The shape that motivates this: the `PostgreSQL` connection pool reports a
    /// failure over two lines, so a warning embedding it would break mid-record.
    #[test]
    fn single_line_collapses_an_embedded_multiline_cause() {
        let collapsed = single_line("PostgreSQL connection failed.\ndb error: FATAL: no database");

        assert_eq!(
            collapsed,
            "PostgreSQL connection failed. db error: FATAL: no database"
        );
        assert!(!collapsed.contains('\n'));
    }

    #[test]
    fn single_line_replaces_every_control_character_in_place() {
        // Replaced rather than dropped, so offsets into the line still match.
        let collapsed = single_line("a\r\nb\tc");

        assert_eq!(collapsed, "a  b c");
        assert_eq!(collapsed.len(), "a\r\nb\tc".len());
    }

    #[test]
    fn single_line_borrows_a_message_with_nothing_to_collapse() {
        assert!(matches!(
            single_line("already one line"),
            std::borrow::Cow::Borrowed(_)
        ));
    }
}
