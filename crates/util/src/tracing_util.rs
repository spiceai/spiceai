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

use tracing::{Dispatch, dispatcher, instrument::WithSubscriber, subscriber};

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

/// Whether events emitted from here already reach a subscriber, global or
/// thread-local.
///
/// [`subscriber::NoSubscriber`] is the dispatcher in effect until one is
/// installed, and it drops every event. Anything else is a real subscriber
/// whose sinks and filter a temporary one would replace, because a
/// thread-local default outranks the global one.
fn a_subscriber_is_listening() -> bool {
    !dispatcher::get_default(Dispatch::is::<subscriber::NoSubscriber>)
}

/// Runs `f` under a temporary subscriber, so events it emits before the global
/// subscriber is installed are not dropped. Defers to a subscriber already
/// listening rather than shadowing it.
pub fn in_tracing_context<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    if a_subscriber_is_listening() {
        return f();
    }
    subscriber::with_default(fmt_subscriber(), f)
}

/// Async equivalent of [`in_tracing_context`]. Use this when the work that
/// needs a temporary subscriber is async (e.g. it `.await`s I/O).
pub async fn in_tracing_context_async<F, R>(f: F) -> R
where
    F: Future<Output = R>,
{
    if a_subscriber_is_listening() {
        return f.await;
    }
    f.with_subscriber(Dispatch::new(fmt_subscriber())).await
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

    use super::{in_tracing_context, in_tracing_context_async, single_line};

    /// Sink for a probe subscriber, so a test can assert on what reached it.
    #[derive(Clone, Default)]
    struct ProbeWriter(Arc<Mutex<Vec<u8>>>);

    impl ProbeWriter {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("probe buffer poisoned")).into_owned()
        }
    }

    impl std::io::Write for ProbeWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("probe buffer poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for ProbeWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

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

    /// Runs `emit` under a subscriber writing to the returned buffer.
    fn logged(emit: impl FnOnce()) -> String {
        let probe = ProbeWriter::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(probe.clone()),
        );

        tracing::subscriber::with_default(subscriber, emit);
        probe.contents()
    }

    /// A thread-local default outranks the global subscriber, so installing one
    /// over a subscriber that is already listening would silently drop the event
    /// from its sinks and apply the wrong filter.
    #[test]
    fn in_tracing_context_defers_to_a_subscriber_already_listening() {
        let logged = logged(|| {
            in_tracing_context(|| tracing::warn!("failed to initialize sql results cache"));
        });

        assert!(
            logged.contains("failed to initialize sql results cache"),
            "the event must reach the subscriber already installed, got: {logged}"
        );
    }

    /// Same decision in the async helper, which is the one callers use to wrap
    /// a whole window.
    #[test]
    fn in_tracing_context_async_defers_to_a_subscriber_already_listening() {
        let logged = logged(|| {
            futures::executor::block_on(in_tracing_context_async(async {
                tracing::warn!("failed to load the secret stores");
            }));
        });

        assert!(
            logged.contains("failed to load the secret stores"),
            "the event must reach the subscriber already installed, got: {logged}"
        );
    }
}
