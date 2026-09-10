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

//! Translates dependency diagnostics before they reach Spice's log sinks.

use tracing_log::{
    LogTracer,
    log::{self, Level, LevelFilter, Log, Metadata, Record, SetLoggerError},
};

const OPENDAL_RETRY_TARGET: &str = "opendal::layers::retry";

pub(super) fn init() -> Result<(), SetLoggerError> {
    log::set_boxed_logger(Box::new(DependencyLogTracer::default()))?;
    log::set_max_level(LevelFilter::Trace);
    Ok(())
}

#[derive(Default)]
struct DependencyLogTracer {
    inner: LogTracer,
}

impl DependencyLogTracer {
    fn forward(&self, record: &Record<'_>, level: Level, args: std::fmt::Arguments<'_>) {
        self.inner.log(
            &Record::builder()
                .metadata(record.metadata().clone())
                .level(level)
                .module_path(record.module_path())
                .file(record.file())
                .line(record.line())
                .args(args)
                .build(),
        );
    }
}

impl Log for DependencyLogTracer {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }

        if record.target() == OPENDAL_RETRY_TARGET && record.level() == Level::Warn {
            // OpenDAL's default retry interceptor formats its error into the
            // log record. Recognize only the supported diagnostic; a different
            // error or dependency format must still reach the normal bridge.
            let diagnostic = record.args().to_string();
            if let Some(message) = s3_read_retry_message(&diagnostic) {
                // Forward directly to preserve source locations and the current
                // span, and avoid re-entering the global dependency logger.
                self.forward(record, Level::Warn, format_args!("{message}"));
                self.forward(
                    record,
                    Level::Debug,
                    format_args!("S3 read retry diagnostic: {diagnostic:?}"),
                );
                return;
            }
        }

        self.inner.log(record);
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

/// Formats the known connection-closure warning without inferring a logical
/// table name from an object path, which can have an arbitrary layout.
fn s3_read_retry_message(diagnostic: &str) -> Option<String> {
    let (retry, error) = diagnostic.split_once(" because: ")?;
    let (attempt, delay) = retry
        .strip_prefix("will retry Read (attempt ")?
        .split_once(") after ")?;
    let attempt = attempt.parse::<u32>().ok().filter(|attempt| *attempt > 0)?;
    let delay = delay.strip_suffix('s')?;
    let seconds = delay.parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    let delay = format!("{seconds:.3}");
    let delay = delay.trim_end_matches('0').trim_end_matches('.');

    let (context, source) = error
        .strip_prefix("Unexpected (temporary) at read => send http request\n\nContext:\n")?
        .split_once("\nSource:\n")?;
    let context_value = |key: &str| {
        let mut values = context.lines().filter_map(|line| {
            let (name, value) = line.trim_start().split_once(": ")?;
            (name == key).then_some(value)
        });
        let value = values.next()?;
        values.next().is_none().then_some(value)
    };
    if context_value("service")? != "s3" || context_value("called")? != "http_util::Client::send" {
        return None;
    }

    let object_url = context_value("url")?;
    if source.trim()
        != format!(
            "error sending request for url ({object_url}): client error (SendRequest): connection closed before message completed"
        )
    {
        return None;
    }
    let mut url = reqwest::Url::parse(object_url).ok()?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return None;
    }
    url.set_username("").ok()?;
    url.set_password(None).ok()?;
    url.set_query(None);
    url.set_fragment(None);
    let url = url.as_str().replace('\'', "%27");

    Some(format!(
        "Reading file '{url}' (S3) was interrupted, so Spice will retry in {delay}s (attempt {attempt}). Cause: The connection closed before the response completed. If this continues, check network access to S3 and any proxy timeouts. See: https://spiceai.org/docs/troubleshooting"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::{EnvFilter, fmt::MakeWriter, prelude::*};

    const URL: &str =
        "https://bucket.s3.us-east-1.amazonaws.com/team_app/task_history/metadata/test-m0.avro";

    fn diagnostic(url: &str) -> String {
        format!(
            "will retry Read (attempt 1) after 1s because: Unexpected (temporary) at read => send http request\n\nContext:\n   url: {url}\n   called: http_util::Client::send\n   service: s3\n   path: team_app/task_history/metadata/test-m0.avro\n   range: 0-\n\nSource:\n   error sending request for url ({url}): client error (SendRequest): connection closed before message completed\n"
        )
    }

    #[test]
    fn connection_closed_message_names_file_impact_retry_and_action() {
        assert_eq!(
            s3_read_retry_message(&diagnostic(URL)).expect("recognized connection closure"),
            format!(
                "Reading file '{URL}' (S3) was interrupted, so Spice will retry in 1s (attempt 1). Cause: The connection closed before the response completed. If this continues, check network access to S3 and any proxy timeouts. See: https://spiceai.org/docs/troubleshooting"
            )
        );
    }

    #[test]
    fn preserves_retry_number_and_fractional_delay() {
        let message = diagnostic(URL)
            .replace("attempt 1", "attempt 3")
            .replace("after 1s", "after 4.125s");
        assert!(
            s3_read_retry_message(&message)
                .expect("recognized fractional delay")
                .contains("Spice will retry in 4.125s (attempt 3)")
        );
        let message = diagnostic(URL).replace("after 1s", "after 1.6064641480000001s");
        assert!(
            s3_read_retry_message(&message)
                .expect("recognized jitter delay")
                .contains("Spice will retry in 1.606s (attempt 1)")
        );
    }

    #[test]
    fn omits_url_credentials_queries_and_fragments() {
        let message = s3_read_retry_message(&diagnostic(
            "https://user:password@bucket.example/object?X-Amz-Signature=secret#fragment",
        ))
        .expect("recognized URL");
        assert!(message.contains("file 'https://bucket.example/object'"));
        for secret in ["user", "password", "X-Amz", "secret", "fragment"] {
            assert!(!message.contains(secret), "must omit {secret}");
        }
    }

    #[test]
    fn unrecognized_errors_and_formats_are_not_rewritten() {
        let original = diagnostic(URL);
        for message in [
            original.replace("Read (attempt", "Write (attempt"),
            original.replace("(temporary)", "(persistent)"),
            original.replace("service: s3", "service: gcs"),
            original.replace("attempt 1", "attempt 0"),
            original.replace("after 1s", "after NaNs"),
            original.replace("after 1s", "after -1s"),
            original.replace("after 1s", "after infs"),
            original.replace(
                "connection closed before message completed",
                "connection refused",
            ),
            original.replace("send http request", "read data from http response"),
            original.replace("Context:", "Details:"),
            original.replace("   url:", "   uri:"),
            original.replace("   url:", &format!("   url: {URL}\n   url:")),
            original.replace("   service:", "   service: s3\n   service:"),
            original.replace("http_util::Client::send", "a different caller"),
            original.replace("client error (SendRequest)", "another error"),
            original.replace(
                "before message completed",
                "before message completed (extra cause)",
            ),
            original.replace(URL, "not a URL"),
            "new upstream diagnostic format".to_string(),
        ] {
            assert!(s3_read_retry_message(&message).is_none(), "{message}");
        }
    }

    #[derive(Clone, Default)]
    struct Writer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Writer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("writer lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Writer {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn output(filter: &str, target: &str, level: Level, message: &str) -> String {
        let writer = Writer::default();
        let subscriber = tracing_subscriber::registry()
            .with(EnvFilter::new(filter))
            .with(
                tracing_subscriber::fmt::layer()
                    .without_time()
                    .with_ansi(false)
                    .with_writer(writer.clone()),
            );
        tracing::subscriber::with_default(subscriber, || {
            DependencyLogTracer::default().log(
                &Record::builder()
                    .target(target)
                    .level(level)
                    .args(format_args!("{message}"))
                    .build(),
            );
        });
        String::from_utf8(writer.0.lock().expect("writer lock").clone()).expect("UTF-8 log output")
    }

    #[test]
    fn warning_is_one_line_and_diagnostics_require_debug() {
        let warning = output("warn", OPENDAL_RETRY_TARGET, Level::Warn, &diagnostic(URL));
        assert_eq!(warning.lines().count(), 1, "{warning}");
        assert!(warning.contains("WARN opendal::layers::retry: Reading file"));
        assert!(!warning.contains("SendRequest"));

        let debug = output("debug", OPENDAL_RETRY_TARGET, Level::Warn, &diagnostic(URL));
        assert_eq!(debug.lines().count(), 2, "{debug}");
        assert!(debug.contains("SendRequest"));
        assert!(debug.contains("S3 read retry diagnostic"));
    }

    #[test]
    fn bridge_preserves_filtering_and_unrecognized_records() {
        assert!(output("error", OPENDAL_RETRY_TARGET, Level::Warn, &diagnostic(URL)).is_empty());
        assert!(
            output(
                "warn,opendal::layers::retry=off",
                OPENDAL_RETRY_TARGET,
                Level::Warn,
                &diagnostic(URL)
            )
            .is_empty()
        );
        for (target, level, message) in [
            (
                OPENDAL_RETRY_TARGET,
                Level::Warn,
                "unknown retry diagnostic",
            ),
            (OPENDAL_RETRY_TARGET, Level::Error, "retries exhausted"),
            ("another_dependency", Level::Warn, "connection closed"),
        ] {
            assert!(output("warn", target, level, message).contains(message));
        }
        for (filter, target, level) in [
            ("info", OPENDAL_RETRY_TARGET, Level::Info),
            ("warn", "another_dependency", Level::Warn),
        ] {
            let message = output(filter, target, level, &diagnostic(URL));
            assert!(message.contains("SendRequest"), "{message}");
            assert!(!message.contains("Reading file"), "{message}");
        }
    }
}
