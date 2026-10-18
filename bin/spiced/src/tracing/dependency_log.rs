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

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tracing_log::{
    LogTracer,
    log::{self, Level, LevelFilter, Log, Metadata, Record, SetLoggerError},
};

static DEFAULT_RULES: &[&dyn LogTransformRule] = &[&OpenDalS3ReadRetryRule];

/// Recognizes a dependency diagnostic and supplies its replacement message.
///
/// Rules are tried in registration order until one returns a transformation.
/// Return `None` for unsupported records so later rules can try them, or the
/// original record can pass through unchanged if no rule matches.
trait LogTransformRule: Send + Sync {
    fn transform(&self, record: &Record<'_>) -> Option<LogTransform>;
}

/// Replaces the message while preserving the record's level and source metadata.
struct LogTransform {
    message: String,
    diagnostic: Option<LogDiagnostic>,
    /// Repeated warnings with the same key collapse to the first one, plus a
    /// count when the burst keeps going. `None` emits every record.
    collapse_key: Option<String>,
}

/// Optional detail emitted at DEBUG, with the message escaped onto one line.
struct LogDiagnostic {
    label: &'static str,
    message: String,
}

pub(super) fn init() -> Result<(), SetLoggerError> {
    log::set_boxed_logger(Box::new(DependencyLogTracer::default()))?;
    log::set_max_level(LevelFilter::Trace);
    Ok(())
}

struct DependencyLogTracer {
    inner: LogTracer,
    rules: &'static [&'static dyn LogTransformRule],
    collapse: Mutex<CollapseGate>,
}

impl Default for DependencyLogTracer {
    fn default() -> Self {
        Self {
            inner: LogTracer::default(),
            rules: DEFAULT_RULES,
            collapse: Mutex::new(CollapseGate::default()),
        }
    }
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

    fn emit(&self, record: &Record<'_>, transform: LogTransform) {
        let skipped = if let Some(key) = transform.collapse_key.as_deref() {
            let decision = {
                let mut gate = self
                    .collapse
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                gate.decide(key, Instant::now())
            };
            match decision {
                // The warning is the sampled line. The debug diagnostic stays
                // so an operator who turned diagnostics on can still see the
                // retries the warning no longer lists.
                Collapse::Suppress => {
                    self.forward_diagnostic(record, transform.diagnostic.as_ref());
                    return;
                }
                Collapse::Emit { skipped } => skipped,
            }
        } else {
            0
        };
        let message = with_skipped(&transform.message, skipped);

        // Forward directly to preserve source locations and the current span,
        // and avoid re-entering the global dependency logger.
        self.forward(record, record.level(), format_args!("{message}"));
        self.forward_diagnostic(record, transform.diagnostic.as_ref());
    }

    fn forward_diagnostic(&self, record: &Record<'_>, diagnostic: Option<&LogDiagnostic>) {
        if let Some(diagnostic) = diagnostic {
            self.forward(
                record,
                Level::Debug,
                format_args!("{}: {:?}", diagnostic.label, diagnostic.message),
            );
        }
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

        if let Some(transform) = self.rules.iter().find_map(|rule| rule.transform(record)) {
            self.emit(record, transform);
            return;
        }

        // A retry format this build does not recognize still has to be one
        // line, and a burst of identical lines has to collapse. The first
        // copy stays so the new format is visible.
        if record.target() == OPENDAL_RETRY_TARGET && record.level() == Level::Warn {
            let message = one_line(&record.args().to_string());
            self.emit(
                record,
                LogTransform {
                    collapse_key: Some(format!("opendal-retry:{message}")),
                    message,
                    diagnostic: None,
                },
            );
            return;
        }

        self.inner.log(record);
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

const OPENDAL_RETRY_TARGET: &str = "opendal::layers::retry";

struct OpenDalS3ReadRetryRule;

impl LogTransformRule for OpenDalS3ReadRetryRule {
    fn transform(&self, record: &Record<'_>) -> Option<LogTransform> {
        if record.target() != OPENDAL_RETRY_TARGET || record.level() != Level::Warn {
            return None;
        }

        // OpenDAL's default retry interceptor formats its error into the log
        // record. Recognize the diagnostics this runtime produces; a different
        // error still reaches the bridge, flattened and collapsed.
        let diagnostic = record.args().to_string();
        let recognized = recognize_opendal_read_retry(&diagnostic)?;
        Some(LogTransform {
            message: recognized.message,
            collapse_key: Some(recognized.collapse_key),
            diagnostic: Some(LogDiagnostic {
                label: "S3 read retry diagnostic",
                message: diagnostic,
            }),
        })
    }
}

struct RecognizedRetry {
    message: String,
    collapse_key: String,
}

fn recognize_opendal_read_retry(diagnostic: &str) -> Option<RecognizedRetry> {
    let (attempt, delay, error) = split_read_retry(diagnostic)?;
    if let Some(message) = s3_connection_closed_message(attempt, &delay, error) {
        return Some(RecognizedRetry {
            message,
            collapse_key: format!("s3-connection-closed:{attempt}"),
        });
    }
    if let Some(message) = io_timeout_message(attempt, &delay, error) {
        return Some(RecognizedRetry {
            message,
            collapse_key: format!("object-store-io-timeout:{attempt}"),
        });
    }
    None
}

/// Formats the known connection-closure warning without inferring a logical
/// table name from an object path, which can have an arbitrary layout.
#[cfg(test)]
fn s3_read_retry_message(diagnostic: &str) -> Option<String> {
    let (attempt, delay, error) = split_read_retry(diagnostic)?;
    s3_connection_closed_message(attempt, &delay, error)
}

fn split_read_retry(diagnostic: &str) -> Option<(u32, String, &str)> {
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
    let delay = delay
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string();
    Some((attempt, delay, error))
}

fn context_value<'a>(context: &'a str, key: &str) -> Option<&'a str> {
    let mut values = context.lines().filter_map(|line| {
        let (name, value) = line.trim_start().split_once(": ")?;
        (name == key).then_some(value)
    });
    let value = values.next()?;
    values.next().is_none().then_some(value)
}

fn s3_connection_closed_message(attempt: u32, delay: &str, error: &str) -> Option<String> {
    let (context, source) = error
        .strip_prefix("Unexpected (temporary) at read => send http request\n\nContext:\n")?
        .split_once("\nSource:\n")?;
    if context_value(context, "service")? != "s3"
        || context_value(context, "called")? != "reqwest::send"
    {
        return None;
    }

    let object_url = context_value(context, "url")?;
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

/// OpenDAL's timeout layer reports only the budget, not the object. The
/// message names that budget so a burst of identical timeouts is actionable.
fn io_timeout_message(attempt: u32, delay: &str, error: &str) -> Option<String> {
    let rest = error.strip_prefix(
        "Unexpected (temporary) at read => io operation timeout reached\n\nContext:\n",
    )?;
    let context = rest
        .split_once("\nSource:\n")
        .map_or(rest, |(context, _)| context);
    let timeout = context_value(context, "timeout")?;
    let seconds = timeout.parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    let timeout = format!("{seconds:.3}");
    let timeout = timeout.trim_end_matches('0').trim_end_matches('.');

    Some(format!(
        "Reading a file timed out after {timeout}s, so Spice will retry in {delay}s (attempt {attempt}). If this continues, check that storage is reachable and that any proxy timeout is longer than {timeout}s. See: https://spiceai.org/docs/troubleshooting"
    ))
}

/// A metadata refresh can time out hundreds of reads in the same second.
/// Emit the first, then again as the burst crosses each threshold, and again
/// after a quiet minute. Everything in between is counted, not logged.
const COLLAPSE_WINDOW: Duration = Duration::from_secs(60);
const COLLAPSE_EMIT_AT: [u64; 4] = [1, 100, 1_000, 10_000];

/// Distinct retry classes remembered at once. A repeated class updates its
/// one entry and does not walk the map. A class this build has not recognized
/// is keyed by its whole diagnostic, so a storm of different objects would
/// otherwise keep one entry each. Past the cap, the class that has been quiet
/// the longest is dropped. Its unreported count goes with it: the next time
/// that class appears, it logs as a new one.
const COLLAPSE_MAX_WINDOWS: usize = 64;

struct CollapseGate {
    windows: HashMap<String, CollapseWindow>,
}

struct CollapseWindow {
    start: Instant,
    last_seen: Instant,
    seen: u64,
    suppressed: u64,
}

enum Collapse {
    Emit { skipped: u64 },
    Suppress,
}

impl Default for CollapseGate {
    fn default() -> Self {
        Self {
            windows: HashMap::new(),
        }
    }
}

impl CollapseWindow {
    fn advance(&mut self, now: Instant) -> Collapse {
        self.last_seen = now;
        if self.seen > 0 && now.duration_since(self.start) >= COLLAPSE_WINDOW {
            let skipped = self.suppressed;
            self.start = now;
            self.seen = 1;
            self.suppressed = 0;
            return Collapse::Emit { skipped };
        }
        self.seen += 1;
        if COLLAPSE_EMIT_AT.contains(&self.seen) {
            let skipped = self.suppressed;
            self.suppressed = 0;
            return Collapse::Emit { skipped };
        }
        self.suppressed += 1;
        Collapse::Suppress
    }
}

impl CollapseGate {
    fn decide(&mut self, key: &str, now: Instant) -> Collapse {
        if let Some(window) = self.windows.get_mut(key) {
            return window.advance(now);
        }
        self.evict_least_recent();
        let mut window = CollapseWindow {
            start: now,
            last_seen: now,
            seen: 0,
            suppressed: 0,
        };
        let decision = window.advance(now);
        self.windows.insert(key.to_string(), window);
        decision
    }

    /// Called only when admitting a class the map does not already hold, and
    /// only once the map is full. The repeated-class path never scans.
    fn evict_least_recent(&mut self) {
        if self.windows.len() < COLLAPSE_MAX_WINDOWS {
            return;
        }
        let Some(oldest) = self
            .windows
            .iter()
            .min_by_key(|(_, window)| window.last_seen)
            .map(|(key, _)| key.clone())
        else {
            return;
        };
        self.windows.remove(&oldest);
    }
}

fn with_skipped(message: &str, skipped: u64) -> String {
    if skipped == 0 {
        return message.to_string();
    }
    let note = format!("{skipped} similar retries were not logged.");
    match message.rsplit_once(" See: ") {
        Some((head, tail)) => format!("{head} {note} See: {tail}"),
        None => format!("{message} {note}"),
    }
}

fn one_line(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut spaced = false;
    for c in message.chars() {
        if c.is_whitespace() {
            if !spaced && !out.is_empty() {
                out.push(' ');
                spaced = true;
            }
        } else {
            out.push(c);
            spaced = false;
        }
    }
    out
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
            "will retry Read (attempt 1) after 1s because: Unexpected (temporary) at read => send http request\n\nContext:\n   url: {url}\n   called: reqwest::send\n   service: s3\n   path: team_app/task_history/metadata/test-m0.avro\n   range: 0-\n\nSource:\n   error sending request for url ({url}): client error (SendRequest): connection closed before message completed\n"
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
            original.replace("reqwest::send", "a different caller"),
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

    fn timeout_diagnostic() -> String {
        "will retry Read (attempt 1) after 1s because: Unexpected (temporary) at read => io operation timeout reached\n\nContext:\n   timeout: 10\n".to_string()
    }

    #[test]
    fn io_timeout_message_names_the_budget_the_retry_and_the_action() {
        assert_eq!(
            recognize_opendal_read_retry(&timeout_diagnostic())
                .expect("recognized io timeout")
                .message,
            "Reading a file timed out after 10s, so Spice will retry in 1s (attempt 1). If this continues, check that storage is reachable and that any proxy timeout is longer than 10s. See: https://spiceai.org/docs/troubleshooting"
        );
    }

    #[test]
    fn io_timeout_burst_logs_the_first_read_and_the_size_of_the_burst() {
        let tracer = DependencyLogTracer::default();
        let diagnostic = timeout_diagnostic();
        let mut combined = String::new();
        for _ in 0..120 {
            combined.push_str(&output_with_tracer(
                &tracer,
                "warn",
                OPENDAL_RETRY_TARGET,
                Level::Warn,
                &diagnostic,
            ));
        }
        let warns: Vec<_> = combined
            .lines()
            .filter(|line| line.contains("WARN"))
            .collect();
        assert_eq!(warns.len(), 2, "{combined}");
        assert!(warns[0].contains("timed out after 10s"), "{combined}");
        assert!(warns[0].contains("attempt 1"), "{combined}");
        assert!(!warns[0].contains("similar retries"), "{combined}");
        assert!(
            warns[1].contains("98 similar retries were not logged"),
            "{combined}"
        );
        assert!(
            !combined.contains("io operation timeout reached"),
            "{combined}"
        );
        for line in &warns {
            assert_eq!(line.matches("WARN").count(), 1, "{line}");
        }

        let second_attempt = diagnostic.replace("attempt 1", "attempt 2");
        let again = output_with_tracer(
            &tracer,
            "warn",
            OPENDAL_RETRY_TARGET,
            Level::Warn,
            &second_attempt,
        );
        assert!(again.contains("attempt 2"), "{again}");
        assert!(!again.contains("similar retries"), "{again}");
    }

    #[test]
    fn collapse_gate_retains_at_most_64_classes() {
        let mut gate = CollapseGate::default();
        let start = Instant::now();
        for index in 0_u32..80 {
            let at = start + Duration::from_secs(u64::from(index));
            let decision = gate.decide(&format!("class-{index}"), at);
            assert!(
                matches!(decision, Collapse::Emit { skipped: 0 }),
                "first sight of class-{index} should log"
            );
        }
        assert_eq!(gate.windows.len(), COLLAPSE_MAX_WINDOWS);
        assert!(!gate.windows.contains_key("class-0"));
        assert!(!gate.windows.contains_key("class-15"));
        assert!(gate.windows.contains_key("class-16"));
        assert!(gate.windows.contains_key("class-79"));

        // Repeating a class the map still holds must not drop another class.
        let repeat_at = start + Duration::from_secs(80);
        for _ in 0..50 {
            let decision = gate.decide("class-79", repeat_at);
            assert!(
                matches!(decision, Collapse::Suppress),
                "class-79 is inside its first window"
            );
        }
        assert_eq!(gate.windows.len(), COLLAPSE_MAX_WINDOWS);
        assert!(gate.windows.contains_key("class-16"));
        assert!(gate.windows.contains_key("class-79"));

        let decision = gate.decide("class-new", repeat_at + Duration::from_secs(1));
        assert!(matches!(decision, Collapse::Emit { skipped: 0 }));
        assert_eq!(gate.windows.len(), COLLAPSE_MAX_WINDOWS);
        assert!(!gate.windows.contains_key("class-16"));
        assert!(gate.windows.contains_key("class-new"));
        assert!(gate.windows.contains_key("class-79"));
    }

    #[test]
    fn suppressed_retry_still_logs_the_debug_diagnostic() {
        let tracer = DependencyLogTracer::default();
        let diagnostic = timeout_diagnostic();
        let first = output_with_tracer(
            &tracer,
            "debug",
            OPENDAL_RETRY_TARGET,
            Level::Warn,
            &diagnostic,
        );
        assert!(first.contains("WARN"), "{first}");
        assert!(first.contains("S3 read retry diagnostic"), "{first}");

        let second = output_with_tracer(
            &tracer,
            "debug",
            OPENDAL_RETRY_TARGET,
            Level::Warn,
            &diagnostic,
        );
        assert!(
            !second.contains("WARN"),
            "the repeated warning is collapsed: {second}"
        );
        assert!(
            second.contains("S3 read retry diagnostic"),
            "the collapsed retry still keeps its diagnostic: {second}"
        );
        assert!(second.contains("io operation timeout reached"), "{second}");

        let at_warn = output_with_tracer(
            &tracer,
            "warn",
            OPENDAL_RETRY_TARGET,
            Level::Warn,
            &diagnostic,
        );
        assert!(
            at_warn.is_empty(),
            "a warn filter does not show the diagnostic: {at_warn}"
        );
    }

    #[test]
    fn collapse_gate_reports_the_quiet_gap_on_the_next_event() {
        let mut gate = CollapseGate::default();
        let start = Instant::now();
        assert!(matches!(
            gate.decide("k", start),
            Collapse::Emit { skipped: 0 }
        ));
        assert!(matches!(gate.decide("k", start), Collapse::Suppress));
        assert!(matches!(
            gate.decide("k", start + COLLAPSE_WINDOW),
            Collapse::Emit { skipped: 1 }
        ));
    }

    #[test]
    fn unrecognized_opendal_retry_is_one_line_and_a_repeat_is_not_logged() {
        let tracer = DependencyLogTracer::default();
        let message = "will retry Read (attempt 1) after 1s because: something new\n\nContext:\n   detail: kept\n";
        let first = output_with_tracer(&tracer, "warn", OPENDAL_RETRY_TARGET, Level::Warn, message);
        assert_eq!(first.lines().count(), 1, "{first}");
        assert!(first.contains("something new"), "{first}");
        assert!(first.contains("detail: kept"), "{first}");
        assert!(!first.contains("timed out after"), "{first}");
        let second =
            output_with_tracer(&tracer, "warn", OPENDAL_RETRY_TARGET, Level::Warn, message);
        assert!(second.is_empty(), "{second}");
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
        output_with_tracer(
            &DependencyLogTracer::default(),
            filter,
            target,
            level,
            message,
        )
    }

    fn output_with_tracer(
        tracer: &DependencyLogTracer,
        filter: &str,
        target: &str,
        level: Level,
        message: &str,
    ) -> String {
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
            tracer.log(
                &Record::builder()
                    .target(target)
                    .level(level)
                    .args(format_args!("{message}"))
                    .build(),
            );
        });
        String::from_utf8(writer.0.lock().expect("writer lock").clone()).expect("UTF-8 log output")
    }

    struct ReplacementRule {
        target: &'static str,
        message: &'static str,
        diagnostic: Option<&'static str>,
    }

    impl LogTransformRule for ReplacementRule {
        fn transform(&self, record: &Record<'_>) -> Option<LogTransform> {
            (record.target() == self.target).then(|| LogTransform {
                message: self.message.to_string(),
                diagnostic: self.diagnostic.map(|message| LogDiagnostic {
                    label: "Dependency diagnostic",
                    message: message.to_string(),
                }),
                collapse_key: None,
            })
        }
    }

    #[test]
    fn registered_rules_use_the_first_matching_transform() {
        let tracer = DependencyLogTracer {
            rules: &[
                &OpenDalS3ReadRetryRule,
                &ReplacementRule {
                    target: "another_dependency",
                    message: "First matching rule",
                    diagnostic: None,
                },
                &ReplacementRule {
                    target: "another_dependency",
                    message: "Later matching rule",
                    diagnostic: None,
                },
            ],
            ..DependencyLogTracer::default()
        };
        let output = output_with_tracer(
            &tracer,
            "debug",
            "another_dependency",
            Level::Error,
            "Original error",
        );
        assert_eq!(output, "ERROR another_dependency: First matching rule\n");
    }

    #[test]
    fn rules_can_supply_debug_diagnostics() {
        let tracer = DependencyLogTracer {
            rules: &[&ReplacementRule {
                target: "another_dependency",
                message: "Friendly message",
                diagnostic: Some("Original error\nDetails"),
            }],
            ..DependencyLogTracer::default()
        };
        for (filter, expected) in [
            ("warn", " WARN another_dependency: Friendly message\n"),
            (
                "debug",
                " WARN another_dependency: Friendly message\nDEBUG another_dependency: Dependency diagnostic: \"Original error\\nDetails\"\n",
            ),
        ] {
            assert_eq!(
                output_with_tracer(
                    &tracer,
                    filter,
                    "another_dependency",
                    Level::Warn,
                    "Original error",
                ),
                expected
            );
        }
    }

    #[test]
    fn records_pass_through_when_no_rule_matches_or_no_rules_are_registered() {
        let rules: &[&'static [&'static dyn LogTransformRule]] = &[
            &[],
            &[&ReplacementRule {
                target: "another_dependency",
                message: "Replacement message",
                diagnostic: None,
            }],
        ];
        for rules in rules {
            let tracer = DependencyLogTracer {
                rules,
                ..DependencyLogTracer::default()
            };
            assert_eq!(
                output_with_tracer(&tracer, "debug", "unmatched", Level::Warn, "Original error"),
                " WARN unmatched: Original error\n"
            );
        }
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
