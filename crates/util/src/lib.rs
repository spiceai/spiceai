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

use std::{
    cmp,
    sync::Arc,
    time::{Duration, SystemTime, SystemTimeError},
};

#[cfg(feature = "http")]
pub mod cancel_guard_body;
pub mod column_reference;
pub mod fibonacci_backoff;
pub mod home_dir;
pub mod instance_id;
pub mod levenshtein;
pub mod redact;
pub mod retry_strategy;
#[cfg(feature = "datafusion")]
pub mod security;
pub mod spice_paths;
pub mod topological_ordering;
pub mod tracers;
pub use backoff::Error as RetryError;
pub use backoff::ExponentialBackoff;
pub use backoff::future::retry;
mod tracing_util;
use tokio::{sync::oneshot, time::Instant};
pub use tracing_util::{in_tracing_context, in_tracing_context_async, single_line};
pub mod arrow;
#[cfg(feature = "datafusion")]
pub mod expr;
#[cfg(feature = "datafusion")]
pub mod session_state;
#[cfg(feature = "datafusion")]
pub mod stream_utils;
pub mod time;
pub mod time_format;
#[cfg(feature = "datafusion")]
pub mod timestamp_filter;
pub mod timezone;

pub const DATAFUSION_BUG_REPORT_MESSAGE: &str = "This issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues";

/// Returns the default User-Agent string for the Spice runtime.
///
/// Format: `spiceai/{version} ({os}; {arch})`
#[must_use]
pub fn spiceai_user_agent() -> String {
    format!(
        "spiceai/{} ({}; {})",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
    )
}

#[must_use]
pub fn sanitize_datafusion_error_message(message: &str) -> String {
    message
        .replace(DATAFUSION_BUG_REPORT_MESSAGE, "")
        .trim()
        .to_string()
}

#[must_use]
pub fn format_datafusion_error(e: &(impl std::fmt::Display + ?Sized)) -> String {
    sanitize_datafusion_error_message(&e.to_string())
}

#[expect(clippy::cast_precision_loss)]
#[expect(clippy::cast_sign_loss)]
#[expect(clippy::cast_possible_truncation)]
#[expect(clippy::cast_possible_wrap)]
#[must_use]
pub fn human_readable_bytes(num: usize) -> String {
    let units = ["B", "kiB", "MiB", "GiB"];
    if num < 1 {
        return format!("{num} B");
    }
    let delimiter = 1024_f64;
    let num = num as f64;
    let exponent = cmp::min(
        (num.ln() / delimiter.ln()).floor() as usize,
        units.len() - 1,
    );
    let unit = units[exponent];
    format!("{:.2} {unit}", num / delimiter.powi(exponent as i32))
}

#[must_use]
pub fn pretty_print_number(num: usize) -> String {
    num.to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .unwrap_or(vec![])
        .join(",")
}

/// Parses a string parameter as an enabled/disabled boolean value.
///
/// Returns `true` if the value is "enabled" (case-insensitive), `false` otherwise.
/// This is a common pattern across Spice configuration parameters.
///
/// # Arguments
///
/// * `value` - The string value to parse (typically from a configuration parameter)
///
/// # Examples
///
/// ```
/// use util::parse_enabled;
///
/// assert_eq!(parse_enabled("enabled"), true);
/// assert_eq!(parse_enabled("ENABLED"), true);
/// assert_eq!(parse_enabled("disabled"), false);
/// assert_eq!(parse_enabled("anything_else"), false);
/// ```
#[must_use]
pub fn parse_enabled(value: &str) -> bool {
    value.eq_ignore_ascii_case("enabled")
}

/// Wait until the process is asked to shut down: SIGTERM or SIGINT, or on Windows
/// Ctrl-C, Ctrl-Break, close or shutdown.
///
/// Every call waits on one process-wide latch, so a caller that starts waiting after the
/// signal arrived still sees it. A listener per call would not: a signal reaches only the
/// listeners that already exist, so one delivered while the runtime starts would stop the
/// startup and never the servers that begin listening later.
pub async fn shutdown_signal() {
    wait_for_latch(&SHUTDOWN_REQUESTED, shutdown_signal_impl).await;
}

/// Whether the process has been asked to shut down, set once by the listener the first
/// [`shutdown_signal`] call starts.
static SHUTDOWN_REQUESTED: std::sync::OnceLock<tokio::sync::watch::Receiver<bool>> =
    std::sync::OnceLock::new();

/// Wait until `latch` is set. The first caller starts `listen` on its runtime, and sets
/// the latch when it completes.
///
/// When that runtime ends before `listen` completes, nothing can set the latch any more,
/// so the caller listens itself rather than report a shutdown nobody asked for.
async fn wait_for_latch<F, Fut>(
    latch: &'static std::sync::OnceLock<tokio::sync::watch::Receiver<bool>>,
    listen: F,
) where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut requested = latch
        .get_or_init(|| {
            let (set, requested) = tokio::sync::watch::channel(false);
            let listening = listen();
            tokio::spawn(async move {
                listening.await;
                // Every waiter may be gone already; the latch stays set for later ones.
                set.send_replace(true);
            });
            requested
        })
        .clone();
    if requested.wait_for(|requested| *requested).await.is_err() {
        listen().await;
    }
}

/// Waits for an additional Ctrl-C after the initial shutdown signal to trigger a forced shutdown.
pub async fn force_shutdown_signal() {
    shutdown_signal().await;

    // use 500ms as a debounce window to prevent the same Ctrl-C signal from being handled multiple times
    let last_signal_time = Instant::now();

    let (notify_ctrl_c, on_second_ctrl_c) = oneshot::channel::<()>();
    let notify_ctrl_c = Arc::new(std::sync::Mutex::new(Some(notify_ctrl_c)));

    if let Err(err) = ctrlc::set_handler({
        move || {
            if Instant::now().duration_since(last_signal_time) < Duration::from_millis(500) {
                return;
            }
            if let Some(tx) = notify_ctrl_c
                .lock()
                .ok()
                .and_then(|mut tx_opt| tx_opt.take())
            {
                tracing::debug!("Received Ctrl-C after the initial shutdown signal");
                tx.send(()).ok();
            }
        }
    }) {
        tracing::error!("Failed to set listener for Ctrl-C: {err}");
        // do not exit; otherwise, it will be interpreted as a force shutdown signal
    }
    on_second_ctrl_c.await.ok();
}

#[cfg(unix)]
async fn shutdown_signal_impl() {
    use tokio::signal::unix::{SignalKind, signal};

    let Ok(mut signal_terminate) = signal(SignalKind::terminate()) else {
        tracing::error!("Failed to listen to terminate signal");
        return;
    };
    let Ok(mut signal_interrupt) = signal(SignalKind::interrupt()) else {
        tracing::error!("Failed to listen to interrupt signal");
        return;
    };

    tokio::select! {
        _ = signal_terminate.recv() => tracing::debug!("Received SIGTERM."),
        _ = signal_interrupt.recv() => tracing::debug!("Received SIGINT."),
    };
}

#[cfg(windows)]
async fn shutdown_signal_impl() {
    use tokio::signal::windows;

    let Ok(mut signal_c) = windows::ctrl_c() else {
        tracing::error!("Failed to listen to ctrl_c signal");
        return;
    };
    let Ok(mut signal_break) = windows::ctrl_break() else {
        tracing::error!("Failed to listen to ctrl_break signal");
        return;
    };
    let Ok(mut signal_close) = windows::ctrl_close() else {
        tracing::error!("Failed to listen to ctrl_close signal");
        return;
    };
    let Ok(mut signal_shutdown) = windows::ctrl_shutdown() else {
        tracing::error!("Failed to listen to ctrl_shutdown signal");
        return;
    };

    tokio::select! {
        _ = signal_c.recv() => tracing::debug!("Received CTRL_C."),
        _ = signal_break.recv() => tracing::debug!("Received CTRL_BREAK."),
        _ = signal_close.recv() => tracing::debug!("Received CTRL_CLOSE."),
        _ = signal_shutdown.recv() => tracing::debug!("Received CTRL_SHUTDOWN."),
    };
}

/**
.

# Errors

This function will propagate `SystemTimeError` from `time.elapsed()`
*/
#[expect(clippy::cast_possible_truncation)]
pub fn humantime_elapsed(time: SystemTime) -> Result<String, SystemTimeError> {
    time.elapsed()
        .map(|elapsed| {
            humantime::format_duration(Duration::from_millis(elapsed.as_millis() as u64))
        })
        .map(|s| s.to_string())
}

/// Create a new array which is `None` at each `null_idxs`. Each element in `data` is in the new
/// array in its provided order.
pub fn distribute_nulls<T>(data: Vec<T>, null_idxs: Vec<usize>) -> Vec<Option<T>> {
    let total_len = data.len() + null_idxs.len();
    let mut result = Vec::with_capacity(total_len);
    let null_set: std::collections::HashSet<usize> = null_idxs.into_iter().collect();
    let mut data_iter = data.into_iter();

    for i in 0..total_len {
        if null_set.contains(&i) {
            result.push(None);
        } else if let Some(value) = data_iter.next() {
            result.push(Some(value));
        }
    }

    result
}

// Construct constant array by concatenating two input arrays.
pub const fn concat_arrays<T: Copy, const N: usize, const M: usize, const S: usize>(
    a: [T; N],
    b: [T; M],
) -> [T; S] {
    let mut out = [a[0]; S];
    let mut i = 0;
    while i < N {
        out[i] = a[i];
        i += 1;
    }
    let mut j = 0;
    while j < M {
        out[N + j] = b[j];
        j += 1;
    }
    out
}

#[cfg(test)]
mod shutdown_latch_tests {
    use super::wait_for_latch;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;
    use tokio::sync::{Notify, watch};

    /// A signal stand-in: like an OS signal, `Notify::notify_waiters` reaches only the
    /// listeners that exist when it fires, and none that start listening afterwards.
    fn listener(
        signal: &Arc<Notify>,
    ) -> impl Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + use<> {
        let signal = Arc::clone(signal);
        move || {
            let signal = Arc::clone(&signal);
            Box::pin(async move { signal.notified().await })
        }
    }

    // regression test for #15666: a shutdown signal that arrives before a caller starts
    // waiting still reaches that caller.
    #[tokio::test]
    async fn a_waiter_that_starts_after_the_signal_still_sees_it() {
        static LATCH: OnceLock<watch::Receiver<bool>> = OnceLock::new();
        let signal = Arc::new(Notify::new());

        // The first waiter starts listening, as the runtime's startup does.
        let first = tokio::spawn(wait_for_latch(&LATCH, listener(&signal)));
        tokio::time::sleep(Duration::from_millis(50)).await;
        signal.notify_waiters();
        tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .expect("the first waiter returns once the signal arrives")
            .expect("the first waiter does not panic");

        // A waiter that starts afterwards, as the servers do, returns at once.
        tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_latch(&LATCH, listener(&signal)),
        )
        .await
        .expect("a later waiter sees the signal that already arrived");
    }

    #[tokio::test]
    async fn no_waiter_returns_before_the_signal() {
        static LATCH: OnceLock<watch::Receiver<bool>> = OnceLock::new();
        let signal = Arc::new(Notify::new());
        let waiting = tokio::spawn(wait_for_latch(&LATCH, listener(&signal)));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!waiting.is_finished(), "no signal has arrived");
        signal.notify_waiters();
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the waiter returns once the signal arrives")
            .expect("the waiter does not panic");
    }
}

#[cfg(test)]
mod tests {
    // generate test for human_readable_bytes

    use crate::{
        DATAFUSION_BUG_REPORT_MESSAGE, distribute_nulls, format_datafusion_error,
        sanitize_datafusion_error_message,
    };

    #[test]
    fn test_human_readable_bytes() {
        assert_eq!(super::human_readable_bytes(0), "0 B");
        assert_eq!(super::human_readable_bytes(1), "1.00 B");
        assert_eq!(super::human_readable_bytes(1023), "1023.00 B");
        assert_eq!(super::human_readable_bytes(1024), "1.00 kiB");
        assert_eq!(super::human_readable_bytes(1025), "1.00 kiB");
        assert_eq!(super::human_readable_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(super::human_readable_bytes(1024 * 1024 * 1024), "1.00 GiB");
    }

    #[test]
    fn test_print_number() {
        assert_eq!(super::pretty_print_number(123), "123");
        assert_eq!(super::pretty_print_number(1023), "1,023");
        assert_eq!(super::pretty_print_number(10_231_024), "10,231,024");
    }

    #[test]
    fn test_distribute_nulls_empty_data_empty_nulls() {
        assert_eq!(distribute_nulls(vec![], vec![]), Vec::<Option<i32>>::new());
    }

    #[test]
    fn test_distribute_nulls_empty_data_with_nulls() {
        assert_eq!(
            distribute_nulls(Vec::<i32>::new(), vec![0, 1, 2]),
            vec![None, None, None]
        );
    }

    #[test]
    fn test_distribute_nulls_data_no_nulls() {
        assert_eq!(
            distribute_nulls(vec![1, 2, 3], vec![]),
            vec![Some(1), Some(2), Some(3)]
        );
    }

    #[test]
    fn test_distribute_nulls_nulls_at_beginning() {
        assert_eq!(
            distribute_nulls(vec![1, 2, 3], vec![0, 1]),
            vec![None, None, Some(1), Some(2), Some(3)]
        );
    }

    #[test]
    fn test_distribute_nulls_nulls_at_end() {
        assert_eq!(
            distribute_nulls(vec![1, 2, 3], vec![3, 4]),
            vec![Some(1), Some(2), Some(3), None, None]
        );
    }

    #[test]
    fn test_distribute_nulls_nulls_interspersed() {
        assert_eq!(
            distribute_nulls(vec![1, 2, 3], vec![0, 2, 4]),
            vec![None, Some(1), None, Some(2), None, Some(3)]
        );
    }

    #[test]
    fn test_distribute_nulls_consecutive_nulls() {
        assert_eq!(
            distribute_nulls(vec![1, 2], vec![1, 2, 3]),
            vec![Some(1), None, None, None, Some(2)]
        );
    }

    #[test]
    fn test_distribute_nulls_all_nulls() {
        assert_eq!(
            distribute_nulls(Vec::<i32>::new(), vec![0, 1, 2, 3, 4]),
            vec![None, None, None, None, None]
        );
    }

    #[test]
    fn test_distribute_nulls_single_value_single_null() {
        assert_eq!(distribute_nulls(vec![42], vec![0]), vec![None, Some(42)]);
    }

    #[test]
    fn test_distribute_nulls_single_value_no_null() {
        assert_eq!(distribute_nulls(vec![42], vec![]), vec![Some(42)]);
    }

    #[test]
    fn test_distribute_nulls_with_strings() {
        assert_eq!(
            distribute_nulls(vec!["hello".to_string(), "world".to_string()], vec![1, 3]),
            vec![
                Some("hello".to_string()),
                None,
                Some("world".to_string()),
                None
            ]
        );
    }

    #[test]
    fn test_distribute_nulls_with_vectors() {
        assert_eq!(
            distribute_nulls(
                vec![vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]],
                vec![0, 2]
            ),
            vec![
                None,
                Some(vec![1.0, 2.0]),
                None,
                Some(vec![3.0, 4.0]),
                Some(vec![5.0, 6.0])
            ]
        );
    }

    #[test]
    fn test_distribute_nulls_null_indices_unordered() {
        assert_eq!(
            distribute_nulls(vec![1, 2], vec![3, 0, 1]),
            vec![None, None, Some(1), None, Some(2)]
        );
    }

    #[test]
    fn sanitize_datafusion_error_message_removes_bug_report_suffix() {
        let input = format!("Internal error: foo. {DATAFUSION_BUG_REPORT_MESSAGE}");
        let sanitized = sanitize_datafusion_error_message(&input);
        assert_eq!(sanitized, "Internal error: foo.");
    }

    #[test]
    fn sanitize_datafusion_error_message_keeps_other_messages() {
        let input = "Error during planning: table x not found";
        let sanitized = sanitize_datafusion_error_message(input);
        assert_eq!(sanitized, input);
    }

    #[test]
    fn format_datafusion_error_removes_bug_report_suffix() {
        let input = format!("Internal error: bar. {DATAFUSION_BUG_REPORT_MESSAGE}");
        let formatted = format_datafusion_error(&input);
        assert_eq!(formatted, "Internal error: bar.");
    }

    #[test]
    fn format_datafusion_error_keeps_other_messages() {
        let input = "Error during planning: column y not found";
        let formatted = format_datafusion_error(input);
        assert_eq!(formatted, input);
    }

    #[test]
    fn sanitize_datafusion_error_message_removes_bug_report_with_newline_separator() {
        let input = format!("Internal error: baz.\n\n{DATAFUSION_BUG_REPORT_MESSAGE}");
        let sanitized = sanitize_datafusion_error_message(&input);
        assert_eq!(sanitized, "Internal error: baz.");
    }

    #[test]
    fn sanitize_datafusion_error_message_matches_actual_datafusion_internal_error() {
        use datafusion::error::DataFusionError;

        let error = DataFusionError::Internal("test internal error".to_string());
        let displayed = error.to_string();

        assert!(
            displayed.contains(DATAFUSION_BUG_REPORT_MESSAGE),
            "DataFusionError::Internal display output did not contain expected bug report suffix. Got: {displayed}"
        );

        let sanitized = sanitize_datafusion_error_message(&displayed);

        // The sanitized message should no longer include the bug report suffix
        assert!(
            !sanitized.contains(DATAFUSION_BUG_REPORT_MESSAGE),
            "Sanitized message still contains DataFusion bug report suffix: {sanitized}"
        );

        // The sanitized message should preserve the core internal error text
        assert!(
            sanitized.starts_with("Internal error: test internal error"),
            "Sanitized message did not preserve expected internal error prefix. Got: {sanitized}"
        );

        // Ensure something was actually removed
        assert!(
            sanitized.len() < displayed.len(),
            "Sanitized message was not shorter than original. Original: {displayed}, Sanitized: {sanitized}"
        );
    }
}
