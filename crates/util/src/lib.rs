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

pub mod fibonacci_backoff;
pub mod retry_strategy;
pub mod security;
pub use backoff::Error as RetryError;
pub use backoff::ExponentialBackoff;
pub use backoff::future::retry;
mod tracing_util;
use tokio::{sync::oneshot, time::Instant};
pub use tracing_util::in_tracing_context;
pub mod arrow;
pub mod expr;
pub mod stream_utils;
pub mod time_format;

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
    value.to_lowercase() == "enabled"
}

pub async fn shutdown_signal() {
    shutdown_signal_impl().await;
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
        .map(|s| format!("{s}"))
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

#[cfg(test)]
mod tests {
    // generate test for human_readable_bytes

    use crate::distribute_nulls;

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
}
