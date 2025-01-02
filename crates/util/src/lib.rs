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
    time::{Duration, SystemTime, SystemTimeError},
};

pub mod fibonacci_backoff;
pub use backoff::future::retry;
pub use backoff::Error as RetryError;

#[allow(clippy::cast_precision_loss)]
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_possible_wrap)]
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

pub async fn shutdown_signal() {
    shutdown_signal_impl().await;
}

#[cfg(unix)]
async fn shutdown_signal_impl() {
    use tokio::signal::unix::{signal, SignalKind};

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
#[allow(clippy::cast_possible_truncation)]
pub fn humantime_elapsed(time: SystemTime) -> Result<String, SystemTimeError> {
    time.elapsed()
        .map(|elapsed| {
            humantime::format_duration(Duration::from_millis(elapsed.as_millis() as u64))
        })
        .map(|s| format!("{s}"))
}

#[cfg(test)]
mod tests {
    // generate test for human_readable_bytes

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
}
