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

//! Suspend-inclusive monotonic time for writer lease deadlines.

/// Nanoseconds on a monotonic clock that counts through host suspend on Linux
/// and macOS, or `None` if the native clock cannot be read. Other targets always
/// return `Some` nanoseconds since a process-wide `Instant` epoch, which does not
/// count suspend. Values are capped at `u64::MAX - 1` to reserve `u64::MAX` for
/// gates without a deadline.
pub(crate) fn now_ns() -> Option<u64> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        native_now_ns().map(|reading| reading.min(u64::MAX - 1))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        static EPOCH: std::sync::LazyLock<std::time::Instant> =
            std::sync::LazyLock::new(std::time::Instant::now);
        Some(
            u64::try_from(EPOCH.elapsed().as_nanos())
                .unwrap_or(u64::MAX - 1)
                .min(u64::MAX - 1),
        )
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn native_now_ns() -> Option<u64> {
    #[cfg(target_os = "linux")]
    let clock = libc::CLOCK_BOOTTIME;
    // macOS CLOCK_MONOTONIC counts time asleep, unlike CLOCK_UPTIME_RAW, which
    // std's Instant uses (see Apple's clock_gettime(3) manual).
    #[cfg(target_os = "macos")]
    let clock = libc::CLOCK_MONOTONIC;
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `time` is a valid, writable timespec for the duration of the call,
    // and `clock` is a supported clock ID on this target.
    if unsafe { libc::clock_gettime(clock, &raw mut time) } != 0 {
        return None;
    }
    let seconds = u64::try_from(time.tv_sec).ok()?;
    let nanos = u64::try_from(time.tv_nsec).ok()?;
    Some(seconds.saturating_mul(1_000_000_000).saturating_add(nanos))
}

#[cfg(test)]
mod tests {
    use super::now_ns;
    use std::time::Duration;

    #[tokio::test]
    async fn boot_clock_advances_across_sleep() {
        let sleep = Duration::from_millis(20);
        let started = now_ns().expect("boot clock reading");
        let mut previous = started;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                tokio::time::sleep(sleep).await;
                let current = now_ns().expect("boot clock reading");
                assert!(current >= previous, "boot clock moved backwards");
                if current - started >= u64::try_from(sleep.as_nanos()).expect("short sleep") {
                    break;
                }
                previous = current;
            }
        })
        .await
        .expect("boot clock must advance by at least the sleep duration");
    }
}
