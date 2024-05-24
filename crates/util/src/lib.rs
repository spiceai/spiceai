/*
Copyright 2024 The Spice.ai OSS Authors

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
}
