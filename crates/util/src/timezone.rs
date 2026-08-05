/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Recognising the many spellings an Arrow schema uses for UTC.
//!
//! Arrow timezone metadata is a free-form string that is never canonicalised, so
//! the same zone arrives spelled differently depending on the source: Iceberg
//! maps every `timestamptz` to the fixed offset `+00:00`, most connectors emit
//! `UTC`, and RFC 3339 parsing yields `Z`. Engines disagree over which of those
//! they accept, so code that hands a zone to an engine has to recognise the
//! equivalent spellings rather than compare against a single one.

/// The spelling to use when a UTC-equivalent zone has to be named for an engine.
pub const CANONICAL_UTC: &str = "UTC";

/// Returns `true` when `tz` denotes UTC, in any spelling an Arrow schema carries.
///
/// Covers the named zones (`UTC`, `Z`, `GMT`, `Etc/UTC`, …) and every zero
/// fixed-offset spelling (`+00:00`, `-00:00`, `+0000`, `+00`). A non-zero offset
/// such as `-05:00` denotes a different zone and returns `false`.
#[must_use]
pub fn is_utc(tz: &str) -> bool {
    let tz = tz.trim();

    if is_zero_offset(tz) {
        return true;
    }

    matches!(
        tz.to_ascii_lowercase().as_str(),
        "utc" | "uct" | "gmt" | "z" | "zulu" | "universal" | "etc/utc" | "etc/gmt"
    )
}

/// Returns `true` for a fixed-offset spelling whose offset is zero.
///
/// Accepts the forms Arrow producers actually emit — `+00:00`, `-00:00`, `+0000`,
/// `+00`, `+0:00` — and requires both halves to be present and zero, so a
/// malformed `+00:` or `+:00` is rejected rather than read as UTC. Any non-zero
/// digit makes it a different zone.
fn is_zero_offset(tz: &str) -> bool {
    let Some(offset) = tz.strip_prefix(['+', '-']) else {
        return false;
    };

    let (hours, minutes) = match offset.split_once(':') {
        Some((hours, minutes)) => (hours, minutes),
        // `+0000` packs both halves; the `is_ascii` guard keeps the split on a
        // character boundary, since a 4-byte offset may be one multi-byte char.
        None if offset.len() == 4 && offset.is_ascii() => (&offset[..2], &offset[2..]),
        // `+00` names only the hours, so the minutes are an implicit zero.
        None => (offset, "0"),
    };

    is_all_zeros(hours) && is_all_zeros(minutes)
}

/// Returns `true` for a non-empty run of ASCII `0`s.
fn is_all_zeros(part: &str) -> bool {
    !part.is_empty() && part.bytes().all(|byte| byte == b'0')
}

#[cfg(test)]
mod tests {
    use super::{is_utc, is_zero_offset};

    #[test]
    fn named_utc_zones_are_utc() {
        for tz in ["UTC", "utc", "uTc", "GMT", "gmt", "Z", "z", "UCT"] {
            assert!(is_utc(tz), "{tz} names UTC");
        }
        for tz in ["Zulu", "Universal", "Etc/UTC", "etc/utc", "Etc/GMT"] {
            assert!(is_utc(tz), "{tz} names UTC");
        }
    }

    /// Surrounding whitespace is metadata noise, not a different zone.
    #[test]
    fn a_padded_zone_name_is_still_utc() {
        assert!(is_utc(" UTC "), "the zone name should be trimmed");
    }

    /// The #12528 case: Iceberg spells every `timestamptz` as `+00:00`, and the
    /// other zero-offset forms denote that same zone.
    #[test]
    fn zero_fixed_offsets_are_utc() {
        for tz in ["+00:00", "-00:00", "+0000", "-0000"] {
            assert!(is_utc(tz), "{tz} is a zero offset, so it is UTC");
        }
        for tz in ["+00", "-00", "+0:00", "+0"] {
            assert!(is_utc(tz), "{tz} is a zero offset, so it is UTC");
        }
    }

    /// A non-zero offset is a different zone and must never be rewritten to UTC.
    #[test]
    fn non_zero_offsets_are_not_utc() {
        for tz in ["-05:00", "+05:30", "+01:00", "-0500"] {
            assert!(!is_utc(tz), "{tz} is not UTC");
        }
        for tz in ["+10", "+00:30", "-00:01"] {
            assert!(!is_utc(tz), "{tz} is not UTC");
        }
    }

    #[test]
    fn named_non_utc_zones_are_not_utc() {
        for tz in ["America/New_York", "Asia/Tokyo", "Europe/London"] {
            assert!(!is_utc(tz), "{tz} is not UTC");
        }
        // `Etc/GMT+5` is a real, non-zero zone despite reading like a zero offset.
        for tz in ["Etc/GMT+5", "EST", ""] {
            assert!(!is_utc(tz), "{tz} is not UTC");
        }
    }

    /// Guards the byte scan against inputs that are not offsets at all — it must
    /// reject them rather than panic or read them as zero. `+°0` is multi-byte, so
    /// a scan that indexed by byte position instead of matching would split a char.
    #[test]
    fn malformed_offsets_are_rejected() {
        for tz in ["+", "-", "+00:", "+:00", "+0:0:0", "+0a"] {
            assert!(!is_zero_offset(tz), "{tz} is not a zero offset");
        }
        for tz in ["00:00", "+°0", "+0 0"] {
            assert!(!is_zero_offset(tz), "{tz} is not a zero offset");
        }
    }
}
