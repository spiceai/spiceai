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

//! Byte quantity type for parsing, validating, and formatting memory/storage values.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{InvalidArgumentSnafu, Result};

const KIB: u64 = 1024;
const MIB: u64 = KIB * 1024;
const GIB: u64 = MIB * 1024;
const KB: u64 = 1000;
const MB: u64 = KB * 1000;
const GB: u64 = MB * 1000;

/// A validated byte quantity.
///
/// Stores the raw byte count and provides parsing from human-readable strings
/// (e.g. `"16Gi"`, `"32GiB"`) and formatting back to the most appropriate unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NumBytes(u64);

impl NumBytes {
    /// Create a `NumBytes` from a raw byte count.
    pub fn from_bytes(bytes: u64) -> Self {
        Self(bytes)
    }

    /// Parse a human-readable byte string (e.g. `"16Gi"`, `"1.5GiB"`, `"16GB"`, or raw bytes like `"512"`).
    ///
    /// Accepted suffixes (case-insensitive): `Gi`, `GiB`, `Mi`, `MiB`, `Ki`, `KiB`, `G`, `GB`, `M`, `MB`, `K`, `KB`, `B`, or no suffix for bytes.
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        let suffix_start = s
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(s.len());
        let (numerator, scale) = parse_decimal_number(&s[..suffix_start], s)?;

        let suffix = &s[suffix_start..];
        let multiplier = match suffix.to_ascii_lowercase().as_str() {
            "" | "b" => 1,
            "gi" | "gib" => GIB,
            "mi" | "mib" => MIB,
            "ki" | "kib" => KIB,
            "g" | "gb" => GB,
            "m" | "mb" => MB,
            "k" | "kb" => KB,
            _ => {
                return InvalidArgumentSnafu {
                    message: format!(
                        "Invalid byte suffix '{suffix}'. Expected one of: Gi, GiB, Mi, MiB, Ki, KiB, G, GB, M, MB, K, KB, B, or no suffix for bytes"
                    ),
                }
                .fail();
            }
        };

        let scaled_bytes = numerator
            .checked_mul(u128::from(multiplier))
            .ok_or_else(|| crate::error::Error::InvalidArgument {
                message: format!("Byte value '{s}' is too large"),
            })?;
        if scaled_bytes % scale != 0 {
            return InvalidArgumentSnafu {
                message: format!("Byte value '{s}' resolves to a fractional number of bytes"),
            }
            .fail();
        }

        let bytes = u64::try_from(scaled_bytes / scale).map_err(|_| {
            crate::error::Error::InvalidArgument {
                message: format!("Byte value '{s}' is too large"),
            }
        })?;

        Ok(Self(bytes))
    }

    /// Return the raw byte count.
    pub fn as_bytes(self) -> u64 {
        self.0
    }

    /// Format as a Kubernetes-style resource string without losing precision.
    ///
    /// This is the format expected by the Spice Cloud API for memory/storage fields.
    pub fn to_resource_string(self) -> String {
        self.to_parse_string()
    }

    /// Format as the most compact lossless string accepted by [`NumBytes::parse`].
    ///
    /// Picks the largest unit (Gi > Mi > Ki) for which the value is exactly divisible.
    fn to_parse_string(self) -> String {
        if self.0.is_multiple_of(GIB) {
            format!("{}Gi", self.0 / GIB)
        } else if self.0.is_multiple_of(MIB) {
            format!("{}Mi", self.0 / MIB)
        } else if self.0.is_multiple_of(KIB) {
            format!("{}Ki", self.0 / KIB)
        } else {
            self.0.to_string()
        }
    }
}

fn parse_decimal_number(value: &str, original: &str) -> Result<(u128, u128)> {
    if value.is_empty() {
        return invalid_byte_value(original);
    }

    let Some((whole, fraction)) = value.split_once('.') else {
        let numerator =
            value
                .parse::<u128>()
                .map_err(|_| crate::error::Error::InvalidArgument {
                    message: format!("Byte value too large: '{value}'"),
                })?;
        return Ok((numerator, 1));
    };

    if whole.is_empty()
        || fraction.is_empty()
        || !whole.chars().all(|c| c.is_ascii_digit())
        || !fraction.chars().all(|c| c.is_ascii_digit())
    {
        return invalid_byte_value(original);
    }

    let fraction_len = fraction.len();
    let whole = whole
        .parse::<u128>()
        .map_err(|_| crate::error::Error::InvalidArgument {
            message: format!("Byte value too large: '{value}'"),
        })?;
    let fraction = fraction
        .parse::<u128>()
        .map_err(|_| crate::error::Error::InvalidArgument {
            message: format!("Byte value too large: '{value}'"),
        })?;
    let scale = 10_u128
        .checked_pow(u32::try_from(fraction_len).map_err(|_| {
            crate::error::Error::InvalidArgument {
                message: format!("Byte value '{original}' is too precise"),
            }
        })?)
        .ok_or_else(|| crate::error::Error::InvalidArgument {
            message: format!("Byte value '{original}' is too precise"),
        })?;
    let numerator = whole
        .checked_mul(scale)
        .and_then(|whole| whole.checked_add(fraction))
        .ok_or_else(|| crate::error::Error::InvalidArgument {
            message: format!("Byte value '{original}' is too large"),
        })?;

    Ok((numerator, scale))
}

fn invalid_byte_value<T>(value: &str) -> Result<T> {
    InvalidArgumentSnafu {
        message: format!(
            "Invalid byte value '{value}'. Expected format: <number><unit> (e.g. 16Gi, 1.5GiB, 512Mi)"
        ),
    }
    .fail()
}

impl std::str::FromStr for NumBytes {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(s).map_err(|e| e.to_string())
    }
}

impl Serialize for NumBytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let value = self.to_parse_string();
        serializer.serialize_str(&value)
    }
}

impl<'de> Deserialize<'de> for NumBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        NumBytes::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for NumBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.0;
        if bytes >= GIB {
            write!(f, "{:.1} GiB", bytes as f64 / GIB as f64)
        } else if bytes >= MIB {
            write!(f, "{:.1} MiB", bytes as f64 / MIB as f64)
        } else if bytes >= KIB {
            write!(f, "{:.1} KiB", bytes as f64 / KIB as f64)
        } else {
            write!(f, "{bytes} B")
        }
    }
}

/// Format a floating-point byte count for display.
///
/// Used for metrics counters that may be fractional (e.g. disk I/O accumulated values).
pub fn format_bytes_f64(bytes: f64) -> String {
    const KIB_F: f64 = 1024.0;
    const MIB_F: f64 = KIB_F * 1024.0;
    const GIB_F: f64 = MIB_F * 1024.0;

    if bytes >= GIB_F {
        format!("{:.1} GiB", bytes / GIB_F)
    } else if bytes >= MIB_F {
        format!("{:.1} MiB", bytes / MIB_F)
    } else if bytes >= KIB_F {
        format!("{:.1} KiB", bytes / KIB_F)
    } else {
        format!("{bytes:.0} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_gi_suffix() {
        let nb = NumBytes::parse("16Gi").expect("16Gi should parse");
        assert_eq!(nb.as_bytes(), 16 * GIB);
        assert_eq!(nb.to_resource_string(), "16Gi");
    }

    #[test]
    fn parse_gib_suffix() {
        let nb = NumBytes::parse("32GiB").expect("32GiB should parse");
        assert_eq!(nb.as_bytes(), 32 * GIB);
        assert_eq!(nb.to_resource_string(), "32Gi");
    }

    #[test]
    fn parse_mi_suffix() {
        let nb = NumBytes::parse("512Mi").expect("512Mi should parse");
        assert_eq!(nb.as_bytes(), 512 * MIB);
    }

    #[test]
    fn parse_case_insensitive() {
        let nb = NumBytes::parse("8gi").expect("8gi should parse");
        assert_eq!(nb.as_bytes(), 8 * GIB);
    }

    #[test]
    fn parse_raw_bytes_without_suffix() {
        let nb = NumBytes::parse("16").expect("raw bytes should parse");
        assert_eq!(nb.as_bytes(), 16);
        assert_eq!(nb.to_resource_string(), "16");
    }

    #[test]
    fn parse_gb_suffix() {
        let nb = NumBytes::parse("16GB").expect("GB suffix should parse");
        assert_eq!(nb.as_bytes(), 16 * GB);
        assert_eq!(nb.to_resource_string(), "15625000Ki");
    }

    #[test]
    fn parse_fractional_gi_suffix() {
        let nb = NumBytes::parse("1.5Gi").expect("fractional Gi suffix should parse");
        assert_eq!(nb.as_bytes(), GIB + (GIB / 2));
        assert_eq!(nb.to_resource_string(), "1536Mi");
    }

    #[test]
    fn parse_rejects_fractional_bytes() {
        NumBytes::parse("1.5").expect_err("fractional bytes should be rejected");
    }

    #[test]
    fn parse_rejects_fractional_sub_byte_value() {
        NumBytes::parse("0.1Ki").expect_err("fractional byte result should be rejected");
    }

    #[test]
    fn parse_rejects_bad_suffix() {
        NumBytes::parse("16gibb").expect_err("unknown suffix should be rejected");
    }

    #[test]
    fn parse_rejects_no_digits() {
        NumBytes::parse("Gi").expect_err("missing digits should be rejected");
    }

    #[test]
    fn display_gib() {
        assert_eq!(
            NumBytes::parse("16Gi")
                .expect("16Gi should parse")
                .to_string(),
            "16.0 GiB"
        );
    }

    #[test]
    fn display_mib() {
        assert_eq!(NumBytes::from_bytes(512 * MIB).to_string(), "512.0 MiB");
    }

    #[test]
    fn display_small() {
        assert_eq!(NumBytes::from_bytes(42).to_string(), "42 B");
    }

    #[test]
    fn format_f64_gib() {
        let s = format_bytes_f64(2.5 * GIB as f64);
        assert_eq!(s, "2.5 GiB");
    }

    #[test]
    fn serde_roundtrip_gi() {
        let nb = NumBytes::parse("16Gi").expect("16Gi should parse");
        let json = serde_json::to_string(&nb).expect("NumBytes should serialize");
        assert_eq!(json, r#""16Gi""#);
        assert_eq!(
            serde_json::from_str::<NumBytes>(&json).expect("NumBytes should deserialize"),
            nb
        );
    }

    #[test]
    fn serde_roundtrip_mi() {
        let nb = NumBytes::parse("512Mi").expect("512Mi should parse");
        let json = serde_json::to_string(&nb).expect("NumBytes should serialize");
        assert_eq!(json, r#""512Mi""#);
        assert_eq!(
            serde_json::from_str::<NumBytes>(&json).expect("NumBytes should deserialize"),
            nb
        );
    }

    #[test]
    fn serde_roundtrip_ki() {
        let nb = NumBytes::from_bytes(4 * KIB);
        let json = serde_json::to_string(&nb).expect("NumBytes should serialize");
        assert_eq!(json, r#""4Ki""#);
        assert_eq!(
            serde_json::from_str::<NumBytes>(&json).expect("NumBytes should deserialize"),
            nb
        );
    }

    #[test]
    fn parse_rejects_overflow() {
        NumBytes::parse("99999999999999999Gi").expect_err("overflow should be rejected");
    }

    #[test]
    fn serde_roundtrip_sub_kib_values() {
        let nb = NumBytes::from_bytes(512);
        let json = serde_json::to_string(&nb).expect("NumBytes should serialize");

        assert_eq!(json, r#""512""#);
        assert_eq!(
            serde_json::from_str::<NumBytes>(&json).expect("NumBytes should deserialize"),
            nb
        );
    }

    #[test]
    fn serde_deserialize_rejects_invalid() {
        serde_json::from_str::<NumBytes>(r#""16gibb""#)
            .expect_err("invalid NumBytes JSON should be rejected");
    }
}
