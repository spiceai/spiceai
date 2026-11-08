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

//! The values a `MySQL` numeric column type holds, so a source
//! `ALTER TABLE ... MODIFY` can be judged by whether it rewrote stored values.
//!
//! `MySQL` converts every stored value when a column changes type, with no row
//! event for any of them. A conversion into a type that holds every value of
//! the old one is exact; any other can round a value (`BIGINT` to `DOUBLE`
//! turns `9007199254740993` into `9007199254740992`, `DOUBLE` to `INT` turns
//! `1.5` into `2`), even under strict SQL mode, which rejects only
//! out-of-range values. Whether the change is safe to follow is therefore a
//! question about the two source types' value sets, not about the Arrow types
//! they map to: `DECIMAL(18,0)` to `BIGINT` loses nothing, though no Arrow
//! widening turns a decimal into an integer.

use super::binlog::{decimal_precision_and_scale, type_base_name};

/// The set of values a `MySQL` numeric column type holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ValueDomain {
    /// Every integer in `min..=max`.
    Integers { min: i128, max: i128 },
    /// `DECIMAL(precision, scale)`: up to `precision - scale` integer digits
    /// and `scale` fraction digits, non-negative when `unsigned`.
    Decimal {
        precision: u8,
        scale: u8,
        unsigned: bool,
    },
    /// IEEE 754 binary floating point with `significand_bits` of precision
    /// (`f32::MANTISSA_DIGITS` for `FLOAT`, `f64::MANTISSA_DIGITS` for
    /// `DOUBLE`), non-negative when `unsigned`, and rounded to `(M, D)`
    /// digits for a `FLOAT(M,D)` or `DOUBLE(M,D)`.
    Binary {
        significand_bits: u32,
        unsigned: bool,
        digits: Option<(u8, i8)>,
    },
}

impl ValueDomain {
    /// The domain of an `information_schema.COLUMNS.COLUMN_TYPE`, or `None`
    /// for a type that is not numeric.
    fn of(column_type: &str) -> Option<Self> {
        let lower = column_type.trim().to_ascii_lowercase();
        let unsigned = lower.split_whitespace().any(|word| word == "unsigned");
        let integers = |bits: u32| {
            if unsigned {
                Self::Integers {
                    min: 0,
                    max: (1i128 << bits) - 1,
                }
            } else {
                Self::Integers {
                    min: -(1i128 << (bits - 1)),
                    max: (1i128 << (bits - 1)) - 1,
                }
            }
        };
        // An unreadable `(M,D)` still bounds the type.
        let digits = lower
            .contains('(')
            .then(|| decimal_precision_and_scale(&lower).unwrap_or((0, 0)));
        let binary = |significand_bits| Self::Binary {
            significand_bits,
            unsigned,
            digits,
        };
        let domain = match type_base_name(&lower).as_str() {
            "tinyint" | "bool" | "boolean" => integers(8),
            "smallint" => integers(16),
            "mediumint" => integers(24),
            "int" | "integer" => integers(32),
            "bigint" => integers(64),
            "decimal" | "numeric" => {
                let (precision, scale) = decimal_precision_and_scale(&lower)?;
                Self::Decimal {
                    precision,
                    scale: u8::try_from(scale).ok()?,
                    unsigned,
                }
            }
            "float" => binary(f32::MANTISSA_DIGITS),
            "double" | "real" => binary(f64::MANTISSA_DIGITS),
            _ => return None,
        };
        Some(domain)
    }

    /// Whether every value of `old` is a value of `self`, so changing a column
    /// from `old` to `self` rewrites no stored value.
    fn holds_every_value_of(&self, old: &Self) -> bool {
        // MySQL rounds and clamps each value it converts into a `FLOAT(M,D)` or
        // `DOUBLE(M,D)` from another type (`FLOAT` 1.234375 becomes
        // `DOUBLE(5,2)` 1.23), so only the same bounded type holds every value.
        if let Self::Binary {
            digits: Some(_), ..
        } = self
        {
            return old == self;
        }
        match (*old, *self) {
            (Self::Integers { min, max }, Self::Integers { min: lo, max: hi }) => {
                lo <= min && max <= hi
            }
            (
                Self::Integers { min, max },
                Self::Decimal {
                    precision,
                    scale,
                    unsigned,
                },
            ) => {
                // A bound past `i128` holds every `MySQL` integer.
                let bound =
                    decimal_integer_bound(precision.saturating_sub(scale)).unwrap_or(i128::MAX);
                min >= floor(bound, unsigned) && max <= bound
            }
            (
                Self::Integers { min, max },
                Self::Binary {
                    significand_bits,
                    unsigned,
                    ..
                },
            ) => {
                // Every integer up to 2^significand_bits in magnitude is exact.
                let bound = 1i128 << significand_bits;
                min >= floor(bound, unsigned) && max <= bound
            }
            (
                Self::Decimal {
                    precision,
                    scale,
                    unsigned,
                },
                Self::Integers { min: lo, max: hi },
            ) => {
                scale == 0
                    && decimal_integer_bound(precision)
                        .is_some_and(|bound| lo <= floor(bound, unsigned) && bound <= hi)
            }
            (
                Self::Decimal {
                    precision,
                    scale,
                    unsigned,
                },
                Self::Decimal {
                    precision: to_precision,
                    scale: to_scale,
                    unsigned: to_unsigned,
                },
            ) => {
                to_scale >= scale
                    && i16::from(to_precision) - i16::from(to_scale)
                        >= i16::from(precision) - i16::from(scale)
                    && sign_fits(unsigned, to_unsigned)
            }
            (
                Self::Decimal {
                    precision,
                    scale,
                    unsigned,
                },
                Self::Binary {
                    significand_bits,
                    unsigned: to_unsigned,
                    ..
                },
            ) => {
                // A fraction digit has no exact binary form in general.
                scale == 0
                    && sign_fits(unsigned, to_unsigned)
                    && decimal_integer_bound(precision)
                        .is_some_and(|bound| bound <= 1i128 << significand_bits)
            }
            (
                Self::Binary {
                    significand_bits,
                    unsigned,
                    ..
                },
                Self::Binary {
                    significand_bits: to_bits,
                    unsigned: to_unsigned,
                    ..
                },
            ) => to_bits >= significand_bits && sign_fits(unsigned, to_unsigned),
            (Self::Binary { .. }, Self::Integers { .. } | Self::Decimal { .. }) => false,
        }
    }
}

/// `10^digits - 1`, the widest integer `digits` decimal digits hold, or `None`
/// past `i128`.
fn decimal_integer_bound(digits: u8) -> Option<i128> {
    10i128.checked_pow(u32::from(digits)).map(|power| power - 1)
}

/// The least value of a domain whose widest value is `bound`.
fn floor(bound: i128, unsigned: bool) -> i128 {
    if unsigned { 0 } else { -bound }
}

/// Whether the values of a type, `unsigned` or not, keep their sign in a type
/// that is `to_unsigned` or not: a signed type has negative values.
fn sign_fits(unsigned: bool, to_unsigned: bool) -> bool {
    unsigned || !to_unsigned
}

/// Whether a `MySQL` column changed from `old_column_type` to
/// `new_column_type` (both `information_schema.COLUMNS.COLUMN_TYPE` strings)
/// keeps every stored value exactly, or `None` when either type is not
/// numeric. A change replication follows only when this is not `Some(false)`.
#[must_use]
pub fn source_retype_keeps_every_value(
    old_column_type: &str,
    new_column_type: &str,
) -> Option<bool> {
    let old = ValueDomain::of(old_column_type)?;
    let new = ValueDomain::of(new_column_type)?;
    Some(new.holds_every_value_of(&old))
}

#[cfg(test)]
mod tests {
    use super::source_retype_keeps_every_value;

    /// Each row is a source `ALTER ... MODIFY` and whether it keeps every
    /// stored value. A comment gives what `MySQL` 8.4 stored for the value that
    /// decides the row; `mysql::retype_domain` in the runtime's integration
    /// tests checks the classification against a live server.
    #[test]
    fn a_retype_keeps_every_value_only_when_the_new_type_holds_the_old_ones() {
        for (old, new, expected) in [
            // Integers into wider integers, including across signedness.
            ("int", "bigint", Some(true)),
            ("int(11)", "bigint(20)", Some(true)),
            ("int unsigned", "bigint", Some(true)),
            ("tinyint(3) unsigned", "smallint", Some(true)),
            ("bigint", "int", Some(false)),
            ("int", "int unsigned", Some(false)),
            ("bigint unsigned", "bigint", Some(false)),
            // Integers into decimals: the integer digits must cover the range.
            ("int", "decimal(10,0)", Some(true)),
            ("int", "decimal(12,2)", Some(true)),
            ("bigint", "decimal(19,0)", Some(true)),
            ("bigint", "decimal(18,0)", Some(false)),
            ("bigint unsigned", "decimal(20,0)", Some(true)),
            ("tinyint", "decimal(3,0)", Some(true)),
            ("smallint", "decimal(3,0)", Some(false)),
            ("int", "decimal(10,0) unsigned", Some(false)),
            // Scale-0 decimals into integers: 999999999999999999 stays exact.
            ("decimal(18,0)", "bigint", Some(true)),
            ("decimal(19,0)", "bigint", Some(false)),
            ("decimal(20,0)", "bigint unsigned", Some(false)),
            ("decimal(19,0) unsigned", "bigint unsigned", Some(true)),
            ("decimal(20,0) unsigned", "bigint unsigned", Some(false)),
            ("decimal(2,0)", "tinyint", Some(true)),
            ("decimal(3,0)", "tinyint", Some(false)),
            ("decimal(10,1)", "bigint", Some(false)),
            // Decimals into decimals: neither digit count may shrink.
            ("decimal(10,2)", "decimal(12,3)", Some(true)),
            ("decimal(10,2)", "decimal(10,1)", Some(false)), // 1.25 -> 1.3
            ("decimal(12,2)", "decimal(12,3)", Some(false)),
            ("decimal(10,2)", "decimal(10,2) unsigned", Some(false)),
            ("decimal(65,30)", "decimal(65,30)", Some(true)),
            // Into binary floating point: integers up to 2^24 or 2^53 only.
            ("int", "double", Some(true)),
            ("bigint", "double", Some(false)), // 9007199254740993 -> ...992
            ("mediumint unsigned", "float", Some(true)),
            ("int", "float", Some(false)), // 16777217 -> 16777216
            ("decimal(15,0)", "double", Some(true)),
            ("decimal(16,0)", "double", Some(false)), // 9999999999999999 -> 1e16
            ("decimal(10,2)", "double", Some(false)),
            ("float", "double", Some(true)),
            // A bounded float holds every value only of its own type.
            ("float(7,4)", "float", Some(true)),
            ("double(5,2)", "double", Some(true)),
            ("double(5,2)", "double(5,2)", Some(true)),
            ("float", "double(5,2)", Some(false)), // 1.234375 -> 1.23
            ("float(30,30)", "double(30,30)", Some(false)), // 1e-30 -> 9.999999999999999e-31
            ("decimal(3,0)", "double(2,0)", Some(false)), // 100 -> 99 without strict mode
            ("int", "double(10,2)", Some(false)),
            ("float", "float(7,4)", Some(false)),
            ("double", "float", Some(false)),
            ("double", "double unsigned", Some(false)),
            // Out of binary floating point: a fraction or an inexact value.
            ("double", "int", Some(false)), // 1.5 -> 2
            ("float", "decimal(30,20)", Some(false)),
            ("double", "decimal(65,30)", Some(false)),
            // Non-numeric types are not judged here.
            ("int", "varchar(20)", None),
            ("datetime", "bigint", None),
        ] {
            assert_eq!(
                source_retype_keeps_every_value(old, new),
                expected,
                "{old} -> {new}"
            );
        }
    }
}
