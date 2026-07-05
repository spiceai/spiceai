//! PostgreSQL Log Sequence Number (LSN) type.
//!
//! PostgreSQL displays LSNs as `X/Y` (uppercase hex), where:
//! - `X` is the high 32 bits
//! - `Y` is the low 32 bits
//!
//! Each part is up to 8 hex digits; leading zeros are typically omitted.

use std::fmt;
use std::str::FromStr;

/// Error returned when parsing an invalid LSN string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseLsnError(pub String);

impl fmt::Display for ParseLsnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid LSN: {}", self.0)
    }
}

impl std::error::Error for ParseLsnError {}

/// PostgreSQL Log Sequence Number.
///
/// Represents a position in the write-ahead log (WAL). LSNs are used to
/// track replication progress and identify specific points in the WAL stream.
///
/// # Format
///
/// PostgreSQL displays LSNs as `XXXXXXXX/YYYYYYYY` where:
/// - `XXXXXXXX` is the high 32 bits (segment file number)
/// - `YYYYYYYY` is the low 32 bits (offset within segment)
///
/// # Example
///
/// ```
/// use pgwire_replication::lsn::Lsn;
///
/// let lsn = Lsn::parse("16/B374D848").unwrap();
/// assert_eq!(lsn.to_string(), "16/B374D848");
///
/// // Or use FromStr
/// let lsn: Lsn = "16/B374D848".parse().unwrap();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    /// The zero LSN, representing "start from the beginning" or "no position".
    pub const ZERO: Lsn = Lsn(0);

    /// Parse an LSN from PostgreSQL's `XXXXXXXX/YYYYYYYY` format.
    ///
    /// # Errors
    ///
    /// Returns `ParseLsnError` if the string is not valid LSN format.
    pub fn parse(s: &str) -> Result<Lsn, ParseLsnError> {
        let (hi_str, lo_str) = s
            .split_once('/')
            .ok_or_else(|| ParseLsnError(format!("missing '/' separator: {s}")))?;

        let hi = u64::from_str_radix(hi_str, 16)
            .map_err(|_| ParseLsnError(format!("invalid high part '{hi_str}': {s}")))?;

        let lo = u64::from_str_radix(lo_str, 16)
            .map_err(|_| ParseLsnError(format!("invalid low part '{lo_str}': {s}")))?;

        Ok(Lsn((hi << 32) | lo))
    }

    /// Format as PostgreSQL's `XXXXXXXX/YYYYYYYY` string.
    #[inline]
    pub fn to_pg_string(self) -> String {
        self.to_string()
    }

    /// Returns `true` if this is the zero LSN.
    #[inline]
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Returns the raw 64-bit value.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Create an LSN from a raw 64-bit value.
    #[inline]
    pub fn from_u64(value: u64) -> Self {
        Lsn(value)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let v: u64 = self.as_u64();
        let hi = (v >> 32) as u32;
        let lo = (v & 0xFFFF_FFFF) as u32;
        write!(f, "{:X}/{:X}", hi, lo)
    }
}

impl FromStr for Lsn {
    type Err = ParseLsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Lsn::parse(s)
    }
}

impl From<u64> for Lsn {
    fn from(value: u64) -> Self {
        Lsn(value)
    }
}

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> Self {
        lsn.0
    }
}
