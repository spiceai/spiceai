/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::fmt::{self, Display};

/// Error type for YAML serialization/deserialization operations.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    location: Option<Location>,
}

#[derive(Debug)]
enum ErrorKind {
    /// Error from the YAML parser.
    Parse(String),
    /// Error during serialization.
    Serialize(String),
    /// Error during deserialization.
    Deserialize(String),
    /// I/O error.
    Io(std::io::Error),
    /// Custom error message.
    Custom(String),
}

/// Location information for errors.
#[derive(Debug, Clone, Copy)]
pub struct Location {
    line: usize,
    column: usize,
}

impl Location {
    /// Create a new location.
    #[must_use]
    pub fn new(line: usize, column: usize) -> Self {
        Self { line, column }
    }

    /// Get the line number (1-indexed).
    #[must_use]
    pub fn line(&self) -> usize {
        self.line
    }

    /// Get the column number (1-indexed).
    #[must_use]
    pub fn column(&self) -> usize {
        self.column
    }
}

impl Error {
    #[cfg(test)]
    pub(crate) fn parse(msg: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Parse(msg.into()),
            location: None,
        }
    }

    pub(crate) fn parse_with_location(msg: impl Into<String>, location: Location) -> Self {
        Self {
            kind: ErrorKind::Parse(msg.into()),
            location: Some(location),
        }
    }

    pub(crate) fn serialize(msg: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Serialize(msg.into()),
            location: None,
        }
    }

    pub(crate) fn deserialize(msg: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Deserialize(msg.into()),
            location: None,
        }
    }

    pub(crate) fn io(err: std::io::Error) -> Self {
        Self {
            kind: ErrorKind::Io(err),
            location: None,
        }
    }

    pub(crate) fn custom(msg: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Custom(msg.into()),
            location: None,
        }
    }

    /// Get the location of the error, if available.
    #[must_use]
    pub fn location(&self) -> Option<Location> {
        self.location
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::Parse(msg) => {
                if let Some(loc) = &self.location {
                    write!(
                        f,
                        "YAML parse error at line {}, column {}: {}",
                        loc.line, loc.column, msg
                    )
                } else {
                    write!(f, "YAML parse error: {msg}")
                }
            }
            ErrorKind::Serialize(msg) => write!(f, "YAML serialization error: {msg}"),
            ErrorKind::Deserialize(msg) => write!(f, "YAML deserialization error: {msg}"),
            ErrorKind::Io(err) => write!(f, "I/O error: {err}"),
            ErrorKind::Custom(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ErrorKind::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::custom(msg.to_string())
    }
}

impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::custom(msg.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::io(err)
    }
}

impl From<yaml_rust2::ScanError> for Error {
    fn from(err: yaml_rust2::ScanError) -> Self {
        let marker = err.marker();
        Error::parse_with_location(err.to_string(), Location::new(marker.line(), marker.col()))
    }
}

impl From<yaml_rust2::EmitError> for Error {
    fn from(err: yaml_rust2::EmitError) -> Self {
        Error::serialize(err.to_string())
    }
}

/// Result type for YAML operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::parse("invalid syntax");
        assert!(err.to_string().contains("YAML parse error"));
        assert!(err.to_string().contains("invalid syntax"));
    }

    #[test]
    fn test_error_with_location() {
        let err = Error::parse_with_location("unexpected character", Location::new(5, 10));
        let msg = err.to_string();
        assert!(msg.contains("line 5"));
        assert!(msg.contains("column 10"));
    }

    #[test]
    fn test_location() {
        let loc = Location::new(42, 7);
        assert_eq!(loc.line(), 42);
        assert_eq!(loc.column(), 7);
    }

    #[test]
    fn test_serde_de_error() {
        let err: Error = serde::de::Error::custom("custom message");
        assert!(err.to_string().contains("custom message"));
    }

    #[test]
    fn test_serde_ser_error() {
        let err: Error = serde::ser::Error::custom("serialization failed");
        assert!(err.to_string().contains("serialization failed"));
    }
}
