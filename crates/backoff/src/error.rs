/*
Copyright 2024-2026 The Spice.ai OSS Authors
Copyright (c) 2016 Tibor Benke

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

use std::fmt;
use std::time::Duration;

/// The error of one attempt, which decides whether the operation is retried.
#[derive(PartialEq)]
pub enum Error<E> {
    /// The operation cannot succeed; retrying stops and `E` is returned.
    Permanent(E),

    /// The operation may succeed later. It is retried after `retry_after` when
    /// set (e.g. from an HTTP 429 `Retry-After`), otherwise after the next
    /// backoff interval.
    Transient {
        err: E,
        retry_after: Option<Duration>,
    },
}

impl<E> Error<E> {
    /// An error that stops retrying.
    #[must_use]
    pub fn permanent(err: E) -> Self {
        Error::Permanent(err)
    }

    /// An error retried after the next backoff interval.
    #[must_use]
    pub fn transient(err: E) -> Self {
        Error::Transient {
            err,
            retry_after: None,
        }
    }
}

impl<E: fmt::Display> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Permanent(err) | Error::Transient { err, .. } => err.fmt(f),
        }
    }
}

impl<E: fmt::Debug> fmt::Debug for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (name, err) = match self {
            Error::Permanent(err) => ("Permanent", err),
            Error::Transient { err, .. } => ("Transient", err),
        };
        f.debug_tuple(name).field(err).finish()
    }
}

impl<E: std::error::Error> std::error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Permanent(err) | Error::Transient { err, .. } => err.source(),
        }
    }
}

/// Errors are transient unless marked permanent, so `?` retries.
impl<E> From<E> for Error<E> {
    fn from(err: E) -> Self {
        Error::transient(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permanent_wraps_the_error() {
        assert_eq!(Error::permanent("err"), Error::Permanent("err"));
    }

    #[test]
    fn transient_and_from_have_no_retry_after() {
        let expected = Error::Transient {
            err: "err",
            retry_after: None,
        };
        assert_eq!(Error::transient("err"), expected);
        assert_eq!(Error::from("err"), expected);
    }

    #[test]
    fn display_and_debug_show_the_inner_error() {
        assert_eq!(Error::permanent("boom").to_string(), "boom");
        assert_eq!(
            format!("{:?}", Error::permanent("boom")),
            r#"Permanent("boom")"#
        );
        assert_eq!(
            format!("{:?}", Error::<&str>::transient("boom")),
            r#"Transient("boom")"#
        );
    }
}
