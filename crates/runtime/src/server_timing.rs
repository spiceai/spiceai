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

//! The server-reported elapsed time of a query, as the W3C `Server-Timing`
//! field (<https://www.w3.org/TR/server-timing/>).
//!
//! A client that times a query itself measures the network and its own
//! processing too. This field carries the time the server spent on it, so the
//! two can be told apart. HTTP sends it as a header or trailer. Flight sends
//! it as gRPC metadata, which is HTTP/2 header and trailer fields. Both use
//! one name and one syntax.
//!
//! The value is always a sample the runtime already records in a duration
//! histogram, read back rather than measured again. The field and the
//! dashboards therefore agree.

use http::{HeaderName, HeaderValue};

/// The field name: `Server-Timing` on HTTP/1.1, `server-timing` as gRPC metadata.
pub(crate) const SERVER_TIMING: HeaderName = HeaderName::from_static("server-timing");

/// The single metric the field carries. W3C names it freely and `total` is
/// the conventional name for the whole of a request's server time.
const METRIC: &str = "total";

/// The field value for `milliseconds`: `total;dur=12.345`.
///
/// `dur` is in milliseconds by the specification. Three decimals keep
/// microsecond resolution without printing float noise.
#[must_use]
pub(crate) fn field_value(milliseconds: f64) -> HeaderValue {
    // Only ASCII digits, `.`, `;` and `=` are ever formatted, so this cannot
    // fail. A non-finite value (it cannot come from an elapsed `Instant`)
    // degrades to zero rather than printing `NaN` into a header.
    let milliseconds = if milliseconds.is_finite() {
        milliseconds.max(0.0)
    } else {
        0.0
    };
    HeaderValue::try_from(format!("{METRIC};dur={milliseconds:.3}"))
        .unwrap_or_else(|_| HeaderValue::from_static("total;dur=0.000"))
}
