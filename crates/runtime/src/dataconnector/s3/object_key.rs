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

//! S3 object keys as the listing table reads them.
//!
//! A dataset's `from:` path keeps its URI escapes, while S3 event
//! notifications and the object store use decoded keys. These helpers move a
//! key between the two forms without reading the wrong object.

use percent_encoding::percent_decode_str;
use url::Url;

/// Percent-decode a `from:` / `s3_changes_key_prefix` path the way `Url` decodes
/// a path (`%20` → space, `%3D` → `=`). Unlike
/// [`s3_event_notifications::event::decode_s3_key`], a `+` stays `+`: it is a
/// literal character in a URI path, not a space.
#[must_use]
pub fn decode_from_path_key(encoded: &str) -> String {
    percent_decode_str(encoded).decode_utf8_lossy().into_owned()
}

/// Error from [`s3_object_from`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectUrlError {
    InvalidBucket { detail: String },
    UnrepresentableKey { key: String, as_url: String },
}

impl std::fmt::Display for ObjectUrlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBucket { detail } => {
                write!(f, "bucket is not a valid URL host: {detail}")
            }
            Self::UnrepresentableKey { key, as_url } => write!(
                f,
                "object key '{key}' cannot be a listing URL without WHATWG path normalization reading {as_url} instead"
            ),
        }
    }
}

impl std::error::Error for ObjectUrlError {}

/// Build an `s3://` URL for a decoded object key. `Url::set_path` percent-encodes
/// spaces and reserved characters so `Url::parse` in the listing connector accepts it.
///
/// `.` and `..` are valid S3 key segments, but `Url::set_path` and a later
/// `Url::parse` apply WHATWG dot-segment removal (`events/a/../b.parquet` →
/// `events/b.parquet`). Those keys are rejected so a notification cannot read
/// the wrong object. `path_segments_mut` is not a fix: it drops `.` / `..`
/// rather than preserving them.
///
/// # Errors
///
/// Returns [`ObjectUrlError::InvalidBucket`] when `bucket` is not a valid URL
/// host, and [`ObjectUrlError::UnrepresentableKey`] when the key does not
/// survive a WHATWG path round-trip.
pub fn s3_object_from(bucket: &str, key: &str) -> Result<String, ObjectUrlError> {
    let mut url =
        Url::parse(&format!("s3://{bucket}")).map_err(|error| ObjectUrlError::InvalidBucket {
            detail: error.to_string(),
        })?;
    url.set_path(key);
    let as_url = url.to_string();
    let reconstructed = decode_from_path_key(url.path().trim_start_matches('/'));
    if reconstructed != key {
        return Err(ObjectUrlError::UnrepresentableKey {
            key: key.to_string(),
            as_url,
        });
    }
    Ok(as_url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_object_from_encodes_spaces_and_preserves_key_path() {
        let uri = s3_object_from("my-bucket", "events/data file.parquet")
            .expect("bucket is a valid URL host");
        assert_eq!(uri, "s3://my-bucket/events/data%20file.parquet");
        assert_eq!(
            s3_object_from("my-bucket", "events/a.parquet").expect("valid"),
            "s3://my-bucket/events/a.parquet"
        );
        assert_eq!(
            s3_object_from("my-bucket", "events/foo..bar.parquet").expect("valid"),
            "s3://my-bucket/events/foo..bar.parquet"
        );
    }

    #[test]
    fn s3_object_from_rejects_keys_that_url_dot_segment_normalization_would_rewrite() {
        let mut collapsed = Url::parse("s3://my-bucket").expect("valid bucket URL");
        collapsed.set_path("events/a/../b.parquet");
        assert_eq!(
            collapsed.as_str(),
            "s3://my-bucket/events/b.parquet",
            "Url::set_path applies WHATWG dot-segment removal"
        );

        let err = s3_object_from("my-bucket", "events/a/../b.parquet")
            .expect_err("a key with a `..` segment must not become a listing URL");
        assert!(
            matches!(
                err,
                ObjectUrlError::UnrepresentableKey { ref key, ref as_url }
                    if key == "events/a/../b.parquet"
                        && as_url == "s3://my-bucket/events/b.parquet"
            ),
            "must name the original key and the object the URL would read, got: {err}"
        );
        assert!(
            s3_object_from("my-bucket", "events/./b.parquet").is_err(),
            "a `.` path segment is also rewritten"
        );
    }

    #[test]
    fn decode_from_path_key_percent_decodes_without_treating_plus_as_space() {
        assert_eq!(
            decode_from_path_key("events/data%20files/"),
            "events/data files/"
        );
        assert_eq!(decode_from_path_key("events/foo+bar/"), "events/foo+bar/");
        assert_eq!(
            decode_from_path_key("events/year%3D2026/"),
            "events/year=2026/"
        );
    }
}
