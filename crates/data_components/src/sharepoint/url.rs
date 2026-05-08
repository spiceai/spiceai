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

//! URL parsing for the `sharepoint://` object-store scheme.
//!
//! Two URL shapes are recognized:
//!
//! - `sharepoint://me/{item-path}` — the authenticated user's OneDrive.
//! - `sharepoint://{kind}/{id}/{item-path}` where `kind` ∈ `drives` | `sites` |
//!   `users` | `groups` and `id` is the resource identifier. The rest of the
//!   URL path is the file path within the resource's default drive.
//!
//! IDs may contain characters that are not valid in URL authority components
//! (commas, `!`, etc.) — putting them in the path segment instead sidesteps
//! percent-encoding gymnastics.

#![expect(
    clippy::doc_markdown,
    reason = "prose-frequent identifiers like SharePoint/OneDrive are clearer without backticks"
)]

use object_store::path::Path;
use snafu::Snafu;
use url::Url;

pub const SHAREPOINT_SCHEME: &str = "sharepoint";

/// Identifies a SharePoint drive target resolvable via an ID only (or the
/// `me` shortcut). Distinct from [`crate::sharepoint::client::PublicDrivePtr`]
/// which also supports name-based resolution requiring an API round-trip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriveRef {
    /// The authenticated user's personal drive.
    Me,
    /// A specific drive by ID.
    Drive(String),
    /// A site's default document library.
    Site(String),
    /// A user's default drive.
    User(String),
    /// A group's default drive.
    Group(String),
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "URL '{url}' does not have the '{SHAREPOINT_SCHEME}' scheme. Expected a URL like 'sharepoint://me/path/to/file.csv' or 'sharepoint://drives/{{drive-id}}/path/to/file.csv'."
    ))]
    WrongScheme { url: String },

    #[snafu(display(
        "URL '{url}' is not a valid SharePoint URL: {reason}. Expected a URL like 'sharepoint://me/path/to/file.csv' or 'sharepoint://drives/{{drive-id}}/path/to/file.csv'."
    ))]
    Malformed { url: String, reason: String },

    #[snafu(display("Failed to parse '{url}' as a URL: {source}"))]
    UrlParse {
        url: String,
        source: url::ParseError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A parsed `sharepoint://` URL: the drive target plus the path within the drive.
///
/// `item_path` uses [`object_store::path::Path`] semantics (slash-delimited,
/// URL-decoded segments). An empty path means the drive root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharepointUrl {
    pub drive: DriveRef,
    pub item_path: Path,
}

impl SharepointUrl {
    /// Parse a `sharepoint://...` URL.
    pub fn parse(url: &str) -> Result<Self> {
        let parsed = Url::parse(url).map_err(|source| Error::UrlParse {
            url: url.to_string(),
            source,
        })?;
        Self::from_url(&parsed)
    }

    /// Parse a pre-parsed [`Url`].
    pub fn from_url(url: &Url) -> Result<Self> {
        if url.scheme() != SHAREPOINT_SCHEME {
            return Err(Error::WrongScheme {
                url: url.to_string(),
            });
        }

        let kind = url.host_str().ok_or_else(|| Error::Malformed {
            url: url.to_string(),
            reason: "missing authority".to_string(),
        })?;

        // `path_segments()` yields raw percent-encoded strings. We need the
        // ID segment decoded (site IDs contain commas that are percent-encoded
        // in the URL) but the item path is built via `Path::from_url_path`
        // which handles decoding correctly without double-encoding.
        let path_segments: Vec<&str> = url
            .path_segments()
            .map(|s| s.filter(|seg| !seg.is_empty()).collect())
            .unwrap_or_default();

        let (drive, remaining_encoded) = match kind {
            "me" => (DriveRef::Me, path_segments.as_slice()),
            "drives" | "sites" | "users" | "groups" => {
                let (id_encoded, rest) =
                    path_segments
                        .split_first()
                        .ok_or_else(|| Error::Malformed {
                            url: url.to_string(),
                            reason: format!(
                                "missing {kind} ID (expected 'sharepoint://{kind}/{{id}}/...')"
                            ),
                        })?;
                // Decode the ID segment (e.g. site IDs with commas encoded as %2C).
                let id = percent_encoding::percent_decode_str(id_encoded)
                    .decode_utf8()
                    .map_err(|_| Error::Malformed {
                        url: url.to_string(),
                        reason: "drive ID contains invalid UTF-8 after percent-decoding"
                            .to_string(),
                    })?
                    .into_owned();
                let drive = match kind {
                    "drives" => DriveRef::Drive(id),
                    "sites" => DriveRef::Site(id),
                    "users" => DriveRef::User(id),
                    "groups" => DriveRef::Group(id),
                    _ => unreachable!(),
                };
                (drive, rest)
            }
            other => {
                return Err(Error::Malformed {
                    url: url.to_string(),
                    reason: format!(
                        "unknown drive kind '{other}' (expected 'me', 'drives', 'sites', 'users', or 'groups')"
                    ),
                });
            }
        };

        // Build the item path from the raw percent-encoded segments joined by '/'.
        // `Path::from_url_path` decodes percent-encoding into the raw field so
        // that `as_ref()` returns the decoded string (e.g. "Shared Documents/..."),
        // avoiding the double-encode that would occur if we decoded first and then
        // collected into a `Path` via `FromIterator`.
        let item_path = if remaining_encoded.is_empty() {
            Path::from("")
        } else {
            let joined = remaining_encoded.join("/");
            Path::from_url_path(&joined).map_err(|e| Error::Malformed {
                url: url.to_string(),
                reason: format!("invalid item path: {e}"),
            })?
        };

        Ok(Self { drive, item_path })
    }
}

impl DriveRef {
    /// Returns a URL authority string for this drive reference. `me` is a
    /// singleton authority; other kinds use a synthetic plural authority and
    /// carry the ID in the first path segment.
    #[must_use]
    pub fn url_authority(&self) -> &'static str {
        match self {
            DriveRef::Me => "me",
            DriveRef::Drive(_) => "drives",
            DriveRef::Site(_) => "sites",
            DriveRef::User(_) => "users",
            DriveRef::Group(_) => "groups",
        }
    }

    /// The ID, if any, that must be carried in the URL path.
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        match self {
            DriveRef::Me => None,
            DriveRef::Drive(id) | DriveRef::Site(id) | DriveRef::User(id) | DriveRef::Group(id) => {
                Some(id)
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests use unwrap to assert happy paths")]
mod tests {
    use super::*;

    #[test]
    fn parse_me_root() {
        let url = SharepointUrl::parse("sharepoint://me/").unwrap();
        assert_eq!(url.drive, DriveRef::Me);
        assert_eq!(url.item_path.as_ref(), "");
    }

    #[test]
    fn parse_me_with_path() {
        let url = SharepointUrl::parse("sharepoint://me/Documents/report.csv").unwrap();
        assert_eq!(url.drive, DriveRef::Me);
        assert_eq!(url.item_path.as_ref(), "Documents/report.csv");
    }

    #[test]
    fn parse_drive_with_id() {
        let url = SharepointUrl::parse(
            "sharepoint://drives/b!abc-def_XYZ/Shared%20Documents/file.parquet",
        )
        .unwrap();
        assert_eq!(url.drive, DriveRef::Drive("b!abc-def_XYZ".to_string()));
        assert_eq!(url.item_path.as_ref(), "Shared Documents/file.parquet");
    }

    #[test]
    fn parse_site_with_comma_id() {
        // Site IDs are of the form `{host},{spsite-guid},{spweb-guid}`.
        let url = SharepointUrl::parse(
            "sharepoint://sites/contoso.sharepoint.com,11111111-2222-3333-4444-555555555555,66666666-7777-8888-9999-aaaaaaaaaaaa/Shared/data.json",
        )
        .unwrap();
        match url.drive {
            DriveRef::Site(id) => {
                assert!(id.starts_with("contoso.sharepoint.com,"));
                assert_eq!(id.matches(',').count(), 2);
            }
            _ => panic!("expected Site"),
        }
        assert_eq!(url.item_path.as_ref(), "Shared/data.json");
    }

    #[test]
    fn parse_user_and_group() {
        let u =
            SharepointUrl::parse("sharepoint://users/48d31887-5fad-4d73-a9f5-3c356e68a038/dir/f")
                .unwrap();
        assert_eq!(
            u.drive,
            DriveRef::User("48d31887-5fad-4d73-a9f5-3c356e68a038".to_string())
        );

        let g = SharepointUrl::parse("sharepoint://groups/gid-abc/dir/f").unwrap();
        assert_eq!(g.drive, DriveRef::Group("gid-abc".to_string()));
    }

    #[test]
    fn reject_wrong_scheme() {
        let err = SharepointUrl::parse("https://example.com/foo").unwrap_err();
        assert!(matches!(err, Error::WrongScheme { .. }));
    }

    #[test]
    fn reject_missing_id() {
        let err = SharepointUrl::parse("sharepoint://drives/").unwrap_err();
        assert!(matches!(err, Error::Malformed { .. }));
    }

    #[test]
    fn reject_unknown_kind() {
        let err = SharepointUrl::parse("sharepoint://bogus/id/path").unwrap_err();
        assert!(matches!(err, Error::Malformed { .. }));
    }
}
