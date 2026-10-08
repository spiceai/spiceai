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

//! `hf://datasets/<owner>/<dataset>[@<revision>][/<path>]` dataset locations.
//!
//! This is the grammar the Hugging Face ecosystem already shares — `huggingface_hub`'s
//! `HfFileSystem`, `DuckDB` and Polars all read it — so a location copied from a dataset
//! card or a `DuckDB` query works unchanged:
//!
//! - the revision follows the dataset name after `@` and may be a branch, a tag or a commit;
//! - a revision containing `/` is written percent-encoded (`@refs%2Fconvert%2Fparquet`), except
//!   the Hub's own `refs/convert/<name>` and `refs/pr/<number>` refs, which are recognized as
//!   written;
//! - `@~parquet` is `DuckDB`'s alias for `refs/convert/parquet`, the branch holding the Hub's
//!   automatic Parquet conversion of every public dataset;
//! - the path is taken literally (it is not percent-decoded) and may end in a glob.

use std::fmt;

use snafu::prelude::*;

/// The URL scheme that selects this connector.
pub const SCHEME: &str = "hf";
const DATASETS: &str = "datasets";
/// `DuckDB`'s alias for [`PARQUET_CONVERSION_REVISION`].
const PARQUET_CONVERSION_ALIAS: &str = "~parquet";
/// The branch the Hub writes its automatic Parquet conversion of a dataset to.
pub const PARQUET_CONVERSION_REVISION: &str = "refs/convert/parquet";
/// The revision read when a location names none.
pub const DEFAULT_REVISION: &str = "main";
/// The Hub's limit on the length of an owner or a repository name.
const MAX_NAME_LEN: usize = 96;
/// Characters that start a glob in the final part of a path (the set `DataFusion` uses).
const GLOB_START_CHARS: [char; 3] = ['*', '?', '['];

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum Error {
    #[snafu(display(
        "Expected a location like 'hf://datasets/<owner>/<dataset>', but got {from:?}. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
    ))]
    NotAHuggingFaceLocation { from: String },

    #[snafu(display(
        "{from:?} reads a Hugging Face {repo_type} repository, but only dataset repositories can be read. Use a location like 'hf://datasets/<owner>/<dataset>'. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
    ))]
    UnsupportedRepoType { from: String, repo_type: String },

    #[snafu(display(
        "{from:?} does not name a dataset repository. Use a location like 'hf://datasets/<owner>/<dataset>'{suggestion}. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
    ))]
    MissingDatasetRepo { from: String, suggestion: String },

    // `{value:?}` escapes control characters in the untrusted input, keeping the message on
    // one line.
    #[snafu(display(
        "Invalid dataset {component} {value:?} in {from:?}: use 1 to 96 letters, digits, '-', '_' or '.', starting and ending with a letter or digit, without '--' or '..'."
    ))]
    InvalidRepoName {
        from: String,
        component: &'static str,
        value: String,
    },

    #[snafu(display(
        "{from:?} has no revision after '@'. Name a branch, tag or commit, for example '@main', or remove the '@'."
    ))]
    EmptyRevision { from: String },

    #[snafu(display(
        "Invalid revision {revision:?} in {from:?}: a revision cannot contain '..', control characters, or start or end with '/'."
    ))]
    InvalidRevision { from: String, revision: String },

    #[snafu(display(
        "Invalid path in {from:?}: empty, '.' and '..' path segments are not allowed."
    ))]
    InvalidPath { from: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A dataset repository on the Hub, `<owner>/<name>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RepoId {
    owner: String,
    name: String,
}

impl RepoId {
    /// A repository id, validated as [`DatasetLocation::parse`] validates one.
    ///
    /// # Errors
    ///
    /// Returns an error if the owner or name is not a valid Hub name.
    pub fn new(owner: &str, name: &str) -> Result<Self> {
        let from = format!("{SCHEME}://{DATASETS}/{owner}/{name}");
        validate_name(&from, "owner", owner)?;
        validate_name(&from, "name", name)?;
        Ok(Self {
            owner: owner.to_string(),
            name: name.to_string(),
        })
    }

    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.owner, self.name)
    }
}

/// A parsed `hf://datasets/...` location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetLocation {
    repo: RepoId,
    revision: String,
    /// The path inside the repository, without a leading or trailing `/`. Empty for the
    /// repository root.
    path: String,
    /// Whether the location names a folder: it is the repository root or ends in `/`.
    is_folder: bool,
}

impl DatasetLocation {
    /// Parses a dataset's `from` value.
    ///
    /// # Errors
    ///
    /// Returns an error if `from` is not an `hf://datasets/...` location, or names an owner,
    /// dataset, revision or path the Hub does not allow.
    pub fn parse(from: &str) -> Result<Self> {
        let rest = from
            .strip_prefix(SCHEME)
            .and_then(|rest| rest.strip_prefix("://"))
            .context(NotAHuggingFaceLocationSnafu { from })?;

        let (repo_type, rest) = rest.split_once('/').unwrap_or((rest, ""));
        if repo_type != DATASETS {
            if matches!(repo_type, "models" | "spaces" | "buckets") {
                return UnsupportedRepoTypeSnafu {
                    from,
                    repo_type: repo_type.trim_end_matches('s'),
                }
                .fail();
            }
            // `hf://<owner>/<dataset>` is the likeliest mistake: suggest the full form.
            let suggestion = if repo_type.is_empty() {
                String::new()
            } else {
                format!(", for example 'hf://datasets/{repo_type}/{rest}'")
            };
            return MissingDatasetRepoSnafu { from, suggestion }.fail();
        }

        let (owner, tail) = rest.split_once('/').unwrap_or((rest, ""));
        if owner.is_empty() {
            return MissingDatasetRepoSnafu {
                from,
                suggestion: String::new(),
            }
            .fail();
        }
        let name_segment = tail.split('/').next().unwrap_or_default();
        let (name, revision, path) = match name_segment.split_once('@') {
            None => (
                name_segment,
                DEFAULT_REVISION.to_string(),
                tail.split_once('/').map(|(_, path)| path),
            ),
            Some((name, _)) => {
                // The revision may continue past the name's segment (`@refs/pr/1/...`), so it
                // is split from everything after the `@`.
                let (revision, path) = split_revision(&tail[name.len() + 1..]);
                (name, decode_revision(from, revision)?, path)
            }
        };
        if name.is_empty() {
            return MissingDatasetRepoSnafu {
                from,
                suggestion: format!(", for example 'hf://datasets/{owner}/<dataset>'"),
            }
            .fail();
        }

        validate_name(from, "owner", owner)?;
        validate_name(from, "name", name)?;

        let path = path.unwrap_or_default();
        let is_folder = path.is_empty() || path.ends_with('/');
        let path = path.trim_end_matches('/');
        ensure!(
            path.is_empty()
                || path
                    .split('/')
                    .all(|segment| !matches!(segment, "" | "." | "..")),
            InvalidPathSnafu { from }
        );

        Ok(Self {
            repo: RepoId {
                owner: owner.to_string(),
                name: name.to_string(),
            },
            revision,
            path: path.to_string(),
            is_folder,
        })
    }

    #[must_use]
    pub fn repo(&self) -> &RepoId {
        &self.repo
    }

    /// The branch, tag or commit to read.
    #[must_use]
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// The path inside the repository, without a leading or trailing `/`.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The path alone, as a URL a file-extension detector can read: neither the revision
    /// (`@v1.0`) nor the dataset's name (`my.dataset`) is a file extension.
    #[must_use]
    pub fn path_url(&self) -> String {
        let mut url = url::Url::parse(&format!("{SCHEME}://{DATASETS}/"))
            .unwrap_or_else(|_| unreachable!("a valid URL literal"));
        if !self.path.is_empty() {
            url.path_segments_mut()
                .unwrap_or_else(|()| unreachable!("an hf:// URL with a host has path segments"))
                .pop_if_empty()
                .extend(self.path.split('/'));
        }
        url.to_string()
    }

    /// Whether the location names a folder rather than a single file or a glob.
    #[must_use]
    pub fn is_folder(&self) -> bool {
        self.is_folder
    }

    /// Splits [`Self::path`] at its first glob into the literal folder prefix (without a
    /// trailing `/`) and the glob relative to it. `None` when the path has no glob.
    #[must_use]
    pub fn glob(&self) -> Option<(&str, &str)> {
        let first_glob = self.path.find(GLOB_START_CHARS)?;
        match self.path[..first_glob].rfind('/') {
            Some(separator) => Some((&self.path[..separator], &self.path[separator + 1..])),
            None => Some(("", self.path.as_str())),
        }
    }
}

/// Splits the text after `@` into the revision and the path that follows it.
///
/// The Hub's `refs/convert/<name>` and `refs/pr/<number>` refs contain `/` and are matched as
/// written, as `HfFileSystem` does; any other revision ends at the first `/`.
fn split_revision(after_at: &str) -> (&str, Option<&str>) {
    if let Some(len) = hub_ref_len(after_at) {
        return (&after_at[..len], after_at[len..].strip_prefix('/'));
    }
    match after_at.split_once('/') {
        Some((revision, path)) => (revision, Some(path)),
        None => (after_at, None),
    }
}

/// The length of a leading `refs/convert/<name>` or `refs/pr/<number>` ref: the prefix and
/// the one path segment after it, which a conversion names however a ref name may and a pull
/// request numbers in digits.
fn hub_ref_len(after_at: &str) -> Option<usize> {
    type IsRefName = fn(&str) -> bool;
    let hub_refs: [(&str, IsRefName); 2] = [
        ("refs/convert/", |name| !name.is_empty()),
        ("refs/pr/", |number| {
            !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit())
        }),
    ];
    hub_refs.iter().find_map(|(prefix, is_ref_name)| {
        let tail = after_at.strip_prefix(prefix)?;
        let name = tail.split('/').next().unwrap_or_default();
        is_ref_name(name).then_some(prefix.len() + name.len())
    })
}

fn decode_revision(from: &str, revision: &str) -> Result<String> {
    ensure!(!revision.is_empty(), EmptyRevisionSnafu { from });
    if revision == PARQUET_CONVERSION_ALIAS {
        return Ok(PARQUET_CONVERSION_REVISION.to_string());
    }
    let decoded = percent_encoding::percent_decode_str(revision)
        .decode_utf8()
        .map_err(|_| Error::InvalidRevision {
            from: from.to_string(),
            revision: revision.to_string(),
        })?;
    ensure!(
        !decoded.is_empty()
            && !decoded.starts_with('/')
            && !decoded.ends_with('/')
            && !decoded.contains("..")
            && !decoded.chars().any(char::is_control),
        InvalidRevisionSnafu {
            from,
            revision: decoded.to_string(),
        }
    );
    Ok(decoded.into_owned())
}

/// Applies the Hub's rule for owner and repository names (`huggingface_hub`'s
/// `validate_repo_id`). The names are interpolated into Hub URLs, so this is also what keeps a
/// location from addressing anything but a dataset repository.
fn validate_name(from: &str, component: &'static str, value: &str) -> Result<()> {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let valid = !value.is_empty()
        && value.len() <= MAX_NAME_LEN
        && value.chars().all(|c| is_word(c) || c == '-' || c == '.')
        && value.starts_with(is_word)
        && value.ends_with(is_word)
        && !value.contains("--")
        && !value.contains("..");
    ensure!(
        valid,
        InvalidRepoNameSnafu {
            from,
            component,
            value,
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(from: &str) -> DatasetLocation {
        DatasetLocation::parse(from).unwrap_or_else(|e| panic!("{from} should parse: {e}"))
    }

    fn parts(from: &str) -> (String, String, String, bool) {
        let location = parse(from);
        (
            location.repo().to_string(),
            location.revision().to_string(),
            location.path().to_string(),
            location.is_folder(),
        )
    }

    fn owned(
        repo: &str,
        revision: &str,
        path: &str,
        is_folder: bool,
    ) -> (String, String, String, bool) {
        (
            repo.to_string(),
            revision.to_string(),
            path.to_string(),
            is_folder,
        )
    }

    #[test]
    fn parses_the_shared_hf_location_grammar() {
        let cases = [
            (
                "hf://datasets/stanfordnlp/imdb",
                owned("stanfordnlp/imdb", "main", "", true),
            ),
            (
                "hf://datasets/stanfordnlp/imdb/",
                owned("stanfordnlp/imdb", "main", "", true),
            ),
            (
                "hf://datasets/stanfordnlp/imdb/plain_text/train-00000-of-00001.parquet",
                owned(
                    "stanfordnlp/imdb",
                    "main",
                    "plain_text/train-00000-of-00001.parquet",
                    false,
                ),
            ),
            (
                "hf://datasets/stanfordnlp/imdb/plain_text/",
                owned("stanfordnlp/imdb", "main", "plain_text", true),
            ),
            (
                "hf://datasets/stanfordnlp/imdb@v1.0/plain_text/train.parquet",
                owned(
                    "stanfordnlp/imdb",
                    "v1.0",
                    "plain_text/train.parquet",
                    false,
                ),
            ),
            (
                "hf://datasets/stanfordnlp/imdb@e6281661ce1c48d982bc483cf8a173c1bbeb5d31",
                owned(
                    "stanfordnlp/imdb",
                    "e6281661ce1c48d982bc483cf8a173c1bbeb5d31",
                    "",
                    true,
                ),
            ),
            // DuckDB's alias for the Hub's Parquet conversion.
            (
                "hf://datasets/stanfordnlp/imdb@~parquet/plain_text/train/",
                owned(
                    "stanfordnlp/imdb",
                    "refs/convert/parquet",
                    "plain_text/train",
                    true,
                ),
            ),
            // The Hub's own refs are recognized as written, as `HfFileSystem` does.
            (
                "hf://datasets/stanfordnlp/imdb@refs/convert/parquet/plain_text/train/0000.parquet",
                owned(
                    "stanfordnlp/imdb",
                    "refs/convert/parquet",
                    "plain_text/train/0000.parquet",
                    false,
                ),
            ),
            (
                "hf://datasets/stanfordnlp/imdb@refs/pr/12",
                owned("stanfordnlp/imdb", "refs/pr/12", "", true),
            ),
            (
                "hf://datasets/stanfordnlp/imdb@refs/pr/12/data.csv",
                owned("stanfordnlp/imdb", "refs/pr/12", "data.csv", false),
            ),
            // Any other revision with a `/` is percent-encoded.
            (
                "hf://datasets/stanfordnlp/imdb@refs%2Fconvert%2Fparquet/plain_text",
                owned(
                    "stanfordnlp/imdb",
                    "refs/convert/parquet",
                    "plain_text",
                    false,
                ),
            ),
            (
                "hf://datasets/o/d@feature%2Fx/data.csv",
                owned("o/d", "feature/x", "data.csv", false),
            ),
            // A conversion ref's name may hold any ref-name characters.
            (
                "hf://datasets/o/d@refs/convert/my-model.v1.2/a.csv",
                owned("o/d", "refs/convert/my-model.v1.2", "a.csv", false),
            ),
            (
                "hf://datasets/o/d@refs/convert/duckdb",
                owned("o/d", "refs/convert/duckdb", "", true),
            ),
            // A pull request is numbered: anything else after `refs/pr/` is a path.
            (
                "hf://datasets/o/d@refs/pr/x/a.csv",
                owned("o/d", "refs", "pr/x/a.csv", false),
            ),
            // The path is literal.
            (
                "hf://datasets/o/d/data%20x/a b.csv",
                owned("o/d", "main", "data%20x/a b.csv", false),
            ),
        ];
        for (from, expected) in cases {
            assert_eq!(parts(from), expected, "{from}");
        }
    }

    #[test]
    fn splits_a_glob_from_its_folder() {
        let location = parse("hf://datasets/o/d/data/train-*.parquet");
        assert_eq!(location.glob(), Some(("data", "train-*.parquet")));
        assert!(!location.is_folder());

        let location = parse("hf://datasets/o/d/*.csv");
        assert_eq!(location.glob(), Some(("", "*.csv")));

        let location = parse("hf://datasets/o/d/data/*/part-[0-9].parquet");
        assert_eq!(location.glob(), Some(("data", "*/part-[0-9].parquet")));

        assert_eq!(parse("hf://datasets/o/d/data/a.parquet").glob(), None);
    }

    #[test]
    fn rejects_locations_that_do_not_name_a_dataset() {
        let cases: [(&str, Error); 6] = [
            (
                "s3://bucket/key",
                Error::NotAHuggingFaceLocation {
                    from: "s3://bucket/key".to_string(),
                },
            ),
            (
                "hf://models/meta-llama/Llama-3.1-8B",
                Error::UnsupportedRepoType {
                    from: "hf://models/meta-llama/Llama-3.1-8B".to_string(),
                    repo_type: "model".to_string(),
                },
            ),
            (
                "hf://stanfordnlp/imdb",
                Error::MissingDatasetRepo {
                    from: "hf://stanfordnlp/imdb".to_string(),
                    suggestion: ", for example 'hf://datasets/stanfordnlp/imdb'".to_string(),
                },
            ),
            (
                "hf://datasets/stanfordnlp",
                Error::MissingDatasetRepo {
                    from: "hf://datasets/stanfordnlp".to_string(),
                    suggestion: ", for example 'hf://datasets/stanfordnlp/<dataset>'".to_string(),
                },
            ),
            (
                "hf://datasets/o/d@/a.csv",
                Error::EmptyRevision {
                    from: "hf://datasets/o/d@/a.csv".to_string(),
                },
            ),
            (
                "hf://datasets/o/d/../../models/x/a.csv",
                Error::InvalidPath {
                    from: "hf://datasets/o/d/../../models/x/a.csv".to_string(),
                },
            ),
        ];
        for (from, expected) in cases {
            assert_eq!(DatasetLocation::parse(from), Err(expected), "{from}");
        }
    }

    #[test]
    fn rejects_names_the_hub_does_not_allow() {
        for (from, component, value) in [
            ("hf://datasets/o/-d", "name", "-d"),
            ("hf://datasets/o/d./a.csv", "name", "d."),
            ("hf://datasets/o/a--b", "name", "a--b"),
            ("hf://datasets/o/a..b", "name", "a..b"),
            ("hf://datasets/../d", "owner", ".."),
            ("hf://datasets/o%2F/d", "owner", "o%2F"),
            ("hf://datasets/o/d?x=1", "name", "d?x=1"),
        ] {
            assert_eq!(
                DatasetLocation::parse(from),
                Err(Error::InvalidRepoName {
                    from: from.to_string(),
                    component,
                    value: value.to_string(),
                }),
                "{from}"
            );
        }
        let long = "a".repeat(MAX_NAME_LEN + 1);
        assert!(matches!(
            DatasetLocation::parse(&format!("hf://datasets/o/{long}")),
            Err(Error::InvalidRepoName {
                component: "name",
                ..
            })
        ));
        DatasetLocation::parse(&format!("hf://datasets/o/{}", "a".repeat(MAX_NAME_LEN)))
            .expect("a name of the maximum length is valid");
    }

    #[test]
    fn rejects_revisions_that_could_escape_the_revision_segment() {
        for (from, revision) in [
            ("hf://datasets/o/d@..%2F..%2Fx", "../../x"),
            ("hf://datasets/o/d@%2Fmain", "/main"),
            ("hf://datasets/o/d@a%0Ab", "a\nb"),
        ] {
            assert_eq!(
                DatasetLocation::parse(from),
                Err(Error::InvalidRevision {
                    from: from.to_string(),
                    revision: revision.to_string(),
                }),
                "{from}"
            );
        }
    }

    #[test]
    fn error_messages_are_single_line_and_actionable() {
        let error =
            DatasetLocation::parse("hf://stanfordnlp/imdb").expect_err("missing 'datasets/'");
        assert_eq!(
            error.to_string(),
            "\"hf://stanfordnlp/imdb\" does not name a dataset repository. Use a location like 'hf://datasets/<owner>/<dataset>', for example 'hf://datasets/stanfordnlp/imdb'. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
        );
        let error =
            DatasetLocation::parse("hf://datasets/o/d@a%0Ab").expect_err("a control character");
        assert!(!error.to_string().contains('\n'), "{error}");
    }
}
