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

//! The digest each container image the integration tests and spicescript
//! start is pinned to, read from `test/container_images.json`.
//!
//! A tag is a name a registry can point at other content at any time, so a
//! container started by tag runs whatever the registry serves that day, in a
//! job that may hold cloud credentials. The test harnesses therefore start a
//! container only from [`pinned`]: the image as `registry/repository:tag@digest`,
//! with the digest the file records for that tag. Docker pulls a digest
//! reference by its digest and checks the content against it, so the tag only
//! labels what runs.
//!
//! An image the file does not pin is refused, in CI and on a developer's
//! machine alike: both hold credentials a test container could reach, and a
//! refusal that only CI made would surface only after a push. The error says
//! how to record the pin (`.github/scripts/pin_container_images.py --write`).

use std::{collections::BTreeMap, fmt, sync::LazyLock};

use serde::Deserialize;

/// Where the pins are kept, relative to the repository root.
pub const PINS_FILE: &str = "test/container_images.json";

const PINS_JSON: &str = include_str!("../../../test/container_images.json");

/// The command that records a pin, for error messages.
const PIN_COMMAND: &str = ".github/scripts/pin_container_images.py --write";

/// Why an image cannot be started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The text is not a container image reference.
    NotAReference { image: String },
    /// The reference names no tag, so Docker would pull `latest`.
    NoTag { image: String },
    /// The tag is one that moves to every new release, such as `latest`.
    MovingTag { image: String, tag: String },
    /// `test/container_images.json` records no digest for the image.
    NotPinned { image: String, key: String },
    /// The reference carries a digest other than the one pinned for its tag.
    OtherDigest {
        image: String,
        key: String,
        pinned: String,
    },
    /// `test/container_images.json` is malformed.
    InvalidPins { reason: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAReference { image } => {
                write!(f, "`{image}` is not a container image reference")
            }
            Self::NoTag { image } => write!(
                f,
                "{image} names no tag, so Docker would run whatever `latest` names. Name the release to run, such as postgres:18, and pin it in {PINS_FILE} with `{PIN_COMMAND} <image>`"
            ),
            Self::MovingTag { image, tag } => write!(
                f,
                "{image} names `{tag}`, a tag that moves to each new release. Name the release to run instead, and pin it in {PINS_FILE} with `{PIN_COMMAND} <image>`"
            ),
            Self::NotPinned { image, key } => write!(
                f,
                "{image} is not pinned: {PINS_FILE} records no digest for {key}. A test starts a container only from an image pinned there, so that a tag moving cannot change what it runs. Record the digest with `{PIN_COMMAND} {key}`, and review it"
            ),
            Self::OtherDigest { image, key, pinned } => write!(
                f,
                "{image} names a digest other than the one {PINS_FILE} pins for {key} ({pinned})"
            ),
            Self::InvalidPins { reason } => write!(f, "{PINS_FILE} is malformed: {reason}"),
        }
    }
}

impl std::error::Error for Error {}

/// An image reference, `[registry/]repository[:tag][@digest]`, read the way
/// Docker reads it: `postgres:18` is `docker.io/library/postgres:18`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    registry: String,
    repository: String,
    tag: Option<String>,
    digest: Option<String>,
}

impl Reference {
    /// Parses `image`.
    ///
    /// # Errors
    ///
    /// [`Error::NotAReference`] when `image` is not a valid reference.
    pub fn parse(image: &str) -> Result<Self, Error> {
        let not_a_reference = || Error::NotAReference {
            image: image.to_string(),
        };
        let (name, digest) = match image.split_once('@') {
            Some((name, digest)) if is_digest(digest) => (name, Some(digest.to_string())),
            Some(_) => return Err(not_a_reference()),
            None => (image, None),
        };
        // The first component is a registry only when it looks like a host;
        // otherwise the whole name is a Docker Hub repository.
        let (registry, rest) = match name.split_once('/') {
            Some((first, rest))
                if first.contains(['.', ':']) || first == "localhost" || first.is_empty() =>
            {
                (first, rest)
            }
            _ => ("docker.io", name),
        };
        let (repository, tag) = match rest.rsplit_once(':') {
            Some((repository, tag)) => (repository, Some(tag)),
            None => (rest, None),
        };
        if !is_registry(registry)
            || !repository.split('/').all(is_path_component)
            || tag.is_some_and(|tag| !is_tag(tag))
        {
            return Err(not_a_reference());
        }
        let registry = match registry {
            "index.docker.io" | "registry-1.docker.io" => "docker.io",
            registry => registry,
        };
        let repository = if registry == "docker.io" && !repository.contains('/') {
            format!("library/{repository}")
        } else {
            repository.to_string()
        };
        Ok(Self {
            registry: registry.to_string(),
            repository,
            tag: tag.map(str::to_string),
            digest,
        })
    }

    /// `registry/repository:tag`, the key [`PINS_FILE`] records the image by.
    ///
    /// # Errors
    ///
    /// When the reference names no tag, or a tag that moves to each release.
    pub fn key(&self, image: &str) -> Result<String, Error> {
        let Some(tag) = &self.tag else {
            return Err(Error::NoTag {
                image: image.to_string(),
            });
        };
        if is_moving(tag) {
            return Err(Error::MovingTag {
                image: image.to_string(),
                tag: tag.clone(),
            });
        }
        Ok(format!("{}/{}:{tag}", self.registry, self.repository))
    }

    /// The registry, with Docker Hub as `docker.io`.
    #[must_use]
    pub fn registry(&self) -> &str {
        &self.registry
    }

    /// The digest the reference names, if any.
    #[must_use]
    pub fn digest(&self) -> Option<&str> {
        self.digest.as_deref()
    }
}

/// The pins: each image's `registry/repository:tag` and its digest.
#[derive(Debug, Clone)]
pub struct Pins {
    images: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PinsFile {
    #[expect(dead_code, reason = "documentation for whoever edits the file")]
    description: String,
    images: BTreeMap<String, String>,
}

impl Pins {
    /// Reads pins from the JSON of [`PINS_FILE`]. Each key must be the key
    /// [`Reference::key`] gives for it, and each digest a `sha256:` digest.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidPins`] when the JSON is malformed or an entry is not
    /// as described.
    pub fn parse(json: &str) -> Result<Self, Error> {
        let invalid = |reason: String| Error::InvalidPins { reason };
        let file: PinsFile = serde_json::from_str(json).map_err(|e| invalid(e.to_string()))?;
        for (key, digest) in &file.images {
            let reference = Reference::parse(key).map_err(|e| invalid(e.to_string()))?;
            if reference.digest.is_some() {
                return Err(invalid(format!(
                    "the key {key} carries a digest; give the digest as its value"
                )));
            }
            let canonical = reference.key(key).map_err(|e| invalid(e.to_string()))?;
            if &canonical != key {
                return Err(invalid(format!(
                    "the key {key} is not in canonical form; write it as {canonical}"
                )));
            }
            if !is_digest(digest) {
                return Err(invalid(format!(
                    "{key} is pinned to `{digest}`, which is not a sha256: digest of 64 lowercase hex digits"
                )));
            }
        }
        Ok(Self {
            images: file.images,
        })
    }

    /// `image` as `registry/repository:tag@digest`, with the digest pinned
    /// for its tag. An `image` that already carries that digest is accepted.
    ///
    /// # Errors
    ///
    /// When `image` is not a reference, names no tag or a moving one, is not
    /// pinned, or carries a digest other than the pinned one.
    pub fn pinned(&self, image: &str) -> Result<String, Error> {
        let reference = Reference::parse(image)?;
        let key = reference.key(image)?;
        let Some(pinned) = self.images.get(&key) else {
            return Err(Error::NotPinned {
                image: image.to_string(),
                key,
            });
        };
        if let Some(digest) = reference.digest()
            && digest != pinned
        {
            return Err(Error::OtherDigest {
                image: image.to_string(),
                key,
                pinned: pinned.clone(),
            });
        }
        Ok(format!("{key}@{pinned}"))
    }

    /// Each pinned `registry/repository:tag` and its digest.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.images.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

static PINS: LazyLock<Result<Pins, Error>> = LazyLock::new(|| Pins::parse(PINS_JSON));

/// The pins checked in at [`PINS_FILE`].
///
/// # Errors
///
/// When the file is malformed.
pub fn pins() -> Result<&'static Pins, Error> {
    PINS.as_ref().map_err(Clone::clone)
}

/// `image` as `registry/repository:tag@digest`, with the digest
/// [`PINS_FILE`] pins for its tag: the reference a test pulls and runs.
///
/// # Errors
///
/// When the image is not pinned there (see [`Pins::pinned`]), or the file is
/// malformed.
pub fn pinned(image: &str) -> Result<String, Error> {
    pins()?.pinned(image)
}

fn is_digest(text: &str) -> bool {
    text.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// `latest`, or a tag that names it as one of its words (`2022-latest`).
fn is_moving(tag: &str) -> bool {
    tag.split(['-', '.', '_'])
        .any(|word| word.eq_ignore_ascii_case("latest"))
}

/// A host name or address, with an optional port.
fn is_registry(text: &str) -> bool {
    let (host, port) = match text.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (text, None),
    };
    !host.is_empty()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        })
        && port.is_none_or(|port| !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()))
}

/// A repository path component: lowercase letters and digits, joined by
/// `.`, `_`, `__` or runs of `-`.
fn is_path_component(text: &str) -> bool {
    let bytes = text.as_bytes();
    let alnum = |b: &u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    bytes.first().is_some_and(alnum)
        && bytes.last().is_some_and(alnum)
        && bytes
            .iter()
            .all(|b| alnum(b) || matches!(b, b'.' | b'_' | b'-'))
        && !text.contains("..")
        && !text.contains("___")
        && !text.contains("._")
        && !text.contains("_.")
}

/// A tag: up to 128 letters, digits, `_`, `.` and `-`, not starting with
/// `.` or `-`.
fn is_tag(text: &str) -> bool {
    text.len() <= 128
        && text
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod coverage;
