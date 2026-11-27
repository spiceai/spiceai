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

//! Every image the test sources name is pinned in `test/container_images.json`,
//! and every pin there is named by something, so a test that starts a new
//! image fails here, in the unit tests every pull request runs, rather than in
//! whichever integration job first starts it.
//!
//! Scanned: each string literal (and each Dockerfile `FROM` line in one) of the
//! crates' integration tests, the test framework and spicescript; the images
//! `test/source_versions.json` lists; and the images spicescript's scripts
//! start, by `image` or by a source `version`. An image built from a
//! placeholder (`${matrix.…}`) is left to the harnesses, which refuse an
//! unpinned image when they start it.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use super::{Reference, pins};

/// Image references in the scanned sources that nothing pulls from a
/// registry: fixtures of the registry-login and pull tests, which run against
/// a fake Docker daemon. Each is excused only in the file named.
const NOT_PULLED: &[(&str, &str)] = &[
    (
        "crates/runtime/tests/docker/tests.rs",
        "ghcr.io/spiceai/image:1",
    ),
    ("tools/spicescript/src/docker.rs", "ghcr.io/spiceai/image:1"),
    ("tools/spicescript/src/docker.rs", "mongo:8.0"),
    (
        "tools/spicescript/src/docker.rs",
        "docker.io/library/mongo:8.0",
    ),
];

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn walk(dir: &Path, files: &mut Vec<PathBuf>, keep: &dyn Fn(&Path) -> bool) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot list {}: {e}", dir.display()))
        .map(|entry| entry.expect("directory entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            walk(&path, files, keep);
        } else if keep(&path) {
            files.push(path);
        }
    }
}

/// The Rust sources that start containers or name their images.
fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let is_rust = |path: &Path| path.extension().is_some_and(|ext| ext == "rs");
    let mut files = Vec::new();
    walk(&root.join("crates"), &mut files, &|path| {
        let rel = relative(root, path);
        is_rust(path)
            && !rel.starts_with("crates/vendor/")
            && !rel.starts_with("crates/container-images/")
            && (rel.contains("/tests/") || rel.starts_with("crates/test-framework/src/"))
    });
    walk(&root.join("tools/spicescript/src"), &mut files, &is_rust);
    files
}

/// The contents of the string literals in Rust `source`: plain, byte and raw
/// strings, outside comments and character literals.
fn string_literals(source: &str) -> Vec<String> {
    let bytes = source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut literals = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
            }
            b'\'' => {
                // A character literal ('x', '\n', '\'', '"'), else a lifetime.
                if bytes.get(i + 1) == Some(&b'\\') {
                    i += 3;
                    while i < bytes.len() && bytes[i] != b'\'' {
                        i += 1;
                    }
                    i += 1;
                } else if let Some(c) = source[i + 1..].chars().next()
                    && source[i + 1 + c.len_utf8()..].starts_with('\'')
                {
                    i += 2 + c.len_utf8();
                } else {
                    i += 1;
                }
            }
            b'r' if (i == 0
                || !is_ident(bytes[i - 1])
                || (bytes[i - 1] == b'b' && (i < 2 || !is_ident(bytes[i - 2]))))
                && matches!(bytes.get(i + 1), Some(b'"' | b'#')) =>
            {
                let hashes = bytes[i + 1..].iter().take_while(|b| **b == b'#').count();
                if bytes.get(i + 1 + hashes) != Some(&b'"') {
                    i += 1;
                    continue;
                }
                let start = i + 2 + hashes;
                let close = format!("\"{}", "#".repeat(hashes));
                let end = source[start..]
                    .find(&close)
                    .map_or(bytes.len(), |end| start + end);
                literals.push(source[start..end].to_string());
                i = end + close.len();
            }
            b'"' => {
                let start = i + 1;
                i = start;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                literals.push(source[start..i.min(bytes.len())].to_string());
                i += 1;
            }
            _ => i += 1,
        }
    }
    literals
}

/// Whether `text` reads as an image reference with a tag, such as
/// `postgres:18` or `ghcr.io/org/image:1`, rather than a host and port
/// (`localhost:8080`) or other text.
fn is_image_reference(text: &str) -> bool {
    let Ok(reference) = Reference::parse(text) else {
        return false;
    };
    let name = text.split('@').next().unwrap_or(text);
    let after_registry = name.rsplit('/').next().unwrap_or(name);
    let Some((repository, tag)) = after_registry.rsplit_once(':') else {
        return false;
    };
    // A registry on this host is one a test runs itself, and a name under a
    // reserved domain (RFC 2606) names no registry at all: a test of a
    // refusal names one so that nothing could be pulled if it were not
    // refused.
    let host = reference.registry().split(':').next().unwrap_or_default();
    if host == "localhost"
        || host == "127.0.0.1"
        || [".invalid", ".example", ".test"]
            .iter()
            .any(|reserved| host.ends_with(reserved))
    {
        return false;
    }
    if name.contains('/') {
        return true;
    }
    // A bare name is an image when it reads like one, `postgres:18` or
    // `python:3.12-alpine`, and not like a host and port (`db:5432`,
    // `host.docker.internal:127.0.0.1`), a `file:line` or a `key:value` pair:
    // a name of letters, digits and dashes starting with a letter, and a tag
    // that is a version, or one that moves.
    let version = tag
        .strip_prefix('v')
        .unwrap_or(tag)
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_digit());
    let port = tag.len() >= 3 && tag.bytes().all(|b| b.is_ascii_digit());
    repository.len() > 1
        && repository
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_lowercase())
        && !repository.contains('.')
        && repository != "localhost"
        && ((version && !port) || super::is_moving(tag))
}

/// The images `literal` names: itself, or, for a Dockerfile, the images of
/// its `FROM` lines.
fn images_in_literal(literal: &str) -> Vec<String> {
    if is_image_reference(literal) {
        return vec![literal.to_string()];
    }
    let lines = || {
        literal
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
    };
    // `FROM` and `COPY` also start lines of SQL. A Dockerfile starts with
    // `FROM` (or the `ARG`s it uses) and goes on with other instructions.
    let dockerfile = lines()
        .next()
        .is_some_and(|first| first.starts_with("FROM ") || first.starts_with("ARG "))
        && lines().any(|line| {
            ["RUN ", "COPY ", "ENTRYPOINT ", "CMD ", "WORKDIR "]
                .iter()
                .any(|instruction| line.starts_with(instruction))
        });
    if !dockerfile {
        return Vec::new();
    }
    lines()
        .filter_map(|line| line.strip_prefix("FROM "))
        .filter_map(|rest| {
            rest.split_whitespace()
                .find(|word| !word.starts_with("--"))
                .map(str::to_string)
        })
        .filter(|image| !image.contains('{'))
        .collect()
}

/// Each `image:version` `test/source_versions.json` lists, by source key.
fn source_version_images(root: &Path) -> BTreeMap<String, (String, Vec<String>)> {
    let path = root.join("test/source_versions.json");
    let file: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("read source_versions.json"))
            .expect("source_versions.json is JSON");
    file.as_object()
        .expect("an object")
        .iter()
        .filter(|(key, _)| *key != "description")
        .map(|(key, entry)| {
            let image = entry["image"].as_str().expect("image").to_string();
            let versions = entry["versions"]
                .as_array()
                .expect("versions")
                .iter()
                .map(|v| v["version"].as_str().expect("version").to_string())
                .collect();
            (key.clone(), (image, versions))
        })
        .collect()
}

/// The images a spicescript script starts: each source's `image`, and the
/// image a source `version` selects for a kind `test/source_versions.json`
/// lists.
fn script_images(
    value: &serde_json::Value,
    sources: &BTreeMap<String, String>,
    found: &mut Vec<String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(kind) = map.get("kind").and_then(serde_json::Value::as_str) {
                if let Some(image) = map.get("image").and_then(serde_json::Value::as_str) {
                    found.push(image.to_string());
                }
                if let (Some(repository), Some(version)) = (
                    sources.get(kind),
                    map.get("version").and_then(serde_json::Value::as_str),
                ) {
                    found.push(format!("{repository}:{version}"));
                }
            }
            for value in map.values() {
                script_images(value, sources, found);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                script_images(value, sources, found);
            }
        }
        _ => {}
    }
}

#[test]
fn every_image_the_tests_name_is_pinned_and_every_pin_is_used() {
    let root = root();
    let pins = pins().expect("test/container_images.json is valid");
    // (where it is named, image)
    let mut named: Vec<(String, String)> = Vec::new();

    for path in rust_sources(&root) {
        let rel = relative(&root, &path);
        let source =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {rel}: {e}"));
        for literal in string_literals(&source) {
            for image in images_in_literal(&literal) {
                named.push((rel.clone(), image));
            }
        }
    }

    let versions = source_version_images(&root);
    for (key, (image, listed)) in &versions {
        for version in listed {
            named.push((
                format!("test/source_versions.json ({key})"),
                format!("{image}:{version}"),
            ));
        }
    }

    let repositories: BTreeMap<String, String> = versions
        .iter()
        .map(|(key, (image, _))| (key.clone(), image.clone()))
        .collect();
    let mut scripts = Vec::new();
    walk(&root.join("test/spicescript"), &mut scripts, &|path| {
        path.extension()
            .is_some_and(|ext| ext == "yaml" || ext == "yml")
    });
    for path in scripts {
        let rel = relative(&root, &path);
        let text = std::fs::read_to_string(&path).expect("read script");
        let Ok(value) = yaml::from_str::<serde_json::Value>(&text) else {
            // Not every YAML file there is a script; spicescript's own tests
            // check that each script parses.
            continue;
        };
        let mut found = Vec::new();
        script_images(&value, &repositories, &mut found);
        named.extend(found.into_iter().map(|image| (rel.clone(), image)));
    }

    let mut problems = BTreeSet::new();
    let mut used = BTreeSet::new();
    let mut excused = BTreeSet::new();
    for (place, image) in &named {
        if image.contains("${") {
            continue;
        }
        if NOT_PULLED.contains(&(place.as_str(), image.as_str())) {
            excused.insert((place.as_str(), image.as_str()));
            continue;
        }
        match pins.pinned(image) {
            Ok(pinned) => {
                used.insert(pinned.split('@').next().unwrap_or(&pinned).to_string());
            }
            Err(e) => {
                problems.insert(format!("{place}: {e}"));
            }
        }
    }
    for (key, _) in pins.iter() {
        if !used.contains(key) {
            problems.insert(format!(
                "test/container_images.json pins {key}, which no test source names; remove it"
            ));
        }
    }
    for entry in NOT_PULLED {
        if !excused.contains(entry) {
            problems.insert(format!(
                "{}: no longer names {}; remove it from NOT_PULLED",
                entry.0, entry.1
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "{} problem(s) with the container image pins:\n{}",
        problems.len(),
        problems.into_iter().collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn reads_the_literals_a_source_holds() {
    let source = r##"
        // "commented:1" is not a literal
        /* "blocked:1" neither */
        let quote = '"'; let tick = '\''; fn f<'a>(x: &'a str) {}
        const IMAGE: &str = "postgres:18";
        let raw = r#"FROM rust:1.91-slim-trixie AS build
RUN true"#;
        let bytes = b"bytes:1"; let escaped = "say \"hi\"";
    "##;
    assert_eq!(
        string_literals(source),
        [
            "postgres:18",
            "FROM rust:1.91-slim-trixie AS build\nRUN true",
            "bytes:1",
            r#"say \"hi\""#
        ]
    );
    let images: Vec<String> = string_literals(source)
        .iter()
        .flat_map(|literal| images_in_literal(literal))
        .collect();
    assert_eq!(images, ["postgres:18", "rust:1.91-slim-trixie", "bytes:1"]);
    for text in [
        "localhost:8080",
        "127.0.0.1:0",
        "db:5432",
        "host.docker.internal:127.0.0.1",
        "t:1",
        "a b:1",
        "{IMAGE}:{}",
        "s3://b/k:1",
        "nosuch/image",
        "registry.invalid/nosuch/image:1",
        "SELECT *\nFROM orders\nWHERE id = 1",
        "INSERT INTO d SELECT * FROM hits;\nCOPY d TO 'd.parquet';\nFROM hits",
    ] {
        assert!(images_in_literal(text).is_empty(), "{text}");
    }
}
