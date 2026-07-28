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

//! Guards the committed conformance artifact.
//!
//! `testdata/conformance_vectors.json` is what implementations in other
//! languages assert against, so it has to keep saying what this crate does. The
//! first test below fails whenever the two drift; the second reads the file the
//! way a consumer that has never seen this crate would, and rebuilds the
//! expected bytes from the rule alone.

use std::path::{Path, PathBuf};

use cloud_connect_crypto::vectors::{ConformanceSuite, conformance_suite_json};

/// Set to any value to rewrite the committed artifact from the generator.
const UPDATE_ENV: &str = "UPDATE_CLOUD_CONNECT_VECTORS";

fn vectors_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/conformance_vectors.json")
}

/// Whether this run is regenerating the artifact rather than checking it.
fn regenerating() -> bool {
    std::env::var_os(UPDATE_ENV).is_some()
}

#[test]
fn the_committed_artifact_matches_the_generator() {
    let path = vectors_path();
    let generated = conformance_suite_json().expect("build the conformance suite");

    if regenerating() {
        std::fs::write(&path, &generated).expect("rewrite the committed conformance artifact");
    }

    let committed =
        std::fs::read_to_string(&path).expect("read the committed conformance artifact");
    assert_eq!(
        committed,
        generated,
        "{} is stale. If the change to the contract is intended, regenerate it with \
         `{UPDATE_ENV}=1 cargo test -p cloud-connect-crypto` and land the same change in every \
         other implementation in the same review — a vector moving on its own means one side \
         will stop being able to open the other's payloads.",
        path.display()
    );
}

/// Rebuilds every AAD from the committed `canonical` components using nothing
/// but the documented rule — NUL-joined UTF-8, no trailing separator — the way
/// an implementation reading this file for the first time would.
///
/// The point is independence: [`conformance_suite_json`] and the crate's own
/// AAD builder are the same code, so comparing them to each other cannot catch
/// a rule that is wrong in both. This can.
#[test]
fn the_committed_aads_are_reproducible_from_the_rule_alone() {
    // On a regeneration run the test above is rewriting the file, so read what
    // the generator produced rather than racing it.
    let artifact = if regenerating() {
        conformance_suite_json().expect("build the conformance suite")
    } else {
        std::fs::read_to_string(vectors_path()).expect("read the committed conformance artifact")
    };
    let suite: ConformanceSuite = serde_json::from_str(&artifact).expect("parse the artifact");

    let separator = hex::decode(&suite.canonicalization.separator_hex).expect("separator hex");
    let join = |parts: &[&str]| -> String {
        let mut bytes: Vec<u8> = Vec::new();
        for (i, part) in parts.iter().enumerate() {
            if i > 0 {
                bytes.extend_from_slice(&separator);
            }
            bytes.extend_from_slice(part.as_bytes());
        }
        hex::encode(bytes)
    };

    for vector in &suite.aad {
        let c = &vector.canonical;
        assert_eq!(
            join(&[&c.external_id, &c.namespace, &c.secret_name, &c.key_id]),
            vector.inner_aad_hex,
            "inner AAD is not reproducible from the rule in {}",
            vector.name
        );
        assert_eq!(
            join(&[
                &c.external_id,
                &c.namespace,
                &c.secret_name,
                &c.key_id,
                &c.command_id
            ]),
            vector.outer_aad_hex,
            "outer AAD is not reproducible from the rule in {}",
            vector.name
        );

        // The trimmed components must already be canonical; the verbatim ones
        // must be untouched.
        assert_eq!(c.namespace, c.namespace.trim(), "{}", vector.name);
        assert_eq!(c.secret_name, c.secret_name.trim(), "{}", vector.name);
        assert_eq!(c.external_id, vector.input.external_id, "{}", vector.name);
        assert_eq!(c.key_id, vector.input.key_id, "{}", vector.name);
        assert_eq!(c.command_id, vector.input.command_id, "{}", vector.name);
    }

    assert!(
        !suite.aad.is_empty() && !suite.rejected.is_empty() && !suite.key_id.is_empty(),
        "the artifact must carry vectors in all three sections"
    );
}
