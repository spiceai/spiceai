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

//! The conformance suite: what an implementation that is not this crate has to
//! reproduce.
//!
//! A Rust crate single-sources every Rust implementation. It cannot
//! single-source one written in another language, and the sealer of the inner
//! layer is exactly that — so the highest-value copy would otherwise sit
//! outside the guarantee.
//!
//! This module closes that gap by making this crate the *normative reference*
//! and emitting what it does as data. [`conformance_suite_json`] renders the
//! whole thing — suite parameters, the canonicalisation rule, `key_id`
//! derivations, both AAD forms over inputs chosen to break a naive
//! implementation, and the inputs that must be **rejected** — as language-
//! neutral JSON with hex-encoded bytes. It is committed at
//! `testdata/conformance_vectors.json`; an implementation in any language reads
//! that file and asserts byte equality in its own CI, so a divergence fails
//! there rather than surfacing as an undiagnosable open failure at a customer.
//!
//! Nothing here is sealed material: HPKE encapsulates a fresh key per seal, so
//! a ciphertext is not reproducible and pinning one would prove nothing. What
//! *is* reproducible is everything that goes into the seal, and that is what
//! every incident has actually been about.
//!
//! # Regenerating
//!
//! `UPDATE_CLOUD_CONNECT_VECTORS=1 cargo test -p cloud-connect-crypto`
//!
//! Regeneration is meant to be a deliberate, reviewable act: the committed file
//! changing in a diff is the signal that the contract moved and that every
//! other implementation has to move with it in the same change.

use serde::{Deserialize, Serialize};
use snafu::{IntoError as _, OptionExt as _, ResultExt as _, Snafu};

use crate::aad::{AAD_SEPARATOR, SecretAddress};
use crate::error::Error;
use crate::key_id::derive_key_id;
use crate::keypair::EncryptionKeypair;
use crate::suite::{
    AEAD_ID, HPKE_INFO, HPKE_INFO_LABEL, KDF_ID, KEM_ID, MAX_SEALED_SECRETS_SIZE,
    MAX_SECRET_PLAINTEXT_SIZE,
};

/// A vector that does not do what the suite says it does.
///
/// Every one of these is a bug in this module rather than in a caller: the
/// inputs are literals a few lines away from the expectations they are paired
/// with. They are errors and not panics so that the crate keeps its no-panic
/// posture, and so a regeneration run says which vector is wrong.
#[derive(Debug, Snafu)]
pub enum VectorError {
    #[snafu(display(
        "Failed to build the Cloud Connect conformance suite: vector {name} is listed as valid \
         but the addressing rule rejected it: {source}"
    ))]
    UnexpectedRejection { name: String, source: Error },

    #[snafu(display(
        "Failed to build the Cloud Connect conformance suite: vector {name} is listed as \
         rejected but the addressing rule accepted it."
    ))]
    UnexpectedAcceptance { name: String },

    #[snafu(display(
        "Failed to build the Cloud Connect conformance suite: vector {name} is listed as \
         rejected, but the addressing rule refused it for a reason the artifact has no name \
         for: {source}"
    ))]
    UnexpectedRejectionReason { name: String, source: Error },

    #[snafu(display("Failed to render the Cloud Connect conformance suite as JSON: {source}"))]
    Render { source: serde_json::Error },
}

type Result<T, E = VectorError> = std::result::Result<T, E>;

/// Bumped when the shape of the artifact changes, so a consumer pinned to an
/// older reader fails loudly instead of silently skipping fields it does not
/// know about. It is *not* a version of the wire contract.
pub const CONFORMANCE_VERSION: u32 = 1;

/// The whole artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConformanceSuite {
    /// See [`CONFORMANCE_VERSION`].
    pub version: u32,
    /// What produced this, and how to reproduce it.
    pub generated_by: String,
    /// The fixed HPKE parameters every implementation announces and uses.
    pub suite: SuiteParameters,
    /// The canonicalisation rule the AAD vectors below demonstrate.
    pub canonicalization: CanonicalizationRule,
    /// `key_id` derivations.
    pub key_id: Vec<KeyIdVector>,
    /// Both AAD forms, over inputs chosen to catch a divergent implementation.
    pub aad: Vec<AadVector>,
    /// Inputs that must be rejected rather than canonicalised into something.
    pub rejected: Vec<RejectionVector>,
}

/// The fixed suite, as constants a consumer can assert against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuiteParameters {
    /// The HPKE `info` label as text.
    pub hpke_info: String,
    /// The same bytes in hex, for a consumer that would rather not depend on
    /// its own string encoding being UTF-8.
    pub hpke_info_hex: String,
    /// RFC 9180 KEM id.
    pub kem_id: u32,
    /// RFC 9180 KDF id.
    pub kdf_id: u32,
    /// RFC 9180 AEAD id.
    pub aead_id: u32,
    /// Cap on the plaintext handed to a seal.
    pub max_secret_plaintext_size: usize,
    /// Cap a recipient applies to a sealed blob as it arrives.
    pub max_sealed_secrets_size: usize,
}

/// The canonicalisation rule, stated as data so a consumer cannot read past it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalizationRule {
    /// Components trimmed exactly once, on ingest.
    pub trimmed: Vec<String>,
    /// Components used exactly as received.
    pub verbatim: Vec<String>,
    /// What "trim" means here, precisely.
    pub trim: String,
    /// The AAD field separator, in hex.
    pub separator_hex: String,
    /// Rules that are easier to get wrong than to state.
    pub notes: Vec<String>,
}

/// One `key_id` derivation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyIdVector {
    /// A stable name, so a failing assertion says which case broke.
    pub name: String,
    /// The raw KEM public key the id is derived from.
    pub public_key_hex: String,
    /// The expected id.
    pub key_id: String,
}

/// The five components an AAD is built from, before canonicalisation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AadInput {
    pub external_id: String,
    pub namespace: String,
    pub secret_name: String,
    pub key_id: String,
    pub command_id: String,
}

/// One AAD case: inputs in, canonical components and both forms out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AadVector {
    /// A stable name, so a failing assertion says which case broke.
    pub name: String,
    /// What this case is for.
    pub note: String,
    /// The components as received.
    pub input: AadInput,
    /// The components after the rule is applied — what an implementation must
    /// also put on the wire, since the seal is bound to these.
    pub canonical: AadInput,
    /// `external_id 0x00 namespace 0x00 secret_name 0x00 key_id`.
    pub inner_aad_hex: String,
    /// The inner form plus `0x00 command_id`.
    pub outer_aad_hex: String,
}

/// One input that must be rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectionVector {
    /// A stable name, so a failing assertion says which case broke.
    pub name: String,
    /// Why accepting it would be wrong.
    pub note: String,
    /// The components as received.
    pub input: AadInput,
    /// A stable machine-readable reason: `empty_component` or
    /// `separator_in_component`.
    pub error: String,
    /// The component the rejection is about.
    pub component: String,
}

/// Build the suite. Deterministic: no randomness, no clock, no environment —
/// two runs of the same code produce byte-identical output.
///
/// # Errors
/// Returns [`VectorError`] when a vector contradicts what it is listed as, which
/// is a bug in this module rather than in the caller.
pub fn conformance_suite() -> Result<ConformanceSuite> {
    Ok(ConformanceSuite {
        version: CONFORMANCE_VERSION,
        generated_by: concat!(
            "cloud-connect-crypto (spiceai/spiceai). Regenerate with ",
            "`UPDATE_CLOUD_CONNECT_VECTORS=1 cargo test -p cloud-connect-crypto`; ",
            "do not hand-edit."
        )
        .to_owned(),
        suite: suite_parameters(),
        canonicalization: canonicalization_rule(),
        key_id: key_id_vectors(),
        aad: aad_vectors()?,
        rejected: rejection_vectors()?,
    })
}

/// The suite as the committed artifact: pretty-printed, newline-terminated.
///
/// # Errors
/// Returns [`VectorError`] when the suite cannot be built or rendered.
pub fn conformance_suite_json() -> Result<String> {
    let mut json = serde_json::to_string_pretty(&conformance_suite()?).context(RenderSnafu)?;
    json.push('\n');
    Ok(json)
}

fn suite_parameters() -> SuiteParameters {
    SuiteParameters {
        hpke_info: HPKE_INFO_LABEL.to_owned(),
        hpke_info_hex: hex::encode(HPKE_INFO),
        kem_id: KEM_ID,
        kdf_id: KDF_ID,
        aead_id: AEAD_ID,
        max_secret_plaintext_size: MAX_SECRET_PLAINTEXT_SIZE,
        max_sealed_secrets_size: MAX_SEALED_SECRETS_SIZE,
    }
}

fn canonicalization_rule() -> CanonicalizationRule {
    CanonicalizationRule {
        trimmed: vec!["namespace".to_owned(), "secret_name".to_owned()],
        verbatim: vec![
            "external_id".to_owned(),
            "key_id".to_owned(),
            "command_id".to_owned(),
        ],
        trim: "Remove leading and trailing characters with the Unicode White_Space property \
               (Rust `str::trim`). This is NOT JavaScript's `String.prototype.trim`, which also \
               removes U+FEFF; see the `bom_is_not_whitespace` vector."
            .to_owned(),
        separator_hex: hex::encode([AAD_SEPARATOR]),
        notes: vec![
            "Both AAD forms are built from the same canonical components; an implementation must \
             not canonicalise per layer."
                .to_owned(),
            "The canonical namespace and secret_name are what goes on the wire, because the seal \
             is bound to them."
                .to_owned(),
            "Nothing is Unicode-normalised or case-folded; NFC and NFD spellings of the same text \
             are different components."
                .to_owned(),
            "Adding a component to either form means deciding its rule first and implementing it \
             in every implementation in one change."
                .to_owned(),
        ],
    }
}

fn key_id_vectors() -> Vec<KeyIdVector> {
    let derived = EncryptionKeypair::derive(b"cloud-connect-crypto/conformance/enrolled-key");
    [
        (
            "all_zero_key",
            "The all-zero point. Not a key anyone should use, but its id must \
             still derive the same way everywhere.",
            vec![0u8; 32],
        ),
        (
            "derived_key",
            "A real X25519 public key, derived from fixed keying material so it \
             is the same on every run.",
            derived.public_key().to_vec(),
        ),
        (
            "empty_input",
            "SHA-256 of the empty string, truncated. Pins the derivation itself \
             rather than any particular key.",
            Vec::new(),
        ),
    ]
    .into_iter()
    .map(|(name, _note, public_key)| KeyIdVector {
        name: name.to_owned(),
        key_id: derive_key_id(&public_key),
        public_key_hex: hex::encode(&public_key),
    })
    .collect()
}

/// The AAD cases. Each one exists because an implementation could plausibly get
/// it wrong in a way that only shows up as "authentication failed".
fn aad_vectors() -> Result<Vec<AadVector>> {
    let key_id = derive_key_id(
        EncryptionKeypair::derive(b"cloud-connect-crypto/conformance/enrolled-key").public_key(),
    );
    let cases: Vec<(&str, &str, AadInput)> = vec![
        (
            "plain",
            "Nothing to canonicalise. The baseline both forms are read against.",
            input(
                "cell_x",
                "publicorg-7",
                "spicepod-secrets",
                &key_id,
                "ctrl-1",
            ),
        ),
        (
            "namespace_padded",
            "Leading and trailing ASCII whitespace on namespace is removed.",
            input(
                "cell_x",
                "  publicorg-7\n",
                "spicepod-secrets",
                &key_id,
                "ctrl-1",
            ),
        ),
        (
            "secret_name_padded",
            "Leading and trailing ASCII whitespace on secret_name is removed.",
            input(
                "cell_x",
                "publicorg-7",
                "\tspicepod-secrets ",
                &key_id,
                "ctrl-1",
            ),
        ),
        (
            "verbatim_components_keep_their_padding",
            "external_id, key_id and command_id are NOT trimmed. An implementation \
             that trims them seals over bytes nobody else produces.",
            input(
                "  cell_x ",
                "publicorg-7",
                "spicepod-secrets",
                " padded-key-id ",
                " ctrl-1\n",
            ),
        ),
        (
            "interior_whitespace_survives",
            "Trimming touches the ends only; a space inside a name is part of it.",
            input("cell x", " my ns ", " my name ", &key_id, "ctrl 1"),
        ),
        (
            "empty_command_id",
            "The one case where a naive join would make the outer form collide with \
             the inner one. The outer form must be exactly one byte longer.",
            input("cell_x", "publicorg-7", "spicepod-secrets", &key_id, ""),
        ),
        (
            "empty_external_id",
            "external_id is verbatim and may be empty; the join still emits its \
             separator.",
            input("", "publicorg-7", "spicepod-secrets", &key_id, "ctrl-1"),
        ),
        (
            "nbsp_is_whitespace",
            "U+00A0 has the Unicode White_Space property, so it is trimmed.",
            input(
                "cell_x",
                "\u{00a0}publicorg-7\u{00a0}",
                "spicepod-secrets",
                &key_id,
                "ctrl-1",
            ),
        ),
        (
            "ideographic_space_is_whitespace",
            "U+3000 has the Unicode White_Space property, so it is trimmed.",
            input(
                "cell_x",
                "\u{3000}publicorg-7\u{3000}",
                "spicepod-secrets",
                &key_id,
                "ctrl-1",
            ),
        ),
        (
            "bom_is_not_whitespace",
            "U+FEFF does NOT have the White_Space property and must survive. \
             JavaScript's String.prototype.trim removes it, so an implementation \
             that reaches for its own trim diverges here and nowhere else.",
            input(
                "cell_x",
                "\u{feff}publicorg-7\u{feff}",
                "spicepod-secrets",
                &key_id,
                "ctrl-1",
            ),
        ),
        (
            "unicode_nfc",
            "Paired with unicode_nfd: the composed spelling.",
            input(
                "cell_x",
                "caf\u{e9}-1",
                "spicepod-secrets",
                &key_id,
                "ctrl-1",
            ),
        ),
        (
            "unicode_nfd",
            "Paired with unicode_nfc: the decomposed spelling of the same text. \
             The two AADs must differ — no implementation may normalise.",
            input(
                "cell_x",
                "cafe\u{301}-1",
                "spicepod-secrets",
                &key_id,
                "ctrl-1",
            ),
        ),
        (
            "non_ascii_verbatim_components",
            "Components are joined as UTF-8 bytes, not as code points.",
            input(
                "セル_x",
                "publicorg-7",
                "spicepod-secrets",
                &key_id,
                "ctrl-\u{1f512}",
            ),
        ),
    ];

    cases
        .into_iter()
        .map(|(name, note, input)| aad_vector(name, note, input))
        .collect()
}

fn aad_vector(name: &str, note: &str, input: AadInput) -> Result<AadVector> {
    let rejected = |source| UnexpectedRejectionSnafu { name }.into_error(source);
    let address = SecretAddress::new(
        &input.external_id,
        &input.namespace,
        &input.secret_name,
        &input.key_id,
    )
    .map_err(rejected)?;
    let outer = address.outer_aad(&input.command_id).map_err(rejected)?;
    Ok(AadVector {
        name: name.to_owned(),
        note: note.to_owned(),
        canonical: AadInput {
            external_id: address.external_id().to_owned(),
            namespace: address.namespace().to_owned(),
            secret_name: address.secret_name().to_owned(),
            key_id: address.key_id().to_owned(),
            command_id: input.command_id.clone(),
        },
        input,
        inner_aad_hex: hex::encode(address.inner_aad()),
        outer_aad_hex: hex::encode(outer),
    })
}

fn rejection_vectors() -> Result<Vec<RejectionVector>> {
    let cases: Vec<(&str, &str, AadInput)> = vec![
        (
            "empty_namespace",
            "Nothing addresses a Kubernetes object, so the payload could never be \
             applied even if it opened.",
            input("cell_x", "", "spicepod-secrets", "abc123", "ctrl-1"),
        ),
        (
            "whitespace_only_namespace",
            "Empty after trimming is empty.",
            input("cell_x", "   \t\n", "spicepod-secrets", "abc123", "ctrl-1"),
        ),
        (
            "empty_secret_name",
            "As empty_namespace, on the other trimmed component.",
            input("cell_x", "publicorg-7", "", "abc123", "ctrl-1"),
        ),
        (
            "whitespace_only_secret_name",
            "Empty after trimming is empty.",
            input("cell_x", "publicorg-7", " \u{00a0} ", "abc123", "ctrl-1"),
        ),
        (
            "separator_in_external_id",
            "A component carrying the separator could forge a field boundary: \
             external_id \"a\\0b\" joins to the same bytes as external_id \"a\" \
             with namespace \"b\".",
            input(
                "a\0b",
                "publicorg-7",
                "spicepod-secrets",
                "abc123",
                "ctrl-1",
            ),
        ),
        (
            "separator_in_namespace",
            "As separator_in_external_id, on a trimmed component.",
            input(
                "cell_x",
                "public\0org-7",
                "spicepod-secrets",
                "abc123",
                "ctrl-1",
            ),
        ),
        (
            "separator_in_secret_name",
            "As separator_in_external_id, on the other trimmed component.",
            input(
                "cell_x",
                "publicorg-7",
                "spicepod\0secrets",
                "abc123",
                "ctrl-1",
            ),
        ),
        (
            "separator_in_key_id",
            "As separator_in_external_id, on a verbatim component.",
            input(
                "cell_x",
                "publicorg-7",
                "spicepod-secrets",
                "abc\u{0}123",
                "ctrl-1",
            ),
        ),
        (
            "separator_in_command_id",
            "Only the outer form carries command_id, so only it can be forged this \
             way — the rejection still belongs to both.",
            input(
                "cell_x",
                "publicorg-7",
                "spicepod-secrets",
                "abc123",
                "ctrl\0-1",
            ),
        ),
    ];

    cases
        .into_iter()
        .map(|(name, note, input)| rejection_vector(name, note, input))
        .collect()
}

fn rejection_vector(name: &str, note: &str, input: AadInput) -> Result<RejectionVector> {
    let rejection = SecretAddress::new(
        &input.external_id,
        &input.namespace,
        &input.secret_name,
        &input.key_id,
    )
    .and_then(|address| address.outer_aad(&input.command_id))
    .err()
    .context(UnexpectedAcceptanceSnafu { name })?;
    // A rejection vector that fails for some *other* reason would publish a
    // misleading `error`, so the reason is carried, not assumed.
    let (error, component) = match rejection {
        Error::EmptyComponent { component } => ("empty_component", component),
        Error::SeparatorInComponent { component } => ("separator_in_component", component),
        source => return Err(UnexpectedRejectionReasonSnafu { name }.into_error(source)),
    };
    Ok(RejectionVector {
        name: name.to_owned(),
        note: note.to_owned(),
        input,
        error: error.to_owned(),
        component: component.to_owned(),
    })
}

fn input(
    external_id: &str,
    namespace: &str,
    secret_name: &str,
    key_id: &str,
    command_id: &str,
) -> AadInput {
    AadInput {
        external_id: external_id.to_owned(),
        namespace: namespace.to_owned(),
        secret_name: secret_name.to_owned(),
        key_id: key_id.to_owned(),
        command_id: command_id.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The artifact is compared byte-for-byte against a committed file, so a
    /// clock, a hash-map iteration order, or an RNG anywhere in here would turn
    /// the comparison into a flake.
    #[test]
    fn generation_is_deterministic() {
        let first = conformance_suite_json().expect("the suite builds");
        let second = conformance_suite_json().expect("the suite builds");
        assert_eq!(first, second);
    }

    /// A vector nobody can tell apart from another vector proves nothing.
    #[test]
    fn every_vector_has_a_distinct_name() {
        let suite = conformance_suite().expect("the suite builds");
        for names in [
            suite.key_id.iter().map(|v| &v.name).collect::<Vec<_>>(),
            suite.aad.iter().map(|v| &v.name).collect(),
            suite.rejected.iter().map(|v| &v.name).collect(),
        ] {
            let mut unique = names.clone();
            unique.sort_unstable();
            unique.dedup();
            assert_eq!(
                unique.len(),
                names.len(),
                "duplicate vector name in {names:?}"
            );
        }
    }

    /// The suite's whole job is to catch an implementation that canonicalises
    /// differently, so it has to contain cases where canonicalisation actually
    /// changes something — and cases where it must not.
    #[test]
    fn the_suite_covers_both_sides_of_the_canonicalisation_rule() {
        let suite = conformance_suite().expect("the suite builds");
        assert!(
            suite
                .aad
                .iter()
                .any(|v| v.input.namespace != v.canonical.namespace),
            "no vector exercises trimming"
        );
        assert!(
            suite.aad.iter().any(|v| {
                v.input.external_id != v.input.external_id.trim()
                    && v.input.external_id == v.canonical.external_id
            }),
            "no vector proves the verbatim components keep their padding"
        );

        let bom = suite
            .aad
            .iter()
            .find(|v| v.name == "bom_is_not_whitespace")
            .expect("the BOM vector");
        assert!(
            bom.canonical.namespace.starts_with('\u{feff}'),
            "U+FEFF must survive canonicalisation"
        );

        let nfc = suite.aad.iter().find(|v| v.name == "unicode_nfc");
        let nfd = suite.aad.iter().find(|v| v.name == "unicode_nfd");
        assert_ne!(
            nfc.map(|v| &v.inner_aad_hex),
            nfd.map(|v| &v.inner_aad_hex),
            "the composed and decomposed spellings must not normalise together"
        );
    }

    /// The outer form is the inner form plus one separator and the `command_id`
    /// — including when the `command_id` is empty, which is where a naive join
    /// collides.
    #[test]
    fn the_outer_form_extends_the_inner_one_in_every_vector() {
        for vector in conformance_suite().expect("the suite builds").aad {
            let inner = hex::decode(&vector.inner_aad_hex).expect("inner hex");
            let outer = hex::decode(&vector.outer_aad_hex).expect("outer hex");
            let expected = [
                inner.as_slice(),
                &[AAD_SEPARATOR],
                vector.canonical.command_id.as_bytes(),
            ]
            .concat();
            assert_eq!(outer, expected, "outer form mismatch in {}", vector.name);
            assert_ne!(outer, inner, "the two forms collide in {}", vector.name);
        }
    }

    #[test]
    fn the_suite_round_trips_through_json() {
        let generated = conformance_suite().expect("the suite builds");
        let parsed: ConformanceSuite =
            serde_json::from_str(&conformance_suite_json().expect("the suite renders"))
                .expect("the artifact parses back");
        assert_eq!(parsed, generated);
    }
}
