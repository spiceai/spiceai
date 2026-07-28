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

//! Additional authenticated data, and the canonicalisation of what goes into
//! it.
//!
//! Neither AAD is carried on the wire: each side derives it from the
//! authoritative fields of the message, so a payload only opens where it was
//! addressed. The two forms are
//!
//! ```text
//! outer: external_id 0x00 namespace 0x00 secret_name 0x00 key_id 0x00 command_id
//! inner: external_id 0x00 namespace 0x00 secret_name 0x00 key_id
//! ```
//!
//! The outer form binds the dispatch as well, via the `command_id` the gateway
//! assigns; the inner form cannot, because the control plane seals before a
//! `command_id` exists. HPKE's fresh per-seal encapsulated key is what makes
//! each inner ciphertext unique regardless.
//!
//! The differing arity also domain-separates the two layers, which share a
//! suite and an `info` label: neither layer's ciphertext opens as the other,
//! including in the one case a naive join would collide — an empty
//! `command_id`.
//!
//! # Canonicalisation
//!
//! [`SecretAddress::new`] applies the rule and is the only way to reach either
//! AAD, so no implementation gets to decide it independently:
//!
//! - `namespace` and `secret_name` are **trimmed**. Both name a Kubernetes
//!   object, whose own name is the trimmed form, so the seal has to be taken
//!   over what the object will actually be called. Trimming is idempotent, so a
//!   caller that already trimmed on ingest loses nothing by passing the trimmed
//!   value here.
//! - `external_id`, `key_id`, and `command_id` are **verbatim**. They are
//!   machine-assigned identifiers; there is no whitespace to lose, and
//!   normalising one would be a silent divergence from every implementation
//!   that does not.
//! - Nothing is Unicode-normalised or case-folded, and no component may contain
//!   the separator byte.
//!
//! **Adding a component means deciding its rule first and implementing it
//! everywhere in one change** — a component one side trims and another does not
//! produces ciphertext that opens nowhere, and HPKE reports only that
//! authentication failed.
//!
//! Trimming means Rust's [`str::trim`]: leading and trailing characters with
//! the Unicode `White_Space` property. That is *not* what every language's
//! `trim` does — JavaScript's also removes `U+FEFF`, which `White_Space` does
//! not include — so an implementation in another language must match this rule
//! rather than reach for its own trim. The conformance suite
//! ([`crate::vectors`]) pins the difference.

use snafu::ensure;

use crate::error::{EmptyComponentSnafu, Result, SeparatorInComponentSnafu};

/// The byte that joins AAD components. Chosen because it cannot occur in any of
/// them (see [`SecretAddress::new`]), so the join is unambiguous.
pub const AAD_SEPARATOR: u8 = 0x00;

/// The canonical, validated fields both AAD forms are built from.
///
/// Holding them in one value is the point: the inner and the outer AAD are
/// derived from the *same* canonical components, so the two layers of a single
/// payload cannot disagree about them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretAddress {
    external_id: String,
    namespace: String,
    secret_name: String,
    key_id: String,
}

impl SecretAddress {
    /// Canonicalise and validate the components of a sealed secret's address.
    ///
    /// See the [module docs](self#canonicalisation) for the rule. Pass the
    /// fields **as they appear on the message being sealed or opened**, not as
    /// they were handed to an earlier hop: an opener that canonicalises what it
    /// received matches a sealer that canonicalised what it sent, whatever
    /// happened in between.
    ///
    /// # Errors
    /// Returns [`Error::EmptyComponent`] when `namespace` or `secret_name` is
    /// empty after trimming — neither addresses a Kubernetes object, so a
    /// payload sealed over one could never be applied — and
    /// [`Error::SeparatorInComponent`] when any component contains
    /// [`AAD_SEPARATOR`].
    pub fn new(
        external_id: &str,
        namespace: &str,
        secret_name: &str,
        key_id: &str,
    ) -> Result<Self> {
        Ok(Self {
            external_id: verbatim(external_id, "external_id")?,
            namespace: trimmed(namespace, "namespace")?,
            secret_name: trimmed(secret_name, "secret_name")?,
            key_id: verbatim(key_id, "key_id")?,
        })
    }

    /// The **inner** AAD: the four canonical components, no `command_id`.
    #[must_use]
    pub fn inner_aad(&self) -> Vec<u8> {
        nul_joined(&[
            &self.external_id,
            &self.namespace,
            &self.secret_name,
            &self.key_id,
        ])
    }

    /// The **outer** AAD: the four canonical components plus the `command_id`
    /// the gateway assigned to this dispatch, verbatim.
    ///
    /// # Errors
    /// Returns [`Error::SeparatorInComponent`] when `command_id` contains
    /// [`AAD_SEPARATOR`].
    pub fn outer_aad(&self, command_id: &str) -> Result<Vec<u8>> {
        let command_id = verbatim(command_id, "command_id")?;
        Ok(nul_joined(&[
            &self.external_id,
            &self.namespace,
            &self.secret_name,
            &self.key_id,
            &command_id,
        ]))
    }

    /// The instance the payload is addressed to, verbatim.
    #[must_use]
    pub fn external_id(&self) -> &str {
        &self.external_id
    }

    /// The canonical namespace — the one to put on the wire and to name the
    /// Kubernetes object with, since it is the one the seal is bound to.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The canonical secret name, on the same terms as [`Self::namespace`].
    #[must_use]
    pub fn secret_name(&self) -> &str {
        &self.secret_name
    }

    /// The `key_id` of the recipient key this layer is sealed to, verbatim.
    #[must_use]
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// The same address, re-pointed at another recipient key. The outer layer
    /// addresses the per-connection key while the inner one it wraps addresses
    /// the enrolled key; everything else about the address is shared.
    ///
    /// # Errors
    /// Returns [`Error::SeparatorInComponent`] when `key_id` contains
    /// [`AAD_SEPARATOR`].
    pub fn with_key_id(&self, key_id: &str) -> Result<Self> {
        Ok(Self {
            key_id: verbatim(key_id, "key_id")?,
            ..self.clone()
        })
    }
}

/// A component the rule trims: rejected when it is empty afterwards.
fn trimmed(value: &str, component: &'static str) -> Result<String> {
    let value = value.trim();
    ensure!(!value.is_empty(), EmptyComponentSnafu { component });
    verbatim(value, component)
}

/// A component the rule leaves alone. Every component still has to be free of
/// the separator, whether or not it is trimmed.
fn verbatim(value: &str, component: &'static str) -> Result<String> {
    ensure!(
        !value.as_bytes().contains(&AAD_SEPARATOR),
        SeparatorInComponentSnafu { component }
    );
    Ok(value.to_owned())
}

/// UTF-8 components joined by [`AAD_SEPARATOR`], with no leading or trailing
/// separator.
fn nul_joined(parts: &[&str]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(parts.iter().map(|p| p.len() + 1).sum());
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            aad.push(AAD_SEPARATOR);
        }
        aad.extend_from_slice(part.as_bytes());
    }
    aad
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    fn address() -> SecretAddress {
        SecretAddress::new("cell_x", "publicorg-7", "spicepod-secrets", "abc123")
            .expect("a well-formed address")
    }

    #[test]
    fn both_forms_are_nul_joined_with_no_leading_or_trailing_separator() {
        let address = address();
        assert_eq!(
            address.inner_aad(),
            b"cell_x\0publicorg-7\0spicepod-secrets\0abc123"
        );
        assert_eq!(
            address.outer_aad("ctrl-1").expect("outer"),
            b"cell_x\0publicorg-7\0spicepod-secrets\0abc123\0ctrl-1"
        );
    }

    /// The layers share a suite and an `info` label, so the AADs are what keeps
    /// a ciphertext from one opening as the other. An empty `command_id` is the
    /// case where a naive join would collide.
    #[test]
    fn the_two_forms_never_collide_even_on_an_empty_command_id() {
        let address = address();
        let inner = address.inner_aad();
        let outer = address.outer_aad("").expect("outer");
        assert_ne!(inner, outer);
        assert_eq!(outer.len(), inner.len() + 1);
        assert_eq!(inner.split(|b| *b == AAD_SEPARATOR).count(), 4);
        assert_eq!(outer.split(|b| *b == AAD_SEPARATOR).count(), 5);
        assert!(!inner.ends_with(&[AAD_SEPARATOR]));
    }

    #[test]
    fn namespace_and_secret_name_are_trimmed_and_the_rest_is_verbatim() {
        let padded =
            SecretAddress::new("  cell_x ", "  publicorg-7\n", "\tspicepod-secrets ", " k ")
                .expect("padded address");
        assert_eq!(padded.namespace(), "publicorg-7");
        assert_eq!(padded.secret_name(), "spicepod-secrets");
        assert_eq!(padded.external_id(), "  cell_x ", "external_id is verbatim");
        assert_eq!(padded.key_id(), " k ", "key_id is verbatim");
    }

    /// Trimming twice must be trimming once — the gateway canonicalises on
    /// ingest and then builds the AAD from what it stored, and both have to
    /// land on the same bytes.
    #[test]
    fn canonicalisation_is_idempotent() {
        let once = SecretAddress::new("cell_x", " ns\n", "\tname ", "abc123").expect("once");
        let twice = SecretAddress::new(
            once.external_id(),
            once.namespace(),
            once.secret_name(),
            once.key_id(),
        )
        .expect("twice");
        assert_eq!(once, twice);
        assert_eq!(once.inner_aad(), twice.inner_aad());
    }

    /// Interior whitespace is part of the name, not padding.
    #[test]
    fn trimming_touches_only_the_ends() {
        let address = SecretAddress::new("cell_x", " my ns ", " my name ", "abc123").expect("ok");
        assert_eq!(address.namespace(), "my ns");
        assert_eq!(address.secret_name(), "my name");
    }

    /// `U+FEFF` is not `White_Space`, so `str::trim` keeps it — while
    /// JavaScript's `String.prototype.trim` removes it. An implementation that
    /// reaches for its own trim seals over different bytes and opens nowhere,
    /// so pin the behaviour here and in the conformance suite.
    #[test]
    fn trim_follows_unicode_white_space_which_excludes_the_byte_order_mark() {
        let nbsp = SecretAddress::new("cell_x", "\u{00a0}ns\u{00a0}", "name", "abc123")
            .expect("nbsp address");
        assert_eq!(nbsp.namespace(), "ns", "U+00A0 is White_Space");

        let bom = SecretAddress::new("cell_x", "\u{feff}ns\u{feff}", "name", "abc123")
            .expect("bom address");
        assert_eq!(
            bom.namespace(),
            "\u{feff}ns\u{feff}",
            "U+FEFF is not White_Space and must survive"
        );
    }

    /// No Unicode normalisation: the composed and decomposed spellings of the
    /// same text are different namespaces, because they are different bytes.
    #[test]
    fn components_are_not_unicode_normalised() {
        let composed = SecretAddress::new("cell_x", "caf\u{e9}", "name", "abc123").expect("nfc");
        let decomposed =
            SecretAddress::new("cell_x", "cafe\u{301}", "name", "abc123").expect("nfd");
        assert_ne!(composed.inner_aad(), decomposed.inner_aad());
    }

    #[test]
    fn an_empty_namespace_or_secret_name_is_rejected() {
        for (namespace, secret_name) in [("", "name"), ("   ", "name"), ("ns", ""), ("ns", " \t\n")]
        {
            assert!(
                matches!(
                    SecretAddress::new("cell_x", namespace, secret_name, "abc123"),
                    Err(Error::EmptyComponent { .. })
                ),
                "expected a rejection for namespace {namespace:?} / secret_name {secret_name:?}"
            );
        }
    }

    /// A component carrying the separator could forge a field boundary — an
    /// `external_id` of `"a\0b"` joins to the same bytes as an `external_id` of
    /// `"a"` with a `namespace` of `"b"`.
    #[test]
    fn a_component_carrying_the_separator_is_rejected() {
        assert!(matches!(
            SecretAddress::new("a\0b", "ns", "name", "abc123"),
            Err(Error::SeparatorInComponent {
                component: "external_id"
            })
        ));
        assert!(matches!(
            SecretAddress::new("cell_x", "n\0s", "name", "abc123"),
            Err(Error::SeparatorInComponent {
                component: "namespace"
            })
        ));
        assert!(matches!(
            SecretAddress::new("cell_x", "ns", "na\0me", "abc123"),
            Err(Error::SeparatorInComponent {
                component: "secret_name"
            })
        ));
        assert!(matches!(
            SecretAddress::new("cell_x", "ns", "name", "abc\u{0}123"),
            Err(Error::SeparatorInComponent {
                component: "key_id"
            })
        ));
        assert!(matches!(
            address().outer_aad("ctrl\0-1"),
            Err(Error::SeparatorInComponent {
                component: "command_id"
            })
        ));
    }

    /// Without this the outer layer would have to rebuild the whole address to
    /// point at the per-connection key, which is where a component could be
    /// canonicalised a second, different way.
    #[test]
    fn with_key_id_repoints_the_address_and_changes_nothing_else() {
        let inner = address();
        let outer = inner.with_key_id("def456").expect("re-pointed");
        assert_eq!(outer.key_id(), "def456");
        assert_eq!(outer.external_id(), inner.external_id());
        assert_eq!(outer.namespace(), inner.namespace());
        assert_eq!(outer.secret_name(), inner.secret_name());
        assert_ne!(outer.inner_aad(), inner.inner_aad());
        assert!(matches!(
            inner.with_key_id("de\0f456"),
            Err(Error::SeparatorInComponent {
                component: "key_id"
            })
        ));
    }
}
