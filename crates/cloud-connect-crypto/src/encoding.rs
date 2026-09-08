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

//! DER and PEM encoding for X25519 keys.
//!
//! Emitting is written out here rather than delegated to the `pem` crate, which
//! takes ownership of a `Vec` — that would put a copy of a private key in a
//! buffer the caller cannot reach or zeroize. Parsing still uses the crate:
//! tolerating whatever a peer wrote is the hard part, and the input is
//! already in the caller's hands.

use base64::Engine as _;

use crate::suite::X25519_KEY_LEN;

/// PEM tag of a PKCS#8 private key.
pub(crate) const PRIVATE_KEY_PEM_TAG: &str = "PRIVATE KEY";
/// PEM tag of a `SubjectPublicKeyInfo` public key.
pub(crate) const PUBLIC_KEY_PEM_TAG: &str = "PUBLIC KEY";

/// Fixed DER prefix of an RFC 8410 X25519 `SubjectPublicKeyInfo`, followed by
/// the raw 32-byte public key:
/// `SEQUENCE { SEQUENCE { OID 1.3.101.110 }, BIT STRING (33, 0 unused) }`.
pub(crate) const X25519_SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x6e, 0x03, 0x21, 0x00,
];

/// Fixed DER prefix of an RFC 8410 X25519 PKCS#8 v1 `PrivateKeyInfo`, followed
/// by the raw 32-byte private key: `SEQUENCE { INTEGER 0,
/// SEQUENCE { OID 1.3.101.110 }, OCTET STRING { OCTET STRING (32) } }`.
pub(crate) const X25519_PKCS8_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x6e, 0x04, 0x22, 0x04, 0x20,
];

/// DER bytes whose base64 encoding is exactly one full PEM line. RFC 7468 wraps
/// the body at 64 characters, and 48 is a multiple of 3, so each chunk encodes
/// without padding and the concatenation equals the encoding of the whole
/// input.
const PEM_DER_BYTES_PER_LINE: usize = 48;

/// Append a PEM document for `der` to `out`, with LF line endings to match the
/// PEM the rest of an instance's key material is written with.
///
/// The base64 body is written **straight into `out`** rather than built
/// separately, so a caller encoding key material ends up with every copy of it
/// inside the one buffer it chose — pass a zeroizing one. For the same reason
/// `out` must already have [`pem_len`] spare capacity: growing mid-write copies
/// what has been encoded so far into a fresh allocation and frees the old one
/// untouched.
pub(crate) fn write_pem(out: &mut String, tag: &str, der: &[u8]) {
    out.push_str("-----BEGIN ");
    out.push_str(tag);
    out.push_str("-----\n");
    for chunk in der.chunks(PEM_DER_BYTES_PER_LINE) {
        base64::engine::general_purpose::STANDARD.encode_string(chunk, out);
        out.push('\n');
    }
    out.push_str("-----END ");
    out.push_str(tag);
    out.push_str("-----\n");
}

/// The exact length of the document [`write_pem`] produces, so a buffer holding
/// key material can be sized once and never reallocated.
pub(crate) fn pem_len(tag: &str, der: &[u8]) -> usize {
    let full_lines = der.len() / PEM_DER_BYTES_PER_LINE;
    let remainder = der.len() % PEM_DER_BYTES_PER_LINE;
    // Base64 emits 4 characters per 3 input bytes, padded up.
    let body = full_lines * (PEM_DER_BYTES_PER_LINE / 3 * 4) + remainder.div_ceil(3) * 4;
    let newlines = full_lines + usize::from(remainder != 0);
    "-----BEGIN -----\n".len() + "-----END -----\n".len() + 2 * tag.len() + body + newlines
}

/// PEM-encode `der` into a fresh `String`. For public material only — key
/// material must go through [`write_pem`] into a buffer the caller protects.
pub(crate) fn encode_pem(tag: &str, der: &[u8]) -> String {
    let mut pem = String::with_capacity(pem_len(tag, der));
    write_pem(&mut pem, tag, der);
    pem
}

/// Wrap a raw X25519 public key in its RFC 8410 `SubjectPublicKeyInfo` DER.
pub(crate) fn spki_der(public_key: &[u8]) -> Vec<u8> {
    let mut der = Vec::with_capacity(X25519_SPKI_PREFIX.len() + X25519_KEY_LEN);
    der.extend_from_slice(&X25519_SPKI_PREFIX);
    der.extend_from_slice(public_key);
    der
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `pem_len` must be exact: it is what sizes the zeroizing buffer a private
    /// key is encoded into, and a short estimate would reallocate mid-write and
    /// strand a copy of the key in the freed allocation.
    #[test]
    fn pem_len_is_exact_for_every_body_shape() {
        // 0, 44 and 48 are the real cases (empty, SPKI, PKCS#8); the rest cover
        // the line-wrap boundary and each base64 padding remainder.
        for len in [0, 1, 2, 3, 44, 47, 48, 49, 95, 96, 97, 200] {
            let der = vec![0xab; len];
            for tag in [PRIVATE_KEY_PEM_TAG, PUBLIC_KEY_PEM_TAG, "X"] {
                let mut out = String::new();
                write_pem(&mut out, tag, &der);
                assert_eq!(
                    out.len(),
                    pem_len(tag, &der),
                    "pem_len mismatch for {len}-byte DER, tag {tag}"
                );
            }
        }
    }

    /// Chunking the DER (rather than the encoded body) must still produce a
    /// document any PEM parser reads back as the original bytes.
    #[test]
    fn write_pem_wraps_at_64_characters_and_round_trips() {
        let der: Vec<u8> = (0..=200u8).collect();
        let encoded = encode_pem(PUBLIC_KEY_PEM_TAG, &der);

        let body: Vec<&str> = encoded
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect();
        assert!(body.len() > 1, "the fixture must exercise wrapping");
        for line in &body {
            assert!(line.len() <= 64, "line over 64 chars: {}", line.len());
        }
        // Only the last line may be short — the rest are full.
        for line in &body[..body.len() - 1] {
            assert_eq!(line.len(), 64);
        }
        assert_eq!(
            pem::parse(&encoded).expect("parses").contents(),
            der.as_slice(),
            "the wrapped document must decode to the original DER"
        );
    }
}
