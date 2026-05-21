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

//! Pure-Rust cryptographic primitives used by the SMB client.
//!
//! Replaces the macOS CommonCrypto FFI in the upstream spiceio port with
//! cross-platform RustCrypto crates (`md-4`, `md-5`, `hmac`, `sha2`, `aes`,
//! `cmac`) so the client runs on Linux, Windows, and macOS.

use aes::Aes128;
use cmac::{Cmac, Mac};
use hmac::Hmac;
use md4::Md4;
use md5::Md5;
use sha2::{Digest, Sha256, Sha512};

type HmacMd5 = Hmac<Md5>;
type HmacSha256 = Hmac<Sha256>;

/// Compute MD4 digest. Used in NTLM password hashing.
#[must_use]
pub fn md4(data: &[u8]) -> [u8; 16] {
    let mut hasher = Md4::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute MD5 digest. Only used in tests, but exposed for symmetry with
/// other digest helpers.
#[cfg(test)]
#[must_use]
pub fn md5(data: &[u8]) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute HMAC-MD5. Core of NTLMv2 authentication.
#[must_use]
pub fn hmac_md5(key: &[u8], data: &[u8]) -> [u8; 16] {
    // HMAC accepts any key length; `new_from_slice` can never fail here.
    let mut mac = <HmacMd5 as hmac::Mac>::new_from_slice(key)
        .unwrap_or_else(|_| unreachable!("HMAC accepts any key length"));
    hmac::Mac::update(&mut mac, data);
    hmac::Mac::finalize(mac).into_bytes().into()
}

/// Compute SHA-256 digest.
#[must_use]
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute SHA-512 digest. Used for SMB 3.1.1 preauth integrity hash.
#[must_use]
pub fn sha512(data: &[u8]) -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute HMAC-SHA256. Used in signing key derivation (SP800-108 KDF).
#[must_use]
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    // HMAC accepts any key length; `new_from_slice` can never fail here.
    let mut mac = <HmacSha256 as hmac::Mac>::new_from_slice(key)
        .unwrap_or_else(|_| unreachable!("HMAC accepts any key length"));
    hmac::Mac::update(&mut mac, data);
    hmac::Mac::finalize(mac).into_bytes().into()
}

/// Compute AES-128-CMAC (RFC 4493). Used for SMB 3.x message signing.
#[must_use]
pub fn aes128_cmac(key: &[u8; 16], data: &[u8]) -> [u8; 16] {
    // AES-128 key is exactly 16 bytes; `new_from_slice` can never fail here.
    let mut mac = <Cmac<Aes128> as Mac>::new_from_slice(key)
        .unwrap_or_else(|_| unreachable!("AES-128 uses a 16-byte key"));
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// Fill `buf` with random bytes.
pub fn random_bytes(buf: &mut [u8]) {
    rand::fill(buf);
}

/// Encode bytes as lowercase hex string.
#[must_use]
pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_hex_vec(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0);
        hex.as_bytes()
            .chunks_exact(2)
            .map(|chunk| {
                u8::from_str_radix(std::str::from_utf8(chunk).expect("test fixture"), 16)
                    .expect("test fixture")
            })
            .collect()
    }

    fn decode_hex_array<const N: usize>(hex: &str) -> [u8; N] {
        assert_eq!(hex.len(), N * 2);
        let mut out = [0u8; N];
        for (i, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
            out[i] = u8::from_str_radix(std::str::from_utf8(chunk).expect("test fixture"), 16)
                .expect("test fixture");
        }
        out
    }

    #[test]
    fn test_md5_empty() {
        let digest = md5(b"");
        let expected: [u8; 16] = [
            0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8,
            0x42, 0x7e,
        ];
        assert_eq!(digest, expected);
    }

    #[test]
    fn test_md5_abc() {
        assert_eq!(hex_encode(&md5(b"abc")), "900150983cd24fb0d6963f7d28e17f72");
    }

    #[test]
    fn test_md4_empty() {
        let digest = md4(b"");
        let expected: [u8; 16] = [
            0x31, 0xd6, 0xcf, 0xe0, 0xd1, 0x6a, 0xe9, 0x31, 0xb7, 0x3c, 0x59, 0xd7, 0xe0, 0xc0,
            0x89, 0xc0,
        ];
        assert_eq!(digest, expected);
    }

    #[test]
    fn test_md4_abc() {
        assert_eq!(hex_encode(&md4(b"abc")), "a448017aaf21d8525fc10ae87aa6729d");
    }

    #[test]
    fn test_hmac_md5() {
        // RFC 2104 test vector 1
        let key = [0x0b_u8; 16];
        let data = b"Hi There";
        let mac = hmac_md5(&key, data);
        let expected: [u8; 16] = [
            0x92, 0x94, 0x72, 0x7a, 0x36, 0x38, 0xbb, 0x1c, 0x13, 0xf4, 0x8e, 0xf8, 0x15, 0x8b,
            0xfc, 0x9d,
        ];
        assert_eq!(mac, expected);
    }

    #[test]
    fn test_hmac_md5_rfc2202_case_2() {
        let mac = hmac_md5(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(hex_encode(&mac), "750c783e6ab0b503eaa86e310a5db738");
    }

    #[test]
    fn test_sha256_empty() {
        let digest = sha256(b"");
        assert_eq!(
            hex_encode(&digest),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_sha256_abc() {
        assert_eq!(
            hex_encode(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn test_sha512_abc() {
        assert_eq!(
            hex_encode(&sha512(b"abc")),
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a\
             2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        );
    }

    #[test]
    fn test_hmac_sha256() {
        // RFC 4231 test case 2
        let key = b"Jefe";
        let data = b"what do ya want for nothing?";
        let mac = hmac_sha256(key, data);
        assert_eq!(
            hex_encode(&mac),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn test_hmac_sha256_rfc4231_case_1() {
        let key = [0x0b_u8; 20];
        let mac = hmac_sha256(&key, b"Hi There");
        assert_eq!(
            hex_encode(&mac),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn test_aes128_cmac_rfc4493_vectors() {
        let key = decode_hex_array::<16>("2b7e151628aed2a6abf7158809cf4f3c");
        let cases = [
            ("", "bb1d6929e95937287fa37d129b756746"),
            (
                "6bc1bee22e409f96e93d7e117393172a",
                "070a16b46b4d4144f79bdd9dd04a287c",
            ),
            (
                "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411",
                "dfa66747de9ae63030ca32611497c827",
            ),
            (
                "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710",
                "51f0bebf7e3b9d92fc49741779363cfe",
            ),
        ];

        for (message, expected) in cases {
            let message = decode_hex_vec(message);
            assert_eq!(
                aes128_cmac(&key, &message),
                decode_hex_array::<16>(expected)
            );
        }
    }

    #[test]
    fn test_hex_encode_lowercase() {
        assert_eq!(hex_encode(&[]), "");
        assert_eq!(hex_encode(&[0x00, 0xab, 0xcd, 0xef]), "00abcdef");
    }

    #[test]
    fn test_random_bytes_not_constant() {
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        random_bytes(&mut a);
        random_bytes(&mut b);
        assert_ne!(a, b, "two successive random draws should not match");
    }
}
