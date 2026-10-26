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

//! Integrity protection for Flight SQL prepared-statement handles.
//!
//! A prepared statement is held by the client, not the server: the handle is the serialized
//! [`PreparedStatement`] (its SQL, bound parameters and parameter schema), and the client sends it
//! back verbatim on every later call. [`seal`] appends a keyed BLAKE3 tag over those bytes and
//! [`open`] verifies it before anything is deserialized, so the server only ever decodes a handle
//! it issued itself.
//!
//! The key is drawn at random once per process and never persisted, so a handle is valid only on
//! the process that issued it: after a restart, or on another replica, the client is told to
//! prepare the statement again.

use std::sync::LazyLock;

use postcard::{from_bytes, to_stdvec};
use tonic::Status;

use super::prepared_statement_query::{PreparedStatement, error_to_status};

const TAG_LEN: usize = blake3::OUT_LEN;

static HANDLE_KEY: LazyLock<[u8; blake3::KEY_LEN]> = LazyLock::new(rand::random);

/// Serializes `statement` into a handle carrying a tag only this process can produce.
pub(crate) fn seal(statement: &PreparedStatement) -> Result<Vec<u8>, Status> {
    seal_with_key(&HANDLE_KEY, statement)
}

/// Verifies a handle issued by [`seal`] and deserializes the statement it carries.
///
/// # Errors
///
/// Returns `InvalidArgument` for any handle this process did not issue, without saying which
/// check failed.
pub(crate) fn open(handle: &[u8]) -> Result<PreparedStatement, Status> {
    open_with_key(&HANDLE_KEY, handle)
}

fn seal_with_key(
    key: &[u8; blake3::KEY_LEN],
    statement: &PreparedStatement,
) -> Result<Vec<u8>, Status> {
    let mut handle = to_stdvec(statement).map_err(error_to_status)?;
    let tag = blake3::keyed_hash(key, &handle);
    handle.extend_from_slice(tag.as_bytes());
    Ok(handle)
}

fn open_with_key(key: &[u8; blake3::KEY_LEN], handle: &[u8]) -> Result<PreparedStatement, Status> {
    let (payload, tag) = handle
        .split_last_chunk::<TAG_LEN>()
        .ok_or_else(invalid_handle)?;
    // `blake3::Hash` compares in constant time.
    if blake3::keyed_hash(key, payload) != blake3::Hash::from_bytes(*tag) {
        return Err(invalid_handle());
    }
    from_bytes(payload).map_err(|_| invalid_handle())
}

fn invalid_handle() -> Status {
    Status::invalid_argument(
        "Invalid prepared statement handle: it was not issued by this Spice runtime, or the runtime \
         has restarted since. Create the prepared statement again and retry. \
         See: https://spiceai.org/docs/api/arrow-flight-sql",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; blake3::KEY_LEN] = [7; blake3::KEY_LEN];

    fn statement(query: &str, parameters: Vec<u8>) -> PreparedStatement {
        PreparedStatement {
            query: query.to_string(),
            parameters,
            parameter_schema: Some(vec![1, 2, 3]),
        }
    }

    fn assert_rejected(result: Result<PreparedStatement, Status>, what: &str) {
        match result {
            Ok(statement) => panic!("{what} was accepted as query {:?}", statement.query),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument, "{what}");
                assert_eq!(status.message(), invalid_handle().message(), "{what}");
            }
        }
    }

    #[test]
    fn a_sealed_handle_opens_to_the_same_statement() {
        let original = statement("SELECT $1", vec![9, 8, 7]);
        let handle = seal_with_key(&KEY, &original).expect("seal");
        let opened = open_with_key(&KEY, &handle).expect("a sealed handle opens");
        assert_eq!(opened.query, original.query);
        assert_eq!(opened.parameters, original.parameters);
        assert_eq!(opened.parameter_schema, original.parameter_schema);
    }

    /// A handle is the postcard encoding of the statement followed by its tag. Postcard writes the
    /// fields in order: a string or byte field as a varint length and then its bytes, and an
    /// `Option` as a `0` or `1` marker before the value.
    #[test]
    fn a_handle_is_the_postcard_encoding_of_the_statement_and_its_tag() {
        let parameters: Vec<u8> = (0..200).collect();
        let mut with_parameters = vec![9];
        with_parameters.extend_from_slice(b"SELECT $1");
        with_parameters.extend_from_slice(&[0xc8, 0x01]);
        with_parameters.extend_from_slice(&parameters);
        with_parameters.extend_from_slice(&[1, 3, 1, 2, 3]);

        let mut without_parameters = vec![8];
        without_parameters.extend_from_slice(b"SELECT 1");
        without_parameters.extend_from_slice(&[0, 0]);

        let cases = [
            (statement("SELECT $1", parameters), with_parameters),
            (
                PreparedStatement {
                    query: "SELECT 1".to_string(),
                    parameters: Vec::new(),
                    parameter_schema: None,
                },
                without_parameters,
            ),
        ];
        for (original, payload) in cases {
            let mut handle = payload.clone();
            handle.extend_from_slice(blake3::keyed_hash(&KEY, &payload).as_bytes());
            assert_eq!(seal_with_key(&KEY, &original).expect("seal"), handle);

            let opened = open_with_key(&KEY, &handle).expect("the handle opens");
            assert_eq!(opened.query, original.query);
            assert_eq!(opened.parameters, original.parameters);
            assert_eq!(opened.parameter_schema, original.parameter_schema);
        }
    }

    /// The handle shape every client could build before handles were tagged: the bare postcard
    /// encoding of a statement, here carrying parameter bytes the server never wrote.
    #[test]
    fn an_untagged_handle_is_rejected() {
        let forged = to_stdvec(&statement("SELECT 1", vec![0xff; 64])).expect("serialize");
        assert_rejected(open_with_key(&KEY, &forged), "an untagged handle");
    }

    #[test]
    fn every_single_bit_flip_is_rejected() {
        let handle = seal_with_key(&KEY, &statement("SELECT $1", vec![1, 2])).expect("seal");
        for byte in 0..handle.len() {
            for bit in 0..8 {
                let mut tampered = handle.clone();
                tampered[byte] ^= 1 << bit;
                assert_rejected(
                    open_with_key(&KEY, &tampered),
                    &format!("bit {bit} of byte {byte} flipped"),
                );
            }
        }
    }

    #[test]
    fn a_handle_tagged_under_another_key_is_rejected() {
        let other_key = [8; blake3::KEY_LEN];
        let handle = seal_with_key(&other_key, &statement("SELECT 1", Vec::new())).expect("seal");
        assert_rejected(open_with_key(&KEY, &handle), "another process's handle");
    }

    #[test]
    fn truncated_and_extended_handles_are_rejected() {
        let handle = seal_with_key(&KEY, &statement("SELECT 1", Vec::new())).expect("seal");
        for len in 0..handle.len() {
            assert_rejected(
                open_with_key(&KEY, &handle[..len]),
                &format!("handle truncated to {len} bytes"),
            );
        }
        let mut extended = handle;
        extended.push(0);
        assert_rejected(
            open_with_key(&KEY, &extended),
            "a handle with a trailing byte",
        );
    }
}
