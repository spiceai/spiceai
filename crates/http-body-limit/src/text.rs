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

//! A held body turned into text, with the text charged before it is made.
//!
//! Text can be larger than the bytes it is decoded from: a byte that is not valid UTF-8
//! becomes U+FFFD, three bytes, and a byte of a legacy charset such as windows-1252 can
//! become a character of up to three bytes. So the charge for the text is its exact
//! decoded length, counted without allocating, and the text is then written into a
//! buffer of exactly that size.

use datafusion_execution::memory_pool::MemoryReservation;
use encoding_rs::{CoderResult, Encoding, UTF_8};
use reqwest::Response;
use reqwest::header::CONTENT_TYPE;
use snafu::prelude::*;

use std::io;

use crate::{IoSnafu, MemoryLimitSnafu, ReadSnafu, Result};

/// `bytes` as text, exactly as [`String::from_utf8_lossy`] makes it, with the text
/// charged to `reservation` before it is made.
///
/// # Errors
///
/// [`Error::MemoryLimit`](crate::Error::MemoryLimit) when the pool cannot hold the text.
pub fn utf8_lossy(bytes: &[u8], reservation: &MemoryReservation) -> Result<String> {
    const REPLACEMENT: &str = "\u{FFFD}";
    // `from_utf8_lossy` writes each valid run as it is and one U+FFFD for each invalid
    // sequence `utf8_chunks` reports, so the text is exactly this long.
    let len = bytes.utf8_chunks().fold(0usize, |len, chunk| {
        let replacement = if chunk.invalid().is_empty() {
            0
        } else {
            REPLACEMENT.len()
        };
        len.saturating_add(chunk.valid().len())
            .saturating_add(replacement)
    });
    reservation.try_grow(len).context(MemoryLimitSnafu)?;
    let mut text = String::with_capacity(len);
    for chunk in bytes.utf8_chunks() {
        text.push_str(chunk.valid());
        if !chunk.invalid().is_empty() {
            text.push_str(REPLACEMENT);
        }
    }
    Ok(text)
}

/// The body of `response` as text, exactly as [`Response::text`] decodes it: in the
/// `charset` of its `Content-Type` (UTF-8 when it names none or one `encoding_rs` does
/// not know), with a byte order mark choosing the encoding and removed, and malformed
/// sequences replaced with U+FFFD. The text is charged to `reservation` before it is
/// made; the body is released when this returns.
///
/// # Errors
///
/// [`Error::Read`](crate::Error::Read) when the body cannot be read, and
/// [`Error::MemoryLimit`](crate::Error::MemoryLimit) when the pool cannot hold the text.
pub async fn text(response: Response, reservation: &MemoryReservation) -> Result<String> {
    let encoding = charset(&response);
    let body = response.bytes().await.context(ReadSnafu)?;
    decode(&body, encoding, reservation)
}

/// The encoding [`Response::text`] decodes `response` in.
fn charset(response: &Response) -> &'static Encoding {
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<mime::Mime>().ok());
    let label = content_type
        .as_ref()
        .and_then(|mime| mime.get_param("charset").map(|charset| charset.as_str()))
        .unwrap_or("utf-8");
    Encoding::for_label(label.as_bytes()).unwrap_or(UTF_8)
}

/// `bytes` decoded as [`Encoding::decode`] decodes them, charged to `reservation` before
/// the text is made.
fn decode(
    bytes: &[u8],
    encoding: &'static Encoding,
    reservation: &MemoryReservation,
) -> Result<String> {
    let (encoding, bytes) = match Encoding::for_bom(bytes) {
        Some((encoding, bom)) => (encoding, &bytes[bom..]),
        None => (encoding, bytes),
    };

    // Input that is already the text it decodes to: valid UTF-8, or ASCII in an
    // encoding that leaves ASCII as it is.
    let unchanged = if encoding == UTF_8 {
        Encoding::utf8_valid_up_to(bytes) == bytes.len()
    } else {
        encoding.is_ascii_compatible() && Encoding::ascii_valid_up_to(bytes) == bytes.len()
    };
    if unchanged && let Ok(text) = std::str::from_utf8(bytes) {
        reservation.try_grow(text.len()).context(MemoryLimitSnafu)?;
        return Ok(text.to_owned());
    }

    let mut len = 0usize;
    decode_in_blocks(bytes, encoding, |block| {
        len = len.saturating_add(block.len());
    })?;
    reservation.try_grow(len).context(MemoryLimitSnafu)?;
    let mut text = String::with_capacity(len);
    decode_in_blocks(bytes, encoding, |block| text.push_str(block))?;
    // Decoding is deterministic, so the second pass wrote exactly `len` bytes and the
    // buffer never grew; were it to, the charge still follows what the text holds.
    if let Some(grown) = text.capacity().checked_sub(len).filter(|grown| *grown > 0) {
        reservation.try_grow(grown).context(MemoryLimitSnafu)?;
    }
    Ok(text)
}

/// Decodes `bytes` in `encoding`, without a byte order mark, a block at a time through a
/// small scratch buffer, handing each decoded block to `block`. Decoding through a
/// buffer of fixed size, rather than into the text's own, is what lets the text be
/// counted without being made, and then made at exactly its size.
fn decode_in_blocks(
    bytes: &[u8],
    encoding: &'static Encoding,
    mut block: impl FnMut(&str),
) -> Result<()> {
    let mut decoder = encoding.new_decoder_without_bom_handling();
    let mut scratch = [0u8; 8 * 1024];
    let mut read = 0;
    loop {
        let (result, consumed, written, _) =
            decoder.decode_to_utf8(&bytes[read..], &mut scratch, true);
        read += consumed;
        // The decoder writes only whole characters of UTF-8, so this never fails; were
        // it to, the read fails rather than lose text.
        let text = std::str::from_utf8(&scratch[..written])
            .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))
            .context(IoSnafu)?;
        block(text);
        if result == CoderResult::InputEmpty {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };

    use super::*;

    /// Inputs that cover every way text differs from its bytes: valid and invalid
    /// UTF-8, sequences cut off at the end, byte order marks, legacy single- and
    /// multi-byte charsets, UTF-16 of odd length, and seeded random bytes.
    fn corpus() -> Vec<Vec<u8>> {
        let mut inputs: Vec<Vec<u8>> = vec![
            Vec::new(),
            b"{\"id\": 1}".to_vec(),
            "na\u{EF}ve caf\u{E9} \u{1F600}".as_bytes().to_vec(),
            vec![0xff; 1000],
            vec![0x80; 1000],
            b"ok \xf0\x9f\x98".to_vec(),
            b"\xe2\x82 \xed\xa0\x80 \xc0\xaf \xf4\x90\x80\x80".to_vec(),
            b"\xef\xbb\xbf{\"bom\": true}".to_vec(),
            b"\xff\xfe{\x00\"\x00a\x00\"\x00}\x00".to_vec(),
            b"\xfe\xff\x00{\x00}\x00".to_vec(),
            b"\xfe\xff\x00{\x00".to_vec(),
            b"\x82\xa0\x82\xa2 \x81".to_vec(),
            b"\x1b$B$\"\x1b(B \x1b".to_vec(),
        ];
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for len in [1usize, 2, 3, 7, 64, 4099, 70_000] {
            let mut bytes = Vec::with_capacity(len);
            for _ in 0..len {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                bytes.push(state.to_le_bytes()[0]);
            }
            inputs.push(bytes);
        }
        inputs
    }

    fn reservation(pool: &Arc<dyn MemoryPool>) -> MemoryReservation {
        MemoryConsumer::new("text test").register(pool)
    }

    #[test]
    fn utf8_lossy_is_from_utf8_lossy_and_charges_exactly_the_text() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        for bytes in corpus() {
            let reservation = reservation(&pool);
            let text = utf8_lossy(&bytes, &reservation).expect("an unbounded pool holds it");
            assert_eq!(text, String::from_utf8_lossy(&bytes), "{bytes:x?}");
            assert_eq!(reservation.size(), text.len(), "{bytes:x?}");
            assert_eq!(text.capacity(), text.len(), "{bytes:x?}");
        }
    }

    #[tokio::test]
    async fn text_is_reqwest_s_text_in_every_charset_and_charges_exactly_the_text() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let content_types = [
            None,
            Some("application/json"),
            Some("application/json; charset=utf-8"),
            Some("text/plain; charset=windows-1252"),
            Some("text/plain; charset=\"Shift_JIS\""),
            Some("text/plain; charset=iso-2022-jp"),
            Some("text/plain; charset=utf-16le"),
            Some("text/plain; charset=utf-16be"),
            Some("text/plain; charset=gb18030"),
            Some("text/plain; charset=iso-2022-kr"),
            Some("text/plain; charset=no-such-charset"),
            Some("not a mime type"),
        ];
        for content_type in content_types {
            for bytes in corpus() {
                let response = |bytes: Vec<u8>| {
                    let mut builder = http::Response::builder();
                    if let Some(content_type) = content_type {
                        builder = builder.header(CONTENT_TYPE, content_type);
                    }
                    Response::from(builder.body(bytes).expect("a response builds"))
                };
                let expected = response(bytes.clone())
                    .text()
                    .await
                    .expect("reqwest decodes it");
                let reservation = reservation(&pool);
                let actual = text(response(bytes.clone()), &reservation)
                    .await
                    .expect("an unbounded pool holds it");
                assert_eq!(actual, expected, "{content_type:?} {bytes:x?}");
                assert_eq!(
                    reservation.size(),
                    actual.len(),
                    "{content_type:?} {bytes:x?}"
                );
                assert_eq!(
                    actual.capacity(),
                    actual.len(),
                    "{content_type:?} {bytes:x?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn text_that_outgrows_its_bytes_is_refused_before_it_is_made() {
        const BODY: usize = 1024 * 1024;
        // windows-1252 0x80 is U+20AC, three bytes: the text is three times the body.
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(3 * BODY - 1));
        let held = reservation(&pool);
        let response = Response::from(
            http::Response::builder()
                .header(CONTENT_TYPE, "text/plain; charset=windows-1252")
                .body(vec![0x80; BODY])
                .expect("a response builds"),
        );
        let error = text(response, &held)
            .await
            .expect_err("a pool one byte short of the text refuses it");
        assert!(
            error
                .to_string()
                .starts_with("holding the decoded data would pass the query memory limit"),
            "{error}"
        );
        assert_eq!(pool.reserved(), 0);

        let held = reservation(&pool);
        let error = utf8_lossy(&vec![0xff; BODY], &held)
            .expect_err("a pool one byte short of the text refuses it");
        assert!(
            matches!(error, crate::Error::MemoryLimit { .. }),
            "{error:?}"
        );
        assert_eq!(pool.reserved(), 0);
    }
}
