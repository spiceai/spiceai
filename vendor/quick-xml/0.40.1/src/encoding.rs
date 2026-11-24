//! A module for wrappers that encode / decode data.

use std::borrow::Cow;
use std::str::Utf8Error;

#[cfg(feature = "encoding")]
use encoding_rs;
#[cfg(feature = "encoding")]
use std::io::{self, BufRead, Read};

/// Unicode "byte order mark" (\u{FEFF}) encoded as UTF-8.
/// See <https://unicode.org/faq/utf_bom.html#bom1>
pub(crate) const UTF8_BOM: &[u8] = &[0xEF, 0xBB, 0xBF];
/// Unicode "byte order mark" (\u{FEFF}) encoded as UTF-16 with little-endian byte order.
/// See <https://unicode.org/faq/utf_bom.html#bom1>
pub(crate) const UTF16_LE_BOM: &[u8] = &[0xFF, 0xFE];
/// Unicode "byte order mark" (\u{FEFF}) encoded as UTF-16 with big-endian byte order.
/// See <https://unicode.org/faq/utf_bom.html#bom1>
pub(crate) const UTF16_BE_BOM: &[u8] = &[0xFE, 0xFF];

/// An error when decoding or encoding
///
/// If feature [`encoding`] is disabled, the [`EncodingError`] is always [`EncodingError::Utf8`]
///
/// [`encoding`]: ../index.html#encoding
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EncodingError {
    /// Input was not valid UTF-8
    Utf8(Utf8Error),
    /// Input did not adhere to the given encoding
    #[cfg(feature = "encoding")]
    Other(&'static encoding_rs::Encoding),
}

impl From<Utf8Error> for EncodingError {
    #[inline]
    fn from(e: Utf8Error) -> Self {
        Self::Utf8(e)
    }
}

impl std::error::Error for EncodingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Utf8(e) => Some(e),
            #[cfg(feature = "encoding")]
            Self::Other(_) => None,
        }
    }
}

impl std::fmt::Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Utf8(e) => write!(f, "cannot decode input using UTF-8: {}", e),
            #[cfg(feature = "encoding")]
            Self::Other(encoding) => write!(f, "cannot decode input using {}", encoding.name()),
        }
    }
}

/// Decoder of byte slices into strings.
///
/// If feature [`encoding`] is enabled, this encoding taken from the `"encoding"`
/// XML declaration or assumes UTF-8, if XML has no <?xml ?> declaration, encoding
/// key is not defined or contains unknown encoding.
///
/// The library supports any UTF-8 compatible encodings that crate `encoding_rs`
/// is supported. [*UTF-16 and ISO-2022-JP are not supported at the present*][utf16].
///
/// If feature [`encoding`] is disabled, the decoder is always UTF-8 decoder:
/// any XML declarations are ignored.
///
/// [utf16]: https://github.com/tafia/quick-xml/issues/158
/// [`encoding`]: ../index.html#encoding
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Decoder {
    #[cfg(feature = "encoding")]
    pub(crate) encoding: &'static encoding_rs::Encoding,
}

impl Decoder {
    pub(crate) const fn utf8() -> Self {
        Decoder {
            #[cfg(feature = "encoding")]
            encoding: encoding_rs::UTF_8,
        }
    }

    #[cfg(all(test, feature = "encoding", feature = "serialize"))]
    pub(crate) const fn utf16() -> Self {
        Decoder {
            encoding: encoding_rs::UTF_16LE,
        }
    }
}

impl Decoder {
    /// Returns the `Reader`s encoding.
    ///
    /// This encoding will be used by [`decode`].
    ///
    /// [`decode`]: Self::decode
    #[cfg(feature = "encoding")]
    pub const fn encoding(&self) -> &'static encoding_rs::Encoding {
        self.encoding
    }

    /// ## Without `encoding` feature
    ///
    /// Decodes an UTF-8 slice regardless of XML declaration and ignoring BOM
    /// if it is present in the `bytes`.
    ///
    /// ## With `encoding` feature
    ///
    /// Decodes specified bytes using encoding, declared in the XML, if it was
    /// declared there, or UTF-8 otherwise, and ignoring BOM if it is present
    /// in the `bytes`.
    ///
    /// ----
    /// Returns an error in case of malformed sequences in the `bytes`.
    pub fn decode<'b>(&self, bytes: &'b [u8]) -> Result<Cow<'b, str>, EncodingError> {
        #[cfg(not(feature = "encoding"))]
        let decoded = Ok(Cow::Borrowed(std::str::from_utf8(bytes)?));

        #[cfg(feature = "encoding")]
        let decoded = decode(bytes, self.encoding);

        decoded
    }

    /// Like [`decode`][Self::decode] but using a pre-allocated buffer.
    pub fn decode_into(&self, bytes: &[u8], buf: &mut String) -> Result<(), EncodingError> {
        #[cfg(not(feature = "encoding"))]
        buf.push_str(std::str::from_utf8(bytes)?);

        #[cfg(feature = "encoding")]
        decode_into(bytes, self.encoding, buf)?;

        Ok(())
    }

    /// Decodes the `Cow` buffer, preserves the lifetime
    pub(crate) fn decode_cow<'b>(
        &self,
        bytes: &Cow<'b, [u8]>,
    ) -> Result<Cow<'b, str>, EncodingError> {
        match bytes {
            Cow::Borrowed(bytes) => self.decode(bytes),
            // Convert to owned, because otherwise Cow will be bound with wrong lifetime
            Cow::Owned(bytes) => Ok(self.decode(bytes)?.into_owned().into()),
        }
    }

    /// Decodes the `Cow` buffer, normalizes XML EOLs, preserves the lifetime
    pub(crate) fn content<'b>(
        &self,
        bytes: &Cow<'b, [u8]>,
        normalize_eol: impl Fn(&str) -> Cow<str>,
    ) -> Result<Cow<'b, str>, EncodingError> {
        match bytes {
            Cow::Borrowed(bytes) => {
                let text = self.decode(bytes)?;
                match normalize_eol(&text) {
                    // If text borrowed after normalization that means that it's not changed
                    Cow::Borrowed(_) => Ok(text),
                    Cow::Owned(s) => Ok(Cow::Owned(s)),
                }
            }
            Cow::Owned(bytes) => {
                let text = self.decode(bytes)?;
                let text = normalize_eol(&text);
                // Convert to owned, because otherwise Cow will be bound with wrong lifetime
                Ok(text.into_owned().into())
            }
        }
    }
}

/// Decodes the provided bytes using the specified encoding.
///
/// Returns an error in case of malformed or non-representable sequences in the `bytes`.
#[cfg(feature = "encoding")]
pub fn decode<'b>(
    bytes: &'b [u8],
    encoding: &'static encoding_rs::Encoding,
) -> Result<Cow<'b, str>, EncodingError> {
    encoding
        .decode_without_bom_handling_and_without_replacement(bytes)
        .ok_or(EncodingError::Other(encoding))
}

/// Like [`decode`] but using a pre-allocated buffer.
#[cfg(feature = "encoding")]
pub fn decode_into(
    bytes: &[u8],
    encoding: &'static encoding_rs::Encoding,
    buf: &mut String,
) -> Result<(), EncodingError> {
    if encoding == encoding_rs::UTF_8 {
        buf.push_str(std::str::from_utf8(bytes)?);
        return Ok(());
    }

    let mut decoder = encoding.new_decoder_without_bom_handling();
    buf.reserve(
        decoder
            .max_utf8_buffer_length_without_replacement(bytes.len())
            // SAFETY: None can be returned only if required size will overflow usize,
            // but in that case String::reserve also panics
            .unwrap(),
    );
    let (result, read) = decoder.decode_to_string_without_replacement(bytes, buf, true);
    match result {
        encoding_rs::DecoderResult::InputEmpty => {
            debug_assert_eq!(read, bytes.len());
            Ok(())
        }
        encoding_rs::DecoderResult::Malformed(_, _) => Err(EncodingError::Other(encoding)),
        // SAFETY: We allocate enough space above
        encoding_rs::DecoderResult::OutputFull => unreachable!(),
    }
}

/// Automatic encoding detection of XML files based using the
/// [recommended algorithm](https://www.w3.org/TR/xml11/#sec-guessing).
///
/// If encoding is detected, `Some` is returned with a [`DetectedEncoding`] that provides
/// the BOM size in bytes (or zero if no BOM was present).
///
/// IF encoding was not recognized, `None` is returned.
///
/// Because the [`encoding_rs`] crate supports only subset of those encodings, only
/// the supported subset are detected, which is UTF-8, UTF-16 BE and UTF-16 LE.
///
/// The algorithm suggests examine up to the first 4 bytes to determine encoding
/// according to the following table:
///
/// | Bytes       |Detected encoding
/// |-------------|------------------------------------------
/// | **BOM**
/// |`FE_FF_##_##`|UTF-16, big-endian
/// |`FF FE ## ##`|UTF-16, little-endian
/// |`EF BB BF`   |UTF-8
/// | **No BOM**
/// |`00 3C 00 3F`|UTF-16 BE or ISO-10646-UCS-2 BE or similar 16-bit BE (use declared encoding to find the exact one)
/// |`3C 00 3F 00`|UTF-16 LE or ISO-10646-UCS-2 LE or similar 16-bit LE (use declared encoding to find the exact one)
/// |`3C 3F 78 6D`|UTF-8, ISO 646, ASCII, some part of ISO 8859, Shift-JIS, EUC, or any other 7-bit, 8-bit, or mixed-width encoding which ensures that the characters of ASCII have their normal positions, width, and values; the actual encoding declaration must be read to detect which of these applies, but since all of these encodings use the same bit patterns for the relevant ASCII characters, the encoding declaration itself may be read reliably
pub fn detect_encoding(bytes: &[u8]) -> Option<DetectedEncoding> {
    // Prevent suggesting "<?xm". We want to have the same formatted lines for all arms.
    #[allow(clippy::byte_char_slices)]
    match bytes {
        // with BOM
        _ if bytes.starts_with(UTF16_BE_BOM) => Some(DetectedEncoding::Utf16BeBom),
        _ if bytes.starts_with(UTF16_LE_BOM) => Some(DetectedEncoding::Utf16LeBom),
        _ if bytes.starts_with(UTF8_BOM) => Some(DetectedEncoding::Utf8Bom),

        // without BOM
        _ if bytes.starts_with(&[0x00, b'<', 0x00, b'?']) => Some(DetectedEncoding::Utf16BeLike), // Some BE encoding, for example, UTF-16 or ISO-10646-UCS-2
        _ if bytes.starts_with(&[b'<', 0x00, b'?', 0x00]) => Some(DetectedEncoding::Utf16LeLike), // Some LE encoding, for example, UTF-16 or ISO-10646-UCS-2
        _ if bytes.starts_with(&[b'<', b'?', b'x', b'm']) => {
            Some(DetectedEncoding::AsciiCompatible)
        } // Some ASCII compatible

        _ => None,
    }
}

/// Possible scenarios for start-of-xml detection of encoding
///
/// See the documentation of [`detect_encoding`]
pub enum DetectedEncoding {
    /// Matches UTF-8 or some other ascii-compatible encoding
    AsciiCompatible,
    /// We saw a UTF-8 BOM
    Utf8Bom,
    /// Matches UTF-16-LE or some other UTF-16 compatible encoding (e.g. ISO-10646-UCS-2)
    Utf16LeLike,
    /// We saw a UTF-16 BOM in little-endian orientation
    Utf16LeBom,
    /// Matches UTF-16-BE or some other UTF-16 compatible encoding (e.g. ISO-10646-UCS-2)
    Utf16BeLike,
    /// We saw a UTF-16 BOM in big-endian orientation
    Utf16BeBom,
}

impl DetectedEncoding {
    /// Return an Encoding object appropriate for the detected encoding
    #[cfg(feature = "encoding")]
    pub const fn encoding(&self) -> &'static encoding_rs::Encoding {
        match self {
            DetectedEncoding::AsciiCompatible | DetectedEncoding::Utf8Bom => encoding_rs::UTF_8,
            DetectedEncoding::Utf16LeLike | DetectedEncoding::Utf16LeBom => encoding_rs::UTF_16LE,
            DetectedEncoding::Utf16BeLike | DetectedEncoding::Utf16BeBom => encoding_rs::UTF_16BE,
        }
    }

    /// Length of the BOM, which may need to be stripped from the input
    pub const fn bom_len(&self) -> usize {
        match self {
            DetectedEncoding::Utf8Bom => 3,
            DetectedEncoding::Utf16LeBom | DetectedEncoding::Utf16BeBom => 2,
            DetectedEncoding::AsciiCompatible
            | DetectedEncoding::Utf16LeLike
            | DetectedEncoding::Utf16BeLike => 0,
        }
    }
}

// Bytes read upfront so `set_encoding()` can be called before the main
// decode loop. Kept small (just enough for an XML declaration) to limit
// bytes decoded with a potentially wrong initial encoding.
#[cfg(feature = "encoding")]
const PREFIX_CAP: usize = 64;

#[cfg(feature = "encoding")]
struct Prefix {
    buf: [u8; PREFIX_CAP],
    len: usize,
    detected: bool,
}

/// A reader wrapper that decodes a byte stream from any encoding into UTF-8.
///
/// This reader wraps a [`BufRead`] source and uses [`encoding_rs::Decoder`] to
/// transcode the input into valid UTF-8. On first access, it detects the encoding
/// from BOM or XML declaration byte patterns and configures the appropriate decoder.
///
/// For UTF-8 input, this acts as a validating passthrough. For UTF-16 or other
/// encodings, the bytes are transcoded into UTF-8 in an internal buffer.
///
/// # Examples
///
/// ```
/// use std::io::Read;
/// use quick_xml::encoding::DecodingReader;
///
/// // UTF-8 input passes through:
/// let data = b"Hello, World!";
/// let mut reader = DecodingReader::new(&data[..]);
/// let mut buf = Vec::new();
/// reader.read_to_end(&mut buf).unwrap();
/// assert_eq!(buf, data);
/// ```
///
/// The example below shows how you can read documents using `DecodingReader`:
/// ```
/// use quick_xml::encoding::DecodingReader;
/// use quick_xml::events::Event;
/// use quick_xml::reader::Reader;
///
/// # fn to_utf16le_with_bom(string: &str) -> Vec<u8> {
/// #     let mut bytes = Vec::new();
/// #     bytes.extend_from_slice(&[0xFF, 0xFE]); // UTF-16 LE BOM
/// #     for ch in string.encode_utf16() {
/// #         bytes.extend_from_slice(&ch.to_le_bytes());
/// #     }
/// #     bytes
/// # }
/// let xml = to_utf16le_with_bom("<?xml encoding='UTF-16'?><element/>");
/// let mut decoder = DecodingReader::new(xml.as_ref());
/// let mut reader = Reader::from_reader(decoder);
///
/// let mut buf = Vec::new();
/// loop {
///     buf.clear();
///     match reader.read_event_into(&mut buf).unwrap() {
///         Event::Decl(e) => {
///             // If XML declaration contains unknown encoding name, None is returned
///             match e.encoder() {
///                 Some(encoding) => reader.get_mut().set_encoding(encoding),
///                 None => panic!("Unsupported encoding {:?}", e.encoding()),
///             }
///         }
///         Event::Eof => break,
///         _ => {}
///     }
/// }
/// ```
#[cfg(feature = "encoding")]
pub struct DecodingReader<R> {
    inner: R,
    decoder: encoding_rs::Decoder,
    /// `encoding_rs::Decoder` panics if called after finalization (`last=true`).
    /// This flag prevents that by short-circuiting `fill_buf` after completion.
    decoder_finished: bool,
    /// Decoded UTF-8 output buffer
    out_buf: Box<[u8]>,
    /// Start of unconsumed data in out_buf
    out_pos: usize,
    /// End of valid data in out_buf
    out_len: usize,
    /// Bytes read upfront for encoding detection and XML declaration buffering.
    /// `Some` until the prefix is fully drained; `None` afterward (main decode
    /// path takes over and the allocation is freed).
    prefix: Option<Box<Prefix>>,
    /// Whether the inner reader has reached EOF
    inner_eof: bool,
}

#[cfg(feature = "encoding")]
impl<R: std::fmt::Debug> std::fmt::Debug for DecodingReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodingReader")
            .field("inner", &self.inner)
            .field("encoding", &self.decoder.encoding())
            .field("out_pos", &self.out_pos)
            .field("out_len", &self.out_len)
            .field("inner_eof", &self.inner_eof)
            .field("prefix_active", &self.prefix.is_some())
            .finish()
    }
}

#[cfg(feature = "encoding")]
impl<R> DecodingReader<R> {
    /// Creates a new decoding reader.
    ///
    /// The encoding is auto-detected from BOM or XML declaration patterns on
    /// first access. Defaults to UTF-8 if no pattern is recognized.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            decoder: encoding_rs::UTF_8.new_decoder_without_bom_handling(),
            decoder_finished: false,
            out_buf: vec![0u8; 8192].into_boxed_slice(),
            out_pos: 0,
            out_len: 0,
            prefix: Some(Box::new(Prefix {
                buf: [0; PREFIX_CAP],
                len: 0,
                detected: false,
            })),
            inner_eof: false,
        }
    }

    /// Returns a reference to the underlying reader
    pub const fn get_ref(&self) -> &R {
        &self.inner
    }

    /// Returns a mutable reference to the underlying reader
    pub const fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Consumes this reader and returns the underlying reader
    pub fn into_inner(self) -> R {
        self.inner
    }

    /// Returns the encoding currently used by the decoder.
    ///
    /// Before the first read, this is always UTF-8. After encoding detection
    /// it reflects the detected (or overridden) encoding.
    pub fn encoding(&self) -> &'static encoding_rs::Encoding {
        self.decoder.encoding()
    }

    /// Replaces the decoder with one for the given encoding. The encoding
    /// must be ASCII-compatible (the parser cannot read the declaration otherwise).
    ///
    /// # Panics
    ///
    /// Panics if the prefix buffer has already been drained. Must be called
    /// before the prefix is exhausted — in practice, right after parsing
    /// the XML declaration.
    pub fn set_encoding(&mut self, encoding: &'static encoding_rs::Encoding) {
        // No-op when the encoding matches - replacing the decoder would discard
        // its internal state (e.g. a partial multi-byte sequence), corrupting output.
        // This check is safe regardless of prefix state since nothing changes.
        if self.decoder.encoding() == encoding {
            return;
        }
        assert!(
            self.prefix.is_some(),
            "set_encoding() called after prefix buffer was drained; \
             encoding can only be changed while the prefix is still active"
        );
        self.decoder = encoding.new_decoder_without_bom_handling();
        self.decoder_finished = false;
    }
}

#[cfg(feature = "encoding")]
impl<R: BufRead> BufRead for DecodingReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        // Fast path: serve already-decoded data
        if self.out_pos < self.out_len {
            return Ok(&self.out_buf[self.out_pos..self.out_len]);
        }

        // Reset output buffer
        self.out_pos = 0;
        self.out_len = 0;

        if let Some(prefix) = &mut self.prefix {
            // On first access, fill the prefix buffer and detect encoding.
            // The prefix is large enough to hold an entire XML declaration,
            // ensuring set_encoding() can be called before the greedy main
            // decode path consumes from inner.
            if !prefix.detected {
                prefix.detected = true;

                while prefix.len < PREFIX_CAP {
                    match self.inner.read(&mut prefix.buf[prefix.len..]) {
                        Ok(0) => {
                            self.inner_eof = true;
                            break;
                        }
                        Ok(n) => prefix.len += n,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                }

                let detection_bytes = &prefix.buf[..prefix.len];
                if let Some(detected) = detect_encoding(detection_bytes) {
                    let bom_len = detected.bom_len();
                    if bom_len > 0 {
                        prefix.buf.copy_within(bom_len..prefix.len, 0);
                        prefix.len -= bom_len;
                    }
                    let encoding = detected.encoding();
                    if encoding != encoding_rs::UTF_8 {
                        self.decoder = encoding.new_decoder_without_bom_handling();
                    }
                }
            }

            if self.decoder_finished {
                return Ok(&[]);
            }

            // Prefix fully decoded on a previous call - drop it and fall
            // through to the main decode path.
            if prefix.len == 0 {
                self.prefix = None;
            } else {
                // Decode from prefix buffer
                let src = &prefix.buf[..prefix.len];
                let (result, read, written) = self.decoder.decode_to_utf8_without_replacement(
                    src,
                    &mut self.out_buf[..],
                    false,
                );
                prefix.buf.copy_within(read..prefix.len, 0);
                prefix.len -= read;
                self.out_len = written;

                match result {
                    encoding_rs::DecoderResult::InputEmpty if written > 0 => {
                        return Ok(&self.out_buf[..self.out_len]);
                    }
                    encoding_rs::DecoderResult::InputEmpty => {
                        // prefix.len is now 0; keep prefix alive for
                        // set_encoding() - it will be dropped on the next call.
                    }
                    encoding_rs::DecoderResult::OutputFull => {
                        return Ok(&self.out_buf[..self.out_len]);
                    }
                    encoding_rs::DecoderResult::Malformed(_, _) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            EncodingError::Other(self.decoder.encoding()),
                        ));
                    }
                }
                // InputEmpty with written == 0: prefix drained, decoder may
                // hold partial internal state (e.g. a lone byte of UTF-16).
                // Drop prefix and fall through to the main decode path.
                if prefix.len == 0 {
                    self.prefix = None;
                }
            }
        }

        if self.decoder_finished {
            return Ok(&[]);
        }

        // Loop until we produce output, hit EOF, or get an error.
        // The decoder may consume input into internal state (e.g., partial
        // UTF-16 code unit) without producing output - we must keep feeding
        // it more input rather than returning an empty slice (which signals EOF).
        loop {
            // EOF flush path: tell decoder this is the last chunk
            if self.inner_eof {
                let (result, _, written) = self.decoder.decode_to_utf8_without_replacement(
                    b"",
                    &mut self.out_buf[..],
                    true,
                );
                self.out_len = written;
                match result {
                    encoding_rs::DecoderResult::InputEmpty => {
                        self.decoder_finished = true;
                        return Ok(&self.out_buf[..self.out_len]);
                    }
                    encoding_rs::DecoderResult::OutputFull => {
                        return Ok(&self.out_buf[..self.out_len]);
                    }
                    encoding_rs::DecoderResult::Malformed(_, _) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            EncodingError::Other(self.decoder.encoding()),
                        ));
                    }
                }
            }

            // Main decode path: read from inner, decode into out_buf
            let (result, read, written) = {
                let src = self.inner.fill_buf()?;
                if src.is_empty() {
                    self.inner_eof = true;
                    continue; // will hit EOF flush path on next iteration
                }
                self.decoder
                    .decode_to_utf8_without_replacement(src, &mut self.out_buf[..], false)
            };
            self.inner.consume(read);
            self.out_len = written;

            match result {
                encoding_rs::DecoderResult::InputEmpty if written > 0 => {
                    return Ok(&self.out_buf[..self.out_len]);
                }
                encoding_rs::DecoderResult::InputEmpty => {
                    // Decoder consumed all input but produced no output
                    // (e.g., 1 byte of a 2-byte UTF-16 code unit stored
                    // in decoder internal state). Loop to get more input.
                }
                encoding_rs::DecoderResult::OutputFull => {
                    // Output buffer full; return what we have. Remaining
                    // input will be decoded on the next fill_buf call.
                    return Ok(&self.out_buf[..self.out_len]);
                }
                encoding_rs::DecoderResult::Malformed(_, _) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        EncodingError::Other(self.decoder.encoding()),
                    ));
                }
            }
        }
    }

    fn consume(&mut self, amt: usize) {
        debug_assert!(
            self.out_pos + amt <= self.out_len,
            "consume({amt}) out of range: out_pos={}, out_len={}",
            self.out_pos,
            self.out_len,
        );
        self.out_pos += amt;
    }
}

#[cfg(feature = "encoding")]
impl<R: BufRead> Read for DecodingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let available = self.fill_buf()?;
        if available.is_empty() {
            return Ok(0);
        }
        let len = available.len().min(buf.len());
        buf[..len].copy_from_slice(&available[..len]);
        self.consume(len);
        Ok(len)
    }
}

#[cfg(all(test, feature = "encoding"))]
mod decoding_reader {
    use super::*;
    use std::io::{BufReader, Read};

    /// Helper reader that returns data in fixed-size chunks
    struct ChunkedReader<'a> {
        data: &'a [u8],
        pos: usize,
        chunk_size: usize,
    }

    impl<'a> ChunkedReader<'a> {
        fn new(data: &'a [u8], chunk_size: usize) -> Self {
            Self {
                data,
                pos: 0,
                chunk_size,
            }
        }
    }

    impl<'a> Read for ChunkedReader<'a> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.pos >= self.data.len() {
                return Ok(0);
            }
            let len = self
                .chunk_size
                .min(buf.len())
                .min(self.data.len() - self.pos);
            buf[..len].copy_from_slice(&self.data[self.pos..self.pos + len]);
            self.pos += len;
            Ok(len)
        }
    }

    /// Encode a string as UTF-16 LE bytes with BOM
    fn utf16le_with_bom(s: &str) -> Vec<u8> {
        let mut out = vec![0xFF, 0xFE]; // UTF-16 LE BOM
        for code_unit in s.encode_utf16() {
            out.extend_from_slice(&code_unit.to_le_bytes());
        }
        out
    }

    /// Encode a string as UTF-16 BE bytes with BOM
    fn utf16be_with_bom(s: &str) -> Vec<u8> {
        let mut out = vec![0xFE, 0xFF]; // UTF-16 BE BOM
        for code_unit in s.encode_utf16() {
            out.extend_from_slice(&code_unit.to_be_bytes());
        }
        out
    }

    /// Encode a string as UTF-16 LE bytes without BOM
    fn utf16le_no_bom(s: &str) -> Vec<u8> {
        let mut out = Vec::new();
        for code_unit in s.encode_utf16() {
            out.extend_from_slice(&code_unit.to_le_bytes());
        }
        out
    }

    /// Encode a string as UTF-16 BE bytes without BOM
    fn utf16be_no_bom(s: &str) -> Vec<u8> {
        let mut out = Vec::new();
        for code_unit in s.encode_utf16() {
            out.extend_from_slice(&code_unit.to_be_bytes());
        }
        out
    }

    /// Read all bytes from a reader into a String
    fn read_all(reader: &mut DecodingReader<impl BufRead>) -> io::Result<String> {
        let mut result = Vec::new();
        reader.read_to_end(&mut result)?;
        Ok(String::from_utf8(result).expect("DecodingReader should produce valid UTF-8"))
    }

    /// Simple edge cases and degenerate inputs
    mod edge_cases {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Zero-length input should immediately return EOF (n == 0).
        #[test]
        fn empty_input() {
            let data = b"";
            let mut reader = DecodingReader::new(&data[..]);
            let mut buf = [0u8; 10];
            let n = reader.read(&mut buf).unwrap();
            assert_eq!(n, 0);
        }

        /// A UTF-8 BOM with no payload should decode to an empty string.
        #[test]
        fn utf8_bom_only() {
            let data = b"\xEF\xBB\xBF";
            let mut reader = DecodingReader::new(&data[..]);
            assert_eq!(read_all(&mut reader).unwrap(), "");
        }

        /// A UTF-16 LE BOM with no payload should decode to an empty string.
        #[test]
        fn utf16le_bom_only() {
            let data = &[0xFF, 0xFE];
            let mut reader = DecodingReader::new(&data[..]);
            assert_eq!(read_all(&mut reader).unwrap(), "");
        }

        /// A UTF-16 BE BOM with no payload should decode to an empty string.
        #[test]
        fn utf16be_bom_only() {
            let data = &[0xFE, 0xFF];
            let mut reader = DecodingReader::new(&data[..]);
            assert_eq!(read_all(&mut reader).unwrap(), "");
        }

        /// Invalid UTF-8 (no BOM, so treated as UTF-8) must produce an error.
        #[test]
        fn invalid_utf8_is_rejected() {
            let data: &[u8] = &[0x48, 0x65, 0x6C, 0xFF, 0xFE];
            let mut reader = DecodingReader::new(&data[..]);
            let err = read_all(&mut reader).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }

        /// An odd trailing byte in UTF-16 is malformed and must produce an error.
        #[test]
        fn truncated_utf16_at_eof() {
            // UTF-16 LE BOM + one valid code unit + one incomplete byte
            let data: &[u8] = &[0xFF, 0xFE, 0x48, 0x00, 0x65];
            let mut reader = DecodingReader::new(&data[..]);
            let err = read_all(&mut reader).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }

        /// A 1-byte output buffer forces one byte per read() call; verifies
        /// multi-byte UTF-8 sequences are still assembled correctly.
        #[test]
        fn read_with_one_byte_buffer() {
            let data = "Hello, 世界!".as_bytes();
            let mut reader = DecodingReader::new(&data[..]);
            let mut result = Vec::new();
            let mut buf = [0u8; 1];
            loop {
                let n = reader.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                result.extend_from_slice(&buf[..n]);
            }
            assert_eq!(String::from_utf8(result).unwrap(), "Hello, 世界!");
        }
    }

    /// Tests that exercise the BufRead contract (fill_buf + consume) directly,
    /// as opposed to the Read-based helpers used elsewhere.
    mod bufread_interface {
        use super::*;
        use pretty_assertions::assert_eq;
        use std::io::BufRead;

        /// Basic fill_buf/consume cycle: partial consume leaves remaining
        /// data available on the next fill_buf call.
        #[test]
        fn fill_buf_and_consume() {
            let data = b"Hello, World!";
            let mut reader = DecodingReader::new(&data[..]);

            let buf = reader.fill_buf().unwrap();
            assert!(!buf.is_empty());
            assert_eq!(buf[0], b'H');

            // Consume only part of the buffer
            reader.consume(5);

            let buf = reader.fill_buf().unwrap();
            assert!(!buf.is_empty());
            assert_eq!(buf[0], b',');
        }

        /// Drain the reader via fill_buf/consume, then confirm it stays at EOF.
        #[test]
        fn partial_consume_then_read_more() {
            let data = b"Hello, World!";
            let mut reader = DecodingReader::new(&data[..]);

            // Collect all output via fill_buf/consume
            let mut result = Vec::new();
            loop {
                let buf = reader.fill_buf().unwrap();
                if buf.is_empty() {
                    break;
                }
                result.extend_from_slice(buf);
                let len = buf.len();
                reader.consume(len);
            }
            assert_eq!(std::str::from_utf8(&result).unwrap(), "Hello, World!");

            // Should remain at EOF
            let buf = reader.fill_buf().unwrap();
            assert!(buf.is_empty());
        }

        /// Calling fill_buf() repeatedly after EOF must keep returning empty
        /// (and not panic - encoding_rs::Decoder panics if called after finalization).
        #[test]
        fn fill_buf_after_eof_is_idempotent() {
            let data = b"Hello";
            let mut reader = DecodingReader::new(&data[..]);

            loop {
                let buf = reader.fill_buf().unwrap();
                if buf.is_empty() {
                    break;
                }
                let len = buf.len();
                reader.consume(len);
            }

            for _ in 0..3 {
                let buf = reader.fill_buf().unwrap();
                assert!(buf.is_empty());
            }
        }

        /// consume() past the buffered length must trigger a debug_assert panic.
        #[test]
        #[should_panic(expected = "consume")]
        fn consume_overflow_panics_in_debug() {
            let data = b"Hi";
            let mut reader = DecodingReader::new(&data[..]);
            let _ = reader.fill_buf().unwrap();
            reader.consume(100);
        }
    }

    mod accessors {
        use super::*;
        use pretty_assertions::assert_eq;
        use std::io::Cursor;

        #[test]
        fn get_ref() {
            let data = b"Hello";
            let cursor = Cursor::new(data.to_vec());
            let reader = DecodingReader::new(cursor);
            assert_eq!(reader.get_ref().get_ref(), data);
        }

        #[test]
        fn get_mut() {
            let data = b"Hello";
            let cursor = Cursor::new(data.to_vec());
            let mut reader = DecodingReader::new(cursor);
            reader.get_mut().set_position(2);
            assert_eq!(reader.get_ref().position(), 2);
        }

        #[test]
        fn into_inner() {
            let data = b"Hello";
            let cursor = Cursor::new(data.to_vec());
            let reader = DecodingReader::new(cursor);
            let inner = reader.into_inner();
            assert_eq!(inner.get_ref(), data);
        }

        /// Default encoding before any reads is UTF-8.
        #[test]
        fn encoding_default_is_utf8() {
            let reader = DecodingReader::new(&b"Hello"[..]);
            assert_eq!(reader.encoding(), encoding_rs::UTF_8);
        }
    }

    // TODO: These tests emulate the updating of the internal decoder after reading the XML decl.
    // Since `Reader` currently only speaks the `BufRead` trait, we can't test that directly.
    // Eventually once `Reader` knows about the underlying `DecodingReader` we should test
    // that directly.

    /// Tests for encoding() and set_encoding(): detection, switching,
    /// same-encoding no-op safety, and mid-stream override behavior.
    mod encoding_switching {
        use super::*;
        use pretty_assertions::assert_eq;
        use std::io::BufRead;

        /// Encoding reflects BOM detection after first read.
        #[test]
        fn encoding_reflects_detection() {
            let data = utf16le_with_bom("Hello");
            let mut reader = DecodingReader::new(&data[..]);
            let _ = read_all(&mut reader).unwrap();
            assert_eq!(reader.encoding(), encoding_rs::UTF_16LE);
        }

        /// set_encoding switches the active decoder.
        #[test]
        fn set_encoding_changes_encoding() {
            let mut reader = DecodingReader::new(&b"Hello"[..]);
            assert_eq!(reader.encoding(), encoding_rs::UTF_8);
            reader.set_encoding(encoding_rs::UTF_16LE);
            assert_eq!(reader.encoding(), encoding_rs::UTF_16LE);
        }

        /// set_encoding after reading preserves already-buffered output.
        #[test]
        fn set_encoding_preserves_buffered_output() {
            let data = b"Hello";
            let mut reader = DecodingReader::new(&data[..]);

            let buf = reader.fill_buf().unwrap();
            assert_eq!(buf, b"Hello");

            reader.set_encoding(encoding_rs::WINDOWS_1252);
            assert_eq!(reader.encoding(), encoding_rs::WINDOWS_1252);

            // Buffered data is unchanged
            let buf = reader.fill_buf().unwrap();
            assert_eq!(buf, b"Hello");
        }

        /// Calling set_encoding with the already-active encoding is a no-op:
        /// the decoder's internal state is preserved and decoding continues
        /// without corruption.
        #[test]
        fn set_encoding_same_as_detected_is_noop() {
            let data = b"Hello, World!";
            let mut reader = DecodingReader::new(&data[..]);

            // Trigger detection and consume the first chunk
            let first_chunk;
            {
                let buf = reader.fill_buf().unwrap();
                assert!(buf.len() > 0);
                first_chunk = std::str::from_utf8(buf).unwrap().to_string();
                let n = buf.len();
                reader.consume(n);
            }
            assert_eq!(reader.encoding(), encoding_rs::UTF_8);

            // "Re-set" to the same encoding - must not reset decoder state
            reader.set_encoding(encoding_rs::UTF_8);
            assert_eq!(reader.encoding(), encoding_rs::UTF_8);

            // Read the rest - combined output must equal the original string
            let rest = read_all(&mut reader).unwrap();
            assert_eq!(format!("{first_chunk}{rest}"), "Hello, World!");
        }

        /// set_encoding mid-stream: read some UTF-8 data, switch encoding,
        /// then verify the encoding accessor reflects the change.
        #[test]
        fn set_encoding_mid_stream() {
            let data = b"Hello, World!";
            let mut reader = DecodingReader::new(&data[..]);

            // Read a few bytes under UTF-8
            let buf = reader.fill_buf().unwrap();
            let n = std::cmp::min(buf.len(), 5);
            reader.consume(n);

            assert_eq!(reader.encoding(), encoding_rs::UTF_8);
            reader.set_encoding(encoding_rs::WINDOWS_1252);
            assert_eq!(reader.encoding(), encoding_rs::WINDOWS_1252);

            // Remaining data still readable (ASCII is identical in both encodings)
            let rest = read_all(&mut reader).unwrap();
            assert_eq!(rest, ", World!");
        }
    }

    /// Tests exercised across a matrix of (input text x encoding x read strategy).
    /// Each test encodes a string, feeds it through DecodingReader, and asserts the
    /// decoded output matches the original. This covers BOM detection, UTF-16
    /// transcoding, surrogate pairs, and multi-byte UTF-8 characters in one sweep.
    ///
    /// Examples:
    ///
    /// - UTF-8 passthrough (ASCII and multibyte) with and without BOM
    /// - UTF-16 LE/BE decoding with and without BOM
    /// - BOM-less UTF-16 detection via `<?xml` byte pattern
    /// - UTF-16 surrogate pairs (astral plane characters)
    /// - Chunked input at misaligned boundaries (odd chunk sizes vs 2-byte code units)
    /// - One-byte-at-a-time delivery for all encodings
    /// - Inputs larger than the 8192-byte internal output buffer
    /// - Empty and single-character inputs (prefix-only decode path)
    mod matrix_decoding_tests {
        use super::*;
        use pretty_assertions::assert_eq;

        struct TestCase {
            label: &'static str,
            text: &'static str,
        }

        /// Short inputs that exercise different Unicode categories.
        const CASES: &[TestCase] = &[
            TestCase {
                label: "empty",
                text: "",
            },
            TestCase {
                label: "single_multibyte",
                // Single 3-byte character - entire content fits in the prefix buffer
                text: "€",
            },
            TestCase {
                label: "ascii",
                text: "Hello",
            },
            TestCase {
                label: "multibyte",
                // 3-byte CJK + 4-byte emoji
                text: "Hello, 世界! 😀",
            },
            TestCase {
                label: "surrogate_pairs",
                // U+1D11E and U+1F3B5 require surrogate pairs in UTF-16
                text: "Music: 𝄞🎵",
            },
            TestCase {
                label: "xml_declaration",
                // Enables BOM-less UTF-16 detection via the <?xml byte pattern
                text: "<?xml version=\"1.0\"?><root/>",
            },
        ];

        /// Inputs larger than the 8192-byte internal output buffer.
        fn large_cases() -> Vec<(&'static str, String)> {
            vec![
                ("large_ascii", "abcdefghij".repeat(1000)),
                ("large_multibyte", "Hello, 世界! 😀 ".repeat(500)),
            ]
        }

        enum Encoding {
            Utf8,
            Utf8Bom,
            Utf16Le,
            Utf16Be,
            Utf16LeNoBom,
            Utf16BeNoBom,
        }

        impl Encoding {
            fn encode(&self, text: &str) -> Vec<u8> {
                match self {
                    Encoding::Utf8 => text.as_bytes().to_vec(),
                    Encoding::Utf8Bom => {
                        let mut out = vec![0xEF, 0xBB, 0xBF];
                        out.extend_from_slice(text.as_bytes());
                        out
                    }
                    Encoding::Utf16Le => utf16le_with_bom(text),
                    Encoding::Utf16Be => utf16be_with_bom(text),
                    Encoding::Utf16LeNoBom => utf16le_no_bom(text),
                    Encoding::Utf16BeNoBom => utf16be_no_bom(text),
                }
            }

            fn label(&self) -> &'static str {
                match self {
                    Encoding::Utf8 => "utf8",
                    Encoding::Utf8Bom => "utf8_bom",
                    Encoding::Utf16Le => "utf16le",
                    Encoding::Utf16Be => "utf16be",
                    Encoding::Utf16LeNoBom => "utf16le_no_bom",
                    Encoding::Utf16BeNoBom => "utf16be_no_bom",
                }
            }

            /// BOM-less UTF-16 detection requires a `<?xml` prefix, so those
            /// encodings are only included for inputs that start with one.
            fn all_for(text: &str) -> Vec<Encoding> {
                let mut encs = vec![
                    Encoding::Utf8,
                    Encoding::Utf8Bom,
                    Encoding::Utf16Le,
                    Encoding::Utf16Be,
                ];
                if text.starts_with("<?xml") {
                    encs.push(Encoding::Utf16LeNoBom);
                    encs.push(Encoding::Utf16BeNoBom);
                }
                encs
            }
        }

        /// Encode -> decode with the entire input available at once.
        #[test]
        fn bulk_read() {
            for case in CASES {
                for enc in Encoding::all_for(case.text) {
                    let data = enc.encode(case.text);
                    let mut reader = DecodingReader::new(&data[..]);
                    assert_eq!(
                        read_all(&mut reader).unwrap(),
                        case.text,
                        "bulk_read failed: case={}, encoding={}",
                        case.label,
                        enc.label(),
                    );
                }
            }
            for (label, text) in large_cases() {
                for enc in Encoding::all_for(&text) {
                    let data = enc.encode(&text);
                    let mut reader = DecodingReader::new(&data[..]);
                    assert_eq!(
                        read_all(&mut reader).unwrap(),
                        text,
                        "bulk_read failed: case={}, encoding={}",
                        label,
                        enc.label(),
                    );
                }
            }
        }

        /// Encode -> decode with the input delivered in fixed-size chunks via
        /// ChunkedReader, testing that the decoder handles arbitrary byte
        /// boundaries (mid-BOM, mid-code-unit, mid-surrogate-pair).
        #[test]
        fn chunked_read() {
            for case in CASES {
                for enc in Encoding::all_for(case.text) {
                    for chunk_size in [1, 2, 3, 4, 5] {
                        let data = enc.encode(case.text);
                        let mut reader = DecodingReader::new(BufReader::new(ChunkedReader::new(
                            &data, chunk_size,
                        )));
                        assert_eq!(
                            read_all(&mut reader).unwrap(),
                            case.text,
                            "chunked_read failed: case={}, encoding={}, chunk_size={}",
                            case.label,
                            enc.label(),
                            chunk_size,
                        );
                    }
                }
            }
        }

        /// Same as chunked_read but with inputs exceeding the 8192-byte
        /// internal output buffer, exercising the multi-fill_buf decode loop.
        #[test]
        fn large_chunked_read() {
            for (label, text) in large_cases() {
                for enc in Encoding::all_for(&text) {
                    for chunk_size in [1, 2, 3, 4, 5] {
                        let data = enc.encode(&text);
                        let mut reader = DecodingReader::new(BufReader::new(ChunkedReader::new(
                            &data, chunk_size,
                        )));
                        assert_eq!(
                            read_all(&mut reader).unwrap(),
                            text,
                            "large_chunked_read failed: case={}, encoding={}, chunk_size={}",
                            label,
                            enc.label(),
                            chunk_size,
                        );
                    }
                }
            }
        }
    }
}
