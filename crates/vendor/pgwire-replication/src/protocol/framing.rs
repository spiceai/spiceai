use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{PgWireError, Result};

/// Maximum backend message size (1GB) - prevents memory exhaustion from malformed length fields
/// This is more than enough.
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendMessage {
    pub tag: u8,
    pub payload: Bytes, // payload excludes the 4-byte length field
}

impl BackendMessage {
    /// Returns true if this is an `ErrorResponse` ('E')
    #[inline]
    pub fn is_error(&self) -> bool {
        self.tag == b'E'
    }

    /// Returns true if this is a `ReadyForQuery` ('Z')
    #[inline]
    pub fn is_ready_for_query(&self) -> bool {
        self.tag == b'Z'
    }

    /// Returns true if this is `CopyBothResponse` ('W')
    #[inline]
    pub fn is_copy_both_response(&self) -> bool {
        self.tag == b'W'
    }

    /// Returns true if this is `CopyData` ('d')
    #[inline]
    pub fn is_copy_data(&self) -> bool {
        self.tag == b'd'
    }

    /// Returns true if this is `AuthenticationRequest` ('R')
    #[inline]
    pub fn is_auth_request(&self) -> bool {
        self.tag == b'R'
    }
}

/// Read a single complete backend message from `rd`.
///
/// # Errors
/// Returns [`PgWireError::Io`] on a read failure or EOF, or
/// [`PgWireError::Protocol`] if the framed length is invalid or oversized.
pub async fn read_backend_message<R: AsyncRead + Unpin>(rd: &mut R) -> Result<BackendMessage> {
    let mut reader = MessageReader::new();
    reader.read(rd).await
}

/// Cancellation-safe backend message reader.
///
/// `PostgreSQL` backend messages span multiple `read` operations (5-byte header,
/// then a variable payload). A naive implementation using `read_exact` is
/// **not** cancellation-safe: if the future is dropped between reads (e.g. by
/// `tokio::select!` or `tokio::time::timeout`), bytes already pulled from the
/// underlying stream are lost and the next read mis-parses the wire stream.
///
/// `MessageReader` externalizes the partial-read state so it survives across
/// dropped futures. Each call to [`read`](Self::read) uses one-shot
/// `AsyncReadExt::read` (which **is** cancel-safe) and accumulates progress
/// on `self`. If the returned future is dropped, no bytes are lost; the next
/// invocation resumes from where the previous one left off.
pub struct MessageReader {
    hdr: [u8; 5],
    hdr_filled: usize,
    payload: BytesMut,
    payload_filled: usize,
    /// `Some` once the header has been fully read and parsed; reset to
    /// `None` after each completed message.
    payload_len: Option<usize>,
    tag: u8,
}

impl MessageReader {
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(4096)
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            hdr: [0u8; 5],
            hdr_filled: 0,
            payload: BytesMut::with_capacity(capacity),
            payload_filled: 0,
            payload_len: None,
            tag: 0,
        }
    }

    /// Read the next complete backend message.
    ///
    /// Cancellation-safe: dropping the returned future preserves all progress
    /// so far on `self`. Re-call to resume.
    ///
    /// # Errors
    /// Returns [`PgWireError::Io`] on a read failure or EOF, or
    /// [`PgWireError::Protocol`] if the framed length is invalid or oversized.
    pub async fn read<R: AsyncRead + Unpin>(&mut self, rd: &mut R) -> Result<BackendMessage> {
        // Phase 1: fill the 5-byte header
        while self.hdr_filled < 5 {
            let n = rd.read(&mut self.hdr[self.hdr_filled..]).await?;
            if n == 0 {
                return Err(PgWireError::Io(std::sync::Arc::new(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF while reading backend message header",
                ))));
            }
            self.hdr_filled += n;
        }

        // Phase 2: parse the header (idempotent — runs once per message)
        if self.payload_len.is_none() {
            let len = i32::from_be_bytes([self.hdr[1], self.hdr[2], self.hdr[3], self.hdr[4]]);

            if len < 4 {
                // Reset so the reader is reusable after a protocol error is
                // surfaced (callers typically tear down on this anyway).
                self.hdr_filled = 0;
                return Err(PgWireError::Protocol(format!(
                    "invalid backend message length: {len}"
                )));
            }

            #[expect(
                clippy::cast_sign_loss,
                reason = "len is checked >= 4 above, so len - 4 is non-negative"
            )]
            let payload_len = (len - 4) as usize;

            if payload_len > MAX_MESSAGE_SIZE {
                self.hdr_filled = 0;
                return Err(PgWireError::Protocol(format!(
                    "backend message too large: {payload_len} bytes (max {MAX_MESSAGE_SIZE})"
                )));
            }

            self.tag = self.hdr[0];
            self.payload.clear();
            self.payload.resize(payload_len, 0);
            self.payload_filled = 0;
            self.payload_len = Some(payload_len);
        }

        // Set either just above (fresh header) or on a prior call resumed after
        // a dropped future; a typed error keeps this non-panicking.
        let Some(payload_len) = self.payload_len else {
            return Err(PgWireError::Internal(
                "MessageReader: payload length unset after header parse".into(),
            ));
        };

        // Phase 3: fill the payload
        while self.payload_filled < payload_len {
            let n = rd.read(&mut self.payload[self.payload_filled..]).await?;
            if n == 0 {
                return Err(PgWireError::Io(std::sync::Arc::new(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF while reading backend message payload",
                ))));
            }
            self.payload_filled += n;
        }

        // Phase 4: take payload, reset state for next message
        let payload = self.payload.split().freeze();
        let tag = self.tag;
        self.hdr_filled = 0;
        self.payload_len = None;
        self.payload_filled = 0;

        Ok(BackendMessage { tag, payload })
    }
}

impl Default for MessageReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Incremental, cancellation-safe, zero-copy backend-message reader.
///
/// This is the reader used by the streaming replication loop. Unlike
/// [`MessageReader`] — which reads each message into a freshly zero-filled
/// buffer (`resize(len, 0)`) via per-byte-range `read` calls — `FrameReader`
/// keeps a single growable buffer that it fills with
/// [`read_buf`](tokio::io::AsyncReadExt::read_buf), reading directly into
/// **uninitialized** spare capacity (no `memset`), and slices complete frames
/// out of it with `split_to` (zero-copy: each returned payload shares the
/// buffer's allocation via refcount). Compared with the previous
/// `BufReader` + `MessageReader` combination this removes both the
/// per-message zero-fill and the extra copy the intermediate `BufReader`
/// added (bytes went kernel → `BufReader` → per-message `BytesMut`; now they
/// go kernel → one `BytesMut`, and frames are views into it).
///
/// # Cancellation safety
///
/// [`next`](Self::next) only ever awaits a single `read_buf` call, which
/// resolves atomically with the buffer advance — a dropped future loses no
/// bytes. Any partially-buffered frame stays in `buf` and the next call
/// resumes. This makes it safe to drive from `tokio::select!` /
/// `tokio::time::timeout`.
///
/// # Memory safety under adversarial input
///
/// Buffer growth is driven by bytes **actually read**, not by the declared
/// frame length: capacity is only grown once existing spare is exhausted, and
/// each grow at most doubles (bounded below by [`READ_CHUNK`], above by the
/// remaining bytes of the in-flight frame). A frame whose declared length
/// exceeds `max_message_size` is rejected before any allocation. Together these
/// bound the amplification of a bogus/oversized length field to ~2× the bytes
/// the peer actually sent, instead of the eager `resize(declared_len, 0)` the
/// old reader performed.
pub struct FrameReader {
    buf: BytesMut,
    max_message_size: usize,
}

/// Minimum spare capacity ensured before each socket read, and the floor for
/// geometric buffer growth. Larger than a typical TCP segment so many small
/// WAL messages arrive per syscall.
const READ_CHUNK: usize = 128 * 1024;

impl FrameReader {
    /// Create a reader that rejects frames whose payload exceeds
    /// `max_message_size` bytes.
    #[must_use]
    pub fn new(max_message_size: usize) -> Self {
        Self::with_capacity(READ_CHUNK, max_message_size)
    }

    #[must_use]
    pub fn with_capacity(capacity: usize, max_message_size: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
            max_message_size,
        }
    }

    /// Bytes currently buffered but not yet consumed.
    #[inline]
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Current allocated capacity of the read buffer. Exposed for diagnostics
    /// and to assert the anti-amplification bound in tests.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Returns `true` if a complete frame is already buffered, so
    /// [`next`](Self::next) will return it without awaiting a socket read.
    /// Used by the streaming loop to drain buffered frames in a tight loop.
    ///
    /// Returns `false` on a header that is not yet fully buffered *or* that is
    /// malformed — a malformed header surfaces as an error from `next`.
    #[inline]
    #[must_use]
    pub fn has_buffered_frame(&self) -> bool {
        matches!(self.frame_len(), Ok(Some(frame_len)) if self.buf.len() >= frame_len)
    }

    /// Total wire length (tag + length field + payload) of the frame at the
    /// front of the buffer, if its 5-byte header is fully buffered. `Ok(None)`
    /// if the header is not yet complete; `Err` if the length is invalid or
    /// exceeds `max_message_size`.
    #[inline]
    #[expect(
        clippy::cast_sign_loss,
        reason = "len is checked >= 4 before the usize casts, so both are non-negative"
    )]
    fn frame_len(&self) -> Result<Option<usize>> {
        if self.buf.len() < 5 {
            return Ok(None);
        }
        let len = i32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        if len < 4 {
            return Err(PgWireError::Protocol(format!(
                "invalid backend message length: {len}"
            )));
        }
        let payload_len = (len - 4) as usize;
        if payload_len > self.max_message_size {
            return Err(PgWireError::Protocol(format!(
                "backend message too large: {payload_len} bytes (max {})",
                self.max_message_size
            )));
        }
        // Wire framing: 1 tag byte + `len`, where `len` already counts the
        // 4-byte length field plus the payload.
        Ok(Some(1 + len as usize))
    }

    /// Slice one complete frame out of the buffer without reading, if present.
    fn try_decode(&mut self) -> Result<Option<BackendMessage>> {
        let Some(frame_len) = self.frame_len()? else {
            return Ok(None);
        };
        if self.buf.len() < frame_len {
            return Ok(None);
        }
        let mut frame = self.buf.split_to(frame_len);
        let tag = frame[0];
        frame.advance(5); // drop tag + 4-byte length field
        Ok(Some(BackendMessage {
            tag,
            payload: frame.freeze(),
        }))
    }

    /// Ensure there is spare capacity to read into, growing geometrically so
    /// accumulating a large frame stays amortized O(n). Growth is driven by
    /// actual fill (called only when spare is low), never by the declared
    /// length, and is capped at the in-flight frame's remaining bytes so we
    /// never over-allocate beyond the message being assembled.
    fn ensure_read_capacity(&mut self) {
        let spare = self.buf.capacity() - self.buf.len();
        if spare >= READ_CHUNK {
            return;
        }
        // At least READ_CHUNK, at most a doubling of current capacity.
        let mut additional = READ_CHUNK.max(self.buf.capacity());
        if let Ok(Some(frame_len)) = self.frame_len() {
            let needed = frame_len.saturating_sub(self.buf.len());
            additional = additional.min(needed.max(READ_CHUNK));
        }
        self.buf.reserve(additional);
    }

    /// Read the next complete backend message.
    ///
    /// Cancellation-safe — see the type-level docs.
    ///
    /// # Errors
    /// Returns [`PgWireError::Io`] (`UnexpectedEof`) if the peer closes the
    /// stream mid-message, or [`PgWireError::Protocol`] if a framed length is
    /// invalid or exceeds `max_message_size`.
    pub async fn next<R: AsyncRead + Unpin>(&mut self, rd: &mut R) -> Result<BackendMessage> {
        loop {
            if let Some(msg) = self.try_decode()? {
                return Ok(msg);
            }
            self.ensure_read_capacity();
            let n = rd.read_buf(&mut self.buf).await?;
            if n == 0 {
                return Err(PgWireError::Io(std::sync::Arc::new(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF while reading backend message",
                ))));
            }
        }
    }
}

/// Read a single backend message, reusing the provided buffer.
///
/// **Not** cancellation-safe — see [`MessageReader`] for a cancel-safe
/// alternative used in the streaming loop.
///
/// # Errors
/// Returns [`PgWireError::Io`] on a read failure or EOF, or
/// [`PgWireError::Protocol`] if the framed length is invalid or oversized.
#[expect(
    clippy::cast_sign_loss,
    reason = "len is checked >= 4 above, so len - 4 is non-negative"
)]
pub async fn read_backend_message_into<R: AsyncRead + Unpin>(
    rd: &mut R,
    buf: &mut BytesMut,
) -> Result<BackendMessage> {
    let mut hdr = [0u8; 5];
    rd.read_exact(&mut hdr).await?;
    let tag = hdr[0];
    let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]);

    if len < 4 {
        return Err(PgWireError::Protocol(format!(
            "invalid backend message length: {len}"
        )));
    }

    let payload_len = (len - 4) as usize;

    if payload_len > MAX_MESSAGE_SIZE {
        return Err(PgWireError::Protocol(format!(
            "backend message too large: {payload_len} bytes (max {MAX_MESSAGE_SIZE})"
        )));
    }

    buf.clear();
    buf.resize(payload_len, 0);
    rd.read_exact(&mut buf[..]).await?;
    Ok(BackendMessage {
        tag,
        payload: buf.split().freeze(),
    })
}

/// Send an `SSLRequest` packet.
///
/// # Errors
/// Returns [`PgWireError::Io`] if the write or flush fails.
pub async fn write_ssl_request<W: AsyncWrite + Unpin>(wr: &mut W) -> Result<()> {
    let mut buf = [0u8; 8];
    buf[0..4].copy_from_slice(&(8i32).to_be_bytes());
    buf[4..8].copy_from_slice(&(80_877_103i32).to_be_bytes());
    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

/// Send a `StartupMessage` with the given protocol version and parameters.
///
/// # Errors
/// Returns [`PgWireError::Io`] if the write or flush fails.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "startup message length is bounded well below i32::MAX"
)]
pub async fn write_startup_message<W: AsyncWrite + Unpin>(
    wr: &mut W,
    protocol_version: i32,
    params: &[(&str, &str)],
) -> Result<()> {
    let mut buf = BytesMut::with_capacity(256);
    buf.put_i32(0); // length placeholder
    buf.put_i32(protocol_version);

    for (k, v) in params {
        buf.extend_from_slice(k.as_bytes());
        buf.put_u8(0);
        buf.extend_from_slice(v.as_bytes());
        buf.put_u8(0);
    }
    buf.put_u8(0); // terminator

    let len = buf.len() as i32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());

    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

/// Send a simple Query message.
///
/// # Errors
/// Returns [`PgWireError::Io`] if the write or flush fails.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "query message length is bounded well below i32::MAX"
)]
pub async fn write_query<W: AsyncWrite + Unpin>(wr: &mut W, sql: &str) -> Result<()> {
    let mut buf = BytesMut::with_capacity(sql.len() + 64);
    buf.put_u8(b'Q');
    buf.put_i32(0);
    buf.extend_from_slice(sql.as_bytes());
    buf.put_u8(0);

    let len = (buf.len() - 1) as i32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());

    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

/// Send a `PasswordMessage` (or SASL response) carrying `payload`.
///
/// # Errors
/// Returns [`PgWireError::Io`] if the write or flush fails.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "password/SASL message length is bounded well below i32::MAX"
)]
pub async fn write_password_message<W: AsyncWrite + Unpin>(
    wr: &mut W,
    payload: &[u8],
) -> Result<()> {
    let mut buf = BytesMut::with_capacity(payload.len() + 16);
    buf.put_u8(b'p');
    buf.put_i32(0);
    buf.extend_from_slice(payload);

    let len = (buf.len() - 1) as i32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());

    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

/// Send a `CopyData` message carrying `payload` (e.g. a standby status update).
///
/// # Errors
/// Returns [`PgWireError::Io`] if the write or flush fails.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "CopyData message length is bounded well below i32::MAX"
)]
pub async fn write_copy_data<W: AsyncWrite + Unpin>(wr: &mut W, payload: &[u8]) -> Result<()> {
    let mut buf = BytesMut::with_capacity(payload.len() + 16);
    buf.put_u8(b'd');
    buf.put_i32(0);
    buf.extend_from_slice(payload);

    let len = (buf.len() - 1) as i32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());

    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

/// Send a `CopyDone` message.
///
/// # Errors
/// Returns [`PgWireError::Io`] if the write or flush fails.
pub async fn write_copy_done<W: AsyncWrite + Unpin>(wr: &mut W) -> Result<()> {
    let mut buf = BytesMut::with_capacity(5);
    buf.put_u8(b'c'); // CopyDone
    buf.put_i32(4); // length includes itself; CopyDone has no payload
    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

#[cfg(test)]
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::unreadable_literal,
    reason = "test wire-frame builders use small, known constants and hex byte vectors"
)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn read_backend_message_parses_valid_message() {
        // Tag 'Z' (ReadyForQuery), length=5 (4 + 1 byte payload), payload='I' (idle)
        let data = [b'Z', 0, 0, 0, 5, b'I'];
        let mut cursor = Cursor::new(&data[..]);

        let msg = read_backend_message(&mut cursor)
            .await
            .expect("should succeed");
        assert_eq!(msg.tag, b'Z');
        assert_eq!(&msg.payload[..], b"I");
        assert!(msg.is_ready_for_query());
    }

    #[tokio::test]
    async fn read_backend_message_handles_empty_payload() {
        // Tag 'N' (NoticeResponse placeholder), length=4 (no payload)
        let data = [b'N', 0, 0, 0, 4];
        let mut cursor = Cursor::new(&data[..]);

        let msg = read_backend_message(&mut cursor)
            .await
            .expect("should succeed");
        assert_eq!(msg.tag, b'N');
        assert!(msg.payload.is_empty());
    }

    #[tokio::test]
    async fn read_backend_message_rejects_invalid_length() {
        // length < 4 is invalid
        let data = [b'Z', 0, 0, 0, 3];
        let mut cursor = Cursor::new(&data[..]);

        let err = read_backend_message(&mut cursor)
            .await
            .expect_err("should error");
        assert!(err.to_string().contains("invalid backend message length"));
    }

    #[tokio::test]
    async fn message_reader_reads_complete_message() {
        // Tag 'Z' (ReadyForQuery), length=5 (4 + 1 byte payload), payload='I'
        let data = [b'Z', 0, 0, 0, 5, b'I'];
        let mut cursor = Cursor::new(&data[..]);

        let mut reader = MessageReader::new();
        let msg = reader.read(&mut cursor).await.expect("should succeed");
        assert_eq!(msg.tag, b'Z');
        assert_eq!(&msg.payload[..], b"I");
    }

    #[tokio::test]
    async fn message_reader_reads_back_to_back_messages() {
        // Two messages on one stream: ReadyForQuery + NoticeResponse w/ empty payload
        let data = [b'Z', 0, 0, 0, 5, b'I', b'N', 0, 0, 0, 4];
        let mut cursor = Cursor::new(&data[..]);

        let mut reader = MessageReader::new();

        let m1 = reader.read(&mut cursor).await.expect("should succeed");
        assert_eq!(m1.tag, b'Z');
        assert_eq!(&m1.payload[..], b"I");

        let m2 = reader.read(&mut cursor).await.expect("should succeed");
        assert_eq!(m2.tag, b'N');
        assert!(m2.payload.is_empty());
    }

    /// Regression test for issue #5: reading a backend message must be
    /// cancellation-safe so that `tokio::select!` / `tokio::time::timeout`
    /// dropping the read future mid-message does not corrupt the stream.
    ///
    /// With the old `read_backend_message_into`, dropping the future after
    /// 3 of 5 header bytes were consumed would lose those 3 bytes and
    /// re-parse the next bytes as a new header, producing a bogus length
    /// and a Protocol error (or worse, a silent desync).
    #[tokio::test]
    async fn message_reader_resumes_after_cancellation_mid_header() {
        let (mut writer, mut rd) = tokio::io::duplex(64);
        let mut reader = MessageReader::new();

        // Tag 'd' (CopyData), length = 8 (4 + 4-byte payload), payload b"abcd"
        let header = [b'd', 0, 0, 0, 8];
        let payload = b"abcd";

        // Deliver only the first 3 header bytes, then cancel.
        writer
            .write_all(&header[..3])
            .await
            .expect("should succeed");

        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(20), reader.read(&mut rd)).await;
        assert!(
            timed_out.is_err(),
            "read must time out while waiting for remaining header bytes"
        );

        // Deliver the remaining bytes. A correct cancel-safe reader resumes
        // and returns the original message intact.
        writer
            .write_all(&header[3..])
            .await
            .expect("should succeed");
        writer.write_all(payload).await.expect("should succeed");

        let msg = reader.read(&mut rd).await.expect("should succeed");
        assert_eq!(msg.tag, b'd');
        assert_eq!(&msg.payload[..], payload);
    }

    /// Ensures partial-payload cancellation also resumes correctly.
    #[tokio::test]
    async fn message_reader_resumes_after_cancellation_mid_payload() {
        let (mut writer, mut rd) = tokio::io::duplex(64);
        let mut reader = MessageReader::new();

        // 16-byte payload to ensure we can split it.
        let payload: [u8; 16] = std::array::from_fn(|i| i as u8);
        let len = (4 + payload.len()) as i32;
        let header = [
            b'd',
            (len >> 24) as u8,
            (len >> 16) as u8,
            (len >> 8) as u8,
            len as u8,
        ];

        // Full header + first 5 bytes of payload, then cancel.
        writer.write_all(&header).await.expect("should succeed");
        writer
            .write_all(&payload[..5])
            .await
            .expect("should succeed");

        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(20), reader.read(&mut rd)).await;
        assert!(
            timed_out.is_err(),
            "read must time out while waiting for remaining payload bytes"
        );

        // Deliver the rest.
        writer
            .write_all(&payload[5..])
            .await
            .expect("should succeed");

        let msg = reader.read(&mut rd).await.expect("should succeed");
        assert_eq!(msg.tag, b'd');
        assert_eq!(&msg.payload[..], &payload[..]);
    }

    #[tokio::test]
    async fn message_reader_rejects_invalid_length() {
        let data = [b'Z', 0, 0, 0, 3];
        let mut cursor = Cursor::new(&data[..]);

        let mut reader = MessageReader::new();
        let err = reader.read(&mut cursor).await.expect_err("should error");
        assert!(err.to_string().contains("invalid backend message length"));
    }

    #[tokio::test]
    async fn read_backend_message_rejects_oversized_message() {
        // length = MAX_MESSAGE_SIZE + 5 (over limit)
        let huge_len = (MAX_MESSAGE_SIZE as i32) + 5;
        let data = [
            b'Z',
            (huge_len >> 24) as u8,
            (huge_len >> 16) as u8,
            (huge_len >> 8) as u8,
            huge_len as u8,
        ];
        let mut cursor = Cursor::new(&data[..]);

        let err = read_backend_message(&mut cursor)
            .await
            .expect_err("should error");
        assert!(err.to_string().contains("too large"));
    }

    #[tokio::test]
    async fn write_ssl_request_produces_valid_bytes() {
        let mut buf = Vec::new();
        write_ssl_request(&mut buf).await.expect("should succeed");

        assert_eq!(buf.len(), 8);
        // length = 8
        assert_eq!(&buf[0..4], &8i32.to_be_bytes());
        // SSL request code = 80877103
        assert_eq!(&buf[4..8], &80877103i32.to_be_bytes());
    }

    #[tokio::test]
    async fn write_startup_message_includes_params() {
        let mut buf = Vec::new();
        let params = [("user", "postgres"), ("database", "test")];
        write_startup_message(&mut buf, 196608, &params)
            .await
            .expect("should succeed");

        // Should contain the parameter strings
        let s = String::from_utf8_lossy(&buf);
        assert!(s.contains("user"));
        assert!(s.contains("postgres"));
        assert!(s.contains("database"));
        assert!(s.contains("test"));

        // Length field should be at start
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(len, buf.len());
    }

    #[tokio::test]
    async fn write_query_produces_valid_message() {
        let mut buf = Vec::new();
        write_query(&mut buf, "SELECT 1")
            .await
            .expect("should succeed");

        // Should start with 'Q'
        assert_eq!(buf[0], b'Q');

        // Length should be correct (excludes tag byte)
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        assert_eq!(len, buf.len() - 1);

        // Should contain the SQL
        assert!(buf[5..].starts_with(b"SELECT 1"));

        // Should be null-terminated
        assert_eq!(buf[buf.len() - 1], 0);
    }

    #[tokio::test]
    async fn write_password_message_produces_valid_message() {
        let mut buf = Vec::new();
        write_password_message(&mut buf, b"secret")
            .await
            .expect("should succeed");

        assert_eq!(buf[0], b'p');
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        assert_eq!(len, buf.len() - 1);
        assert_eq!(&buf[5..], b"secret");
    }

    #[tokio::test]
    async fn write_copy_data_produces_valid_message() {
        let mut buf = Vec::new();
        write_copy_data(&mut buf, b"payload")
            .await
            .expect("should succeed");

        assert_eq!(buf[0], b'd');
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        assert_eq!(len, buf.len() - 1);
        assert_eq!(&buf[5..], b"payload");
    }

    #[tokio::test]
    async fn write_copy_done_produces_valid_message() {
        let mut buf = Vec::new();
        write_copy_done(&mut buf).await.expect("should succeed");

        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], b'c');
        // Length = 4 (just the length field itself, no payload)
        assert_eq!(&buf[1..5], &4i32.to_be_bytes());
    }

    // ==================== FrameReader tests ====================

    const TEST_MAX: usize = 1024 * 1024 * 1024;

    /// Build one wire frame: tag + i32 length (len counts itself + payload) + payload.
    fn wire_frame(tag: u8, payload: &[u8]) -> Vec<u8> {
        let len = (4 + payload.len()) as i32;
        let mut v = Vec::with_capacity(5 + payload.len());
        v.push(tag);
        v.extend_from_slice(&len.to_be_bytes());
        v.extend_from_slice(payload);
        v
    }

    #[tokio::test]
    async fn frame_reader_reads_complete_message() {
        let data = [b'Z', 0, 0, 0, 5, b'I'];
        let mut cursor = Cursor::new(&data[..]);
        let mut reader = FrameReader::new(TEST_MAX);
        let msg = reader.next(&mut cursor).await.expect("should succeed");
        assert_eq!(msg.tag, b'Z');
        assert_eq!(&msg.payload[..], b"I");
    }

    #[tokio::test]
    async fn frame_reader_reads_back_to_back_messages() {
        let mut data = wire_frame(b'Z', b"I");
        data.extend_from_slice(&wire_frame(b'N', b""));
        data.extend_from_slice(&wire_frame(b'd', b"copydata"));
        let mut cursor = Cursor::new(data.as_slice());
        let mut reader = FrameReader::new(TEST_MAX);

        let m1 = reader.next(&mut cursor).await.expect("should succeed");
        assert_eq!(m1.tag, b'Z');
        assert_eq!(&m1.payload[..], b"I");
        // With a single buffered read, the following frames must already be
        // available without another socket read.
        assert!(reader.has_buffered_frame());

        let m2 = reader.next(&mut cursor).await.expect("should succeed");
        assert_eq!(m2.tag, b'N');
        assert!(m2.payload.is_empty());

        let m3 = reader.next(&mut cursor).await.expect("should succeed");
        assert_eq!(m3.tag, b'd');
        assert_eq!(&m3.payload[..], b"copydata");
    }

    /// Cancellation-safety: dropping the read future mid-header must not lose
    /// the bytes already buffered.
    #[tokio::test]
    async fn frame_reader_resumes_after_cancellation_mid_header() {
        let (mut writer, mut rd) = tokio::io::duplex(64);
        let mut reader = FrameReader::new(TEST_MAX);

        let frame = wire_frame(b'd', b"abcd");
        writer.write_all(&frame[..3]).await.expect("should succeed");

        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(20), reader.next(&mut rd)).await;
        assert!(
            timed_out.is_err(),
            "must time out awaiting remaining header"
        );

        writer.write_all(&frame[3..]).await.expect("should succeed");
        let msg = reader.next(&mut rd).await.expect("should succeed");
        assert_eq!(msg.tag, b'd');
        assert_eq!(&msg.payload[..], b"abcd");
    }

    /// Cancellation-safety: dropping the read future mid-payload must resume.
    #[tokio::test]
    async fn frame_reader_resumes_after_cancellation_mid_payload() {
        let (mut writer, mut rd) = tokio::io::duplex(64);
        let mut reader = FrameReader::new(TEST_MAX);

        let payload: [u8; 16] = std::array::from_fn(|i| i as u8);
        let frame = wire_frame(b'd', &payload);
        writer.write_all(&frame[..9]).await.expect("should succeed"); // header + 4 payload bytes

        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(20), reader.next(&mut rd)).await;
        assert!(
            timed_out.is_err(),
            "must time out awaiting remaining payload"
        );

        writer.write_all(&frame[9..]).await.expect("should succeed");
        let msg = reader.next(&mut rd).await.expect("should succeed");
        assert_eq!(msg.tag, b'd');
        assert_eq!(&msg.payload[..], &payload[..]);
    }

    #[tokio::test]
    async fn frame_reader_rejects_invalid_length() {
        let data = [b'Z', 0, 0, 0, 3]; // len < 4
        let mut cursor = Cursor::new(&data[..]);
        let mut reader = FrameReader::new(TEST_MAX);
        let err = reader.next(&mut cursor).await.expect_err("should error");
        assert!(err.to_string().contains("invalid backend message length"));
    }

    #[tokio::test]
    async fn frame_reader_rejects_oversized_message() {
        // Declared payload of 1000 bytes with a 100-byte cap.
        let header = wire_frame(b'd', &vec![0u8; 1000]);
        let mut cursor = Cursor::new(header.as_slice());
        let mut reader = FrameReader::new(100);
        let err = reader.next(&mut cursor).await.expect_err("should error");
        assert!(err.to_string().contains("too large"));
    }

    /// A large-but-valid frame must reassemble byte-for-byte across many reads.
    #[tokio::test]
    async fn frame_reader_assembles_large_message_incrementally() {
        let payload: Vec<u8> = (0..(2 * 1024 * 1024)).map(|i| (i % 251) as u8).collect();
        let frame = wire_frame(b'd', &payload);
        // Cursor hands out at most `spare` bytes per read_buf, so this drives
        // many reads + geometric growth for one message.
        let mut cursor = Cursor::new(frame.as_slice());
        let mut reader = FrameReader::new(TEST_MAX);
        let msg = reader.next(&mut cursor).await.expect("should succeed");
        assert_eq!(msg.tag, b'd');
        assert_eq!(msg.payload.len(), payload.len());
        assert_eq!(&msg.payload[..], &payload[..]);
    }

    /// Anti-amplification: a header claiming a huge (but under-cap) payload,
    /// followed by only a handful of bytes, must NOT pre-allocate the declared
    /// size. Buffer growth is driven by bytes actually received.
    #[tokio::test]
    async fn frame_reader_bogus_length_does_not_preallocate() {
        let (mut writer, mut rd) = tokio::io::duplex(64);
        let mut reader = FrameReader::new(TEST_MAX);

        // Claim ~900 MiB of payload but send only the header + 8 bytes.
        let claimed = 900 * 1024 * 1024i32;
        let len = 4 + claimed;
        let mut header = Vec::new();
        header.push(b'd');
        header.extend_from_slice(&len.to_be_bytes());
        header.extend_from_slice(&[0u8; 8]);
        writer.write_all(&header).await.expect("should succeed");

        // The frame never completes, so `next` stays pending.
        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(50), reader.next(&mut rd)).await;
        assert!(timed_out.is_err(), "incomplete frame must not resolve");

        assert!(
            reader.capacity() < 4 * 1024 * 1024,
            "must not pre-allocate the declared 900 MiB; capacity was {}",
            reader.capacity()
        );
    }

    #[test]
    fn backend_message_helper_methods() {
        let error = BackendMessage {
            tag: b'E',
            payload: Bytes::new(),
        };
        assert!(error.is_error());
        assert!(!error.is_ready_for_query());

        let ready = BackendMessage {
            tag: b'Z',
            payload: Bytes::from_static(b"I"),
        };
        assert!(ready.is_ready_for_query());
        assert!(!ready.is_error());

        let copy_both = BackendMessage {
            tag: b'W',
            payload: Bytes::new(),
        };
        assert!(copy_both.is_copy_both_response());

        let copy_data = BackendMessage {
            tag: b'd',
            payload: Bytes::new(),
        };
        assert!(copy_data.is_copy_data());

        let auth = BackendMessage {
            tag: b'R',
            payload: Bytes::new(),
        };
        assert!(auth.is_auth_request());
    }
}
