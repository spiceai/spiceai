use bytes::{Buf, Bytes};

use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;

/// Replication protocol CopyData message types.
///
/// During logical replication streaming, PostgreSQL sends data wrapped in CopyData
/// messages. This enum represents the two primary message types:
/// - `XLogData`: Contains actual WAL data (transaction changes)
/// - `KeepAlive`: Server heartbeat, optionally requesting client response
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationCopyData {
    /// WAL data message containing transaction changes.
    XLogData {
        /// WAL position where this data starts
        wal_start: Lsn,
        /// Current WAL end position on server (may be 0 for in-transaction messages)
        wal_end: Lsn,
        /// Server timestamp in microseconds since 2000-01-01
        server_time_micros: i64,
        /// The actual pgoutput/wal2json/etc. payload
        data: Bytes,
    },
    /// Server heartbeat message.
    KeepAlive {
        /// Current WAL end position on server
        wal_end: Lsn,
        /// Server timestamp in microseconds since 2000-01-01
        server_time_micros: i64,
        /// If true, server expects StandbyStatusUpdate reply
        reply_requested: bool,
    },
}

impl ReplicationCopyData {
    /// Returns true if this is an XLogData message
    #[inline]
    pub fn is_xlog_data(&self) -> bool {
        matches!(self, ReplicationCopyData::XLogData { .. })
    }

    /// Returns true if this is a KeepAlive message
    #[inline]
    pub fn is_keepalive(&self) -> bool {
        matches!(self, ReplicationCopyData::KeepAlive { .. })
    }

    /// Returns true if this is a KeepAlive that requests a reply
    #[inline]
    pub fn requires_reply(&self) -> bool {
        matches!(
            self,
            ReplicationCopyData::KeepAlive {
                reply_requested: true,
                ..
            }
        )
    }
}

/// Parse a CopyData payload into a replication message.
///
/// The payload should be the CopyData content (after stripping the 'd' tag and length).
/// Returns either `XLogData` or `KeepAlive` depending on the first byte.
pub fn parse_copy_data(payload: Bytes) -> Result<ReplicationCopyData> {
    if payload.is_empty() {
        return Err(PgWireError::Protocol("empty CopyData payload".into()));
    }

    let mut b = payload;
    let kind = b.get_u8();

    match kind {
        b'w' => {
            // XLogData: wal_start(8) + wal_end(8) + server_time(8) + data(variable)
            if b.remaining() < 24 {
                return Err(PgWireError::Protocol(format!(
                    "XLogData payload too short: {} bytes (need at least 24)",
                    b.remaining()
                )));
            }
            let wal_start = Lsn(b.get_i64() as u64);
            let wal_end = Lsn(b.get_i64() as u64);
            let server_time_micros = b.get_i64();
            let data = b.copy_to_bytes(b.remaining());

            Ok(ReplicationCopyData::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            })
        }
        b'k' => {
            // KeepAlive: wal_end(8) + server_time(8) + reply_requested(1)
            if b.remaining() < 17 {
                return Err(PgWireError::Protocol(format!(
                    "KeepAlive payload too short: {} bytes (need 17)",
                    b.remaining()
                )));
            }
            let wal_end = Lsn(b.get_i64() as u64);
            let server_time_micros = b.get_i64();
            let reply_requested = b.get_u8() != 0;

            Ok(ReplicationCopyData::KeepAlive {
                wal_end,
                server_time_micros,
                reply_requested,
            })
        }
        _ => Err(PgWireError::Protocol(format!(
            "unknown CopyData kind: 0x{kind:02x} ('{}')",
            kind as char
        ))),
    }
}

/// Encode a StandbyStatusUpdate message.
///
/// This message reports the client's replay position to the server.
/// All three LSN fields (write, flush, apply) are set to the same value.
///
/// # Arguments
/// * `applied` - The LSN up to which the client has processed data
/// * `client_time_micros` - Client timestamp in microseconds since 2000-01-01
/// * `reply_requested` - If true, server should send a reply (usually false)
///
/// # Returns
/// Raw bytes suitable for sending via CopyData
pub fn encode_standby_status_update(
    applied: Lsn,
    client_time_micros: i64,
    reply_requested: bool,
) -> Vec<u8> {
    // Format: 'r' + write(8) + flush(8) + apply(8) + client_time(8) + reply(1) = 34 bytes
    let mut out = Vec::with_capacity(34);
    out.push(b'r');

    // Write position - last WAL position written to disk
    out.extend_from_slice(&(applied.0 as i64).to_be_bytes());
    // Flush position - last WAL position flushed to disk
    out.extend_from_slice(&(applied.0 as i64).to_be_bytes());
    // Apply position - last WAL position applied to standby
    out.extend_from_slice(&(applied.0 as i64).to_be_bytes());
    // Client system clock
    out.extend_from_slice(&client_time_micros.to_be_bytes());
    // Reply requested
    out.push(if reply_requested { 1 } else { 0 });

    out
}

/// PostgreSQL epoch (2000-01-01) in microseconds since Unix epoch.
pub const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// Convert Unix timestamp (micros) to PostgreSQL timestamp (micros since 2000-01-01).
#[inline]
pub fn unix_to_pg_timestamp(unix_micros: i64) -> i64 {
    unix_micros - PG_EPOCH_MICROS
}

/// Convert PostgreSQL timestamp to Unix timestamp (micros).
#[inline]
pub fn pg_to_unix_timestamp(pg_micros: i64) -> i64 {
    pg_micros + PG_EPOCH_MICROS
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== XLogData tests ====================

    #[test]
    fn parse_xlogdata_minimal() {
        let mut v = Vec::new();
        v.push(b'w');
        v.extend_from_slice(&1i64.to_be_bytes()); // wal_start
        v.extend_from_slice(&2i64.to_be_bytes()); // wal_end
        v.extend_from_slice(&3i64.to_be_bytes()); // server_time
                                                  // no data payload

        let msg = parse_copy_data(Bytes::from(v)).unwrap();
        match msg {
            ReplicationCopyData::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            } => {
                assert_eq!(wal_start.0, 1);
                assert_eq!(wal_end.0, 2);
                assert_eq!(server_time_micros, 3);
                assert!(data.is_empty());
            }
            _ => panic!("expected XLogData"),
        }
    }

    #[test]
    fn parse_xlogdata_with_payload() {
        let mut v = Vec::new();
        v.push(b'w');
        v.extend_from_slice(&0x0123456789ABCDEFu64.to_be_bytes());
        v.extend_from_slice(&0xFEDCBA9876543210u64.to_be_bytes());
        v.extend_from_slice(&(-12345i64).to_be_bytes());
        v.extend_from_slice(b"hello world pgoutput data");

        let msg = parse_copy_data(Bytes::from(v)).unwrap();
        match msg {
            ReplicationCopyData::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            } => {
                assert_eq!(wal_start.0, 0x0123456789ABCDEF);
                assert_eq!(wal_end.0, 0xFEDCBA9876543210);
                assert_eq!(server_time_micros, -12345);
                assert_eq!(&data[..], b"hello world pgoutput data");
            }
            _ => panic!("expected XLogData"),
        }
    }

    #[test]
    fn parse_xlogdata_too_short() {
        let mut v = Vec::new();
        v.push(b'w');
        v.extend_from_slice(&[0u8; 23]); // only 23 bytes, need 24

        let err = parse_copy_data(Bytes::from(v)).unwrap_err();
        assert!(err.to_string().contains("XLogData"));
        assert!(err.to_string().contains("too short"));
    }

    // ==================== KeepAlive tests ====================

    #[test]
    fn parse_keepalive_reply_requested() {
        let mut v = Vec::new();
        v.push(b'k');
        v.extend_from_slice(&100i64.to_be_bytes()); // wal_end
        v.extend_from_slice(&200i64.to_be_bytes()); // server_time
        v.push(1); // reply_requested = true

        let msg = parse_copy_data(Bytes::from(v)).unwrap();
        match msg {
            ReplicationCopyData::KeepAlive {
                wal_end,
                server_time_micros,
                reply_requested,
            } => {
                assert_eq!(wal_end.0, 100);
                assert_eq!(server_time_micros, 200);
                assert!(reply_requested);
            }
            _ => panic!("expected KeepAlive"),
        }
    }

    #[test]
    fn parse_keepalive_no_reply() {
        let mut v = Vec::new();
        v.push(b'k');
        v.extend_from_slice(&999i64.to_be_bytes());
        v.extend_from_slice(&888i64.to_be_bytes());
        v.push(0); // reply_requested = false

        let msg = parse_copy_data(Bytes::from(v)).unwrap();
        match msg {
            ReplicationCopyData::KeepAlive {
                reply_requested, ..
            } => {
                assert!(!reply_requested);
            }
            _ => panic!("expected KeepAlive"),
        }
    }

    #[test]
    fn parse_keepalive_nonzero_reply_byte_is_true() {
        // Any non-zero byte should be treated as true
        let mut v = Vec::new();
        v.push(b'k');
        v.extend_from_slice(&0i64.to_be_bytes());
        v.extend_from_slice(&0i64.to_be_bytes());
        v.push(42); // non-zero = true

        let msg = parse_copy_data(Bytes::from(v)).unwrap();
        assert!(matches!(
            msg,
            ReplicationCopyData::KeepAlive {
                reply_requested: true,
                ..
            }
        ));
    }

    #[test]
    fn parse_keepalive_too_short() {
        let mut v = Vec::new();
        v.push(b'k');
        v.extend_from_slice(&[0u8; 16]); // only 16 bytes, need 17

        let err = parse_copy_data(Bytes::from(v)).unwrap_err();
        assert!(err.to_string().contains("KeepAlive"));
        assert!(err.to_string().contains("too short"));
    }

    // ==================== Error cases ====================

    #[test]
    fn parse_empty_payload() {
        let err = parse_copy_data(Bytes::new()).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn parse_unknown_kind() {
        let v = vec![b'X', 0, 0, 0]; // unknown kind 'X'
        let err = parse_copy_data(Bytes::from(v)).unwrap_err();
        assert!(err.to_string().contains("unknown CopyData kind"));
        assert!(err.to_string().contains("0x58")); // 'X' in hex
    }

    // ==================== Helper method tests ====================

    #[test]
    fn xlogdata_helper_methods() {
        let msg = ReplicationCopyData::XLogData {
            wal_start: Lsn(0),
            wal_end: Lsn(0),
            server_time_micros: 0,
            data: Bytes::new(),
        };
        assert!(msg.is_xlog_data());
        assert!(!msg.is_keepalive());
        assert!(!msg.requires_reply());
    }

    #[test]
    fn keepalive_helper_methods() {
        let msg_reply = ReplicationCopyData::KeepAlive {
            wal_end: Lsn(0),
            server_time_micros: 0,
            reply_requested: true,
        };
        assert!(!msg_reply.is_xlog_data());
        assert!(msg_reply.is_keepalive());
        assert!(msg_reply.requires_reply());

        let msg_no_reply = ReplicationCopyData::KeepAlive {
            wal_end: Lsn(0),
            server_time_micros: 0,
            reply_requested: false,
        };
        assert!(msg_no_reply.is_keepalive());
        assert!(!msg_no_reply.requires_reply());
    }

    // ==================== StandbyStatusUpdate tests ====================

    #[test]
    fn encode_status_update_structure() {
        let p = encode_standby_status_update(Lsn(0x123456789ABCDEF0), 987654321, false);

        assert_eq!(p.len(), 34); // 1 + 8*4 + 1
        assert_eq!(p[0], b'r');

        // All three LSN fields should be the same
        let lsn_bytes = &0x123456789ABCDEF0u64.to_be_bytes();
        assert_eq!(&p[1..9], lsn_bytes); // write
        assert_eq!(&p[9..17], lsn_bytes); // flush
        assert_eq!(&p[17..25], lsn_bytes); // apply

        // Client time
        assert_eq!(&p[25..33], &987654321i64.to_be_bytes());

        // Reply requested = false
        assert_eq!(p[33], 0);
    }

    #[test]
    fn encode_status_update_reply_requested() {
        let p = encode_standby_status_update(Lsn(42), 0, true);
        assert_eq!(p[33], 1);
    }

    #[test]
    fn encode_status_update_zero_lsn() {
        let p = encode_standby_status_update(Lsn(0), 0, false);
        assert_eq!(&p[1..9], &[0u8; 8]);
        assert_eq!(&p[9..17], &[0u8; 8]);
        assert_eq!(&p[17..25], &[0u8; 8]);
    }

    // ==================== Timestamp conversion tests ====================

    #[test]
    fn timestamp_conversion_roundtrip() {
        let unix_micros = 1_704_067_200_000_000_i64; // 2024-01-01 00:00:00 UTC

        let pg_time = unix_to_pg_timestamp(unix_micros);
        let back = pg_to_unix_timestamp(pg_time);

        assert_eq!(back, unix_micros);
    }

    #[test]
    fn pg_epoch_is_correct() {
        // 2000-01-01 00:00:00 UTC in Unix microseconds
        // Days from 1970-01-01 to 2000-01-01 = 10957 days
        let expected = 10957i64 * 24 * 60 * 60 * 1_000_000;
        assert_eq!(PG_EPOCH_MICROS, expected);
    }

    #[test]
    fn unix_to_pg_at_epoch() {
        // At PG epoch, pg timestamp should be 0
        assert_eq!(unix_to_pg_timestamp(PG_EPOCH_MICROS), 0);
    }

    #[test]
    fn pg_to_unix_at_zero() {
        // PG time 0 = Unix PG_EPOCH_MICROS
        assert_eq!(pg_to_unix_timestamp(0), PG_EPOCH_MICROS);
    }
}
