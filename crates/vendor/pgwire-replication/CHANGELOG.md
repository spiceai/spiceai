# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [0.3.2] - 2026-05-08

### Fixed
- **Cancellation-safe message reading** in `stream_loop` (issue #5)
  - The wait-phase `tokio::select!` previously raced `stop_rx.changed()` and `tokio::time::timeout` against `read_backend_message_into`, whose internal `read_exact` calls are not cancellation-safe. If the timeout fired while a backend message was mid-flight, partially-read header/payload bytes were dropped along with the cancelled future, leaving the next iteration to mis-parse the wire stream - typically surfacing as a bogus `payload_len` followed by a hang or `Protocol` error
  - New `MessageReader` externalizes partial-read state (header/payload counters, parsed tag/length) and uses one-shot `AsyncReadExt::read` so dropped futures never lose bytes; the next call resumes exactly where the previous one left off
  - `stream_loop` now owns a single `MessageReader` reused across drain and wait phases

### Notes
- `read_backend_message_into` is retained for non-`select!` callers (startup, auth, replication-start) and is now documented as **not cancellation-safe**

---

## [0.3.1] - 2026-03-28

### Improved
- **Drain-phase tight loop** in `stream_loop`: while the BufReader has buffered data, reads up to 256 messages without `select!`/timeout overhead, then falls back to the wait path only when the buffer is empty
  - Eliminates per-message tokio timer registration/deregistration during burst reads
- **Reusable read buffer** via `read_backend_message_into()`: reuses a `BytesMut` across messages instead of allocating a new `Vec` per WAL message
  - Reduces allocator pressure during high-throughput streaming

### Benchmarked
- Postgres CDC backlog drain: **37K -> 48K events/s (+32%)** in DeltaForge on Docker (direct connection, batch=4000, kafka linger.ms=0)

---

## [0.3.0] - 2026-03-27

### Changed
- **Breaking:** `PgWireError::Io` now wraps `Arc<std::io::Error>` instead of `String`
  - Consumers can match on `.kind()` (e.g. `ErrorKind::UnexpectedEof`, `ConnectionReset`) instead of brittle substring matching on error messages
  - `PgWireError` remains `Clone` via the `Arc` wrapper

### Improved
- Wrapped the replication stream in a 128KB `BufReader`, batching WAL messages into fewer `recv()` syscalls
  - Reduces syscall overhead significantly during backlog drain scenarios

### Removed
- Replaced `rustls-pemfile` dependency with `rustls-pki-types` PEM parsing (already in the dependency tree via `rustls`)
  - Resolves RUSTSEC-2025-0134 (unmaintained crate)

---

## [0.2.0] - 2026-02-08

### Added
- Support for `pg_logical_emit_message()` via new `ReplicationEvent::Message` variant
  - Messages are always enabled (`messages 'true'` in pgoutput protocol options); zero overhead when unused
  - Both transactional and non-transactional messages are supported
  - Events include `prefix` (application-defined string), `content` (raw bytes), `lsn`, and `transactional` flag
- Unix domain socket connections: set `host` to a socket directory path (e.g. `/var/run/postgresql`)
  - Follows libpq convention: any `host` starting with `/` is treated as a socket directory
  - Socket path is `{host}/.s.PGSQL.{port}`
  - `ReplicationConfig::unix()` convenience constructor
  - `ReplicationConfig::is_unix_socket()` and `ReplicationConfig::unix_socket_path()` helpers

### Changed
- `START_REPLICATION` now includes `messages 'true'` in pgoutput options (previously only `proto_version` and `publication_names`)

---

## [0.1.2] - 2025-01-17
### Fixed
- Corrected documentation on LSN monotonicity semantics (event LSNs are not monotonic across transactions; `(commit_lsn, event_lsn)` tuple provides total ordering)

---

## [0.1.1] - 2025-01-09

### Fixed
- `stop()` is now immediately responsive regardless of `idle_wakeup_interval`
  - Previously, the worker only checked the stop signal at the top of the stream loop, meaning `stop()` could take up to `idle_wakeup_interval` to take effect if blocked waiting for Postgres messages
  - Now uses `tokio::select!` to concurrently watch for stop signals while waiting, ensuring prompt shutdown

---

## [0.1.0] - 2025-12-31

### Added
- Initial release of `pgwire-replication`
- Tokio-based async PostgreSQL logical replication client
- PostgreSQL wire protocol (pgwire) implementation
- SCRAM-SHA-256 authentication (default)
- MD5 authentication (optional, feature-gated)
- TLS support via rustls with modes: disable, require, verify-ca, verify-full
- Mutual TLS (mTLS) client certificate support
- Explicit replication start via `start_lsn`
- Bounded replay via `stop_at_lsn`
- Periodic standby status updates (feedback)
- Keepalive handling with automatic replies
- Clean shutdown using `CopyDone`
- Integration tests using Docker (`testcontainers`)

### Notes
- pgoutput payloads are returned as opaque bytes (no parsing yet)
- Replay bounds use WAL positions observed during streaming

---

### Planned
- Commit-boundary LSN extraction from pgoutput
- Stronger replay guarantees based on commit end LSN
- Additional pgoutput message parsing helpers
- Fuzz testing for pgwire framing


[Unreleased]: https://github.com/vnvo/pgwire-replication/compare/v0.3.2...HEAD
[0.3.2]: https://github.com/vnvo/pgwire-replication/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/vnvo/pgwire-replication/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/vnvo/pgwire-replication/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/vnvo/pgwire-replication/releases/tag/v0.2.0
[0.1.2]: https://github.com/vnvo/pgwire-replication/releases/tag/v0.1.2
[0.1.1]: https://github.com/vnvo/pgwire-replication/releases/tag/v0.1.1
[0.1.0]: https://github.com/vnvo/pgwire-replication/releases/tag/v0.1.0
