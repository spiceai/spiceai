This directory contains vendored dependencies and optionally their dependency chains - for example, vendoring unmaintained dependencies which no longer receive updates.

## kanal

`kanal/` is kanal 0.1.1 (https://github.com/fereidani/kanal, release commit
`a43ca69`) plus one fix to `src/future.rs`: the async send/receive futures now
re-register a changed waker under the channel's internal lock. Unpatched 0.1.1
leaves a stale waker registered (the send-side re-registration is commented
out entirely; the receive-side update runs outside the lock), so a future
re-polled with a new waker identity — e.g. inside `buffered()` /
`buffer_unordered()` — can miss its wake and park forever. Vortex's file
writer drives segment/column data through `kanal::bounded_async(1)` channels,
where this parked Cayenne cold-tier promotions mid-write while they held the
table write lock. Wired in via `[patch.crates-io]` in the workspace
`Cargo.toml`; it is workspace-`exclude`d so workspace lints do not apply to
the vendored code. Drop the patch and this copy when a fixed upstream release
(> 0.1.1) ships.
