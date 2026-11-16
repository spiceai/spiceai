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

//! The kernel key quota that bounds how many containers can run at once
//! under a rootless Docker daemon.
//!
//! runc gives every container a new session keyring, and the kernel charges
//! each key to the uid that creates it. A rootful daemon creates them as
//! root, whose quota (`kernel.keys.root_maxkeys`) is effectively unlimited.
//! A rootless daemon creates them as its own uid, which is the uid this
//! runner runs as, and that uid's quota (`kernel.keys.maxkeys`, 200 keys and
//! 20000 bytes by default) is shared with every other process of that uid on
//! the host, including other jobs' containers. Once it is full, runc refuses
//! to start a container with "unable to join session keyring: unable to
//! create session key: disk quota exceeded".
//!
//! So before starting containers the runner waits until the quota has room
//! for them, and retries a start that failed on the quota. Where the quota
//! cannot be read (not Linux), there is nothing to wait for.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

/// Bytes one container's session keyring is charged: measured with a
/// rootless daemon, where each running container added one key and 70 bytes
/// to the daemon uid's line of `/proc/key-users`.
const BYTES_PER_CONTAINER: u64 = 70;

/// Keys left free for whatever else this uid runs (a login session, other
/// tools), so a start is not attempted with the quota on its last key.
const RESERVE: u64 = 4;

/// How long to wait for room before giving up: long enough for other jobs on
/// the host to finish a script and release their containers' keys.
const WAIT_LIMIT: Duration = Duration::from_mins(15);

const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// One uid's use of its kernel key quota, from `/proc/key-users`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyQuota {
    pub uid: u32,
    pub keys: u64,
    pub max_keys: u64,
    pub bytes: u64,
    pub max_bytes: u64,
}

impl KeyQuota {
    /// This process's uid's quota, or `None` where it cannot be read.
    pub fn read() -> Option<Self> {
        use std::os::unix::fs::MetadataExt;
        let uid = std::fs::metadata("/proc/self").ok()?.uid();
        let key_users = std::fs::read_to_string("/proc/key-users").ok()?;
        if let Some(quota) = parse_key_users(&key_users, uid) {
            return Some(quota);
        }
        // A uid that holds no keys has no line; its limits are the sysctls.
        let (keys_file, bytes_file) = if uid == 0 {
            ("root_maxkeys", "root_maxbytes")
        } else {
            ("maxkeys", "maxbytes")
        };
        let sysctl = |name: &str| -> Option<u64> {
            std::fs::read_to_string(format!("/proc/sys/kernel/keys/{name}"))
                .ok()?
                .trim()
                .parse()
                .ok()
        };
        Some(Self {
            uid,
            keys: 0,
            max_keys: sysctl(keys_file)?,
            bytes: 0,
            max_bytes: sysctl(bytes_file)?,
        })
    }

    /// How many more containers the quota can start, keeping [`RESERVE`]
    /// keys free.
    pub fn room(&self) -> u64 {
        let by_keys = self.max_keys.saturating_sub(self.keys);
        let by_bytes = self.max_bytes.saturating_sub(self.bytes) / BYTES_PER_CONTAINER;
        by_keys.min(by_bytes).saturating_sub(RESERVE)
    }

    pub fn describe(&self) -> String {
        format!(
            "uid {} holds {} of {} kernel keys ({} of {} bytes)",
            self.uid, self.keys, self.max_keys, self.bytes, self.max_bytes
        )
    }
}

/// The line of `/proc/key-users` for `uid`:
/// `<uid>: <usage> <nkeys>/<nikeys> <qnkeys>/<maxkeys> <qnbytes>/<maxbytes>`.
fn parse_key_users(text: &str, uid: u32) -> Option<KeyQuota> {
    text.lines().find_map(|line| {
        let (owner, rest) = line.split_once(':')?;
        if owner.trim().parse::<u32>().ok()? != uid {
            return None;
        }
        let fields: Vec<&str> = rest.split_whitespace().collect();
        let pair = |field: &str| -> Option<(u64, u64)> {
            let (used, max) = field.split_once('/')?;
            Some((used.parse().ok()?, max.parse().ok()?))
        };
        let (keys, max_keys) = pair(fields.get(2)?)?;
        let (bytes, max_bytes) = pair(fields.get(3)?)?;
        Some(KeyQuota {
            uid,
            keys,
            max_keys,
            bytes,
            max_bytes,
        })
    })
}

/// Whether a container start failed because the key quota was full.
pub fn is_quota_error(message: &str) -> bool {
    message.contains("unable to join session keyring")
}

/// Containers that combinations of this run have been admitted for but not
/// yet started. Admission counts them against the room, so two combinations
/// admitted together cannot each start part of their containers and then wait
/// on the rest of each other's room.
pub struct Reservations(parking_lot::Mutex<u64>);

impl Reservations {
    pub const fn new() -> Self {
        Self(parking_lot::const_mutex(0))
    }
}

static RESERVATIONS: Reservations = Reservations::new();

/// Room held for a combination's containers. Each container gives back its
/// share with [`Reservation::release`] once it has started and holds a key of
/// its own; whatever is left goes back when the reservation is dropped.
pub struct Reservation<'a> {
    pool: &'a Reservations,
    /// Containers still to start. Changed only under `pool`'s lock.
    remaining: AtomicU64,
}

impl Reservation<'_> {
    /// Gives back room for `containers` that have started (or will not), at
    /// most what is still held.
    pub fn release(&self, containers: u64) {
        let mut reserved = self.pool.0.lock();
        let remaining = self.remaining.load(Ordering::Relaxed);
        let released = containers.min(remaining);
        self.remaining
            .store(remaining - released, Ordering::Relaxed);
        *reserved = reserved.saturating_sub(released);
    }

    /// Containers still to start.
    #[cfg(test)]
    pub fn remaining(&self) -> u64 {
        self.remaining.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
impl<'a> Reservation<'a> {
    /// A reservation of `containers` in `pool`, without reading a quota.
    pub fn held_in(pool: &'a Reservations, containers: u64) -> Self {
        *pool.0.lock() += containers;
        Self {
            pool,
            remaining: AtomicU64::new(containers),
        }
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.release(u64::MAX);
    }
}

/// Whether a wait may start its containers, and if not, what it waits on.
enum Admission {
    Admitted,
    /// Room the quota has is reserved by other combinations of this run, which
    /// start their containers as they get `--jobs` slots: progress, not a full
    /// quota, so it does not count toward [`WAIT_LIMIT`].
    WaitOnThisRun,
    /// The quota itself has no room.
    WaitOnQuota,
}

/// Waits until the quota has room for `containers` more containers, saying
/// so once in the script's output if it has to wait. Returns without waiting
/// where the quota cannot be read, and fails after [`WAIT_LIMIT`].
pub async fn wait_for_room(containers: u64, what: &str) -> anyhow::Result<()> {
    wait_until(
        containers,
        what,
        KeyQuota::read,
        WAIT_LIMIT,
        POLL_INTERVAL,
        |quota| {
            if quota.room() >= containers {
                Admission::Admitted
            } else {
                Admission::WaitOnQuota
            }
        },
    )
    .await
}

/// Waits until the quota has room for `containers` beyond what other
/// combinations have reserved, and reserves it until it is released or the
/// returned [`Reservation`] is dropped.
///
/// Take it before a `--jobs` slot, never while holding one: a combination
/// that waits for room while holding a slot can wait on a combination that
/// holds the room but needs the slot.
pub async fn reserve_room(containers: u64, what: &str) -> anyhow::Result<Reservation<'static>> {
    reserve_room_in(
        &RESERVATIONS,
        containers,
        what,
        KeyQuota::read,
        WAIT_LIMIT,
        POLL_INTERVAL,
    )
    .await
}

async fn reserve_room_in<'a>(
    pool: &'a Reservations,
    containers: u64,
    what: &str,
    read: impl FnMut() -> Option<KeyQuota>,
    limit: Duration,
    interval: Duration,
) -> anyhow::Result<Reservation<'a>> {
    let mut reserved_now = 0;
    wait_until(containers, what, read, limit, interval, |quota| {
        let mut reserved = pool.0.lock();
        if quota.room().saturating_sub(*reserved) >= containers {
            *reserved += containers;
            reserved_now = containers;
            Admission::Admitted
        } else if quota.room() >= containers {
            Admission::WaitOnThisRun
        } else {
            Admission::WaitOnQuota
        }
    })
    .await?;
    Ok(Reservation {
        pool,
        remaining: AtomicU64::new(reserved_now),
    })
}

/// Polls `read` until `admit` admits the quota (or it cannot be read). Fails
/// once the quota has had no room for `limit`; time spent waiting on this
/// run's own reservations restarts that clock.
async fn wait_until(
    containers: u64,
    what: &str,
    mut read: impl FnMut() -> Option<KeyQuota>,
    limit: Duration,
    interval: Duration,
    mut admit: impl FnMut(&KeyQuota) -> Admission,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let mut full_since = started;
    // What the output last said this waits on, to say it again only when
    // that changes.
    let mut said: Option<&str> = None;
    loop {
        let Some(quota) = read() else {
            return Ok(());
        };
        if containers == 0 {
            return Ok(());
        }
        let waiting_on = match admit(&quota) {
            Admission::Admitted => {
                if said.is_some() {
                    crate::runner::emit(&format!(
                        "   kernel key quota has room again after {:.0?}; starting {what}",
                        started.elapsed()
                    ));
                }
                return Ok(());
            }
            Admission::WaitOnThisRun => {
                full_since = Instant::now();
                "room in the kernel key quota, which other runs of this spicescript have reserved for their containers,"
            }
            Admission::WaitOnQuota => "room in the kernel key quota",
        };
        anyhow::ensure!(
            full_since.elapsed() < limit,
            "waited {limit:?} for room in the kernel key quota to start {what} ({containers} container(s)), but {}. \
             Under a rootless Docker daemon each running container holds one key of that quota; \
             raise `kernel.keys.maxkeys` and `kernel.keys.maxbytes` on this host, or run fewer scripts at once (`--jobs`).",
            quota.describe()
        );
        if said != Some(waiting_on) {
            crate::runner::emit(&format!(
                "   waiting for {waiting_on} to start {what} ({containers} container(s)): {}",
                quota.describe()
            ));
            said = Some(waiting_on);
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admit_if(room: bool) -> Admission {
        if room {
            Admission::Admitted
        } else {
            Admission::WaitOnQuota
        }
    }

    // Lines read from a rootless Docker daemon's host: root's, and the
    // daemon uid's with 20 containers running.
    const KEY_USERS: &str =
        "    0:    40 39/39 35/1000000 1912/25000000\n 1000:    20 20/20 20/2000 1400/2000000\n";

    #[test]
    fn parses_the_line_of_the_uid_asked_for() {
        assert_eq!(
            parse_key_users(KEY_USERS, 1000),
            Some(KeyQuota {
                uid: 1000,
                keys: 20,
                max_keys: 2000,
                bytes: 1400,
                max_bytes: 2_000_000,
            })
        );
        assert_eq!(parse_key_users(KEY_USERS, 1001), None);
    }

    #[test]
    fn room_is_the_tighter_of_keys_and_bytes_less_the_reserve() {
        let quota = |keys, max_keys, bytes, max_bytes| KeyQuota {
            uid: 1000,
            keys,
            max_keys,
            bytes,
            max_bytes,
        };
        // Ubuntu's defaults: 200 keys, 20000 bytes; bytes bind first (285).
        assert_eq!(quota(0, 200, 0, 20_000).room(), 196);
        assert_eq!(quota(0, 1000, 0, 20_000).room(), 285 - RESERVE);
        // Full, and over full (a quota lowered under its use).
        assert_eq!(quota(200, 200, 14_000, 20_000).room(), 0);
        assert_eq!(quota(250, 200, 0, 20_000).room(), 0);
    }

    #[test]
    fn quota_errors_are_recognized_by_runc_message() {
        assert!(is_quota_error(
            "Docker responded with status code 500: failed to create task for container: failed to create shim task: OCI runtime create failed: runc create failed: unable to start container process: error during container init: unable to join session keyring: unable to create session key: disk quota exceeded"
        ));
        assert!(!is_quota_error(
            "Docker responded with status code 500: no space left on device"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn waits_until_room_appears_then_starts() {
        let mut samples = [195, 195, 190, 150].into_iter();
        let mut reads = 0;
        wait_until(
            3,
            "test",
            || {
                reads += 1;
                let keys = samples.next().unwrap_or(150);
                Some(KeyQuota {
                    uid: 1000,
                    keys,
                    max_keys: 200,
                    bytes: 0,
                    max_bytes: 20_000,
                })
            },
            Duration::from_secs(60),
            Duration::from_secs(1),
            |quota| admit_if(quota.room() >= 3),
        )
        .await
        .expect("room appears");
        // 195 and 195 leave 1 key of room, 190 leaves 6: the third read starts.
        assert_eq!(reads, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn fails_with_the_quota_and_the_fix_when_room_never_appears() {
        let full = KeyQuota {
            uid: 1000,
            keys: 200,
            max_keys: 200,
            bytes: 14_000,
            max_bytes: 20_000,
        };
        let error = wait_until(
            1,
            "source 'src'",
            || Some(full),
            Duration::from_secs(10),
            Duration::from_secs(1),
            |quota| admit_if(quota.room() >= 1),
        )
        .await
        .expect_err("never has room");
        let message = format!("{error:#}");
        assert!(
            message.contains("uid 1000 holds 200 of 200 kernel keys"),
            "{message}"
        );
        assert!(message.contains("kernel.keys.maxkeys"), "{message}");
    }

    #[tokio::test]
    async fn does_not_wait_where_the_quota_cannot_be_read() {
        wait_until(
            1_000_000,
            "test",
            || None,
            Duration::ZERO,
            Duration::from_secs(1),
            |_| Admission::WaitOnQuota,
        )
        .await
        .expect("nothing to wait for");
    }

    /// 200 max keys with `keys` used: room = 200 - keys - RESERVE(4).
    fn quota_with(keys: u64) -> impl FnMut() -> Option<KeyQuota> {
        move || {
            Some(KeyQuota {
                uid: 1000,
                keys,
                max_keys: 200,
                bytes: 0,
                max_bytes: 20_000,
            })
        }
    }

    /// Two combinations that each fit the room alone but not together: the
    /// second waits for the first's containers to start, instead of both
    /// starting part of theirs and waiting on each other. Waiting on this
    /// run's own reservation is not a full quota, so it never times out.
    #[tokio::test(start_paused = true)]
    async fn a_second_set_waits_for_the_first_to_start_its_containers() {
        let pool = Reservations::new();
        // Room for 2.
        let first = reserve_room_in(
            &pool,
            2,
            "A",
            quota_with(194),
            Duration::from_secs(60),
            Duration::from_secs(1),
        )
        .await
        .expect("A fits");
        let second = reserve_room_in(
            &pool,
            2,
            "B",
            quota_with(194),
            Duration::from_secs(3),
            Duration::from_secs(1),
        );
        tokio::pin!(second);
        // Far past B's 3s limit, B still waits rather than failing.
        tokio::select! {
            result = &mut second => panic!("B must wait while A holds the room: {:?}", result.err()),
            () = tokio::time::sleep(Duration::from_mins(30)) => {}
        }
        // A starts one of its containers and gives back its share; it still
        // holds room for the other, so B still waits.
        first.release(1);
        assert_eq!(first.remaining(), 1);
        assert_eq!(*pool.0.lock(), 1);
        drop(first);
        assert_eq!(*pool.0.lock(), 0);
        let second = second.await.expect("B fits once A gives back its room");
        assert_eq!(second.remaining(), 2);
        assert_eq!(*pool.0.lock(), 2);
        // Releasing more than is held gives back only what is held.
        second.release(5);
        assert_eq!(second.remaining(), 0);
        drop(second);
        assert_eq!(*pool.0.lock(), 0);
    }

    /// A set that does not fit the quota even with nothing reserved waits on
    /// the quota itself, and that wait is bounded.
    #[tokio::test(start_paused = true)]
    async fn a_set_the_quota_cannot_hold_fails_after_the_limit() {
        let pool = Reservations::new();
        let error = reserve_room_in(
            &pool,
            3,
            "C",
            // Room for 2.
            quota_with(194),
            Duration::from_secs(3),
            Duration::from_secs(1),
        )
        .await
        .err()
        .expect("C never fits");
        assert!(format!("{error:#}").contains("waited 3s"), "{error:#}");
        assert_eq!(*pool.0.lock(), 0);
    }
}
