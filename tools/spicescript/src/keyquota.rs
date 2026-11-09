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

/// Containers a source set has been admitted for but not yet finished
/// starting. Admission counts them against the room, so two sets admitted
/// together cannot each start part of their containers and then wait on the
/// rest of each other's room.
pub struct Reservations(parking_lot::Mutex<u64>);

impl Reservations {
    pub const fn new() -> Self {
        Self(parking_lot::const_mutex(0))
    }
}

static RESERVATIONS: Reservations = Reservations::new();

/// Room held for a source set until it is dropped, once the set has started
/// (its containers then hold their own keys) or failed.
pub struct Reservation<'a> {
    pool: &'a Reservations,
    containers: u64,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        let mut reserved = self.pool.0.lock();
        *reserved = reserved.saturating_sub(self.containers);
    }
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
        |quota| quota.room() >= containers,
    )
    .await
}

/// Waits until the quota has room for a whole set of `containers` beyond what
/// other sets have reserved, and reserves it until the returned
/// [`Reservation`] is dropped.
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
            true
        } else {
            false
        }
    })
    .await?;
    Ok(Reservation {
        pool,
        containers: reserved_now,
    })
}

/// Polls `read` until `admit` accepts the quota (or it cannot be read).
async fn wait_until(
    containers: u64,
    what: &str,
    mut read: impl FnMut() -> Option<KeyQuota>,
    limit: Duration,
    interval: Duration,
    mut admit: impl FnMut(&KeyQuota) -> bool,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let mut said = false;
    loop {
        let Some(quota) = read() else {
            return Ok(());
        };
        if containers == 0 || admit(&quota) {
            if said {
                crate::runner::emit(&format!(
                    "   kernel key quota has room again after {:.0?}; starting {what}",
                    started.elapsed()
                ));
            }
            return Ok(());
        }
        anyhow::ensure!(
            started.elapsed() < limit,
            "waited {limit:?} for room in the kernel key quota to start {what} ({containers} container(s)), but {}. \
             Under a rootless Docker daemon each running container holds one key of that quota; \
             raise `kernel.keys.maxkeys` and `kernel.keys.maxbytes` on this host, or run fewer scripts at once (`--jobs`).",
            quota.describe()
        );
        if !said {
            crate::runner::emit(&format!(
                "   waiting for room in the kernel key quota to start {what} ({containers} container(s)): {}",
                quota.describe()
            ));
            said = true;
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            |quota| quota.room() >= 3,
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
            |quota| quota.room() >= 1,
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
            |_| false,
        )
        .await
        .expect("nothing to wait for");
    }

    /// Two source sets that each fit the room alone but not together: the
    /// second waits until the first's reservation is released, instead of both
    /// starting part of their containers and waiting on each other.
    #[tokio::test(start_paused = true)]
    async fn a_second_set_waits_for_the_first_sets_reservation() {
        let pool = Reservations::new();
        // 200 max keys, 194 used: room = 200 - 194 - RESERVE(4) = 2.
        let quota = || {
            Some(KeyQuota {
                uid: 1000,
                keys: 194,
                max_keys: 200,
                bytes: 0,
                max_bytes: 20_000,
            })
        };
        let first = reserve_room_in(
            &pool,
            2,
            "A",
            quota,
            Duration::from_secs(60),
            Duration::from_secs(1),
        )
        .await
        .expect("A fits");
        let second = reserve_room_in(
            &pool,
            2,
            "B",
            quota,
            Duration::from_secs(3),
            Duration::from_secs(1),
        )
        .await;
        assert!(
            format!(
                "{:#}",
                second.err().expect("B must wait while A holds the room")
            )
            .contains("waited 3s"),
        );
        drop(first);
        let second = reserve_room_in(
            &pool,
            2,
            "B",
            quota,
            Duration::from_secs(3),
            Duration::from_secs(1),
        )
        .await
        .expect("B fits once A's reservation is released");
        assert_eq!(*pool.0.lock(), 2);
        drop(second);
        assert_eq!(*pool.0.lock(), 0);
    }
}
