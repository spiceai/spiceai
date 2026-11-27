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

//! The space on the volume holding the cache, and the watermarks the cache's
//! own disk use is kept between.

use std::fmt::Debug;
use std::path::Path;

/// Total and available bytes of a volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Space {
    pub total: u64,
    /// Bytes an unprivileged writer can still use.
    pub available: u64,
}

/// Reports the space of the volume holding a directory. The default reads it
/// from the operating system ([`Statvfs`]); tests substitute a volume of a
/// fixed size.
pub trait VolumeSpace: Send + Sync + Debug {
    /// # Errors
    ///
    /// The operating system's error reading the volume's space.
    fn space(&self, dir: &Path) -> std::io::Result<Space>;
}

/// The volume's space reported by `fs4`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Statvfs;

impl VolumeSpace for Statvfs {
    fn space(&self, dir: &Path) -> std::io::Result<Space> {
        Ok(Space {
            total: fs4::total_space(dir)?,
            available: fs4::available_space(dir)?,
        })
    }
}

/// The cache's disk-use limits: a write that would take the cache over
/// `high` first reclaims until the cache is at or under `low`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Watermarks {
    pub high: u64,
    pub low: u64,
}

/// The free space the cache leaves to other writers on its volume:
/// `permille` of the volume's total, at least `min_bytes`. Once other writers
/// take the free space below it, the cache gives back until the free space is
/// `refill_permille` of the total above it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reserve {
    pub permille: u64,
    pub min_bytes: u64,
    pub refill_permille: u64,
}

impl Reserve {
    /// The free space kept on a volume of `total` bytes.
    #[must_use]
    pub fn bytes(&self, total: u64) -> u64 {
        permille_of(total, self.permille).max(self.min_bytes)
    }
}

fn permille_of(n: u64, permille: u64) -> u64 {
    u64::try_from(u128::from(n) * u128::from(permille) / 1000).unwrap_or(u64::MAX)
}

/// The watermarks for a cache of `budget` bytes using `own` bytes of a volume
/// with `space`.
///
/// `high`, the capacity, is the budget, or what the cache can hold while the
/// volume keeps `reserve` free (its own bytes plus the volume's available
/// bytes, less the reserve) when that is less. `low` is `low_permille` of the
/// capacity; while the volume's free space is below the reserve (other
/// writers took it), it is the capacity less the reserve's refill, so the
/// cache frees the refill on top of what it is over by. Without the volume's
/// space, the budget alone applies.
pub(crate) fn watermarks(
    budget: u64,
    low_permille: u64,
    own: u64,
    space: Option<Space>,
    reserve: Reserve,
) -> Watermarks {
    let Some(space) = space else {
        return Watermarks {
            high: budget,
            low: permille_of(budget, low_permille),
        };
    };
    let reserved = reserve.bytes(space.total);
    let room = own.saturating_add(space.available).saturating_sub(reserved);
    let high = budget.min(room);
    let low = if space.available < reserved {
        let refill = permille_of(space.total, reserve.refill_permille);
        room.saturating_sub(refill)
            .min(permille_of(high, low_permille))
    } else {
        permille_of(high, low_permille)
    };
    Watermarks { high, low }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;

    /// The cache's defaults: 15 % of the volume, at least 4 GiB, refilled
    /// by 5 % of the volume.
    const RESERVE: Reserve = Reserve {
        permille: 150,
        min_bytes: 4 * GIB,
        refill_permille: 50,
    };

    #[test]
    fn watermarks_keep_the_volume_reserve() {
        let space = |total, available| Some(Space { total, available });
        // A 1 TiB volume reserves 153.6 GiB and refills 51.2 GiB.
        let tib = 1024 * GIB;
        let reserved = tib * 15 / 100;
        let refill = tib * 5 / 100;
        // Plenty of free space: the budget rules, low at 90 % of it.
        assert_eq!(
            watermarks(100 * GIB, 900, 10 * GIB, space(tib, 600 * GIB), RESERVE),
            Watermarks {
                high: 100 * GIB,
                low: 90 * GIB
            }
        );
        // A 300 GiB cache filled the volume to its reserve: the capacity is
        // where it stands, the low watermark 90 % of that.
        let full = 300 * GIB;
        let w = watermarks(500 * GIB, 900, full, space(tib, reserved), RESERVE);
        assert_eq!(w.high, full);
        assert_eq!(w.low, full * 9 / 10);
        // Other writers took the volume to 100 GiB free next to a 200 GiB
        // cache: the cache may hold 300 GiB less the reserve, and gives back
        // the refill on top.
        let w = watermarks(500 * GIB, 900, 200 * GIB, space(tib, 100 * GIB), RESERVE);
        assert_eq!(w.high, 300 * GIB - reserved);
        assert_eq!(w.low, 300 * GIB - reserved - refill);
        // Reclaiming frees volume space one for one: the watermarks stay put
        // while the free space is below the reserve.
        assert_eq!(
            watermarks(500 * GIB, 900, 150 * GIB, space(tib, 150 * GIB), RESERVE),
            w
        );
        // A 10 GiB volume reserves the 4 GiB floor, not 15 %.
        let w = watermarks(8 * GIB, 900, 0, space(10 * GIB, 9 * GIB), RESERVE);
        assert_eq!(w.high, 5 * GIB);
        assert_eq!(w.low, 5 * GIB * 9 / 10);
        let w = watermarks(8 * GIB, 900, 4 * GIB, space(10 * GIB, 2 * GIB), RESERVE);
        assert_eq!(w.high, 2 * GIB);
        assert_eq!(w.low, 2 * GIB - 10 * GIB * 5 / 100);
        // Free space below the reserve and an empty cache: nothing may be
        // written.
        assert_eq!(
            watermarks(8 * GIB, 900, 0, space(10 * GIB, 3 * GIB), RESERVE),
            Watermarks { high: 0, low: 0 }
        );
        // The volume could not be read: the budget alone.
        assert_eq!(
            watermarks(32 * MIB, 900, 24 * MIB, None, RESERVE),
            Watermarks {
                high: 32 * MIB,
                low: 32 * MIB * 9 / 10
            }
        );
        // No overflow at the extremes.
        let full = Reserve {
            permille: 1000,
            min_bytes: u64::MAX,
            refill_permille: 1000,
        };
        assert_eq!(
            watermarks(u64::MAX, 1000, u64::MAX, space(u64::MAX, u64::MAX), RESERVE).high,
            u64::MAX - permille_of(u64::MAX, 150)
        );
        assert_eq!(
            watermarks(u64::MAX, 1000, u64::MAX, space(u64::MAX, u64::MAX), full),
            Watermarks { high: 0, low: 0 }
        );
    }
}
