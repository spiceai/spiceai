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

//! What a Spicepod's Cayenne accelerations demand of the host.
//!
//! The Runtime builder classifies each enabled Cayenne acceleration before
//! initialization and folds the result into a [`CayenneWorkload`] with
//! [`CayenneWorkload::with_acceleration`].

/// What the Cayenne accelerations configured in a Spicepod will demand of the host,
/// aggregated over every enabled one. Decides how much memory the runtime reserves
/// outside the query pool and which dedicated thread pools it brings up.
///
/// Both flags are unions, so one CDC table in a pod of full-refresh tables still
/// gets the full CDC-shaped reservation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CayenneWorkload {
    /// Any enabled Cayenne acceleration at all.
    configured: bool,
    /// Any table on a profile that can hold rows in the off-pool in-memory CDC
    /// tier. Gates the coordinated host-memory partition — the reduced query-pool
    /// default and the global mem-tier byte budget — which exists solely to leave
    /// room for that tier. A pod without one cannot fill it
    /// (`cdc_durability` is forced to `file` off the small-write profile), so
    /// fencing ~20% of host for it would shrink the query pool for nothing.
    ///
    /// Deliberately NOT narrowed to a file acceleration mode, unlike
    /// `needs_compaction`: a `mode: memory` table holds its whole dataset in that
    /// tier permanently, so it is the case that most needs the room reserved.
    uses_cdc_tier: bool,
    /// Any table that accumulates Vortex files for compaction to consolidate — a
    /// file acceleration mode on a profile that is not a whole-table replace.
    /// Gates the dedicated compaction runtime and its carved memory pool.
    needs_compaction: bool,
}

/// What one enabled Cayenne acceleration demands of the host. Folded into a
/// [`CayenneWorkload`] by [`CayenneWorkload::with_acceleration`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CayenneAccelerationDemand {
    /// The acceleration's write profile can hold rows in the in-memory CDC tier.
    pub uses_cdc_tier: bool,
    /// The acceleration produces Vortex files for compaction to consolidate.
    pub needs_compaction: bool,
}

impl CayenneWorkload {
    /// This workload with one more enabled Cayenne acceleration added. Marks the
    /// workload as configured and unions the acceleration's demands into it.
    #[must_use]
    pub const fn with_acceleration(self, demand: CayenneAccelerationDemand) -> Self {
        Self {
            configured: true,
            uses_cdc_tier: self.uses_cdc_tier || demand.uses_cdc_tier,
            needs_compaction: self.needs_compaction || demand.needs_compaction,
        }
    }

    #[must_use]
    pub const fn is_configured(self) -> bool {
        self.configured
    }

    #[must_use]
    pub const fn uses_cdc_tier(self) -> bool {
        self.uses_cdc_tier
    }

    #[must_use]
    pub const fn needs_compaction(self) -> bool {
        self.needs_compaction
    }

    /// Whether bringing up the dedicated compaction runtime is worthwhile. True
    /// unless no configured Cayenne acceleration can produce files to compact — a
    /// pod with no Cayenne at all still gets one, because a table created later by
    /// DDL may compact and would otherwise fall back to the ambient runtime.
    #[must_use]
    pub const fn may_compact(self) -> bool {
        !self.configured || self.needs_compaction
    }
}

#[cfg(test)]
mod tests {
    use super::{CayenneAccelerationDemand, CayenneWorkload};

    const CDC: CayenneAccelerationDemand = CayenneAccelerationDemand {
        uses_cdc_tier: true,
        needs_compaction: false,
    };
    const COMPACTING: CayenneAccelerationDemand = CayenneAccelerationDemand {
        uses_cdc_tier: false,
        needs_compaction: true,
    };

    #[test]
    fn default_is_unconfigured_and_may_compact() {
        let workload = CayenneWorkload::default();
        assert!(!workload.is_configured());
        assert!(!workload.uses_cdc_tier());
        assert!(!workload.needs_compaction());
        // A pod with no Cayenne keeps the compaction runtime for DDL-created tables.
        assert!(workload.may_compact());
    }

    #[test]
    fn acceleration_without_demands_is_configured_and_cannot_compact() {
        let workload =
            CayenneWorkload::default().with_acceleration(CayenneAccelerationDemand::default());
        assert!(workload.is_configured());
        assert!(!workload.uses_cdc_tier());
        assert!(!workload.needs_compaction());
        assert!(!workload.may_compact());
    }

    #[test]
    fn demands_are_unions() {
        let workload = CayenneWorkload::default()
            .with_acceleration(CDC)
            .with_acceleration(COMPACTING)
            .with_acceleration(CayenneAccelerationDemand::default());
        assert!(workload.is_configured());
        assert!(workload.uses_cdc_tier());
        assert!(workload.needs_compaction());
        assert!(workload.may_compact());
    }
}
