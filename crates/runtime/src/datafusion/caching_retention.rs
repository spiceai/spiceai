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

//! What bounds the size of a `refresh_mode: caching` accelerator.
//!
//! A caching accelerator stores one entry per distinct request and nothing in
//! the read path removes one, so eviction is a retention policy or nothing.
//! This module decides whether the caching parameters can supply that policy,
//! and what to tell an operator whose configuration leaves the accelerator
//! without one.
//!
//! Kept apart from the registration code that calls it, and expressed as a pure
//! function over the parameters rather than as branches around a builder, so the
//! decision and the wording an operator acts on are asserted by tests instead of
//! inferred from whatever a log capture happens to retain.

use std::time::Duration;

/// `caching_ttl`'s own default, applied when the dataset does not set one.
const DEFAULT_CACHING_TTL: Duration = Duration::from_secs(30);

/// Floor on the derived check interval, so a sub-second `caching_ttl` does not
/// put the accelerator under a delete every tick of it.
const MIN_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Ceiling on the derived check interval.
///
/// The interval is derived from the retention period, and the period is as long
/// as the operator's `caching_stale_while_revalidate_ttl` — which is routinely
/// hours or longer. Uncapped, a year-long window schedules the *second* check
/// for a year's time; `tokio::time::interval` fires its first tick immediately,
/// so what an operator sees is one check at startup, over an accelerator that is
/// still empty, and then no eviction for the life of the process. Everything
/// this policy deletes is past `caching_ttl + caching_stale_while_revalidate_ttl`
/// and can no longer be served, so checking more often than the period only ever
/// costs a no-op delete: the ceiling is what makes the policy actually run.
const MAX_CHECK_INTERVAL: Duration = Duration::from_hours(1);

/// Where an operator reads about `refresh_mode: caching`.
const CACHING_DOCS_URL: &str =
    "https://spiceai.org/docs/features/data-acceleration/refresh-modes/caching";

/// What the caching parameters can say about evicting cache entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CachingRetention {
    /// Evict entries older than `period`, checking every `check_interval`.
    /// This replaces whatever retention the dataset declared.
    Derive {
        period: Duration,
        check_interval: Duration,
    },
    /// The caching parameters cannot supply a policy, and the dataset's own is
    /// already running. Leave that one alone.
    LeaveDeclared,
    /// The caching parameters cannot supply a policy and no other one runs, so
    /// nothing bounds the accelerator. The caller warns.
    Unbounded,
}

/// Clamp a retention period into a check interval that will actually run.
fn check_interval_for(period: Duration) -> Duration {
    period.clamp(MIN_CHECK_INTERVAL, MAX_CHECK_INTERVAL)
}

/// What a `refresh_mode: caching` dataset's caching parameters imply about
/// evicting its cache entries.
///
/// With `caching_stale_if_error` disabled, an entry past
/// `caching_ttl + caching_stale_while_revalidate_ttl` can never be served again,
/// so that sum is a retention period, and one derived from the caching
/// parameters is more specific than anything the dataset declared.
///
/// Enabling `caching_stale_if_error` changes what an expired entry *is*: it is
/// the copy served when the source fails, with no upper bound on its age.
/// Evicting at the sum above would delete exactly the data the setting exists to
/// serve, and no other duration in the caching parameters bounds one — so the
/// caching parameters supply no policy at all, and the only thing that can bound
/// the accelerator is a retention policy the dataset declares itself.
///
/// `declared_retention_runs` is whether one does — whether the dataset's
/// retention policy *built*, not whether it was configured. The two differ, and
/// the difference is the silent case: a policy missing its
/// `retention_check_interval` or its `time_column` is assembled into nothing at
/// all, with no task started and no error raised, so reading intent off the
/// config would suppress this warning for an accelerator that is just as
/// unbounded as one configured with no policy at all.
pub(crate) fn caching_retention(
    stale_if_error: bool,
    caching_ttl: Option<Duration>,
    caching_stale_while_revalidate_ttl: Option<Duration>,
    declared_retention_runs: bool,
) -> CachingRetention {
    if !stale_if_error {
        let period = caching_ttl.unwrap_or(DEFAULT_CACHING_TTL)
            + caching_stale_while_revalidate_ttl.unwrap_or_default();

        return CachingRetention::Derive {
            period,
            check_interval: check_interval_for(period),
        };
    }

    if declared_retention_runs {
        CachingRetention::LeaveDeclared
    } else {
        CachingRetention::Unbounded
    }
}

/// What to tell an operator whose caching accelerator has no retention at all.
///
/// The consequence is the message: `caching_ttl` and
/// `caching_stale_while_revalidate_ttl` read as size controls, and an operator
/// who set them has no other reason to expect unbounded growth. Both remedies
/// are named because they answer different questions — the dataset's own
/// retention policy bounds how long an entry is kept while keeping the
/// stale-on-error fallback, and disabling the fallback gets the derived eviction
/// back.
pub(crate) fn unbounded_caching_retention_warning(dataset_name: &str) -> String {
    // `escape_debug` rather than the raw name: a Spicepod identifier may be
    // quoted, and a quoted one may legally contain a newline, so a validated
    // name can otherwise break this line in two and forge a second one.
    let dataset_name = dataset_name.escape_debug();
    format!(
        "Dataset '{dataset_name}' sets `caching_stale_if_error: enabled` and declares no retention \
        policy, so no cached entry is ever evicted and the accelerator grows with every distinct \
        request it serves. An expired entry is the copy served when the source fails, so \
        `caching_ttl` and `caching_stale_while_revalidate_ttl` bound how long an entry is served \
        fresh, not how long it is stored. Set `retention_check_enabled: true` with a \
        `retention_period`, a `retention_check_interval` and the dataset's `time_column` to bound \
        how long an entry is kept, or set `caching_stale_if_error: disabled` to evict at \
        `caching_ttl` + `caching_stale_while_revalidate_ttl`. See: {CACHING_DOCS_URL}"
    )
}

#[cfg(test)]
mod tests {
    use super::{
        CachingRetention, MAX_CHECK_INTERVAL, MIN_CHECK_INTERVAL, caching_retention,
        unbounded_caching_retention_warning,
    };
    use std::time::Duration;

    #[test]
    fn a_disabled_stale_if_error_evicts_at_ttl_plus_stale_while_revalidate() {
        let retention = caching_retention(false, Some(Duration::from_secs(5)), None, false);

        assert_eq!(
            retention,
            CachingRetention::Derive {
                period: Duration::from_secs(5),
                check_interval: MIN_CHECK_INTERVAL,
            }
        );
    }

    #[test]
    fn an_unset_caching_ttl_falls_back_to_its_own_default() {
        let CachingRetention::Derive { period, .. } =
            caching_retention(false, None, Some(Duration::from_secs(10)), false)
        else {
            panic!("a dataset with `caching_stale_if_error` disabled always derives a policy");
        };

        assert_eq!(period, Duration::from_secs(30) + Duration::from_secs(10));
    }

    /// A derived policy is more specific than a declared one and replaces it, so
    /// a running declared policy must not change what is derived.
    #[test]
    fn a_declared_retention_does_not_change_what_a_disabled_stale_if_error_derives() {
        assert_eq!(
            caching_retention(false, Some(Duration::from_secs(5)), None, true),
            caching_retention(false, Some(Duration::from_secs(5)), None, false),
        );
    }

    /// The reported half of #13525: enabling `caching_stale_if_error` left the
    /// dataset with no retention policy at all, and said nothing about it.
    #[test]
    fn an_enabled_stale_if_error_with_no_declared_retention_is_unbounded() {
        let retention = caching_retention(true, Some(Duration::from_secs(5)), None, false);

        assert_eq!(retention, CachingRetention::Unbounded);
    }

    /// The dataset's own policy is the one thing that can bound a stale-on-error
    /// cache, so the caching branch must leave it in place rather than replace it
    /// with a policy keyed on a different column.
    #[test]
    fn an_enabled_stale_if_error_leaves_a_running_declared_retention_alone() {
        let retention = caching_retention(true, Some(Duration::from_secs(5)), None, true);

        assert_eq!(retention, CachingRetention::LeaveDeclared);
    }

    /// A retention policy that was configured but did not build — no
    /// `retention_check_interval`, say — starts no task, so the accelerator is
    /// exactly as unbounded as one with no policy configured and must be told so.
    /// This is the caller's contract: it passes whether the policy *runs*.
    #[test]
    fn a_declared_retention_that_did_not_build_is_still_unbounded() {
        let retention = caching_retention(true, Some(Duration::from_secs(5)), None, false);

        assert_eq!(retention, CachingRetention::Unbounded);
    }

    /// The second half of #13525: a long `caching_stale_while_revalidate_ttl`
    /// made the derived check interval as long as the retention period, so the
    /// policy ran once at startup — over an accelerator still empty — and never
    /// again.
    #[test]
    fn a_year_long_window_still_checks_within_the_hour() {
        // Expressed as a multiple of the ceiling rather than a literal, so the
        // assertion below reads against the bound it is testing.
        let year = MAX_CHECK_INTERVAL * 24 * 365;
        let CachingRetention::Derive {
            period,
            check_interval,
        } = caching_retention(false, Some(Duration::from_secs(1)), Some(year), false)
        else {
            panic!("a dataset with `caching_stale_if_error` disabled always derives a policy");
        };

        assert_eq!(period, Duration::from_secs(1) + year);
        assert_eq!(check_interval, MAX_CHECK_INTERVAL);
    }

    #[test]
    fn the_warning_names_the_dataset_the_consequence_and_both_remedies() {
        let warning = unbounded_caching_retention_warning("api_cache");

        assert!(warning.contains("'api_cache'"), "{warning}");
        assert!(
            warning.contains("grows with every distinct request"),
            "{warning}"
        );
        assert!(
            warning.contains("`retention_check_enabled: true`"),
            "{warning}"
        );
        assert!(warning.contains("`retention_period`"), "{warning}");
        assert!(warning.contains("`retention_check_interval`"), "{warning}");
        assert!(
            warning.contains("`caching_stale_if_error: disabled`"),
            "{warning}"
        );
        assert!(warning.contains("https://spiceai.org/docs"), "{warning}");
        assert!(!warning.contains('\n'), "{warning}");
    }

    #[test]
    fn a_dataset_name_carrying_a_newline_cannot_forge_a_second_log_line() {
        let warning = unbounded_caching_retention_warning("api\ncache");

        assert!(!warning.contains('\n'), "{warning}");
        assert!(warning.contains(r"api\ncache"), "{warning}");
    }
}
