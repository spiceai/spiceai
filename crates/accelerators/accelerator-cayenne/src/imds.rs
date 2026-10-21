/*
Copyright 2026 The Spice.ai OSS Authors

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

//! EC2 Instance Metadata Service (IMDS) probe — best-effort detection of the
//! instance type so the Cayenne auto-tuner can reason about two AWS-specific
//! cliffs that block-device inspection cannot see:
//!
//! * **EBS-optimized baseline bandwidth.** Small Nitro sizes (`large`/`xlarge`/
//!   `2xlarge`) have a *low* sustained EBS bandwidth and rely on a burst bucket;
//!   under sustained CDC the bucket drains and throughput collapses to baseline.
//!   The baseline (authoritative, published per size) bounds the aggregate upload
//!   concurrency so parallel uploads don't oversubscribe the instance pipe.
//! * **T-family burstable CPU.** `t2`/`t3`/`t4g` throttle to a low CPU baseline
//!   once CPU credits deplete; the controller withholds CPU-stealing moves sooner
//!   on a burstable instance (the CPU sampler alone is credit-blind).
//!
//! The transport is the AWS SDK's `IMDSv2` client ([`Client`],
//! already a runtime dependency), so token handling, `IMDSv1` fallback, the
//! `AWS_EC2_METADATA_DISABLED` env var, and IPv6 endpoints come for free. It is
//! non-blocking and fail-open: a tight connect/read timeout plus a single attempt
//! means that off-AWS (no route to the link-local address) detection fails in
//! milliseconds and returns `None`, leaving the cloud-agnostic calibration probe
//! ([`super::storage`]) as the sole storage signal. Set `SPICE_DISABLE_IMDS=1` to
//! skip the probe entirely. The result is memoized for the process lifetime.

use std::time::Duration;

use aws_config::imds::client::Client;
use tokio::sync::OnceCell;

/// Tight connect/read timeout, single attempt: this runs once at first table
/// registration and must never meaningfully delay startup, on or off AWS.
const IMDS_TIMEOUT: Duration = Duration::from_millis(250);

/// Detected EC2 instance characteristics relevant to CDC tuning. Absent fields
/// fall back to class/probe-derived behavior, so every field is advisory.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct InstanceProfile {
    /// T-family burstable CPU (`t2`/`t3`/`t4g`): CPU credits deplete under
    /// sustained load and the instance throttles to a low baseline. The tuner
    /// withholds CPU-stealing moves at a lower busy-fraction here.
    pub burstable: bool,
    /// EBS-optimized *baseline* (sustained) bandwidth in MiB/s for the burst-prone
    /// small Nitro sizes where the baseline is low and authoritative; `None` for
    /// sizes/families that run at/above baseline continuously (no cap needed) —
    /// the calibration probe covers those.
    pub ebs_baseline_mbps: Option<f64>,
}

static INSTANCE_PROFILE: OnceCell<Option<InstanceProfile>> = OnceCell::const_new();

/// Detect the EC2 instance profile via `IMDSv2`, memoized for the process. Returns
/// `None` when disabled, off-AWS, or the metadata service is unreachable within
/// [`IMDS_TIMEOUT`].
pub(crate) async fn detect_instance_profile() -> Option<InstanceProfile> {
    INSTANCE_PROFILE
        .get_or_init(|| async {
            if std::env::var_os("SPICE_DISABLE_IMDS").is_some() {
                return None;
            }
            let instance_type = fetch_instance_type().await?;
            let profile = profile_from_instance_type(&instance_type);
            tracing::debug!(
                instance_type = %instance_type,
                burstable = profile.burstable,
                ebs_baseline_mbps = ?profile.ebs_baseline_mbps,
                "Detected EC2 instance profile via IMDS"
            );
            Some(profile)
        })
        .await
        .clone()
}

/// Fetch the instance type string via the SDK's `IMDSv2` client. Any error (no
/// route, non-2xx, timeout) yields `None`.
async fn fetch_instance_type() -> Option<String> {
    let client = Client::builder()
        .connect_timeout(IMDS_TIMEOUT)
        .read_timeout(IMDS_TIMEOUT)
        .max_attempts(1)
        .build();
    let instance_type: String = client
        .get("/latest/meta-data/instance-type")
        .await
        .ok()?
        .into();
    let trimmed = instance_type.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Map an instance type string to its tuning-relevant profile. Pure (no I/O), so
/// the family/size classification is unit-testable without IMDS.
fn profile_from_instance_type(instance_type: &str) -> InstanceProfile {
    InstanceProfile {
        burstable: is_burstable_family(instance_type),
        ebs_baseline_mbps: ebs_baseline_mbps(instance_type),
    }
}

/// T-family burstable: `t` followed by a generation digit (`t2`/`t3`/`t4g`),
/// excluding the unrelated families that merely start with `t` (none in EC2 today,
/// but the digit guard keeps it robust to future families).
fn is_burstable_family(instance_type: &str) -> bool {
    let mut chars = instance_type.chars();
    chars.next() == Some('t') && chars.next().is_some_and(|c| c.is_ascii_digit())
}

/// EBS-optimized baseline bandwidth in MiB/s for the burst-prone small Nitro
/// sizes. These baselines (Mbps ÷ 8) are low enough that sustained CDC depletes
/// the burst bucket and collapses to them — the cases where bounding aggregate
/// upload concurrency matters. Larger sizes run at/above baseline continuously, so
/// they return `None` (no cap; the calibration probe handles them). Keyed on the
/// size suffix only: EBS baselines vary by family/generation, but encoding the
/// full family×size matrix would over-fit (and rot) for an advisory, fail-open,
/// cap-only-lowering signal — the size-only approximation is deliberate.
fn ebs_baseline_mbps(instance_type: &str) -> Option<f64> {
    let size = instance_type.split('.').nth(1)?;
    let mbps = match size {
        // ~1250 / ~2500 / ~5000 Mbps baselines, in MiB/s.
        "large" => 156.0,
        "xlarge" => 312.0,
        "2xlarge" => 625.0,
        _ => return None,
    };
    Some(mbps)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_burstable_families() {
        assert!(is_burstable_family("t3.large"));
        assert!(is_burstable_family("t4g.medium"));
        assert!(is_burstable_family("t2.micro"));
        assert!(!is_burstable_family("m7i.large"));
        assert!(!is_burstable_family("c7g.xlarge"));
        // Defensive: a bare "t" or "trn1" (Trainium) must not count as burstable.
        assert!(!is_burstable_family("t"));
        assert!(!is_burstable_family("trn1.2xlarge"));
    }

    #[test]
    fn ebs_baseline_covers_burst_prone_small_sizes_only() {
        assert_eq!(ebs_baseline_mbps("m7i.large"), Some(156.0));
        assert_eq!(ebs_baseline_mbps("c6i.xlarge"), Some(312.0));
        assert_eq!(ebs_baseline_mbps("r7g.2xlarge"), Some(625.0));
        // Larger sizes run at baseline continuously → no cap (probe handles them).
        assert_eq!(ebs_baseline_mbps("m7i.8xlarge"), None);
        assert_eq!(ebs_baseline_mbps("m7i.metal-48xl"), None);
        // Malformed input never panics.
        assert_eq!(ebs_baseline_mbps("garbage"), None);
    }

    #[test]
    fn profile_from_instance_type_combines_signals() {
        let p = profile_from_instance_type("t3.large");
        assert!(p.burstable);
        assert_eq!(p.ebs_baseline_mbps, Some(156.0));

        let p = profile_from_instance_type("m7i.4xlarge");
        assert!(!p.burstable);
        assert_eq!(p.ebs_baseline_mbps, None);
    }
}
