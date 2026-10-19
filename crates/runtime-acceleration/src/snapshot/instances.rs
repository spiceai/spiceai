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

//! What each Spice instance that uses a dataset serves of its pin, as
//! `GET /v1/datasets/{name}/acceleration/snapshots` lists it.

use serde::{Deserialize, Serialize};

use super::api::{InstanceRole, InstanceStatus, PinServing};

/// The reason listed for a writer whose lease records no pin support.
const OLDER_VERSION: &str = "an older Spice version";
/// The reason listed for an instance that has not yet loaded the pin.
const PENDING: &str = "loads it at its next check";

/// What one instance reports it serves of its dataset's pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceReport {
    pub instance_id: String,
    /// The snapshot it serves, or `None` while it serves its live data.
    pub serves: Option<u64>,
    /// Why it does not serve the pin.
    pub not_served: Option<String>,
}

/// What the writer serves of its dataset's pin, as its lease records it. A lease that
/// records one is held by a version of Spice that serves pins.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct WriterStatus {
    /// The snapshot the writer serves, or `None` while it serves its live data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serves_snapshot_id: Option<u64>,
    /// Why the writer does not serve the pin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_served: Option<String>,
}

/// The instance that holds the dataset's snapshot writer lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHolder {
    pub instance_id: String,
    /// What it serves of the pin, or `None` when its version of Spice has no pins.
    pub status: Option<WriterStatus>,
}

/// The writer's and this instance's status for a dataset pinned to `pinned`: the writer
/// as its lease records it, and this instance as it reports itself, also when it is the
/// writer.
#[must_use]
pub fn instance_statuses(
    pinned: Option<u64>,
    writer: Option<LeaseHolder>,
    this: Option<InstanceReport>,
) -> Vec<InstanceStatus> {
    let mut statuses = Vec::new();
    let this_is_writer = this
        .as_ref()
        .zip(writer.as_ref())
        .is_some_and(|(this, writer)| this.instance_id == writer.instance_id);
    if let Some(writer) = writer.filter(|_| !this_is_writer) {
        let report = match writer.status {
            Some(status) => InstanceReport {
                instance_id: writer.instance_id,
                serves: status.serves_snapshot_id,
                not_served: status.not_served,
            },
            None => InstanceReport {
                instance_id: writer.instance_id,
                serves: None,
                not_served: Some(OLDER_VERSION.to_string()),
            },
        };
        statuses.push(status(pinned, report, InstanceRole::Writer));
    }
    if let Some(this) = this {
        let role = if this_is_writer {
            InstanceRole::Writer
        } else {
            InstanceRole::This
        };
        statuses.push(status(pinned, this, role));
    }
    statuses
}

fn status(pinned: Option<u64>, report: InstanceReport, role: InstanceRole) -> InstanceStatus {
    let serves_pin = pinned.is_some() && report.serves == pinned;
    InstanceStatus {
        instance_id: report.instance_id,
        role,
        serves_snapshot_id: report.serves,
        status: if serves_pin {
            PinServing::Pinned
        } else {
            PinServing::NotPinned
        },
        reason: (pinned.is_some() && !serves_pin)
            .then(|| report.not_served.unwrap_or_else(|| PENDING.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DISK: &str = "not enough disk for snapshot 41 (2.1 GiB needed, 1.4 GiB free)";

    fn report(id: &str, serves: Option<u64>, not_served: Option<&str>) -> InstanceReport {
        InstanceReport {
            instance_id: id.to_string(),
            serves,
            not_served: not_served.map(ToString::to_string),
        }
    }

    fn writer(id: &str, serves: Option<u64>, not_served: Option<&str>) -> LeaseHolder {
        LeaseHolder {
            instance_id: id.to_string(),
            status: Some(WriterStatus {
                serves_snapshot_id: serves,
                not_served: not_served.map(ToString::to_string),
            }),
        }
    }

    fn status(
        id: &str,
        role: InstanceRole,
        serves: Option<u64>,
        pinned: bool,
        reason: Option<&str>,
    ) -> InstanceStatus {
        InstanceStatus {
            instance_id: id.to_string(),
            role,
            serves_snapshot_id: serves,
            status: if pinned {
                PinServing::Pinned
            } else {
                PinServing::NotPinned
            },
            reason: reason.map(ToString::to_string),
        }
    }

    #[test]
    fn the_writer_and_this_instance_show_whether_they_serve_the_pin_and_why_not() {
        assert_eq!(
            instance_statuses(
                Some(41),
                Some(writer("writer-0", Some(41), None)),
                Some(report("reader-1", Some(44), Some(DISK))),
            ),
            vec![
                status("writer-0", InstanceRole::Writer, Some(41), true, None),
                status("reader-1", InstanceRole::This, Some(44), false, Some(DISK)),
            ]
        );
    }

    #[test]
    fn the_writer_answering_shows_its_own_state_once() {
        assert_eq!(
            instance_statuses(
                Some(41),
                Some(writer("writer-0", None, None)),
                Some(report("writer-0", Some(41), None)),
            ),
            vec![status(
                "writer-0",
                InstanceRole::Writer,
                Some(41),
                true,
                None
            )]
        );
    }

    #[test]
    fn an_instance_that_has_not_loaded_the_pin_says_when_it_will() {
        assert_eq!(
            instance_statuses(Some(41), Some(writer("writer-0", None, None)), None),
            vec![status(
                "writer-0",
                InstanceRole::Writer,
                None,
                false,
                Some(PENDING)
            )]
        );
    }

    #[test]
    fn a_writer_without_pins_shows_as_an_older_spice_version() {
        let older = LeaseHolder {
            instance_id: "writer-0".to_string(),
            status: None,
        };
        assert_eq!(
            instance_statuses(Some(41), Some(older.clone()), None),
            vec![status(
                "writer-0",
                InstanceRole::Writer,
                None,
                false,
                Some(OLDER_VERSION)
            )]
        );
        assert_eq!(
            instance_statuses(None, Some(older), None),
            vec![status("writer-0", InstanceRole::Writer, None, false, None)],
            "no reason while the dataset is not pinned"
        );
    }

    #[test]
    fn no_instance_has_a_reason_while_the_dataset_is_not_pinned() {
        assert_eq!(
            instance_statuses(None, None, Some(report("reader-0", Some(44), Some(DISK)))),
            vec![status(
                "reader-0",
                InstanceRole::This,
                Some(44),
                false,
                None
            )]
        );
    }
}
