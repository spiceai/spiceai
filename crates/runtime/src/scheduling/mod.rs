/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

pub mod dataset;
pub mod view;
pub mod worker;

use crate::accelerated::refresh_completion::RefreshCompletionOutcome;

/// Maps a refresh-completion wait onto a scheduled-task result.
///
/// Cron-triggered refreshes have no `refresh_check_interval`, so a failed
/// run records [`RefreshCompletionOutcome::TerminalFailure`]. That must
/// fail the scheduled task; treating it as `Ok(())` reports a successful
/// cron run for a load that did not land. Removal/shutdown still returns
/// `Ok` on [`RefreshCompletionOutcome::Abandoned`].
pub(crate) fn scheduled_refresh_wait_result(
    outcome: RefreshCompletionOutcome,
    name: impl std::fmt::Display,
) -> scheduler::Result<()> {
    match outcome {
        RefreshCompletionOutcome::Answered => Ok(()),
        RefreshCompletionOutcome::Abandoned => {
            tracing::debug!("{name} was removed before its scheduled refresh completed.");
            Ok(())
        }
        RefreshCompletionOutcome::TerminalFailure => Err(scheduler::Error::RefreshTaskFailure {
            source: Box::new(ScheduledRefreshFailed {
                name: name.to_string(),
            }),
        }),
    }
}

#[derive(Debug)]
struct ScheduledRefreshFailed {
    name: String,
}

impl std::fmt::Display for ScheduledRefreshFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "scheduled refresh of '{}' failed, so the accelerator did not load. Check the dataset source and try again",
            self.name
        )
    }
}

impl std::error::Error for ScheduledRefreshFailed {}

#[cfg(test)]
mod tests {
    use super::{RefreshCompletionOutcome, scheduled_refresh_wait_result};

    #[test]
    fn cron_terminal_failure_is_a_scheduled_task_failure() {
        let wait_outcome = RefreshCompletionOutcome::TerminalFailure;
        let scheduled_task_result = scheduled_refresh_wait_result(wait_outcome, "orders");
        eprintln!("wait_outcome={wait_outcome:?} scheduled_task_result={scheduled_task_result:?}");
        assert!(
            matches!(
                scheduled_task_result,
                Err(scheduler::Error::RefreshTaskFailure { .. })
            ),
            "a cron-triggered one-shot failure must not report Ok(())"
        );
    }

    #[test]
    fn cron_abandoned_refresh_is_not_a_task_failure() {
        assert!(
            scheduled_refresh_wait_result(RefreshCompletionOutcome::Abandoned, "orders").is_ok(),
            "removal during a scheduled refresh is not a task failure"
        );
    }

    #[test]
    fn cron_answered_refresh_is_ok() {
        assert!(
            scheduled_refresh_wait_result(RefreshCompletionOutcome::Answered, "orders").is_ok(),
            "a successful scheduled refresh must remain Ok(())"
        );
    }
}
