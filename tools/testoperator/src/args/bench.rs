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

use std::num::NonZeroUsize;
use std::time::Duration;

use clap::Parser;
use test_framework::spicetest::datasets::EndCondition;

use super::DatasetTestArgs;

/// Timed runs of each query when neither `--iterations` nor `--duration` is set.
const DEFAULT_ITERATIONS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(4);

/// Arguments for `run bench`.
#[derive(Parser, Debug, Clone)]
pub struct BenchArgs {
    #[command(flatten)]
    pub(crate) test_args: DatasetTestArgs,

    /// Timed runs of each query by each `--concurrency` client, after one untimed warmup
    /// run [default: 5]. Cannot be used with `--duration`.
    #[arg(long, conflicts_with = "duration")]
    pub(crate) iterations: Option<NonZeroUsize>,
}

impl BenchArgs {
    /// When each bench client stops: after `--duration` if it is set, otherwise after
    /// `--iterations` runs of each query.
    #[must_use]
    pub fn runs(&self) -> BenchRuns {
        match self.test_args.common.duration {
            Some(secs) => BenchRuns::Duration(Duration::from_secs(secs)),
            None => BenchRuns::Iterations(self.iterations.unwrap_or(DEFAULT_ITERATIONS)),
        }
    }
}

/// When each `run bench` client stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchRuns {
    /// Run every query this many times, after one untimed warmup run.
    Iterations(NonZeroUsize),
    /// Run the query set again and again until this much time has passed.
    Duration(Duration),
}

impl Default for BenchRuns {
    fn default() -> Self {
        Self::Iterations(DEFAULT_ITERATIONS)
    }
}

impl From<BenchRuns> for EndCondition {
    fn from(runs: BenchRuns) -> Self {
        match runs {
            BenchRuns::Iterations(n) => EndCondition::QuerySetCompleted(n.get()),
            BenchRuns::Duration(d) => EndCondition::Duration(d),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(flags: &[&str]) -> Result<BenchArgs, clap::Error> {
        BenchArgs::try_parse_from(std::iter::once("bench").chain(flags.iter().copied()))
    }

    #[test]
    fn defaults_to_five_iterations() {
        let args = parse(&[]).expect("parses");
        assert_eq!(args.runs(), BenchRuns::default());
        assert_eq!(
            args.runs(),
            BenchRuns::Iterations(NonZeroUsize::new(5).expect("not zero"))
        );
        assert_eq!(args.test_args.common.concurrency, 1);
    }

    #[test]
    fn iterations_sets_the_run_count() {
        let args = parse(&["--iterations", "35", "--concurrency", "4"]).expect("parses");
        assert_eq!(
            args.runs(),
            BenchRuns::Iterations(NonZeroUsize::new(35).expect("not zero"))
        );
        assert_eq!(args.test_args.common.concurrency, 4);
    }

    #[test]
    fn duration_runs_for_a_time() {
        let args = parse(&["--duration", "120"]).expect("parses");
        assert_eq!(args.runs(), BenchRuns::Duration(Duration::from_secs(120)));
    }

    #[test]
    fn iterations_and_duration_conflict() {
        let err = parse(&["--iterations", "10", "--duration", "120"]).expect_err("conflict");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn zero_iterations_is_rejected() {
        assert!(parse(&["--iterations", "0"]).is_err());
    }
}
