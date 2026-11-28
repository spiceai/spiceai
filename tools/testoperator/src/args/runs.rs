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

//! How long a test runs its queries. Each command takes only the flags it uses, so a flag a
//! command would ignore is a parse error instead.

use std::num::NonZeroUsize;
use std::time::Duration;

use clap::Parser;
use test_framework::spicetest::datasets::EndCondition;

use super::DatasetTestArgs;

/// Timed runs of each query when neither `--iterations` nor `--duration` is set.
const DEFAULT_ITERATIONS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(4);

/// When each client of `run bench` or `run query` stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Runs {
    /// Run every query this many times, after one untimed warmup run.
    Iterations(NonZeroUsize),
    /// Run the query set again and again until this much time has passed.
    Duration(Duration),
}

impl Default for Runs {
    fn default() -> Self {
        Self::Iterations(DEFAULT_ITERATIONS)
    }
}

impl From<Runs> for EndCondition {
    fn from(runs: Runs) -> Self {
        match runs {
            Runs::Iterations(n) => EndCondition::QuerySetCompleted(n.get()),
            Runs::Duration(d) => EndCondition::Duration(d),
        }
    }
}

/// `--iterations` or `--duration`, for the commands that can stop after either.
#[derive(Parser, Debug, Clone)]
pub struct RunsArgs {
    /// Timed runs of each query by each `--concurrency` client, after one untimed warmup
    /// run [default: 5]. Cannot be used with `--duration`.
    #[arg(long, conflicts_with = "duration")]
    pub(crate) iterations: Option<NonZeroUsize>,

    /// Run the query set again and again for this many seconds, in place of `--iterations`.
    /// This mode has no warmup run, so it takes no result or explain-plan snapshots.
    #[arg(long)]
    pub(crate) duration: Option<u64>,
}

impl RunsArgs {
    #[must_use]
    pub fn runs(&self) -> Runs {
        match self.duration {
            Some(secs) => Runs::Duration(Duration::from_secs(secs)),
            None => Runs::Iterations(self.iterations.unwrap_or(DEFAULT_ITERATIONS)),
        }
    }
}

/// `--duration`, for the commands that always run for a time.
#[derive(Parser, Debug, Clone)]
pub struct DurationArgs {
    /// The duration of the test in seconds
    #[arg(long, default_value_t = 60)]
    pub(crate) duration: u64,
}

impl DurationArgs {
    #[must_use]
    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.duration)
    }
}

/// Arguments for `run bench` and `run query`.
#[derive(Parser, Debug, Clone)]
pub struct BenchArgs {
    #[command(flatten)]
    pub(crate) test_args: DatasetTestArgs,

    #[command(flatten)]
    pub(crate) runs: RunsArgs,
}

/// Arguments for `run throughput`.
#[derive(Parser, Debug, Clone)]
pub struct ThroughputArgs {
    #[command(flatten)]
    pub(crate) test_args: DatasetTestArgs,

    /// Timed runs of each query by each `--concurrency` client in the measured phase
    #[arg(long, default_value = "2")]
    pub(crate) iterations: NonZeroUsize,

    /// Timed runs of each query by the one client of the baseline phase
    #[arg(long, default_value = "6")]
    pub(crate) baseline_iterations: NonZeroUsize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iterations(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("not zero")
    }

    fn bench(flags: &[&str]) -> Result<BenchArgs, clap::Error> {
        BenchArgs::try_parse_from(["bench", "--query-set", "tpch"].iter().chain(flags))
    }

    fn throughput(flags: &[&str]) -> Result<ThroughputArgs, clap::Error> {
        ThroughputArgs::try_parse_from(["throughput", "--query-set", "tpch"].iter().chain(flags))
    }

    #[test]
    fn bench_defaults_to_five_iterations_on_one_client() {
        let args = bench(&[]).expect("parses");
        assert_eq!(args.runs.runs(), Runs::Iterations(iterations(5)));
        assert_eq!(args.test_args.concurrency, 1);
    }

    #[test]
    fn bench_iterations_and_concurrency() {
        let args = bench(&["--iterations", "35", "--concurrency", "4"]).expect("parses");
        assert_eq!(args.runs.runs(), Runs::Iterations(iterations(35)));
        assert_eq!(args.test_args.concurrency, 4);
    }

    #[test]
    fn bench_duration_runs_for_a_time() {
        let args = bench(&["--duration", "120"]).expect("parses");
        assert_eq!(args.runs.runs(), Runs::Duration(Duration::from_secs(120)));
    }

    #[test]
    fn bench_iterations_and_duration_conflict() {
        let err = bench(&["--iterations", "10", "--duration", "120"]).expect_err("conflict");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn zero_iterations_is_rejected() {
        assert!(bench(&["--iterations", "0"]).is_err());
        assert!(throughput(&["--iterations", "0"]).is_err());
    }

    #[test]
    fn throughput_defaults_keep_six_baseline_and_two_measured_runs() {
        let args = throughput(&[]).expect("parses");
        assert_eq!(args.baseline_iterations, iterations(6));
        assert_eq!(args.iterations, iterations(2));
    }

    #[test]
    fn throughput_sets_both_phases() {
        let args =
            throughput(&["--iterations", "10", "--baseline-iterations", "20"]).expect("parses");
        assert_eq!(args.iterations, iterations(10));
        assert_eq!(args.baseline_iterations, iterations(20));
    }

    #[test]
    fn throughput_rejects_duration() {
        let err = throughput(&["--duration", "60"]).expect_err("no --duration");
        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn duration_args_default_to_sixty_seconds() {
        let args = DurationArgs::try_parse_from(["load"]).expect("parses");
        assert_eq!(args.duration(), Duration::from_secs(60));
    }
}
