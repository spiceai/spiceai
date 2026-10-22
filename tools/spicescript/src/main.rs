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

//! `spicescript` runs YAML scenario scripts against prebuilt `spiced`
//! binaries. A script starts the sources it needs, starts `spiced` on a
//! spicepod, changes things while it runs — source writes, outages, restarts,
//! kills — and checks each answer against an oracle that shares no code with
//! Spice. See `test/spicescript/README.md`.

use std::{path::PathBuf, process::ExitCode};

use anyhow::{Context, Result, bail};
use clap::Parser;

mod cleanup;
mod compare;
mod docker;
mod generate;
mod ledger;
mod reap;
mod rqg;
mod runner;
mod script;
mod slt;
mod source;
mod spiced;
mod template;
mod tlp;
mod tpch;

#[derive(Parser)]
#[command(about = "Run scenario scripts against spiced")]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent command-line switches"
)]
struct Cli {
    /// Script files, or directories of `*.yaml` scripts.
    #[arg(required = true)]
    scripts: Vec<PathBuf>,

    /// A `spiced` binary to test, as `PATH` or `NAME=PATH`. Repeat it to run
    /// every script on several binaries and require identical answers, such as
    /// a release against the current build.
    #[arg(long = "spiced", value_name = "[NAME=]PATH")]
    spiced: Vec<String>,

    /// Start every instance on this `spiced` binary instead; an `upgrade`
    /// step then moves it to the binary under test, over the same data. Without
    /// it, `upgrade` is a restart.
    #[arg(long, value_name = "PATH")]
    from: Option<PathBuf>,

    /// Run every instance as a cluster: a scheduler with one executor,
    /// over node mTLS from the nearest `cluster-pki/` above the script.
    #[arg(long)]
    cluster: bool,

    /// Where instances keep their spicepods, data and logs.
    #[arg(long, default_value = "target/spicescript")]
    work_dir: PathBuf,

    /// Leave source containers running after the run, for debugging.
    #[arg(long)]
    keep: bool,

    /// The `spice` CLI that `spice` steps run.
    #[arg(long = "spice-cli", value_name = "PATH", default_value = "spice")]
    spice: PathBuf,

    /// Parse the scripts and stop, without running anything.
    #[arg(long)]
    check: bool,

    /// Run up to this many `spiced` processes at once. Every instance of every
    /// matrix combination is a job of its own, taking a slot per process (its
    /// executors too); instances that work together run as one job, and a
    /// script with `defaults.exclusive` runs alone. Each script's output is
    /// printed when it finishes.
    #[arg(long, default_value_t = 1, value_name = "N")]
    jobs: usize,

    /// Record each script's run time in `recordings/durations.json` beside
    /// it, the estimate `--jobs` orders scripts by; scripts without one may
    /// run. Times from a `--jobs 1` run are the least disturbed by neighbours.
    #[arg(long = "record-durations")]
    record_durations: bool,

    /// Rewrite each run script's entries in `recordings/expected-failures.json`
    /// with the failures this run saw, instead of checking against them. A new
    /// entry's issue group is its `known-failing/<group>/` directory or the
    /// generator's defect labels, else `unassigned` until set by hand.
    #[arg(long = "record-failures")]
    record_failures: bool,

    /// Run only this share of the scripts, `I/N` (1-based), as one of N CI
    /// jobs. Scripts are dealt longest first, by recorded duration, to the
    /// share with the least work so far, so the N shares take about as long.
    #[arg(long, value_name = "I/N", value_parser = parse_partition)]
    partition: Option<(usize, usize)>,

    /// Cap every wait for a condition (`eventually`, readiness, a refresh)
    /// at this long, such as `20s`: for scripts expected to fail, which
    /// otherwise wait out every limit in full.
    #[arg(long = "max-wait", value_name = "DURATION", value_parser = humantime::parse_duration)]
    max_wait: Option<std::time::Duration>,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}

async fn run(cli: Cli) -> Result<bool> {
    // Retried comparisons would print every intermediate mismatch; the runner
    // reports the last one with both answers.
    result_compare::set_print_diagnostics(false);
    let targets = targets(&cli.spiced)?;
    let mut paths = script_paths(&cli.scripts)?;
    // Parse everything first, so a typo in the last script fails before the
    // first one spends minutes running.
    let mut scripts = paths
        .iter()
        .map(|p| script::Script::load(p, cli.record_durations || cli.check))
        .collect::<Result<Vec<_>>>()?;
    if let Some((index, count)) = cli.partition {
        let share = partition(&scripts, count);
        (paths, scripts) = paths
            .into_iter()
            .zip(scripts)
            .zip(share)
            .filter_map(|(pair, share)| (share == index - 1).then_some(pair))
            .unzip();
    }
    if cli.check {
        return check(&paths, &scripts);
    }

    let work_dir = std::path::absolute(&cli.work_dir)?;
    reap::reap(&work_dir);
    if cli.keep {
        cleanup::keep_containers();
    }
    cleanup::install_signal_handler();
    if let Some(limit) = cli.max_wait {
        runner::set_max_wait(limit);
    }
    if let Some(from) = &cli.from {
        anyhow::ensure!(
            from.exists(),
            "spiced binary {} does not exist",
            from.display()
        );
    }
    let options = runner::Options {
        targets,
        from: cli.from,
        cluster: cli.cluster,
        work_dir,
        keep: cli.keep,
        spice_cli: cli.spice,
    };
    let mut ledger = ledger::Ledger::load(&scripts)?;
    let mut passed = 0;
    let mut expected = 0;
    let mut failures = Vec::new();
    let mut notes: Vec<String> = Vec::new();
    let total = scripts.len();
    let run_started = std::time::Instant::now();
    // Each script, its time and failure count, for the summary.
    let mut timings: Vec<(&script::Script, std::time::Duration, usize)> = Vec::new();
    let mut done = 0;
    // Longest first, by recorded duration (a script without one counts as
    // long): each asks for slots in this order, and slots are granted in the
    // order asked.
    let mut order: Vec<&script::Script> = scripts.iter().collect();
    order.sort_by_key(|s| std::cmp::Reverse(s.duration.unwrap_or(std::time::Duration::MAX)));
    let slots = runner::Slots::new(cli.jobs);
    let mut running = futures::stream::FuturesUnordered::new();
    for script in order {
        let (options, slots) = (&options, &slots);
        running.push(async move {
            let started = std::time::Instant::now();
            let (report, out) = runner::run_script_buffered(script, options, slots).await;
            (script, report, out, started.elapsed())
        });
    }
    while let Some((script, report, out, waited)) = futures::StreamExt::next(&mut running).await {
        print!("{out}");
        let report = report?;
        // From its first slot, not from when it started waiting for one.
        let took = report.started.map_or(waited, |t| t.elapsed());
        done += 1;
        let problems = if cli.record_failures {
            ledger.record(script, &report.failures);
            report.failures.iter().map(ToString::to_string).collect()
        } else {
            let verdict = ledger.judge(script, &report.failures);
            expected += verdict.expected;
            notes.extend(verdict.notes.iter().cloned());
            verdict.problems()
        };
        let shown_expected = if cli.record_failures {
            0
        } else {
            report.failures.len().saturating_sub(problems.len())
        };
        println!(
            "[{done}/{total}] done    {} in {:.1}s: {} passed, {} failed{}",
            script.name,
            took.as_secs_f64(),
            report.passed,
            problems.len(),
            expected_failures_note(shown_expected),
        );
        timings.push((script, took, problems.len()));
        passed += report.passed;
        failures.extend(problems);
    }
    if cli.record_failures {
        ledger.save()?;
        println!(
            "recorded the expected failures of {} script(s)",
            scripts.len()
        );
    }
    if cli.record_durations {
        // By directory, each into its own durations file.
        let mut by_dir: std::collections::BTreeMap<
            std::path::PathBuf,
            Vec<(String, std::time::Duration)>,
        > = std::collections::BTreeMap::new();
        for (script, took, _) in &timings {
            by_dir
                .entry(script.dir.clone())
                .or_default()
                .push((script.file_stem.clone(), *took));
        }
        for (dir, took) in &by_dir {
            script::Script::record_durations(dir, took)?;
        }
        println!("recorded the duration of {} script(s)", timings.len());
    }
    println!();
    for failure in &failures {
        println!("FAILED {failure}\n");
    }
    for note in &notes {
        println!("note: {note}");
    }
    timings.sort_by_key(|t| std::cmp::Reverse(t.1));
    if timings.len() > 1 {
        println!("slowest scripts:");
        for (script, took, failed) in timings.iter().take(10) {
            let note = if *failed > 0 {
                format!("  ({failed} failed)")
            } else {
                String::new()
            };
            println!("  {:>8.1}s  {}{note}", took.as_secs_f64(), script.name);
        }
        println!();
    }
    println!(
        "{} scripts: {passed} checks passed, {} failed{}, in {:.1}s",
        scripts.len(),
        failures.len(),
        expected_failures_note(expected),
        run_started.elapsed().as_secs_f64()
    );
    Ok(failures.is_empty() || cli.record_failures)
}

/// The scripts named on the command line, directories expanded.
fn script_paths(args: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for path in args {
        if !path.is_dir() {
            paths.push(path.clone());
            continue;
        }
        paths.extend(scripts_in(path)?);
        // `known-failing/` keeps its scripts a directory per issue group.
        if path.file_name().is_some_and(|n| n == "known-failing") {
            let mut groups: Vec<PathBuf> = std::fs::read_dir(path)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.is_dir())
                .collect();
            groups.sort();
            for group in groups {
                paths.extend(scripts_in(&group)?);
            }
        }
    }
    Ok(paths)
}

/// `--check`: prints each parsed script's size, and every `avoid:` entry no
/// expected failure needs; passes when there is none.
fn check(paths: &[PathBuf], scripts: &[script::Script]) -> Result<bool> {
    for (path, script) in paths.iter().zip(scripts) {
        println!(
            "{}: {} steps, {} variants",
            path.display(),
            script.steps.len(),
            script.variants.len()
        );
    }
    let stale = ledger::stale_avoids(scripts)?;
    let mut by_defect: std::collections::BTreeMap<&str, Vec<&str>> =
        std::collections::BTreeMap::new();
    for (script, name) in &stale {
        by_defect.entry(name).or_default().push(script);
    }
    for (name, users) in &by_defect {
        println!(
            "`avoid: [{name}]` matches no expected failure (in {}): if the defect is fixed, remove it from `avoid:`; otherwise record a run without it (--record-failures)",
            users.join(", ")
        );
    }
    Ok(stale.is_empty())
}

/// ` (N expected failures)` after a failure count, when there are any.
fn expected_failures_note(expected: usize) -> String {
    if expected > 0 {
        format!(" ({expected} expected failures)")
    } else {
        String::new()
    }
}

fn parse_partition(text: &str) -> Result<(usize, usize), String> {
    let parsed = text
        .split_once('/')
        .and_then(|(i, n)| Some((i.parse::<usize>().ok()?, n.parse::<usize>().ok()?)));
    match parsed {
        Some((i, n)) if (1..=n).contains(&i) => Ok((i, n)),
        _ => Err(format!("`{text}` is not I/N with 1 <= I <= N")),
    }
}

/// Which of `count` shares each script falls in: longest first (a script
/// without a recorded duration counts as an hour), each to the share with
/// the least recorded time so far, ties to the lower share. Depends only on
/// the scripts and their recorded durations, so every job computes the same
/// split.
fn partition(scripts: &[script::Script], count: usize) -> Vec<usize> {
    let cost = |s: &script::Script| s.duration.unwrap_or(std::time::Duration::from_secs(3600));
    let mut order: Vec<usize> = (0..scripts.len()).collect();
    order.sort_by(|&a, &b| {
        cost(&scripts[b])
            .cmp(&cost(&scripts[a]))
            .then_with(|| scripts[a].dir.cmp(&scripts[b].dir))
            .then_with(|| scripts[a].file_stem.cmp(&scripts[b].file_stem))
    });
    let mut load = vec![std::time::Duration::ZERO; count];
    let mut share = vec![0; scripts.len()];
    for i in order {
        let least = (0..count).min_by_key(|&k| (load[k], k)).unwrap_or(0);
        load[least] += cost(&scripts[i]);
        share[i] = least;
    }
    share
}

/// The scripts directly in a directory, in name order.
fn scripts_in(dir: &std::path::Path) -> Result<Vec<PathBuf>> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "yaml" || x == "yml"))
        .collect();
    entries.sort();
    Ok(entries)
}

fn targets(specs: &[String]) -> Result<Vec<runner::Target>> {
    if specs.is_empty() {
        let home = std::env::var("HOME").context("HOME is not set")?;
        let binary = PathBuf::from(home).join(".spice/bin/spiced");
        return Ok(vec![runner::Target {
            name: "spiced".into(),
            binary,
        }]);
    }
    let targets = specs
        .iter()
        .enumerate()
        .map(|(i, spec)| {
            let (name, path) = match spec.split_once('=') {
                Some((name, path)) => (name.to_string(), path),
                None => (format!("spiced{i}"), spec.as_str()),
            };
            let binary = std::path::absolute(path)?;
            if !binary.is_file() {
                bail!("spiced binary {} does not exist", binary.display());
            }
            Ok(runner::Target { name, binary })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_deals_longest_first_to_the_least_loaded_share() {
        let scripts: Vec<script::Script> = [5, 4, 3, 2, 1]
            .into_iter()
            .enumerate()
            .map(|(i, secs)| {
                let mut s = script::Script::parse(&format!(
                    "name: s{i}\nspicepod: {{}}\nsteps: [ {{ start: {{}} }} ]"
                ))
                .expect("script parses");
                s.file_stem = format!("s{i}");
                s.duration = Some(std::time::Duration::from_secs(secs));
                s
            })
            .collect();
        // 5 → share 0; 4 → 1; 3 → 1 (4 < 5); 2 → 0 (5 < 7); 1 → 0 (7 = 7, lower share).
        assert_eq!(partition(&scripts, 2), vec![0, 1, 1, 0, 0]);
        assert_eq!(parse_partition("2/3"), Ok((2, 3)));
        assert!(
            parse_partition("0/3").is_err()
                && parse_partition("4/3").is_err()
                && parse_partition("x").is_err()
        );
    }
}
