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
mod json_assert;
mod keyquota;
mod ledger;
mod pki;
mod reap;
mod release;
mod results;
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

    /// Start every instance on this `spiced` binary instead of the release
    /// its script names with `from:`; an `upgrade` step then moves it to the
    /// binary under test, over the same data. Without either, `upgrade` is a
    /// restart.
    #[arg(long, value_name = "PATH")]
    from: Option<PathBuf>,

    /// Start the runs whose `from:` is `FROM` (such as `2.3.x`) on release
    /// `VERSION` (such as `2.3.2`) instead of the one it resolves to. Repeat
    /// it for several.
    #[arg(long = "from-release", value_name = "FROM=VERSION", value_parser = parse_from_release)]
    from_release: Vec<(String, String)>,

    /// Run every instance as a cluster: a scheduler with one executor,
    /// over node mTLS with a CA and certificates generated for the run.
    #[arg(long)]
    cluster: bool,

    /// The image a script's `cluster: { containers: true }` runs each
    /// executor in, with the `spiced` under test mounted in. The default has
    /// only the base libraries a Linux `spiced` built with the default
    /// features needs; a build that links more, such as unixODBC with the
    /// `odbc` feature, needs an image that has them. A local image is used
    /// as is; another is pulled.
    #[arg(
        long = "executor-image",
        value_name = "IMAGE",
        env = "SPICESCRIPT_EXECUTOR_IMAGE",
        default_value = runner::DEFAULT_EXECUTOR_IMAGE
    )]
    executor_image: String,

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

    /// Run each selected script this many times, with independent sources and instances.
    #[arg(long, default_value_t = 1, value_name = "N", value_parser = clap::value_parser!(u32).range(1..))]
    runs_per_script: u32,

    /// Record each script's run time in its `duration` line, the estimate
    /// `--partition` and `--jobs` order scripts by; scripts without one may
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

    /// Select scripts with any of these tags. Repeat for several tags.
    #[arg(long, value_enum, value_name = "TAG")]
    only_tag: Vec<script::Tag>,

    /// Exclude scripts with any of these tags, even when --only-tag matches.
    #[arg(long, value_enum, value_name = "TAG")]
    exclude_tag: Vec<script::Tag>,

    /// Fail the run when a selected script is not run because a variable its
    /// `requires-env` names is unset. Without it such a script is listed as
    /// not run and the run's result does not count it: for CI jobs that are
    /// meant to hold every credential, so a lost one fails rather than
    /// quietly testing less.
    #[arg(long)]
    require_all_env: bool,

    /// Cap every check's wait (`eventually`, a refresh) at this long, such
    /// as `20s`: for scripts expected to fail, which otherwise wait out every
    /// limit in full. Readiness after start, restart and upgrade keeps its
    /// own limit (`ready-timeout`, default 3 min).
    #[arg(long = "max-wait", value_name = "DURATION", value_parser = humantime::parse_duration)]
    max_wait: Option<std::time::Duration>,

    /// Run only the matrix combinations with this value, as `KEY=VALUE` (a
    /// key under the script's `matrix:`, such as `src=clickhouse`). Each
    /// combination is a separate run with its own sources; this picks which
    /// runs happen. Repeat a key for several values. Scripts without the key
    /// or any of its values are skipped.
    #[arg(long = "matrix", value_name = "KEY=VALUE", value_parser = parse_matrix_value)]
    matrix: Vec<(String, String)>,

    /// Run only this variant (a name under the script's `variants:`, such as
    /// `cayenne`). Variants are the instances within each run, compared with
    /// each other; this picks which of them start. Repeat it for several.
    /// Scripts with none of them are skipped.
    #[arg(long = "variant", value_name = "NAME", value_parser = parse_variant_name)]
    variants: Vec<String>,

    /// Write every script's outcome, passes included, to this JSON file,
    /// rewritten as each script finishes.
    #[arg(long, value_name = "PATH")]
    results: Option<PathBuf>,

    /// Leave the logs of each script with a failure, expected or not, in a
    /// directory of its own under this one: the script and what the run
    /// printed for it, each source container's output, and each instance's
    /// `spiced.log` and spicepod, a directory per matrix combination.
    #[arg(long = "log-capture", value_name = "DIR")]
    log_capture: Option<PathBuf>,

    /// The `DuckDB` release `duckdb` and `ducklake` sources use when their
    /// spec names no `version`: their `duckdb` CLI writes and checks the
    /// data, and `spiced` loads its library. Defaults to the newest release
    /// `test/duckdb_versions.json` lists. The expected-failures ledger is
    /// recorded at that default, so `--record-failures` refuses another.
    #[arg(
        long = "duckdb-version",
        value_name = "VERSION",
        env = "SPICESCRIPT_DUCKDB_VERSION"
    )]
    duckdb_version: Option<String>,

    /// The widest a value is drawn in a failure report's table, in
    /// characters; a longer one keeps its start and end. 0 draws every value
    /// whole.
    #[arg(long = "cell-width", value_name = "CHARS", default_value_t = compare::DEFAULT_CELL_WIDTH)]
    cell_width: usize,
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
    validate_repetitions(&cli)?;
    // Retried comparisons would print every intermediate mismatch; the runner
    // reports the last one with both answers.
    result_compare::set_print_diagnostics(false);
    compare::set_cell_width(cli.cell_width);
    let targets = targets(&cli.spiced)?;
    let mut paths = script_paths(&cli.scripts)?;
    // Parse everything first, so a typo in the last script fails before the
    // first one spends minutes running.
    let mut scripts = paths
        .iter()
        .map(|p| script::Script::load(p, cli.record_durations || cli.check))
        .collect::<Result<Vec<_>>>()?;
    let mut selection = script::Selection::default();
    for (key, value) in &cli.matrix {
        selection
            .matrix
            .entry(key.clone())
            .or_default()
            .push(value.clone());
    }
    selection.variants.clone_from(&cli.variants);
    if !selection.is_empty() {
        anyhow::ensure!(
            !cli.record_durations,
            "--record-durations records whole scripts' run times; run it without {}",
            selection.describe()
        );
        let before = scripts.len();
        (paths, scripts) = paths
            .into_iter()
            .zip(scripts)
            .filter_map(|(path, mut script)| script.select(&selection).then_some((path, script)))
            .unzip();
        anyhow::ensure!(
            !scripts.is_empty(),
            "No script has {}",
            selection.describe()
        );
        if scripts.len() < before {
            println!(
                "{} of {before} scripts have {}; running those",
                scripts.len(),
                selection.describe()
            );
        }
    }
    let selection = select_scripts(&scripts, &cli.only_tag, &cli.exclude_tag, cli.partition);
    if !cli.only_tag.is_empty() || !cli.exclude_tag.is_empty() {
        let selected = selection.as_ref().map_or(0, |(_, count)| *count);
        eprintln!(
            "Tag filter selected {selected} of {} scripts",
            scripts.len()
        );
    }
    let (indices, _) = selection?;
    (paths, scripts) = paths
        .into_iter()
        .zip(scripts)
        .enumerate()
        .filter_map(|(i, pair)| indices.contains(&i).then_some(pair))
        .unzip();
    if cli.check {
        return check(&paths, &scripts);
    }
    if cli.cluster {
        // Before any source starts: a script that cannot run distributed
        // should not fail on a source it would never use.
        (paths, scripts) = paths
            .into_iter()
            .zip(scripts)
            .filter_map(|(path, script)| match &script.defaults.single_node {
                Some(reason) => {
                    println!("skip {} under --cluster: {reason}", script.name);
                    None
                }
                None => Some((path, script)),
            })
            .unzip();
    }
    let not_run: Vec<results::NotRun>;
    (paths, scripts, not_run) = without_missing_env(paths, scripts);
    for script in &not_run {
        println!("not run: {script}");
    }

    // Each base release the selected runs start on, resolved and downloaded
    // once; `--from` replaces them all.
    let mut releases = std::collections::BTreeMap::new();
    if cli.from.is_none() {
        let mut specs = std::collections::BTreeSet::new();
        for script in &scripts {
            for combo in script::matrix_combinations(&script.matrix) {
                specs.extend(script.render_from(&combo)?);
            }
        }
        let pins: std::collections::BTreeMap<_, _> = cli.from_release.into_iter().collect();
        if let Some(unused) = pins.keys().find(|spec| !specs.contains(*spec)) {
            anyhow::bail!("--from-release {unused}=…: no selected run starts from `{unused}`");
        }
        let mut list = None;
        let mut fetched = std::collections::BTreeMap::<String, std::path::PathBuf>::new();
        for spec in specs {
            let (version, how) = if let Some(version) = pins.get(&spec) {
                (version.clone(), "--from-release")
            } else if release::is_exact(&spec)? {
                (spec.clone(), "exact")
            } else {
                if list.is_none() {
                    list = Some(release::Releases::load()?);
                }
                let list = list.as_ref().context("release list was just loaded")?;
                (list.resolve(&spec)?, "newest recorded")
            };
            let binary = if let Some(binary) = fetched.get(&version) {
                binary.clone()
            } else {
                let binary = release::fetch(&version).await?;
                fetched.insert(version.clone(), binary.clone());
                binary
            };
            println!("from {spec}: v{version} ({how}), {}", binary.display());
            releases.insert(
                spec.clone(),
                release::BaseRelease {
                    spec,
                    version,
                    binary,
                },
            );
        }
    } else if !cli.from_release.is_empty() {
        anyhow::bail!(
            "--from starts every instance on one binary, so --from-release has nothing to choose"
        );
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
    if let Some(version) = cli.duckdb_version.clone() {
        let default = source::duckdb_cli::version(None)?;
        anyhow::ensure!(
            !cli.record_failures || version.trim_start_matches('v') == default,
            "--record-failures records the ledger at DuckDB {default}, the newest release test/duckdb_versions.json lists; drop --duckdb-version {version} to record"
        );
        source::duckdb_cli::set_default_version(version);
    }
    if let Some(from) = &cli.from {
        anyhow::ensure!(
            from.exists(),
            "spiced binary {} does not exist",
            from.display()
        );
    }
    // A fresh CA and node certificates for this run's cluster instances.
    let pki = pki::generate(&work_dir.join(format!("pki-{}", std::process::id())))?;
    let options = runner::Options {
        targets,
        repetition: None,
        from_override: cli.from,
        releases,
        cluster: cli.cluster,
        work_dir,
        keep: cli.keep,
        // `spice` steps run in the instance's directory, so a relative path
        // is resolved here, against this one; a bare name stays a PATH lookup.
        spice_cli: if cli.spice.components().count() > 1 && cli.spice.is_relative() {
            std::path::absolute(&cli.spice)?
        } else {
            cli.spice
        },
        pki,
        log_capture: cli
            .log_capture
            .as_deref()
            .map(std::path::absolute)
            .transpose()?,
        executor_image: cli.executor_image,
    };
    let mut ledger = ledger::Ledger::load(&scripts)?;
    let mut passed = 0;
    let mut expected = 0;
    let mut failures = Vec::new();
    let mut notes: Vec<String> = Vec::new();
    let total = scripts.len() * cli.runs_per_script as usize;
    let run_started = std::time::Instant::now();
    // Each script, its time and failure count, for the summary.
    let mut timings: Vec<(&script::Script, std::time::Duration, usize, String)> = Vec::new();
    let mut done = 0;
    // Longest first, by recorded duration (a script without one counts as
    // long): each asks for slots in this order, and slots are granted in the
    // order asked.
    let mut order: Vec<(&PathBuf, &script::Script)> = paths.iter().zip(&scripts).collect();
    order.sort_by_key(|(_, s)| std::cmp::Reverse(s.duration.unwrap_or(std::time::Duration::MAX)));
    let slots = runner::Slots::new(cli.jobs);
    let mut running = futures::stream::FuturesUnordered::new();
    for (path, script) in order {
        for repetition in 1..=cli.runs_per_script {
            let slots = &slots;
            let mut options = options.clone();
            let repeated = cli.runs_per_script > 1;
            if repeated {
                options.repetition = Some(repetition);
                options.log_capture = options
                    .log_capture
                    .map(|root| root.join(format!("repetition-{repetition}")));
            }
            running.push(async move {
                let started = std::time::Instant::now();
                let (report, out) = runner::run_script_buffered(script, &options, slots).await;
                (
                    path,
                    script,
                    repetition,
                    options,
                    report,
                    out,
                    started.elapsed(),
                )
            });
        }
    }
    let mut results = results::Results {
        partition: cli.partition.map(|(i, n)| format!("{i}/{n}")),
        complete: false,
        seconds: 0.0,
        scripts: Vec::new(),
        summaries: Vec::new(),
        not_run: not_run.clone(),
    };
    while let Some((path, script, repetition, run_options, report, out, waited)) =
        futures::StreamExt::next(&mut running).await
    {
        let label = if cli.runs_per_script > 1 {
            format!("{} #{repetition}/{}", script.name, cli.runs_per_script)
        } else {
            script.name.clone()
        };
        let out = annotate_output(&out, &label, cli.runs_per_script > 1);
        print!("{out}");
        let report = report?;
        // The time it held slots, not the time it waited for them.
        let took = if report.ran.is_zero() {
            waited
        } else {
            report.ran
        };
        done += 1;
        let (problems, verdict): (Vec<String>, _) = if cli.record_failures {
            ledger.record(script, &report.failures);
            (
                report.failures.iter().map(ToString::to_string).collect(),
                None,
            )
        } else {
            let verdict = ledger.judge(script, &report.failures);
            expected += verdict.expected;
            notes.extend(
                verdict
                    .notes
                    .iter()
                    .map(|note| annotate_output(note, &label, cli.runs_per_script > 1)),
            );
            (verdict.problems(), Some(verdict))
        };
        // A log that cannot be saved is a warning, as in the runner: the
        // script's own outcome still counts, and the other scripts still run.
        if let Some(root) = &run_options.log_capture
            && !report.failures.is_empty()
            && let Err(e) = capture_script(
                &runner::capture_dir(root, script),
                path,
                &out,
                &report.failures,
                (cli.runs_per_script > 1).then_some(label.as_str()),
            )
        {
            println!("warning: {e:#}");
        }
        if cli.results.is_some() || cli.runs_per_script > 1 {
            results.scripts.push(results::ScriptResult::new(
                script,
                path,
                &report,
                took,
                verdict.as_ref(),
                &problems,
            ));
            if cli.runs_per_script > 1 {
                if let Some(result) = results.scripts.last_mut() {
                    result.repetition = Some(repetition);
                }
                results.update_summaries();
            }
            results.seconds = run_started.elapsed().as_secs_f64();
            if let Some(file) = &cli.results {
                results.write(file)?;
            }
        }
        let shown_expected = if cli.record_failures {
            0
        } else {
            report.failures.len().saturating_sub(problems.len())
        };
        println!(
            "[{done}/{total}] done    {} in {:.1}s: {} passed, {} failed{}",
            label,
            took.as_secs_f64(),
            report.passed,
            problems.len(),
            expected_failures_note(shown_expected),
        );
        timings.push((script, took, problems.len(), label.clone()));
        passed += report.passed;
        failures.extend(
            problems
                .into_iter()
                .map(|p| annotate_output(&p, &label, cli.runs_per_script > 1)),
        );
    }
    if cli.record_failures {
        ledger.save()?;
        println!(
            "recorded the expected failures of {} script(s)",
            scripts.len()
        );
    }
    if cli.record_durations {
        for (script, took, _, _) in &timings {
            script::Script::record_duration(&script.path, *took)?;
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
        for (_, took, failed, label) in timings.iter().take(10) {
            let note = if *failed > 0 {
                format!("  ({failed} failed)")
            } else {
                String::new()
            };
            println!("  {:>8.1}s  {}{note}", took.as_secs_f64(), label);
        }
        println!();
    }
    if cli.runs_per_script > 1 {
        for summary in &results.summaries {
            println!("{summary}");
        }
    }
    if let Some(file) = &cli.results {
        results.complete = true;
        results.seconds = run_started.elapsed().as_secs_f64();
        results.write(file)?;
        println!("results: {}", file.display());
    }
    // Listed last, beside the totals: a script that did not run is not a
    // pass, whatever the totals say.
    for script in &not_run {
        if cli.require_all_env {
            println!("FAILED not run: {script} (--require-all-env)");
        } else {
            println!("not run: {script}");
        }
    }
    println!(
        "{} scripts: {passed} checks passed, {} failed{}{}, in {:.1}s",
        scripts.len(),
        failures.len(),
        expected_failures_note(expected),
        not_run_note(not_run.len()),
        run_started.elapsed().as_secs_f64()
    );
    let missing_env_fails = cli.require_all_env && !not_run.is_empty();
    Ok((failures.is_empty() || cli.record_failures) && !missing_env_fails)
}

/// Splits off the scripts whose `requires-env` names an unset variable.
fn without_missing_env(
    paths: Vec<PathBuf>,
    scripts: Vec<script::Script>,
) -> (Vec<PathBuf>, Vec<script::Script>, Vec<results::NotRun>) {
    let mut not_run = Vec::new();
    let (paths, scripts) = paths
        .into_iter()
        .zip(scripts)
        .filter_map(|(path, script)| {
            let missing = script.missing_env();
            if missing.is_empty() {
                return Some((path, script));
            }
            not_run.push(results::NotRun {
                name: script.name,
                file: path,
                missing_env: missing,
            });
            None
        })
        .unzip();
    (paths, scripts, not_run)
}

fn not_run_note(count: usize) -> String {
    match count {
        0 => String::new(),
        1 => ", 1 script not run".to_string(),
        n => format!(", {n} scripts not run"),
    }
}

fn validate_repetitions(cli: &Cli) -> Result<()> {
    anyhow::ensure!(
        cli.runs_per_script == 1 || !(cli.record_durations || cli.record_failures),
        "--runs-per-script >1 cannot be combined with --record-durations or --record-failures: repetitions would overwrite the recordings"
    );
    Ok(())
}

fn annotate_output(text: &str, label: &str, repeated: bool) -> String {
    if repeated {
        use std::fmt::Write as _;
        text.split_inclusive('\n')
            .fold(String::new(), |mut output, line| {
                let _ = write!(output, "[{label}] {line}");
                output
            })
    } else {
        text.to_string()
    }
}

/// `--log-capture`'s files for a whole script, beside its combinations'
/// logs: the script as run, and what the run printed for it, with each
/// failure in full.
fn capture_script(
    dir: &std::path::Path,
    path: &std::path::Path,
    out: &str,
    failures: &[runner::Failure],
    repetition_label: Option<&str>,
) -> Result<()> {
    use std::fmt::Write as _;
    std::fs::create_dir_all(dir).with_context(|| format!("Failed to create {}", dir.display()))?;
    std::fs::copy(path, dir.join("script.yaml"))
        .with_context(|| format!("Failed to copy {} into {}", path.display(), dir.display()))?;
    // The progress lines name each failure; what went wrong is printed with
    // the run's summary, so it is added here as the summary prints it.
    let mut text = out.to_string();
    for failure in failures {
        let failure = annotate_output(
            &failure.to_string(),
            repetition_label.unwrap_or_default(),
            repetition_label.is_some(),
        );
        let _ = write!(text, "\nFAILED {failure}\n");
    }
    std::fs::write(dir.join("output.txt"), text)
        .with_context(|| format!("Failed to write {}", dir.join("output.txt").display()))
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

fn parse_matrix_value(text: &str) -> Result<(String, String), String> {
    match text.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key.to_string(), value.to_string())),
        _ => Err(format!(
            "`{text}` is not KEY=VALUE, a key under the script's `matrix:`; to run one of its `variants:`, use --variant {text}"
        )),
    }
}

fn parse_from_release(text: &str) -> Result<(String, String), String> {
    let (spec, version) = text
        .split_once('=')
        .ok_or_else(|| format!("`{text}` is not FROM=VERSION, such as 2.3.x=2.3.2"))?;
    release::validate(spec).map_err(|e| e.to_string())?;
    match release::is_exact(version) {
        Ok(true) => Ok((spec.to_string(), version.to_string())),
        Ok(false) => Err(format!(
            "`{version}` names a minor line; give one release, such as 2.3.2"
        )),
        Err(e) => Err(e.to_string()),
    }
}

fn parse_variant_name(text: &str) -> Result<String, String> {
    if text.contains('=') {
        return Err(format!(
            "`{text}` is a matrix value, not a variant name; to run the combinations with it, use --matrix {text}"
        ));
    }
    Ok(text.to_string())
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

/// Select by tags before balancing partitions. Return original indices and the
/// total selected before partitioning, including when a share is empty.
fn select_scripts(
    scripts: &[script::Script],
    only: &[script::Tag],
    exclude: &[script::Tag],
    partition_share: Option<(usize, usize)>,
) -> Result<(Vec<usize>, usize)> {
    let selected: Vec<usize> = scripts
        .iter()
        .enumerate()
        .filter(|(_, script)| {
            (only.is_empty() || only.iter().any(|tag| script.tags.contains(tag)))
                && !exclude.iter().any(|tag| script.tags.contains(tag))
        })
        .map(|(i, _)| i)
        .collect();
    let count = selected.len();
    anyhow::ensure!(
        (only.is_empty() && exclude.is_empty()) || count > 0,
        "--only-tag/--exclude-tag selected none of the {} scripts given; check the tags in the scripts and on the command line",
        scripts.len()
    );
    let Some((index, shares)) = partition_share else {
        return Ok((selected, count));
    };
    let selected_scripts: Vec<_> = selected.iter().map(|&i| &scripts[i]).collect();
    let assignments = partition(&selected_scripts, shares);
    Ok((
        selected
            .into_iter()
            .zip(assignments)
            .filter_map(|(i, share)| (share == index - 1).then_some(i))
            .collect(),
        count,
    ))
}

/// Which of `count` shares each script falls in: longest first (a script
/// without a recorded duration counts as an hour), each to the share with
/// the least recorded time so far, ties to the lower share. Depends only on
/// the scripts and their recorded durations, so every job computes the same
/// split.
fn partition(scripts: &[impl std::borrow::Borrow<script::Script>], count: usize) -> Vec<usize> {
    let cost = |s: &script::Script| s.duration.unwrap_or(std::time::Duration::from_secs(3600));
    let mut order: Vec<usize> = (0..scripts.len()).collect();
    order.sort_by(|&a, &b| {
        cost(scripts[b].borrow())
            .cmp(&cost(scripts[a].borrow()))
            .then_with(|| scripts[a].borrow().dir.cmp(&scripts[b].borrow().dir))
            .then_with(|| {
                scripts[a]
                    .borrow()
                    .file_stem
                    .cmp(&scripts[b].borrow().file_stem)
            })
    });
    let mut load = vec![std::time::Duration::ZERO; count];
    let mut share = vec![0; scripts.len()];
    for i in order {
        let least = (0..count).min_by_key(|&k| (load[k], k)).unwrap_or(0);
        load[least] += cost(scripts[i].borrow());
        share[i] = least;
    }
    share
}

/// The scripts directly in a directory, in name order: every YAML file but
/// the shared variants.
fn scripts_in(dir: &std::path::Path) -> Result<Vec<PathBuf>> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "yaml" || x == "yml"))
        .filter(|p| p.file_name().is_none_or(|n| n != script::VARIANTS_FILE))
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

    fn tagged_scripts() -> Vec<script::Script> {
        ["[]", "[long]", "[]"]
            .into_iter()
            .map(|tags| {
                script::Script::parse(&format!(
                    "name: s\ntags: {tags}\nspicepod: {{}}\nsteps: [{{start: {{}}}}]"
                ))
                .expect("tagged script parses")
            })
            .collect()
    }

    #[test]
    fn only_tag_selects_the_tagged_scripts() {
        assert_eq!(
            select_scripts(&tagged_scripts(), &[script::Tag::Long], &[], None).expect("selection"),
            (vec![1], 1)
        );
    }

    #[test]
    fn exclude_tag_drops_the_tagged_scripts() {
        assert_eq!(
            select_scripts(&tagged_scripts(), &[], &[script::Tag::Long], None).expect("selection"),
            (vec![0, 2], 2)
        );
    }

    #[test]
    fn no_tag_flags_select_all() {
        assert_eq!(
            select_scripts(&tagged_scripts(), &[], &[], None).expect("selection"),
            (vec![0, 1, 2], 3)
        );
    }

    #[test]
    fn tags_filter_before_partition() {
        // untagged 100s, long 95s, untagged 10s, untagged 5s. Over the three
        // untagged scripts, share 2/2 gets 10s and 5s (both behind 100s).
        // Dealing all four first would put 95s in share 2, the 10s with it and
        // the 5s in share 1, so filtering afterwards would leave share 2 with
        // only 10s.
        let scripts: Vec<script::Script> = [("[]", 100), ("[long]", 95), ("[]", 10), ("[]", 5)]
            .into_iter()
            .enumerate()
            .map(|(i, (tags, secs))| {
                let mut s = script::Script::parse(&format!(
                    "name: s{i}\ntags: {tags}\nspicepod: {{}}\nsteps: [{{start: {{}}}}]"
                ))
                .expect("tagged script parses");
                s.file_stem = format!("s{i}");
                s.duration = Some(std::time::Duration::from_secs(secs));
                s
            })
            .collect();
        let long = [script::Tag::Long];
        assert_eq!(
            select_scripts(&scripts, &[], &long, Some((2, 2))).expect("selection"),
            (vec![2, 3], 3)
        );
        assert_eq!(
            select_scripts(&scripts, &[], &long, Some((1, 2))).expect("selection"),
            (vec![0], 3)
        );
        assert_eq!(
            select_scripts(&scripts, &[], &long, Some((4, 4))).expect("empty share is allowed"),
            (vec![], 3)
        );
    }

    #[test]
    fn zero_tag_selection_is_an_error() {
        assert_eq!(
            select_scripts(
                &tagged_scripts(),
                &[script::Tag::Long],
                &[script::Tag::Long],
                None
            )
            .expect_err("empty selection")
            .to_string(),
            "--only-tag/--exclude-tag selected none of the 3 scripts given; check the tags in the scripts and on the command line"
        );
    }

    #[test]
    fn repetition_output_preserves_single_run_bytes() {
        let text = "== script\n   FAIL instance step 1\n\n";
        assert_eq!(annotate_output(text, "script", false), text);
        assert_eq!(
            annotate_output("== script\nlast line", "script #2/3", true),
            "[script #2/3] == script\n[script #2/3] last line"
        );
    }

    #[test]
    fn repetition_flags_are_validated() {
        assert!(Cli::try_parse_from(["spicescript", "--runs-per-script", "0", "s.yaml"]).is_err());
        for flag in ["--record-durations", "--record-failures"] {
            let cli =
                Cli::try_parse_from(["spicescript", "--runs-per-script", "3", flag, "s.yaml"])
                    .expect("valid CLI syntax");
            assert_eq!(
                validate_repetitions(&cli)
                    .expect_err("recording repetitions is rejected")
                    .to_string(),
                "--runs-per-script >1 cannot be combined with --record-durations or --record-failures: repetitions would overwrite the recordings"
            );
            let cli =
                Cli::try_parse_from(["spicescript", flag, "s.yaml"]).expect("default repetition");
            validate_repetitions(&cli).expect("one run may record");
        }
    }

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

    #[test]
    fn a_selection_meant_for_the_other_flag_names_it() {
        assert_eq!(
            parse_matrix_value("src=clickhouse"),
            Ok(("src".to_string(), "clickhouse".to_string()))
        );
        assert_eq!(
            parse_matrix_value("cayenne"),
            Err("`cayenne` is not KEY=VALUE, a key under the script's `matrix:`; to run one of its `variants:`, use --variant cayenne".to_string())
        );
        assert_eq!(parse_variant_name("cayenne"), Ok("cayenne".to_string()));
        assert_eq!(
            parse_variant_name("src=clickhouse"),
            Err("`src=clickhouse` is a matrix value, not a variant name; to run the combinations with it, use --matrix src=clickhouse".to_string())
        );
    }
}
