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

//! The expected-failure ledger: `recordings/expected-failures.json` beside
//! the scripts, listing each check known to fail, how it fails, and the issue
//! group it belongs to.
//!
//! Every script runs every check; the ledger decides what a failure means. A
//! failure the ledger lists with the same signature is expected. A failure it
//! lists with another signature has changed, one it does not list is new, a
//! listed check that no longer fails has been fixed (or no longer ran), and
//! an entry with no issue group is unassigned: each of these fails the run.
//! A listed check of a run that failed outside its steps (a source or
//! instance that did not start) is reported as possibly not run, not as fixed.
//! Only `--record-failures` writes the ledger, so adding an expected failure
//! is always a reviewed change to the file.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::LazyLock,
};

use anyhow::{Context, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::{runner::Failure, script::Script};

pub const FILE: &str = "expected-failures.json";
const UNASSIGNED: &str = "unassigned";

/// One expected failure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct Entry {
    pub run: String,
    pub who: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<u64>,
    pub kind: String,
    pub signature: String,
    /// Other signatures the same bug has shown, for one whose symptom
    /// depends on timing; any other signature is still a change.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub also: Vec<String>,
    /// The symptom depends on timing: recording adds a new signature to
    /// `also` instead of replacing the entry's.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub varies: bool,
    /// A regex the signature must match, in place of `signature` and `also`:
    /// for a failure whose details vary too much to list (which reader saw a
    /// torn read, which operator ran out of memory) around a stable core.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    /// The issue this failure belongs to.
    pub group: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    /// Timing-dependent: the check may also pass, but must not fail any other
    /// way.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub may_pass: bool,
}

impl Entry {
    fn id(&self) -> (&str, &str, &str, Option<u64>) {
        (&self.run, &self.who, &self.key, self.query)
    }

    /// Whether a failure's first line, as `normalized` gives it, is this
    /// entry's. A `pattern` sees the whole line: the part it names can lie
    /// past the cut a signature makes, as an error in the second of several
    /// datasets a readiness failure lists does.
    fn matches(&self, normalized: &str) -> bool {
        if let Some(pattern) = &self.pattern {
            return Regex::new(pattern).is_ok_and(|re| re.is_match(normalized));
        }
        let signature = truncated(normalized);
        self.signature == signature || self.also.contains(&signature)
    }
}

fn failure_id(f: &Failure) -> (&str, &str, &str, Option<u64>) {
    (&f.run, &f.who, &f.key, f.query)
}

/// Whether two failures are of the same check, the one a ledger entry names.
pub fn same_check(a: &Failure, b: &Failure) -> bool {
    failure_id(a) == failure_id(b)
}

/// What a script's failures mean against its ledger entries.
#[derive(Default)]
pub struct Verdict {
    pub expected: usize,
    /// Expected failures that failed another way: (now, recorded).
    pub changed: Vec<(Failure, Entry)>,
    pub new: Vec<Failure>,
    /// Entries whose check did not fail.
    pub gone: Vec<Entry>,
    /// Entries whose check did not fail in a run, or an instance of one, that
    /// failed outside its steps: they may never have run, which says nothing
    /// about whether their bugs are fixed.
    pub not_run: Vec<Entry>,
    pub unassigned: Vec<Entry>,
    /// A script in `known-failing/` that failed nothing and expects nothing.
    pub passes_in_known_failing: Option<String>,
    /// Every expected failure of a script that may also pass did pass: a fix,
    /// or a lucky run. Reported, not failed on, since a racy script does this
    /// by design.
    pub notes: Vec<String>,
}

impl Verdict {
    /// Each problem as a report line, like a failed check.
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (now, was) in &self.changed {
            out.push(format!(
                "{now}\nThis expected failure ({}) changed:\n  recorded: {} {}\n  now:      {} {}\nRe-record with --record-failures once the change is understood.",
                was.group,
                was.kind,
                was.signature,
                kind(&now.message),
                signature(&now.message)
            ));
        }
        for failure in &self.new {
            out.push(failure.to_string());
        }
        for entry in &self.gone {
            out.push(format!(
                "{} {} {}{}\nThis expected failure ({}) no longer fails: if it is fixed, move it out of the ledger with --record-failures (and its script out of known-failing/).",
                entry.run,
                entry.who,
                entry.key,
                entry.query.map(|q| format!(" query {q}")).unwrap_or_default(),
                entry.group
            ));
        }
        let mut not_run: BTreeMap<&str, usize> = BTreeMap::new();
        for entry in &self.not_run {
            *not_run.entry(&entry.run).or_default() += 1;
        }
        for (run, n) in not_run {
            let failures = if n == 1 {
                "expected failure"
            } else {
                "expected failures"
            };
            out.push(format!(
                "{run}\n{n} {failures} did not show, but the run failed outside its steps (reported above), so their checks may not have run. Fix that failure; do not re-record the ledger from this run."
            ));
        }
        if let Some(script) = &self.passes_in_known_failing {
            out.push(format!(
                "{script}\nThis script is in known-failing/ but nothing in it failed: if its bug is fixed, move it up to the passing scripts; if it fails only sometimes, record a failing run with --record-failures."
            ));
        }
        for entry in &self.unassigned {
            out.push(format!(
                "{} {} {}\nThis expected failure has no issue group: set `group` in {FILE}.",
                entry.run, entry.who, entry.key
            ));
        }
        out
    }
}

/// The ledger files of the directories scripts were loaded from.
#[derive(Default)]
pub struct Ledger {
    files: BTreeMap<PathBuf, BTreeMap<String, Vec<Entry>>>,
}

impl Ledger {
    pub fn load(scripts: &[Script]) -> Result<Self> {
        let mut files = BTreeMap::new();
        for script in scripts {
            if files.contains_key(&script.dir) {
                continue;
            }
            let path = path(&script.dir);
            let entries: BTreeMap<String, Vec<Entry>> = match std::fs::read_to_string(&path) {
                Ok(text) => serde_json::from_str(&text)
                    .with_context(|| format!("Failed to parse {}", path.display()))?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
                Err(e) => {
                    return Err(e).with_context(|| format!("Failed to read {}", path.display()));
                }
            };
            for entry in entries.values().flatten() {
                if let Some(pattern) = &entry.pattern {
                    Regex::new(pattern).with_context(|| {
                        format!(
                            "Invalid `pattern` for {} {} in {}",
                            entry.run,
                            entry.key,
                            path.display()
                        )
                    })?;
                }
            }
            files.insert(script.dir.clone(), entries);
        }
        Ok(Self { files })
    }

    fn entries(&self, script: &Script) -> &[Entry] {
        self.files
            .get(&script.dir)
            .and_then(|f| f.get(&script.file_stem))
            .map_or(&[], Vec::as_slice)
    }

    /// Weighs a script's failures against its entries.
    pub fn judge(&self, script: &Script, failures: &[Failure]) -> Verdict {
        let entries = self.entries(script);
        let mut verdict = Verdict::default();
        let mut matched = vec![false; entries.len()];
        for failure in failures {
            match entries.iter().position(|e| e.id() == failure_id(failure)) {
                Some(i) => {
                    matched[i] = true;
                    if entries[i].matches(&normalized(&failure.message)) {
                        verdict.expected += 1;
                    } else {
                        verdict.changed.push((failure.clone(), entries[i].clone()));
                    }
                }
                None => verdict.new.push(failure.clone()),
            }
        }
        let none_failed = failures.is_empty();
        // Entries for runs and variants `--matrix` or `--variant` left out
        // were not checked, so their passing says nothing.
        let ran = |e: &Entry| !script.left_out.covers(&e.run, &e.who);
        let checked = entries.iter().filter(|e| ran(e)).count();
        if none_failed && checked > 0 && entries.iter().filter(|e| ran(e)).all(|e| e.may_pass) {
            verdict.notes.push(format!(
                "{}: none of its {checked} expected failures failed this run; if this repeats, its bug may be fixed",
                script.name,
            ));
        }
        // A run, or one instance of it, that failed outside its steps: starting
        // a source or an instance, or waiting for slots.
        let stopped: Vec<(&str, &str)> = failures
            .iter()
            .filter(|f| f.is_outside_steps())
            .map(|f| (f.run.as_str(), f.who.as_str()))
            .collect();
        for (entry, matched) in entries.iter().zip(matched) {
            if !matched && !entry.may_pass && ran(entry) {
                let stopped = stopped
                    .iter()
                    .any(|&(run, who)| run == entry.run && (who.is_empty() || who == entry.who));
                if stopped {
                    verdict.not_run.push(entry.clone());
                } else {
                    verdict.gone.push(entry.clone());
                }
            }
            if entry.group.is_empty() || entry.group == UNASSIGNED {
                verdict.unassigned.push(entry.clone());
            }
        }
        if none_failed && entries.is_empty() && known_failing_group(&script.dir).is_some() {
            verdict.passes_in_known_failing = Some(script.name.clone());
        }
        verdict
    }

    /// Replaces a script's entries with its current failures, keeping the
    /// group and `may-pass` of an entry that is still there, a `may-pass`
    /// entry that passed this time, and the entries of runs and variants
    /// `--matrix` or `--variant` left out.
    pub fn record(&mut self, script: &Script, failures: &[Failure]) {
        let old = self.entries(script).to_vec();
        let group_of_dir = known_failing_group(&script.dir);
        let mut entries: Vec<Entry> = failures
            .iter()
            .map(|f| {
                let previous = old.iter().find(|e| e.id() == failure_id(f));
                // A generated query's defect labels name its issue, the one an
                // `avoid:` of that name stands in for.
                let group = previous.map_or_else(
                    || {
                        (!f.labels.is_empty())
                            .then(|| f.labels.join("+"))
                            .or_else(|| group_of_dir.clone())
                            .unwrap_or_else(|| UNASSIGNED.to_string())
                    },
                    |e| e.group.clone(),
                );
                let now = signature(&f.message);
                let varies = previous.is_some_and(|e| e.varies);
                let (signature, also) = match previous {
                    Some(e) if e.varies => {
                        let mut also = e.also.clone();
                        if !e.matches(&normalized(&f.message)) {
                            also.push(now);
                        }
                        (e.signature.clone(), also)
                    }
                    Some(e) => (now, e.also.clone()),
                    None => (now, Vec::new()),
                };
                Entry {
                    run: f.run.clone(),
                    who: f.who.clone(),
                    key: f.key.clone(),
                    query: f.query,
                    kind: kind(&f.message),
                    signature,
                    also,
                    varies,
                    pattern: previous.and_then(|e| e.pattern.clone()),
                    group,
                    labels: f.labels.clone(),
                    may_pass: previous.is_some_and(|e| e.may_pass),
                }
            })
            .collect();
        entries.extend(old.into_iter().filter(|e| {
            (e.may_pass || script.left_out.covers(&e.run, &e.who))
                && !failures.iter().any(|f| failure_id(f) == e.id())
        }));
        let file = self.files.entry(script.dir.clone()).or_default();
        if entries.is_empty() {
            file.remove(&script.file_stem);
        } else {
            file.insert(script.file_stem.clone(), entries);
        }
    }

    /// Writes every ledger that has entries, and removes those left empty.
    pub fn save(&self) -> Result<()> {
        for (dir, entries) in &self.files {
            let path = path(dir);
            if entries.is_empty() {
                if path.exists() {
                    std::fs::remove_file(&path)
                        .with_context(|| format!("Failed to remove {}", path.display()))?;
                }
                continue;
            }
            std::fs::create_dir_all(dir.join("recordings"))?;
            std::fs::write(&path, serde_json::to_string_pretty(entries)? + "\n")
                .with_context(|| format!("Failed to write {}", path.display()))?;
        }
        Ok(())
    }
}

/// The `avoid:` names no expected failure carries a label for: the defect no
/// longer fails, or the run that shows it (the same generator without the
/// `avoid:`) was never recorded. Reads every ledger in the suite, the
/// directory that holds `known-failing/`, so it holds whichever scripts were
/// named.
pub fn stale_avoids(scripts: &[Script]) -> Result<Vec<(String, &'static str)>> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    for script in scripts {
        let root = script
            .dir
            .ancestors()
            .find(|d| d.join("known-failing").is_dir())
            .unwrap_or(&script.dir)
            .to_path_buf();
        if !dirs.contains(&root) {
            dirs.push(root.clone());
            if let Ok(groups) = std::fs::read_dir(root.join("known-failing")) {
                dirs.extend(
                    groups
                        .filter_map(|g| g.ok().map(|g| g.path()))
                        .filter(|g| g.is_dir()),
                );
            }
        }
    }
    let mut labels = std::collections::BTreeSet::new();
    for dir in &dirs {
        let path = path(dir);
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let file: BTreeMap<String, Vec<Entry>> = serde_json::from_str(&text)
            .with_context(|| format!("Failed to parse {}", path.display()))?;
        labels.extend(file.into_values().flatten().flat_map(|e| e.labels));
    }
    let mut stale = Vec::new();
    for script in scripts {
        for name in script.avoided() {
            if !labels.contains(name) {
                stale.push((script.name.clone(), name));
            }
        }
    }
    Ok(stale)
}

fn path(dir: &Path) -> PathBuf {
    dir.join("recordings").join(FILE)
}

/// `known-failing/<group>/`: the group a script there reproduces.
fn known_failing_group(dir: &Path) -> Option<String> {
    let parent = dir.parent()?;
    if parent.file_name()? != "known-failing" {
        return None;
    }
    Some(dir.file_name()?.to_string_lossy().into_owned())
}

/// The broad kind of a failure, for reading the ledger.
pub fn kind(message: &str) -> String {
    let first = message.lines().next().unwrap_or_default();
    let kind = if first.contains("disagree") {
        "disagreement"
    } else if first.contains("differs")
        || first.contains("Mismatch")
        || first.contains("are not the table")
    {
        "wrong-answer"
    } else if first.contains("no rows") || first.contains("empty answer") {
        "empty"
    } else if first.contains("rejected") || first.contains("error") || first.contains("Error") {
        "error"
    } else if first.contains("status") {
        "status"
    } else if first.contains("not ready") || first.contains("timed out") || first.contains("within")
    {
        "timeout"
    } else {
        "other"
    };
    kind.to_string()
}

/// A failure's first line with what differs from run to run replaced: ports,
/// paths, durations, attempt and iteration counts, timestamps, ids, file
/// part numbers, instance namespaces and container names. JSON in the message
/// (a dataset status list, a schema's metadata) is written with sorted keys,
/// since its key order varies from run to run. Unstructured detail after the
/// failure itself is dropped: a row dump (` --- `) and a memory report. Error
/// text and row counts stay, so a different failure has a different
/// signature.
pub fn signature(message: &str) -> String {
    truncated(&normalized(message))
}

/// A signature's length: enough to tell failures apart, short enough to
/// read in the ledger.
const SIGNATURE_CHARS: usize = 300;

fn truncated(normalized: &str) -> String {
    normalized.chars().take(SIGNATURE_CHARS).collect()
}

/// A failure's first line normalized as [`signature`] does, uncut.
fn normalized(message: &str) -> String {
    const DETAIL: &[&str] = &[" --- ", " with top memory consumers"];
    static RULES: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
        [
            (r"(127\.0\.0\.1|localhost|\[::1\]):\d+", "$1:<port>"),
            (r"(/Users|/private|/tmp|/var|/home|/opt)/\S+", "<path>"),
            (r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?", "<time>"),
            (
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                "<id>",
            ),
            (r"spicescript-[a-z0-9_-]+", "<container>"),
            (r"\bi\d+[_-][a-z0-9_-]+", "<namespace>"),
            (r"\b\d+ attempts", "<n> attempts"),
            (r"\btask \d+", "task <n>"),
            (r"PageType \d+", "PageType <n>"),
            (r"([A-Za-z])\[\d+\]", "$1[<n>]"),
            (r"\d+ of \d+ iterations", "<n> of <m> iterations"),
            (r"part-\d+", "part-<n>"),
            (r"\b\d+(\.\d+)?\s?(ns|µs|us|ms|s|m|h)\b", "<duration>"),
        ]
        .into_iter()
        .filter_map(|(re, to)| Regex::new(re).ok().map(|re| (re, to)))
        .collect()
    });
    // A background invariant reports the value it saw; which side of the
    // expected value it fell on is the failure, the exact value is timing.
    static SAW: LazyLock<Option<Regex>> =
        LazyLock::new(|| Regex::new(r#"Some\("(-?\d+)"\), expected Some\("(-?\d+)"\)"#).ok());
    // Long hex runs are ids (an encoded table name, a hash), but an all-digit
    // run is a value, such as a 38-digit decimal, and stays.
    static HEX: LazyLock<Option<Regex>> = LazyLock::new(|| Regex::new(r"[0-9A-Fa-f]{16,}").ok());
    let first = message.lines().next().unwrap_or_default();
    let cut = DETAIL
        .iter()
        .filter_map(|d| first.find(d))
        .min()
        .unwrap_or(first.len());
    let mut text = sorted_json(first[..cut].trim());
    if let Some(saw) = SAW.as_ref() {
        text = saw
            .replace_all(&text, |c: &regex::Captures| {
                let (got, want) = (
                    c[1].parse::<i128>().unwrap_or(0),
                    c[2].parse::<i128>().unwrap_or(0),
                );
                let side = match got.cmp(&want) {
                    std::cmp::Ordering::Less => "below",
                    std::cmp::Ordering::Equal => "equal to",
                    std::cmp::Ordering::Greater => "above",
                };
                format!("{side} expected Some(\"{want}\")")
            })
            .into_owned();
    }
    if let Some(hex) = HEX.as_ref() {
        text = hex
            .replace_all(&text, |c: &regex::Captures| {
                let run = &c[0];
                let digits = run.bytes().all(|b| b.is_ascii_digit());
                let lower = run.bytes().all(|b| !b.is_ascii_uppercase());
                // Lowercase hex from 16 characters (hashes); mixed case only
                // from 24, so a shorter uppercase word is left alone.
                if !digits && (lower || run.len() >= 24) {
                    "<id>".to_string()
                } else {
                    run.to_string()
                }
            })
            .into_owned();
    }
    for (re, to) in RULES.iter() {
        text = re.replace_all(&text, *to).into_owned();
    }
    text
}

/// `text` with each JSON object or array in it rewritten with sorted keys.
fn sorted_json(text: &str) -> String {
    fn write(value: &serde_json::Value, out: &mut String) {
        match value {
            serde_json::Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                out.push('{');
                for (i, key) in keys.into_iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&serde_json::Value::String(key.clone()).to_string());
                    out.push(':');
                    write(&map[key], out);
                }
                out.push('}');
            }
            serde_json::Value::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write(item, out);
                }
                out.push(']');
            }
            other => out.push_str(&other.to_string()),
        }
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(['{', '[']) {
        out.push_str(&rest[..start]);
        let candidate = &rest[start..];
        let mut values =
            serde_json::Deserializer::from_str(candidate).into_iter::<serde_json::Value>();
        if let Some(Ok(value)) = values.next() {
            write(&value, &mut out);
            rest = &candidate[values.byte_offset()..];
        } else {
            out.push_str(&candidate[..1]);
            rest = &candidate[1..];
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_keeps_the_failure_and_drops_what_varies() {
        assert_eq!(
            signature(
                "still failing after 61 attempts over 30s: Spice rejected the query: tcp connect error 127.0.0.1:61234 in /Users/x/work/a.log at 2026-10-19T05:05:42.001Z\nmore"
            ),
            "still failing after <n> attempts over <duration>: Spice rejected the query: tcp connect error 127.0.0.1:<port> in <path> at <time>"
        );
        assert_eq!(
            signature(
                "answer differs from the expected one: RowCountMismatch { expected: 990, actual: 1000 }"
            ),
            "answer differs from the expected one: RowCountMismatch { expected: 990, actual: 1000 }"
        );
        assert_eq!(
            signature(
                "the partitions are not the table: RowCountMismatch { expected: 400, actual: 800 } --- spice (800 rows) +----+"
            ),
            "the partitions are not the table: RowCountMismatch { expected: 400, actual: 800 }"
        );
        assert_eq!(
            signature(
                r#"6 of 5156 iterations failed; first: invariant broken: n = Some("993"), expected Some("1000")"#
            ),
            signature(
                r#"4 of 5760 iterations failed; first: invariant broken: n = Some("999"), expected Some("1000")"#
            ),
        );
        assert_ne!(
            signature(
                r#"1 of 9 iterations failed; first: invariant broken: n = Some("1001"), expected Some("1000")"#
            ),
            signature(
                r#"1 of 9 iterations failed; first: invariant broken: n = Some("999"), expected Some("1000")"#
            ),
        );
        assert_eq!(
            signature(
                r#"spiced not ready after 30s; dataset status: [{"status":"Refreshing","properties":{"vector_search":"y","search":"x"}}]"#
            ),
            signature(
                r#"spiced not ready after 30s; dataset status: [{"properties":{"search":"x","vector_search":"y"},"status":"Refreshing"}]"#
            ),
        );
        assert_ne!(
            signature(r#"dataset status: [{"status":"Refreshing"}]"#),
            signature(r#"dataset status: [{"status":"Error"}]"#),
        );
        assert_eq!(
            signature(
                r#"metadata mismatch: Expected original metadata: {"b": "2", "a": "1"}, got [1, {"y": 1, "x": 2}]"#
            ),
            r#"metadata mismatch: Expected original metadata: {"a":"1","b":"2"}, got [1,{"x":2,"y":1}]"#
        );
        assert_eq!(
            signature(
                "expected 12345678901234567890123456789012345678 got 12345678901234567890123456789012345679"
            ),
            "expected 12345678901234567890123456789012345678 got 12345678901234567890123456789012345679"
        );
        assert_eq!(
            signature("object 0123456789abcdef0123 missing"),
            "object <id> missing"
        );
        assert_eq!(
            signature(
                "task 184 panicked; ExternalSorterMerge[0] failed; Unexpected PageType 52; stopped after 2s (51 attempts)"
            ),
            signature(
                "task 183 panicked; ExternalSorterMerge[14] failed; Unexpected PageType 736; stopped after 3s (29 attempts)"
            ),
        );
        assert_eq!(
            signature(
                "Internal error in table 'ev_p76313A31373A76312E757466382E763631373336393631': not found at facts/part-00002.parquet"
            ),
            "Internal error in table 'ev_p<id>': not found at facts/part-<n>.parquet"
        );
    }

    fn script_in(dir: &str) -> Script {
        let mut script = Script::parse("name: s\nspicepod: {}\nsteps: [ { start: {} } ]")
            .expect("script parses");
        script.dir = PathBuf::from(dir);
        script.file_stem = "s".into();
        script
    }

    fn failure(who: &str) -> Failure {
        Failure {
            run: "s".into(),
            who: who.into(),
            step: 1,
            key: "assert-status: a".into(),
            what: "assert-status".into(),
            query: None,
            labels: Vec::new(),
            message: "expected a message containing 'x'".into(),
        }
    }

    fn entry(who: &str, may_pass: bool) -> Entry {
        let f = failure(who);
        Entry {
            run: f.run,
            who: f.who,
            key: f.key,
            query: None,
            kind: kind(&f.message),
            signature: signature(&f.message),
            also: Vec::new(),
            varies: false,
            pattern: None,
            group: "g".into(),
            labels: Vec::new(),
            may_pass,
        }
    }

    fn ledger(script: &Script, entries: Vec<Entry>) -> Ledger {
        let mut files = BTreeMap::new();
        files.insert(
            script.dir.clone(),
            BTreeMap::from([(script.file_stem.clone(), entries)]),
        );
        Ledger { files }
    }

    #[test]
    fn may_pass_lets_racy_checks_pass_and_notes_when_all_of_them_do() {
        let script = script_in("t/known-failing/g");
        let mixed = ledger(
            &script,
            vec![entry("a", true), entry("b", true), entry("c", false)],
        );
        let one = mixed.judge(&script, &[failure("a"), failure("c")]);
        assert_eq!(
            (one.expected, one.problems().len(), one.notes.len()),
            (2, 0, 0)
        );
        let racy_only = mixed.judge(&script, &[failure("c")]);
        assert_eq!((racy_only.problems().len(), racy_only.notes.len()), (0, 0));
        let fixed = mixed.judge(&script, &[]);
        assert_eq!(
            fixed.gone.len(),
            1,
            "the entry without may-pass is gone: {:?}",
            fixed.problems()
        );
        let all_racy = ledger(&script, vec![entry("a", true), entry("b", true)]);
        let none = all_racy.judge(&script, &[]);
        assert_eq!((none.problems().len(), none.notes.len()), (0, 1));
    }

    #[test]
    fn recording_keeps_a_may_pass_entry_that_passed_this_time() {
        let script = script_in("t/known-failing/g");
        let mut ledger = ledger(
            &script,
            vec![entry("a", true), entry("b", true), entry("c", false)],
        );
        ledger.record(&script, &[failure("b")]);
        let whos: Vec<(&str, bool)> = ledger
            .entries(&script)
            .iter()
            .map(|e| (e.who.as_str(), e.may_pass))
            .collect();
        assert_eq!(whos, vec![("b", true), ("a", true)]);
    }

    #[test]
    fn recording_a_varying_entry_collects_its_symptoms() {
        let script = script_in("t/known-failing/g");
        let mut first = entry("a", false);
        first.varies = true;
        let mut ledger = ledger(&script, vec![first]);
        let mut other = failure("a");
        other.message = "Object at location facts/part-00003.csv not found".into();
        ledger.record(&script, &[other.clone()]);
        ledger.record(&script, &[other.clone()]);
        let e = &ledger.entries(&script)[0];
        assert_eq!(e.signature, signature(&failure("a").message));
        assert_eq!(
            e.also,
            vec!["Object at location facts/part-<n>.csv not found".to_string()]
        );
        assert_eq!(ledger.judge(&script, &[other]).expected, 1);
        let mut third = failure("a");
        third.message = "Csv error: incorrect number of fields".into();
        assert_eq!(
            ledger.judge(&script, &[third]).changed.len(),
            1,
            "an unseen symptom is still a change"
        );
    }

    #[test]
    fn a_pattern_accepts_varying_details_around_its_core_and_nothing_else() {
        let script = script_in("t/known-failing/g");
        let mut torn = entry("a", false);
        torn.pattern = Some("iterations failed; first: invariant broken:".into());
        let ledger = ledger(&script, vec![torn]);
        let mut reader = failure("a");
        reader.message = r#"6 of 900 iterations failed; first: invariant broken: total = Some("100007"), expected Some("100000")"#.into();
        assert_eq!(ledger.judge(&script, &[reader]).expected, 1);
        let mut lost = failure("a");
        lost.message =
            "connection lost while reading the answer: error decoding response body".into();
        assert_eq!(
            ledger.judge(&script, &[lost]).changed.len(),
            1,
            "another failure is still a change"
        );
    }

    #[test]
    fn a_pattern_matches_past_where_a_signature_is_cut() {
        let script = script_in("t/known-failing/g");
        let mut conflict = entry("a", false);
        conflict.pattern =
            Some("spiced not ready: a dataset is in Error: .*Database schema conflict".into());
        let ledger = ledger(&script, vec![conflict]);
        // The readiness failure lists every dataset; with a healthy one first,
        // the erroring one's message starts past the signature's 300 characters.
        let mut start = failure("a");
        start.message = r#"spiced not ready: a dataset is in Error: [{"from":"mysql:orders","name":"orders","replication_enabled":false,"acceleration_enabled":true,"status":"Refreshing","properties":{"search":"unsupported","vector_search":"unsupported"}},{"from":"mysql:customers","name":"customers","replication_enabled":false,"acceleration_enabled":true,"status":"Error","error":{"category":"dataset","type":"validation","code":"dataset.validation"},"error_message":"External error: Invalid operation: Failed to commit inline mutation transaction Database error: Failed to commit transaction: Database schema conflict","properties":{"vector_search":"unsupported","search":"unsupported"}}]; see /tmp/w/spiced.log"#.into();
        assert!(
            !signature(&start.message).contains("Database schema conflict"),
            "the test needs the core past the cut"
        );
        let verdict = ledger.judge(&script, &[start]);
        assert_eq!((verdict.expected, verdict.changed.len()), (1, 0));
    }

    #[test]
    fn entries_of_what_a_selection_left_out_are_neither_gone_nor_dropped() {
        let mut script = script_in("x");
        script.left_out.variants.insert("other".into());
        let mut skipped = entry("other", false);
        skipped.key = "assert-status: b".into();
        let mut ledger = ledger(&script, vec![entry("a", false), skipped.clone()]);
        // The selected variant's expected failure fails; the other's never ran.
        let verdict = ledger.judge(&script, &[failure("a")]);
        assert_eq!(verdict.expected, 1);
        assert!(verdict.gone.is_empty(), "{:?}", verdict.gone);
        // Recording keeps the entry the run could not have seen.
        ledger.record(&script, &[failure("a")]);
        assert_eq!(
            ledger.entries(&script),
            [entry("a", false), skipped.clone()]
        );
        // Without the selection, the same run reports it gone.
        let verdict = ledger.judge(&script_in("x"), &[failure("a")]);
        assert_eq!(verdict.gone, [skipped]);
    }

    #[test]
    fn a_known_failing_script_that_fails_nothing_is_reported() {
        let script = script_in("t/known-failing/g");
        let verdict = ledger(&script, Vec::new()).judge(&script, &[]);
        assert_eq!(verdict.passes_in_known_failing.as_deref(), Some("s"));
        let passing = script_in("t");
        assert!(
            ledger(&passing, Vec::new())
                .judge(&passing, &[])
                .problems()
                .is_empty()
        );
    }

    #[test]
    fn a_run_that_stopped_at_setup_does_not_report_its_entries_fixed() {
        let script = script_in("t/known-failing/g");
        let ledger = ledger(&script, vec![entry("a", false), entry("b", false)]);
        let setup = Failure {
            step: 0,
            key: "setup".into(),
            what: "setup".into(),
            message: "Failed to start source 'src' (postgres): Failed to connect to Docker".into(),
            ..failure("")
        };
        let verdict = ledger.judge(&script, &[setup]);
        let problems = verdict.problems();
        assert!(
            !problems.iter().any(|p| p.contains("no longer fails")),
            "entries that never ran are not fixed: {problems:?}"
        );
        assert_eq!(
            problems.len(),
            2,
            "the setup failure and one line for the run: {problems:?}"
        );
        assert!(problems[1].contains("2 expected failures"), "{problems:?}");

        // One instance that never got its slots stops only that instance's checks.
        let instance = Failure {
            step: 0,
            key: "setup".into(),
            what: "setup".into(),
            message: "slots closed".into(),
            ..failure("a")
        };
        let verdict = ledger.judge(&script, &[instance]);
        assert_eq!(
            verdict
                .gone
                .iter()
                .map(|e| e.who.as_str())
                .collect::<Vec<_>>(),
            vec!["b"],
            "the other instance ran, so its passing check is still fixed"
        );

        // A failing step is not a stop: the checks after it ran.
        let verdict = ledger.judge(&script, &[failure("a")]);
        assert_eq!(verdict.gone.len(), 1);
    }

    #[test]
    fn a_known_failing_directory_names_the_group() {
        assert_eq!(
            known_failing_group(Path::new("test/spicescript/known-failing/clickhouse")),
            Some("clickhouse".into())
        );
        assert_eq!(known_failing_group(Path::new("test/spicescript")), None);
    }
}
