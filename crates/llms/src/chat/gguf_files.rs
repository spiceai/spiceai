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

//! Which GGUF files of a repository a model loads, from the repository's file listing alone.
//!
//! A GGUF repository usually offers several quantizations of one model, each one file or a
//! series of shards (`Model-Q4_K_M-00001-of-00003.gguf` …), often one folder each
//! (`UD-Q2_K_XL/…`), and sometimes MTP draft heads (`MTP/mtp-Model-Q8_0.gguf`) and vision
//! projectors (`mmproj-F16.gguf`). A model's `files` may name exact files, a quantization by
//! name or folder, or nothing; [`resolve`] turns each into the files to download, and
//! [`choose`] picks a quantization when none is named.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;
use snafu::Snafu;
use spicepod::component::model::ModelFile;

/// A file of the repository: its path in the repository and its size in bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedFile {
    pub path: String,
    pub size: u64,
}

/// One quantization of the model: every shard of it, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quantization {
    /// The name a model's `files` can give it, unique in the repository: its folder when the
    /// folder holds no other quantization, or else the quantization tag of its file names
    /// (`UD-Q4_K_XL`, `Q8_0`, `BF16`), or its path when another quantization could go by that
    /// name (has it, has it as its tag or path, or is in a folder of that name) or the name ends
    /// in `.gguf`, which names a file. Its tag (its
    /// file name, untagged) and its path stem name it too, so a name given while the Hub listed
    /// the repository still resolves against the cache's shorter listing.
    pub name: String,
    /// Repository paths, shard 1 first.
    pub files: Vec<String>,
    /// Total bytes of its files.
    pub bytes: u64,
}

/// What a model loads: the weight files, in shard order, and the draft head's, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub weights: Vec<String>,
    /// The draft head's files, shard 1 first, which the engine opens; empty without a head.
    pub draft: Vec<String>,
    /// The quantization the weights are, when they are one.
    pub quantization: Option<String>,
}

impl Resolved {
    /// Every file to download.
    #[must_use]
    pub fn files(&self) -> Vec<&str> {
        self.weights
            .iter()
            .chain(&self.draft)
            .map(String::as_str)
            .collect()
    }
}

/// Why `files` cannot be resolved against the listing; each reads as the clause after a colon.
#[derive(Debug, Clone, PartialEq, Eq, Snafu)]
pub enum ResolveError {
    /// A named `.gguf` file is not in the repository.
    #[snafu(display("the repository has no file '{path}'"))]
    MissingFile { path: String },
    /// A shard series some of whose shards the repository lacks.
    #[snafu(display(
        "'{path}' is a shard of a series the repository lacks part of: {}",
        missing.join(", ")
    ))]
    IncompleteShards { path: String, missing: Vec<String> },
    /// A name several quantizations answer to: a folder that holds several, or a tag one has
    /// and another's folder bears.
    #[snafu(display(
        "'{name}' matches {} quantizations of the repository, {}; name one of them",
        quantizations.len(),
        quantizations.join(", ")
    ))]
    AmbiguousQuantization {
        name: String,
        quantizations: Vec<String>,
    },
    /// A name that is neither a file nor a quantization of the repository.
    #[snafu(display(
        "the repository has no file or quantization '{name}'; its quantizations are {}",
        available.join(", ")
    ))]
    UnknownQuantization {
        name: String,
        available: Vec<String>,
    },
}

/// `…-NNNNN-of-NNNNN.gguf`: one shard of a series.
static SHARD: LazyLock<Option<Regex>> =
    LazyLock::new(|| Regex::new(r"^(?P<stem>.+)-(?P<i>\d{5})-of-(?P<n>\d{5})\.gguf$").ok());

/// A quantization tag at the end of a file stem, or the whole stem: `Q4_K_M`, `UD-Q2_K_XL`,
/// `IQ4_XS`, `BF16`, …
static TAG: LazyLock<Option<Regex>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(?:^|[-_.])(?P<tag>(?:UD-)?(?:I?Q\d(?:_[0-9A-Z]+)*|BF16|F16|F32|MXFP4(?:_MOE)?|NVFP4|TQ\d_\d))$",
    )
    .ok()
});

fn is_gguf(path: &str) -> bool {
    path.to_ascii_lowercase().ends_with(".gguf")
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn folder(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(dir, _)| dir)
}

/// An MTP draft head, by the `mtp-` name the GGUF publishers give them.
#[must_use]
pub fn is_draft_head(path: &str) -> bool {
    file_name(path).to_ascii_lowercase().starts_with("mtp-")
}

/// A vision projector, which a text model does not load.
fn is_projector(path: &str) -> bool {
    file_name(path).to_ascii_lowercase().starts_with("mmproj")
}

/// The shard series a file belongs to, `(series key, index, count)`, or `None` for a lone file.
fn shard(path: &str) -> Option<(String, u32, u32)> {
    let caps = SHARD.as_ref()?.captures(path)?;
    let i = caps.name("i")?.as_str().parse().ok()?;
    let n = caps.name("n")?.as_str().parse().ok()?;
    Some((caps.name("stem")?.as_str().to_string(), i, n))
}

fn shard_path(stem: &str, i: u32, n: u32) -> String {
    format!("{stem}-{i:05}-of-{n:05}.gguf")
}

/// The files `path` loads: itself, or every shard of the series it is a shard of.
///
/// # Errors
///
/// Returns [`ResolveError::IncompleteShards`] when `present` lacks a shard of the series.
fn series(path: &str, present: &BTreeSet<&str>) -> std::result::Result<Vec<String>, ResolveError> {
    let Some((stem, _, n)) = shard(path) else {
        return Ok(vec![path.to_string()]);
    };
    let series: Vec<String> = (1..=n).map(|i| shard_path(&stem, i, n)).collect();
    let missing: Vec<String> = series
        .iter()
        .filter(|p| !present.contains(p.as_str()))
        .cloned()
        .collect();
    if missing.is_empty() {
        Ok(series)
    } else {
        Err(ResolveError::IncompleteShards {
            path: path.to_string(),
            missing,
        })
    }
}

/// The names a quantization answers to besides its own: the name it goes by when its folder
/// does not name it, and its path stem, which [`quantizations`] names it by when its own name
/// is shared.
fn aliases(quantization: &Quantization) -> impl Iterator<Item = String> {
    let first = quantization.files.first().map_or("", String::as_str);
    let stem = shard(first).map_or_else(
        || first.trim_end_matches(".gguf").to_string(),
        |(stem, _, _)| stem,
    );
    [unfoldered_name(first), stem].into_iter()
}

/// The name a quantization goes by when its folder does not name it: the quantization tag of
/// its first file's name, or that name without `.gguf` when it carries none.
fn unfoldered_name(first: &str) -> String {
    tag(first).unwrap_or_else(|| file_name(first).trim_end_matches(".gguf").to_string())
}

/// The quantization tag of a file name, if it carries one.
fn tag(path: &str) -> Option<String> {
    let name = file_name(path);
    let stem = shard(name).map_or_else(
        || name[..name.len().saturating_sub(".gguf".len())].to_string(),
        |(stem, _, _)| stem,
    );
    let caps = TAG.as_ref()?.captures(&stem)?;
    Some(caps.name("tag")?.as_str().to_string())
}

/// The model's complete quantizations in the listing, largest first. Draft heads and
/// projectors are not quantizations of the model; a shard series with a shard missing is not
/// complete and is left out.
#[must_use]
pub fn quantizations(listing: &[ListedFile]) -> Vec<Quantization> {
    let size: BTreeMap<&str, u64> = listing.iter().map(|f| (f.path.as_str(), f.size)).collect();
    // Files grouped by (folder, stem without the shard suffix, shard count): a series is one
    // shard count, so a partial layout of another count beside it neither hides it nor joins it.
    let mut groups: BTreeMap<(String, String, Option<u32>), Vec<&str>> = BTreeMap::new();
    for file in listing {
        let path = file.path.as_str();
        if !is_gguf(path) || is_draft_head(path) || is_projector(path) {
            continue;
        }
        let (stem, count) =
            shard(path).map_or_else(|| (path.to_string(), None), |(stem, _, n)| (stem, Some(n)));
        groups
            .entry((folder(path).to_string(), stem, count))
            .or_default()
            .push(path);
    }
    let mut complete = Vec::new();
    for ((dir, stem, _), mut files) in groups {
        let whole = match shard(files[0]) {
            None => files.len() == 1,
            Some((_, _, n)) => {
                (1..=n).all(|i| size.contains_key(shard_path(&stem, i, n).as_str()))
                    && files.len() == n as usize
            }
        };
        if whole {
            files.sort_unstable();
            complete.push((dir, stem, files));
        }
    }
    // A quantization offered both merged and split (`M-Q4_K_M.gguf` beside its shards) is one
    // quantization in two layouts: the merged file stands for it, and a shard's path still
    // loads the series.
    let merged: BTreeSet<(String, String)> = complete
        .iter()
        .filter(|(_, _, files)| shard(files[0]).is_none())
        .map(|(dir, stem, _)| (dir.clone(), stem.trim_end_matches(".gguf").to_string()))
        .collect();
    complete.retain(|(dir, stem, files)| {
        shard(files[0]).is_none() || !merged.contains(&(dir.clone(), stem.clone()))
    });
    let mut in_folder: BTreeMap<String, usize> = BTreeMap::new();
    for (dir, _, _) in &complete {
        *in_folder.entry(dir.clone()).or_default() += 1;
    }
    let mut out = Vec::new();
    // Each quantization's names, most preferred first; the last, its first file's path, is
    // unique in the repository.
    let mut candidates = Vec::new();
    // Every name each quantization could go by in this listing or a shorter one, as the cache's
    // with the Hub out of reach, and so every name `resolve` could match it by there.
    let mut claims = Vec::new();
    for (dir, stem, files) in complete {
        let leaf = dir.rsplit('/').next().unwrap_or(&dir).to_string();
        let own = unfoldered_name(files[0]);
        let stem = stem.trim_end_matches(".gguf").to_string();
        // A folder names the one quantization it holds; several in one folder go by their tags.
        let name = if !dir.is_empty() && in_folder.get(&dir) == Some(&1) {
            leaf.clone()
        } else {
            own.clone()
        };
        let bytes = files
            .iter()
            .map(|f| size.get(f).copied().unwrap_or(0))
            .sum();
        claims.push(
            [leaf, dir.clone(), own, stem.clone(), files[0].to_string()]
                .into_iter()
                .filter(|claim| !claim.is_empty())
                .map(|claim| claim.to_ascii_lowercase())
                .collect::<BTreeSet<_>>(),
        );
        candidates.push([name.clone(), stem, files[0].to_string()]);
        out.push(Quantization {
            name,
            files: files.into_iter().map(ToString::to_string).collect(),
            bytes,
        });
    }
    // A name another quantization could go by would resolve to that one in a listing where it
    // does, as `Q4_K_M` to the only quantization a cached `Q4_K_M/` folder holds; a name ending
    // in `.gguf`, as a `model.gguf/` folder's, is read as a file's path; and a name a Spicepod's
    // `files` drop, as a `Q4_K_M.bin/` folder's, never reaches resolution: each takes its next
    // name until none holds, which its first file's path guarantees.
    for (i, (q, names)) in out.iter_mut().zip(&candidates).enumerate() {
        let mut at = 0;
        while at + 1 < names.len()
            && (is_gguf(&q.name)
                || !ModelFile::from_path(Path::new(&q.name)).is_gguf_selector()
                || claims
                    .iter()
                    .enumerate()
                    .any(|(j, claim)| j != i && claim.contains(&q.name.to_ascii_lowercase())))
        {
            at += 1;
            q.name.clone_from(&names[at]);
        }
    }
    // Largest first; a stable tie-break by name keeps the order reproducible.
    out.sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.name.cmp(&b.name)));
    out
}

/// The files of the repository's MTP draft head for `quantization`, shard 1 first: a `Q8_0`
/// head if it offers one, otherwise the head with the tag its weight files carry, which its
/// name may not (a folder or a path names it then). `shared` variants, which expect the
/// target's own embeddings, are passed over, and so is a head with a shard missing. Empty when
/// there is no head.
#[must_use]
pub fn draft_head(listing: &[ListedFile], quantization: &Quantization) -> Vec<String> {
    let present: BTreeSet<&str> = listing.iter().map(|f| f.path.as_str()).collect();
    let heads: Vec<&str> = listing
        .iter()
        .map(|f| f.path.as_str())
        .filter(|p| is_gguf(p) && is_draft_head(p))
        .filter(|p| !file_name(p).to_ascii_lowercase().contains("-shared"))
        .collect();
    let with_tag = |want: &str| {
        heads
            .iter()
            .filter(|p| tag(p).is_some_and(|t| t.eq_ignore_ascii_case(want)))
            .find_map(|p| series(p, &present).ok())
    };
    with_tag("Q8_0")
        .or_else(|| {
            let own = tag(quantization.files.first()?)?;
            // the tag without its `UD-` prefix, which `TAG` matches in any case
            let plain = own
                .get(..3)
                .filter(|prefix| prefix.eq_ignore_ascii_case("UD-"))
                .and_then(|_| own.get(3..))
                .unwrap_or(&own);
            with_tag(&own).or_else(|| with_tag(plain))
        })
        .unwrap_or_default()
}

/// The `files` entries that load what choosing `quantization` with no `files` loads: its
/// weights and its draft head. Its name brings both, unless the name is a `.gguf` path, which
/// is honoured as exactly that file and brings no head; the head is then named too.
#[must_use]
pub fn pin(listing: &[ListedFile], quantization: &Quantization) -> Vec<String> {
    let mut entries = vec![quantization.name.clone()];
    if is_gguf(&quantization.name)
        && let Some(head) = draft_head(listing, quantization).into_iter().next()
    {
        // a shard of the head names its whole series
        entries.push(head);
    }
    entries
}

/// Resolves a model's `files` against the repository listing. An exact `.gguf` path is
/// honoured, with the sibling shards of a shard, a draft head's included; any other entry names
/// a quantization, by its name, the folder that holds only it, or its tag or path stem, which
/// brings all its shards and, unless a draft head is named, the repository's draft head.
/// Another file the repository has is left to the engine, and so is every entry of a
/// repository that offers no GGUF quantization. `Ok(None)` when `files` names no GGUF weights.
///
/// # Errors
///
/// Returns an error when a named file is missing, a shard series is incomplete, a name matches
/// several quantizations, or a name is neither a file nor a quantization of the repository.
pub fn resolve(
    listing: &[ListedFile],
    files: &[&str],
) -> std::result::Result<Option<Resolved>, ResolveError> {
    if files.is_empty() {
        return Ok(None);
    }
    let present: BTreeSet<&str> = listing.iter().map(|f| f.path.as_str()).collect();
    let quantizations = quantizations(listing);
    let mut weights = Vec::new();
    let mut draft = Vec::new();
    let mut quantization = None;
    let mut named_quantization = None;
    for &entry in files {
        let entry = entry.trim().trim_start_matches("./");
        if is_gguf(entry) {
            if !present.contains(entry) {
                return Err(ResolveError::MissingFile {
                    path: entry.to_string(),
                });
            }
            if is_draft_head(entry) {
                draft.extend(series(entry, &present)?);
            } else {
                weights.extend(series(entry, &present)?);
            }
        } else {
            let name = entry.trim_end_matches('/');
            // Every quantization the entry could name: the folder that holds it, or, unless
            // `name/` makes it a folder, its name, its tag or its path stem. A listing of fewer
            // files, as the cache's with the Hub out of reach, names by its folder what the
            // whole listing named by its tag, so a name more than one answers to names none.
            let matched: Vec<&Quantization> = quantizations
                .iter()
                .filter(|q| {
                    q.files.iter().all(|f| folder(f).eq_ignore_ascii_case(name))
                        || (!entry.ends_with('/')
                            && (q.name.eq_ignore_ascii_case(name)
                                || aliases(q).any(|alias| alias.eq_ignore_ascii_case(name))))
                })
                .collect();
            let q = match matched.as_slice() {
                [q] => *q,
                [] if present.contains(entry) || quantizations.is_empty() => continue,
                [] => {
                    return Err(ResolveError::UnknownQuantization {
                        name: name.to_string(),
                        available: quantizations.iter().map(|q| q.name.clone()).collect(),
                    });
                }
                several => {
                    return Err(ResolveError::AmbiguousQuantization {
                        name: name.to_string(),
                        quantizations: several.iter().map(|q| q.name.clone()).collect(),
                    });
                }
            };
            weights.extend(q.files.iter().cloned());
            named_quantization = Some(q);
        }
    }
    // Each entry naming a shard expands to its whole series, so naming two shards repeats it.
    let mut seen = BTreeSet::new();
    weights.retain(|w| seen.insert(w.clone()));
    draft.retain(|d| seen.insert(d.clone()));
    if weights.is_empty() && draft.is_empty() {
        return Ok(None);
    }
    if let Some(q) = named_quantization {
        if draft.is_empty() {
            draft = draft_head(listing, q);
        }
        quantization = Some(q.name.clone());
    } else if let Some(q) = quantizations.iter().find(|q| q.files == weights) {
        quantization = Some(q.name.clone());
    }
    Ok(Some(Resolved {
        weights,
        draft,
        quantization,
    }))
}

/// How much of `memory` the weights may take: the rest is left for the context and the
/// compute buffers, until the engine's own fit replaces this check.
#[must_use]
pub fn weights_budget(memory: u64) -> u64 {
    const RESERVE: u64 = 4 << 30;
    (memory - memory / 10).saturating_sub(RESERVE)
}

/// The largest complete quantization whose weights fit `budget` bytes, or the smallest one,
/// as the error, when none does.
///
/// # Errors
///
/// Returns the smallest quantization when even it does not fit, and `None` when the listing
/// has no quantization at all.
pub fn choose(
    quantizations: &[Quantization],
    budget: u64,
) -> std::result::Result<&Quantization, Option<&Quantization>> {
    quantizations
        .iter()
        .find(|q| q.bytes <= budget)
        .ok_or_else(|| quantizations.last())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listing(files: &[(&str, u64)]) -> Vec<ListedFile> {
        files
            .iter()
            .map(|&(path, size)| ListedFile {
                path: path.to_string(),
                size,
            })
            .collect()
    }

    const GB: u64 = 1_000_000_000;

    /// A cut of unsloth/GLM-5.3-Flash-GGUF's listing (2026-10-26): one folder per
    /// quantization, the first shard metadata only, no draft head; `UD-IQ1_S` here lacks a shard.
    fn glm() -> Vec<ListedFile> {
        listing(&[
            ("README.md", 10_000),
            ("Q8_0/GLM-5.3-Flash-Q8_0-00001-of-00003.gguf", 10_000_000),
            (
                "Q8_0/GLM-5.3-Flash-Q8_0-00002-of-00003.gguf",
                49_700_000_000,
            ),
            (
                "Q8_0/GLM-5.3-Flash-Q8_0-00003-of-00003.gguf",
                49_800_000_000,
            ),
            (
                "UD-Q2_K_XL/GLM-5.3-Flash-UD-Q2_K_XL-00001-of-00004.gguf",
                10_000_000,
            ),
            (
                "UD-Q2_K_XL/GLM-5.3-Flash-UD-Q2_K_XL-00002-of-00004.gguf",
                49_300_000_000,
            ),
            (
                "UD-Q2_K_XL/GLM-5.3-Flash-UD-Q2_K_XL-00003-of-00004.gguf",
                49_900_000_000,
            ),
            (
                "UD-Q2_K_XL/GLM-5.3-Flash-UD-Q2_K_XL-00004-of-00004.gguf",
                9_500_000_000,
            ),
            (
                "UD-IQ1_S/GLM-5.3-Flash-UD-IQ1_S-00001-of-00003.gguf",
                10_000_000,
            ),
            (
                "UD-IQ1_S/GLM-5.3-Flash-UD-IQ1_S-00003-of-00003.gguf",
                43_500_000_000,
            ),
        ])
    }

    /// A cut of unsloth/Qwen3.8-Flash-Next-GGUF's listing: quantization folders, an `MTP/`
    /// folder of draft heads (`shared` variants among them), and vision projectors.
    fn qwen38() -> Vec<ListedFile> {
        listing(&[
            ("MTP/mtp-Qwen3.8-Flash-Next-BF16.gguf", 7_800_000_000),
            ("MTP/mtp-Qwen3.8-Flash-Next-Q4_K_M.gguf", 2_800_000_000),
            ("MTP/mtp-Qwen3.8-Flash-Next-Q8_0.gguf", 4_100_000_000),
            ("MTP/mtp-Qwen3.8-Flash-Next-shared-Q8_0.gguf", 2_800_000_000),
            (
                "UD-Q3_K_XL/Qwen3.8-Flash-Next-UD-Q3_K_XL-00001-of-00003.gguf",
                11_000_000,
            ),
            (
                "UD-Q3_K_XL/Qwen3.8-Flash-Next-UD-Q3_K_XL-00002-of-00003.gguf",
                50_000_000_000,
            ),
            (
                "UD-Q3_K_XL/Qwen3.8-Flash-Next-UD-Q3_K_XL-00003-of-00003.gguf",
                40_000_000_000,
            ),
            (
                "UD-Q2_K_XL/Qwen3.8-Flash-Next-UD-Q2_K_XL-00001-of-00003.gguf",
                11_000_000,
            ),
            (
                "UD-Q2_K_XL/Qwen3.8-Flash-Next-UD-Q2_K_XL-00002-of-00003.gguf",
                50_000_000_000,
            ),
            (
                "UD-Q2_K_XL/Qwen3.8-Flash-Next-UD-Q2_K_XL-00003-of-00003.gguf",
                28_900_000_000,
            ),
            ("mmproj-BF16.gguf", 900_000_000),
            ("mmproj-F16.gguf", 900_000_000),
        ])
    }

    /// A cut of unsloth/Qwen3-0.6B-GGUF's listing: one file per quantization, no folders.
    fn qwen3() -> Vec<ListedFile> {
        listing(&[
            ("config.json", 1_000),
            ("Qwen3-0.6B-BF16.gguf", 1_198_000_000),
            ("Qwen3-0.6B-Q4_K_M.gguf", 397_000_000),
            ("Qwen3-0.6B-Q8_0.gguf", 639_000_000),
            ("Qwen3-0.6B-UD-Q4_K_XL.gguf", 405_000_000),
            ("Qwen3-0.6B-UD-IQ1_S.gguf", 215_000_000),
        ])
    }

    fn names(qs: &[Quantization]) -> Vec<&str> {
        qs.iter().map(|q| q.name.as_str()).collect()
    }

    #[test]
    fn quantizations_are_complete_series_largest_first() {
        let qs = quantizations(&glm());
        // UD-IQ1_S lacks its second shard, so it is not offered.
        assert_eq!(names(&qs), vec!["UD-Q2_K_XL", "Q8_0"]);
        assert_eq!(qs[0].files.len(), 4);
        assert_eq!(qs[0].bytes, 108_710_000_000);
        assert_eq!(
            qs[1].files,
            vec![
                "Q8_0/GLM-5.3-Flash-Q8_0-00001-of-00003.gguf",
                "Q8_0/GLM-5.3-Flash-Q8_0-00002-of-00003.gguf",
                "Q8_0/GLM-5.3-Flash-Q8_0-00003-of-00003.gguf",
            ]
        );
    }

    #[test]
    fn draft_heads_and_projectors_are_not_quantizations() {
        assert_eq!(
            names(&quantizations(&qwen38())),
            vec!["UD-Q3_K_XL", "UD-Q2_K_XL"]
        );
        assert_eq!(
            names(&quantizations(&qwen3())),
            vec!["BF16", "Q8_0", "UD-Q4_K_XL", "Q4_K_M", "UD-IQ1_S"]
        );
    }

    #[test]
    fn a_quantization_by_name_brings_its_shards_and_the_q8_0_head() {
        let resolved = resolve(&qwen38(), &["UD-Q3_K_XL"])
            .expect("resolves")
            .expect("names files");
        assert_eq!(
            resolved.weights,
            vec![
                "UD-Q3_K_XL/Qwen3.8-Flash-Next-UD-Q3_K_XL-00001-of-00003.gguf",
                "UD-Q3_K_XL/Qwen3.8-Flash-Next-UD-Q3_K_XL-00002-of-00003.gguf",
                "UD-Q3_K_XL/Qwen3.8-Flash-Next-UD-Q3_K_XL-00003-of-00003.gguf",
            ]
        );
        assert_eq!(resolved.draft, ["MTP/mtp-Qwen3.8-Flash-Next-Q8_0.gguf"]);
        assert_eq!(resolved.quantization.as_deref(), Some("UD-Q3_K_XL"));
        // By folder, and by tag in a flat repository, case-insensitively.
        assert_eq!(
            resolve(&qwen38(), &["ud-q3_k_xl/"]).expect("resolves"),
            Some(resolved)
        );
        let flat = resolve(&qwen3(), &["q8_0"])
            .expect("resolves")
            .expect("names files");
        assert_eq!(flat.weights, vec!["Qwen3-0.6B-Q8_0.gguf"]);
        assert_eq!(flat.draft, Vec::<String>::new());
    }

    #[test]
    fn one_shard_names_its_series_and_adds_no_head() {
        let resolved = resolve(
            &qwen38(),
            &["UD-Q2_K_XL/Qwen3.8-Flash-Next-UD-Q2_K_XL-00002-of-00003.gguf"],
        )
        .expect("resolves")
        .expect("names files");
        assert_eq!(resolved.weights.len(), 3);
        assert_eq!(
            resolved.weights[0],
            "UD-Q2_K_XL/Qwen3.8-Flash-Next-UD-Q2_K_XL-00001-of-00003.gguf"
        );
        assert_eq!(resolved.draft, Vec::<String>::new());
        assert_eq!(resolved.quantization.as_deref(), Some("UD-Q2_K_XL"));
        // A head named beside it is the one taken.
        let with_head = resolve(
            &qwen38(),
            &[
                "UD-Q2_K_XL/Qwen3.8-Flash-Next-UD-Q2_K_XL-00001-of-00003.gguf",
                "MTP/mtp-Qwen3.8-Flash-Next-Q4_K_M.gguf",
            ],
        )
        .expect("resolves")
        .expect("names files");
        assert_eq!(with_head.draft, ["MTP/mtp-Qwen3.8-Flash-Next-Q4_K_M.gguf"]);
    }

    #[test]
    fn what_is_not_there_is_named_in_the_error() {
        assert_eq!(resolve(&qwen3(), &[]), Ok(None));
        assert_eq!(
            resolve(&qwen3(), &["Qwen3-0.6B-Q5_K_M.gguf"]),
            Err(ResolveError::MissingFile {
                path: "Qwen3-0.6B-Q5_K_M.gguf".to_string()
            })
        );
        assert_eq!(
            resolve(
                &glm(),
                &["UD-IQ1_S/GLM-5.3-Flash-UD-IQ1_S-00001-of-00003.gguf"]
            ),
            Err(ResolveError::IncompleteShards {
                path: "UD-IQ1_S/GLM-5.3-Flash-UD-IQ1_S-00001-of-00003.gguf".to_string(),
                missing: vec!["UD-IQ1_S/GLM-5.3-Flash-UD-IQ1_S-00002-of-00003.gguf".to_string()],
            })
        );
        let err = resolve(&glm(), &["Q4_K_M"]).expect_err("no such quantization");
        assert_eq!(
            err.to_string(),
            "the repository has no file or quantization 'Q4_K_M'; its quantizations are UD-Q2_K_XL, Q8_0"
        );
    }

    #[test]
    fn choose_takes_the_largest_that_fits() {
        let qs = quantizations(&qwen3());
        assert_eq!(choose(&qs, 2 * GB).map(|q| q.name.as_str()), Ok("BF16"));
        assert_eq!(
            choose(&qs, 700_000_000).map(|q| q.name.as_str()),
            Ok("Q8_0")
        );
        assert_eq!(
            choose(&qs, 400_000_000).map(|q| q.name.as_str()),
            Ok("Q4_K_M")
        );
        assert_eq!(
            choose(&qs, 100_000_000).map_err(|smallest| smallest.map(|q| q.name.as_str())),
            Err(Some("UD-IQ1_S"))
        );
        assert_eq!(
            choose(&[], GB).map_err(|smallest| smallest.is_none()),
            Err(true)
        );
    }

    #[test]
    fn the_weights_budget_leaves_a_tenth_and_four_gib() {
        let gib = 1u64 << 30;
        assert_eq!(
            weights_budget(128 * gib),
            128 * gib - 128 * gib / 10 - 4 * gib
        );
        assert_eq!(weights_budget(2 * gib), 0);
    }

    #[test]
    fn tags_come_off_the_end_of_the_stem() {
        for (file, want) in [
            ("Qwen3-0.6B-Q4_K_M.gguf", Some("Q4_K_M")),
            ("Qwen3-0.6B-UD-Q4_K_XL.gguf", Some("UD-Q4_K_XL")),
            ("Qwen3.5-0.8B-BF16.gguf", Some("BF16")),
            (
                "GLM-5.3-Flash-UD-IQ2_XXS-00002-of-00004.gguf",
                Some("UD-IQ2_XXS"),
            ),
            ("mtp-Qwen3.8-Flash-Next-Q8_0.gguf", Some("Q8_0")),
            ("gpt-oss-20b-MXFP4.gguf", Some("MXFP4")),
            // a stem that is the tag alone
            ("Q4_K_M.gguf", Some("Q4_K_M")),
            ("UD-Q4_K_XL.gguf", Some("UD-Q4_K_XL")),
            ("BF16-00001-of-00002.gguf", Some("BF16")),
            ("model.gguf", None),
        ] {
            assert_eq!(tag(file).as_deref(), want, "{file}");
        }
    }

    /// Two single-file quantizations in one folder, the shape of a repository that keeps its
    /// GGUFs under `gguf/`.
    fn one_folder() -> Vec<ListedFile> {
        listing(&[
            ("gguf/Model-Q8_0.gguf", 8 * GB),
            ("gguf/Model-Q4_K_M.gguf", 4 * GB),
            ("config.json", 1_000),
        ])
    }

    #[test]
    fn quantizations_sharing_a_folder_are_named_by_their_tags() {
        let qs = quantizations(&one_folder());
        assert_eq!(names(&qs), ["Q8_0", "Q4_K_M"]);
        assert_eq!(
            resolve(&one_folder(), &["gguf"]),
            Err(ResolveError::AmbiguousQuantization {
                name: "gguf".to_string(),
                quantizations: vec!["Q8_0".to_string(), "Q4_K_M".to_string()],
            })
        );
        let err = resolve(&one_folder(), &["gguf/"]).expect_err("the folder holds two");
        assert_eq!(
            err.to_string(),
            "'gguf' matches 2 quantizations of the repository, Q8_0, Q4_K_M; name one of them"
        );
        // The quantization a budget chooses is pinned by the name it is advertised under.
        let chosen = choose(&qs, 5 * GB).expect("Q4_K_M fits");
        let pinned = resolve(&one_folder(), &[chosen.name.as_str()])
            .expect("the advertised name resolves")
            .expect("it names weights");
        assert_eq!(pinned.weights, ["gguf/Model-Q4_K_M.gguf"]);
        assert_eq!(pinned.weights, chosen.files);
    }

    /// One quantization offered merged and split, beside another.
    fn merged_and_split() -> Vec<ListedFile> {
        listing(&[
            ("M-Q4_K_M.gguf", 4 * GB),
            ("M-Q4_K_M-00001-of-00002.gguf", 2 * GB),
            ("M-Q4_K_M-00002-of-00002.gguf", 2 * GB),
            ("M-Q8_0.gguf", 8 * GB),
        ])
    }

    #[test]
    fn a_quantization_offered_merged_and_split_is_one_quantization() {
        let qs = quantizations(&merged_and_split());
        assert_eq!(names(&qs), ["Q8_0", "Q4_K_M"]);
        assert_eq!(qs[1].files, ["M-Q4_K_M.gguf"]);
        let chosen = choose(&qs, 5 * GB).expect("Q4_K_M fits");
        let pinned = resolve(&merged_and_split(), &[chosen.name.as_str()])
            .expect("resolves")
            .expect("names weights");
        assert_eq!(pinned.weights, chosen.files);
        // A shard's path still loads the split series.
        let split = resolve(&merged_and_split(), &["M-Q4_K_M-00002-of-00002.gguf"])
            .expect("resolves")
            .expect("names weights");
        assert_eq!(
            split.weights,
            [
                "M-Q4_K_M-00001-of-00002.gguf",
                "M-Q4_K_M-00002-of-00002.gguf"
            ]
        );
    }

    #[test]
    fn a_tag_in_two_folders_goes_by_the_folders_and_a_shared_folder_name_by_the_paths() {
        let two_folders = listing(&[
            ("a/Model-Q4_K_M.gguf", 4 * GB),
            ("b/Other-Q4_K_M.gguf", 5 * GB),
        ]);
        assert_eq!(names(&quantizations(&two_folders)), ["b", "a"]);
        let same_leaf = listing(&[
            ("x/q/Model-Q4_K_M.gguf", 4 * GB),
            ("y/q/Model-Q4_K_M.gguf", 5 * GB),
        ]);
        assert_eq!(
            names(&quantizations(&same_leaf)),
            ["y/q/Model-Q4_K_M", "x/q/Model-Q4_K_M"]
        );
    }

    /// A path a collision falls back to that is also another quantization's folder: Copilot's
    /// case on #15590.
    fn fallback_meets_folder() -> Vec<ListedFile> {
        listing(&[
            ("Model-Q4_K_M.gguf", 4 * GB),
            ("Other-Q4_K_M.gguf", 5 * GB),
            ("Model-Q4_K_M/Third-Q8_0.gguf", 8 * GB),
        ])
    }

    #[test]
    fn a_fallback_name_never_takes_another_quantizations_name() {
        let names: Vec<String> = quantizations(&fallback_meets_folder())
            .into_iter()
            .map(|q| q.name)
            .collect();
        let unique: BTreeSet<String> = names.iter().map(|n| n.to_ascii_lowercase()).collect();
        assert_eq!(unique.len(), names.len(), "names {names:?}");
        assert_eq!(
            names,
            [
                "Model-Q4_K_M/Third-Q8_0",
                "Other-Q4_K_M",
                "Model-Q4_K_M.gguf"
            ]
        );
    }

    /// Every quantization resolves, by the name it is listed and advertised under, to exactly
    /// its own files.
    #[test]
    fn every_quantization_name_resolves_to_its_own_files() {
        let fixtures = [
            ("glm", glm()),
            ("qwen38", qwen38()),
            ("qwen3", qwen3()),
            ("one_folder", one_folder()),
            ("merged_and_split", merged_and_split()),
            ("fallback_meets_folder", fallback_meets_folder()),
            ("tag_meets_folder", tag_meets_folder()),
            ("folder_named_like_a_tag", folder_named_like_a_tag()),
            ("untagged_in_a_folder", untagged_in_a_folder()),
            ("folder_named_like_a_file", folder_named_like_a_file()),
            (
                "two_folders",
                listing(&[
                    ("a/Model-Q4_K_M.gguf", 4 * GB),
                    ("b/Other-Q4_K_M.gguf", 5 * GB),
                ]),
            ),
            (
                "same_leaf",
                listing(&[
                    ("x/q/Model-Q4_K_M.gguf", 4 * GB),
                    ("y/q/Model-Q4_K_M.gguf", 5 * GB),
                ]),
            ),
        ];
        for (fixture, files) in fixtures {
            let qs = quantizations(&files);
            assert!(!qs.is_empty(), "{fixture} has quantizations");
            for q in &qs {
                let resolved = resolve(&files, &[q.name.as_str()])
                    .unwrap_or_else(|e| panic!("{fixture}: '{}' does not resolve: {e}", q.name))
                    .unwrap_or_else(|| panic!("{fixture}: '{}' names no weights", q.name));
                assert_eq!(resolved.weights, q.files, "{fixture}: '{}'", q.name);
                assert_eq!(resolved.quantization.as_deref(), Some(q.name.as_str()));
            }
        }
    }

    /// A quantization a folder names, beside a head with its tag and none for `Q8_0`: Copilot's
    /// case on #15590.
    fn folder_and_own_head() -> Vec<ListedFile> {
        listing(&[
            ("gguf/Model-Q4_K_M.gguf", 4 * GB),
            ("MTP/mtp-Model-Q4_K_M.gguf", GB / 10),
        ])
    }

    /// Weights named by their tag alone, beside heads named for the model, none of them
    /// `Q8_0`: Copilot's case on #15590.
    fn bare_tags() -> Vec<ListedFile> {
        listing(&[
            ("Q4_K_M.gguf", 4 * GB),
            ("BF16.gguf", 16 * GB),
            ("MTP/mtp-Model-Q4_K_M.gguf", GB / 10),
            ("MTP/mtp-Model-BF16.gguf", GB / 5),
        ])
    }

    /// A tag's `UD-` prefix, in any case, is dropped to find the head of its plain tag when the
    /// repository has no `Q8_0` head: Copilot's case on #15590.
    #[test]
    fn a_ud_tag_in_any_case_drafts_with_the_head_of_its_plain_tag() {
        for weights in [
            "Model-UD-Q4_K_XL.gguf",
            "Model-ud-Q4_K_XL.gguf",
            "Model-Ud-Q4_K_XL.gguf",
        ] {
            let files = listing(&[(weights, 4 * GB), ("MTP/mtp-Model-Q4_K_XL.gguf", GB / 10)]);
            let qs = quantizations(&files);
            assert_eq!(qs.len(), 1, "{weights}");
            assert_eq!(
                draft_head(&files, &qs[0]),
                ["MTP/mtp-Model-Q4_K_XL.gguf"],
                "{weights}"
            );
        }
    }

    #[test]
    fn weights_named_by_their_tag_alone_draft_with_the_head_of_that_tag() {
        let files = bare_tags();
        for (name, weights, head) in [
            ("Q4_K_M", "Q4_K_M.gguf", "MTP/mtp-Model-Q4_K_M.gguf"),
            ("BF16", "BF16.gguf", "MTP/mtp-Model-BF16.gguf"),
        ] {
            let resolved = resolve(&files, &[name])
                .unwrap_or_else(|e| panic!("'{name}' does not resolve: {e}"))
                .unwrap_or_else(|| panic!("'{name}' names no weights"));
            assert_eq!(resolved.weights, [weights], "{name}");
            assert_eq!(resolved.draft, [head], "{name}");
        }
    }

    /// Two quantizations in one folder, which go by their tags.
    fn tags_in_one_folder() -> Vec<ListedFile> {
        listing(&[
            ("gguf/Model-Q4_K_M.gguf", 4 * GB),
            ("gguf/Model-Q8_0.gguf", 8 * GB),
        ])
    }

    /// Two quantizations in a folder, beside a folder named like one of their tags that holds
    /// two others: Copilot's case on #15590.
    fn tag_meets_folder() -> Vec<ListedFile> {
        listing(&[
            ("gguf/Model-Q4_K_M.gguf", 4 * GB),
            ("gguf/Model-Q8_0.gguf", 8 * GB),
            ("Q4_K_M/Other-Q2_K.gguf", 2 * GB),
            ("Q4_K_M/Other-Q8_0.gguf", 8 * GB),
        ])
    }

    /// A file with no quantization tag beside a tagged one in their folder: Copilot's case on
    /// #15590.
    fn untagged_in_a_folder() -> Vec<ListedFile> {
        listing(&[
            ("gguf/model.gguf", 4 * GB),
            ("gguf/Other-Q8_0.gguf", 8 * GB),
        ])
    }

    /// A quantization alone in a folder whose name ends in `.gguf`: Copilot's case on #15590.
    fn folder_named_like_a_file() -> Vec<ListedFile> {
        listing(&[("model.gguf/Model-Q4_K_M.gguf", 4 * GB)])
    }

    /// A quantization alone in a folder whose name ends like weights of another format: Copilot's
    /// case on #15590, whose folder name the Spicepod's `files` filter drops.
    fn folder_named_like_other_weights() -> Vec<ListedFile> {
        listing(&[
            ("Q4_K_M.bin/Model-Q4_K_M.gguf", 4 * GB),
            ("Model-Q8_0.gguf", 8 * GB),
        ])
    }

    /// A draft head published as a series of two shards.
    fn sharded_head() -> Vec<ListedFile> {
        listing(&[
            ("Model-Q4_K_M.gguf", 4 * GB),
            ("MTP/mtp-Model-Q8_0-00001-of-00002.gguf", GB / 10),
            ("MTP/mtp-Model-Q8_0-00002-of-00002.gguf", GB / 10),
        ])
    }

    #[test]
    fn a_quantization_named_by_its_folder_drafts_with_the_head_of_its_tag() {
        let files = folder_and_own_head();
        let qs = quantizations(&files);
        assert_eq!(names(&qs), ["gguf"]);
        let resolved = resolve(&files, &["gguf"])
            .expect("resolves")
            .expect("names files");
        assert_eq!(resolved.draft, ["MTP/mtp-Model-Q4_K_M.gguf"]);
        assert_eq!(draft_head(&files, &qs[0]), ["MTP/mtp-Model-Q4_K_M.gguf"]);
        // a `Q8_0` head is still preferred
        let mut with_q8 = files;
        with_q8.extend(listing(&[("MTP/mtp-Model-Q8_0.gguf", GB / 5)]));
        assert_eq!(
            resolve(&with_q8, &["gguf"])
                .expect("resolves")
                .expect("names files")
                .draft,
            ["MTP/mtp-Model-Q8_0.gguf"]
        );
    }

    /// The `files` a load log advertises load exactly what choosing that quantization with no
    /// `files` loads, its draft head included, whatever its name is, and load it again from a
    /// cache holding those files and any part of the rest of the repository, which is what the
    /// repository lists with the Hub out of reach.
    #[test]
    fn every_pin_loads_what_choosing_its_quantization_loads() {
        let mut path_named = fallback_meets_folder();
        path_named.extend(listing(&[("MTP/mtp-Model-Q8_0.gguf", GB / 5)]));
        let fixtures = [
            ("qwen38", qwen38()),
            ("glm", glm()),
            ("folder_and_own_head", folder_and_own_head()),
            ("fallback_meets_folder_with_a_head", path_named),
            ("tags_in_one_folder", tags_in_one_folder()),
            ("tag_meets_folder", tag_meets_folder()),
            ("folder_named_like_a_tag", folder_named_like_a_tag()),
            ("untagged_in_a_folder", untagged_in_a_folder()),
            ("folder_named_like_a_file", folder_named_like_a_file()),
            (
                "folder_named_like_other_weights",
                folder_named_like_other_weights(),
            ),
            ("sharded_head", sharded_head()),
            ("bare_tags", bare_tags()),
        ];
        let mut pinned_a_head = false;
        let mut pinned_a_sharded_head = false;
        for (fixture, files) in fixtures {
            for q in &quantizations(&files) {
                let head = draft_head(&files, q);
                pinned_a_head |= !head.is_empty();
                pinned_a_sharded_head |= head.len() > 1;
                let pin = pin(&files, q);
                // the pin as a Spicepod's `files` carries it to resolution
                let mut model = spicepod::component::model::Model::new(
                    "huggingface:huggingface.co/org/Model-GGUF",
                    "model",
                );
                model.files = pin
                    .iter()
                    .map(|p| ModelFile::from_path(Path::new(p)))
                    .collect();
                assert_eq!(
                    model.gguf_selectors(),
                    pin,
                    "{fixture}: the Spicepod's `files` keep all of {pin:?}"
                );
                let entries: Vec<&str> = pin.iter().map(String::as_str).collect();
                let resolved = resolve(&files, &entries)
                    .unwrap_or_else(|e| panic!("{fixture}: {pin:?} does not resolve: {e}"))
                    .unwrap_or_else(|| panic!("{fixture}: {pin:?} names no weights"));
                assert_eq!(resolved.weights, q.files, "{fixture}: {pin:?}");
                assert_eq!(resolved.draft, head, "{fixture}: {pin:?}");
                // every subset of the files the pin did not download, beside those it did
                let rest: Vec<&ListedFile> = files
                    .iter()
                    .filter(|f| !resolved.files().contains(&f.path.as_str()))
                    .collect();
                assert!(rest.len() <= 10, "{fixture}: {} subsets", 1 << rest.len());
                for subset in 0..1_u32 << rest.len() {
                    let cached: Vec<ListedFile> = files
                        .iter()
                        .filter(|f| {
                            resolved.files().contains(&f.path.as_str())
                                || rest
                                    .iter()
                                    .position(|r| r.path == f.path)
                                    .is_some_and(|i| subset & (1 << i) != 0)
                        })
                        .cloned()
                        .collect();
                    let paths: Vec<&str> = cached.iter().map(|f| f.path.as_str()).collect();
                    let again = resolve(&cached, &entries)
                        .unwrap_or_else(|e| {
                            panic!("{fixture}: {pin:?} does not resolve from {paths:?}: {e}")
                        })
                        .unwrap_or_else(|| panic!("{fixture}: {pin:?} names nothing in {paths:?}"));
                    assert_eq!(
                        (&again.weights, &again.draft),
                        (&resolved.weights, &resolved.draft),
                        "{fixture}: {pin:?} from {paths:?}"
                    );
                }
            }
        }
        assert!(pinned_a_head, "a fixture's quantization drafts with a head");
        assert!(
            pinned_a_sharded_head,
            "a fixture's quantization drafts with a sharded head"
        );
        // the quantization named by its path is pinned with its head beside it
        let mut files = fallback_meets_folder();
        files.extend(listing(&[("MTP/mtp-Model-Q8_0.gguf", GB / 5)]));
        let by_path = quantizations(&files)
            .into_iter()
            .find(|q| q.name == "Model-Q4_K_M.gguf")
            .expect("a quantization named by its path");
        assert_eq!(
            pin(&files, &by_path),
            ["Model-Q4_K_M.gguf", "MTP/mtp-Model-Q8_0.gguf"]
        );
    }

    /// A folder named like a quantization's tag, holding two others: Copilot's case on #15590.
    fn folder_named_like_a_tag() -> Vec<ListedFile> {
        listing(&[
            ("Model-Q4_K_M.gguf", 4 * GB),
            ("Q4_K_M/A-Q8_0.gguf", 8 * GB),
            ("Q4_K_M/B-Q5_K_M.gguf", 5 * GB),
        ])
    }

    /// A tag a folder bears names neither the quantization with that tag nor any in the folder,
    /// which a cache holding only that one would name by it.
    #[test]
    fn a_tag_another_quantizations_folder_bears_is_not_a_name() {
        assert_eq!(
            names(&quantizations(&tag_meets_folder())),
            [
                "Q4_K_M/Other-Q8_0",
                "gguf/Model-Q8_0",
                "gguf/Model-Q4_K_M",
                "Q2_K"
            ]
        );
        assert_eq!(
            names(&quantizations(&folder_named_like_a_tag())),
            ["Q8_0", "Q5_K_M", "Model-Q4_K_M"]
        );
    }

    #[test]
    fn a_trailing_slash_names_only_a_folder() {
        let files = folder_named_like_a_tag();
        assert_eq!(
            resolve(&files, &["Q4_K_M/"]),
            Err(ResolveError::AmbiguousQuantization {
                name: "Q4_K_M".to_string(),
                quantizations: vec!["Q8_0".to_string(), "Q5_K_M".to_string()],
            })
        );
        // Without the slash the tag matches too, and a cache holding one of the folder's two
        // would take it for the folder's, so the name is refused and the tagged quantization
        // goes by its path.
        assert_eq!(
            resolve(&files, &["Q4_K_M"]),
            Err(ResolveError::AmbiguousQuantization {
                name: "Q4_K_M".to_string(),
                quantizations: vec![
                    "Q8_0".to_string(),
                    "Q5_K_M".to_string(),
                    "Model-Q4_K_M".to_string()
                ],
            })
        );
        assert_eq!(
            resolve(&files, &["Model-Q4_K_M"])
                .expect("resolves")
                .expect("names files")
                .weights,
            ["Model-Q4_K_M.gguf"]
        );
    }

    #[test]
    fn another_file_of_the_repository_is_left_to_the_engine() {
        assert_eq!(resolve(&one_folder(), &["config.json"]), Ok(None));
        let resolved = resolve(&one_folder(), &["config.json", "Q8_0"])
            .expect("resolves")
            .expect("names weights");
        assert_eq!(resolved.weights, ["gguf/Model-Q8_0.gguf"]);
        // A repository with no GGUF quantization leaves every entry to the engine.
        let safetensors = listing(&[("model.safetensors", 4 * GB), ("tokenizer.model", 1_000)]);
        assert_eq!(
            resolve(&safetensors, &["tokenizer.model", "extra.v1"]),
            Ok(None)
        );
    }

    /// A partial layout of a quantization beside a complete one with the same stem, as a
    /// re-split upload left half done, is its own series: it neither hides the complete layout
    /// nor counts as one.
    #[test]
    fn a_partial_layout_does_not_hide_a_complete_one_with_the_same_stem() {
        let files = listing(&[
            ("Qwen3-0.6B-Q4_K_M-00001-of-00002.gguf", 3 * GB),
            ("Qwen3-0.6B-Q4_K_M-00002-of-00002.gguf", 2 * GB),
            ("Qwen3-0.6B-Q4_K_M-00001-of-00003.gguf", 2 * GB),
        ]);
        let found = quantizations(&files);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(
            found[0].files,
            [
                "Qwen3-0.6B-Q4_K_M-00001-of-00002.gguf",
                "Qwen3-0.6B-Q4_K_M-00002-of-00002.gguf"
            ]
        );
        assert_eq!(found[0].bytes, 5 * GB);
        assert_eq!(found[0].name, "Q4_K_M");
    }

    /// Naming every shard of a series loads each once and still names its quantization.
    #[test]
    fn naming_every_shard_identifies_the_quantization() {
        let files = listing(&[
            ("Qwen3-0.6B-Q4_K_M-00001-of-00002.gguf", 3 * GB),
            ("Qwen3-0.6B-Q4_K_M-00002-of-00002.gguf", 2 * GB),
        ]);
        let resolved = resolve(
            &files,
            &[
                "Qwen3-0.6B-Q4_K_M-00001-of-00002.gguf",
                "Qwen3-0.6B-Q4_K_M-00002-of-00002.gguf",
            ],
        )
        .expect("the files resolve")
        .expect("GGUF files");
        assert_eq!(
            resolved.weights,
            [
                "Qwen3-0.6B-Q4_K_M-00001-of-00002.gguf",
                "Qwen3-0.6B-Q4_K_M-00002-of-00002.gguf"
            ]
        );
        assert_eq!(resolved.quantization.as_deref(), Some("Q4_K_M"));
    }
}
