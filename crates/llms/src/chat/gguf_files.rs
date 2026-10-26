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
use std::sync::LazyLock;

use regex::Regex;
use snafu::Snafu;

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
    /// (`UD-Q4_K_XL`, `Q8_0`, `BF16`), or its path when two would share a tag.
    pub name: String,
    /// Repository paths, shard 1 first.
    pub files: Vec<String>,
    /// Total bytes of its files.
    pub bytes: u64,
}

/// What a model loads: the weight files, in shard order, and the draft head if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub weights: Vec<String>,
    pub draft: Option<String>,
    /// The quantization the weights are, when they are one.
    pub quantization: Option<String>,
}

impl Resolved {
    /// Every file to download.
    #[must_use]
    pub fn files(&self) -> Vec<&str> {
        self.weights
            .iter()
            .chain(self.draft.as_ref())
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
    /// A folder that holds several quantizations, which would each be its name.
    #[snafu(display(
        "'{name}' holds {} quantizations of the repository, {}; name one of them",
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

/// A quantization tag at the end of a file stem: `Q4_K_M`, `UD-Q2_K_XL`, `IQ4_XS`, `BF16`, …
static TAG: LazyLock<Option<Regex>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)[-_.](?P<tag>(?:UD-)?(?:I?Q\d(?:_[0-9A-Z]+)*|BF16|F16|F32|MXFP4(?:_MOE)?|NVFP4|TQ\d_\d))$",
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
    // Files grouped by (folder, stem without the shard suffix).
    let mut groups: BTreeMap<(String, String), Vec<&str>> = BTreeMap::new();
    for file in listing {
        let path = file.path.as_str();
        if !is_gguf(path) || is_draft_head(path) || is_projector(path) {
            continue;
        }
        let stem = shard(path).map_or_else(|| path.to_string(), |(stem, _, _)| stem);
        groups
            .entry((folder(path).to_string(), stem))
            .or_default()
            .push(path);
    }
    let mut complete = Vec::new();
    for ((dir, stem), mut files) in groups {
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
    let mut in_folder: BTreeMap<String, usize> = BTreeMap::new();
    for (dir, _, _) in &complete {
        *in_folder.entry(dir.clone()).or_default() += 1;
    }
    let mut out = Vec::new();
    let mut paths = Vec::new();
    for (dir, stem, files) in complete {
        // A folder names the one quantization it holds; several in one folder go by their tags.
        let name = if !dir.is_empty() && in_folder.get(&dir) == Some(&1) {
            dir.rsplit('/').next().unwrap_or(&dir).to_string()
        } else {
            tag(files[0])
                .unwrap_or_else(|| file_name(files[0]).trim_end_matches(".gguf").to_string())
        };
        let bytes = files
            .iter()
            .map(|f| size.get(f).copied().unwrap_or(0))
            .sum();
        paths.push(stem.trim_end_matches(".gguf").to_string());
        out.push(Quantization {
            name,
            files: files.into_iter().map(ToString::to_string).collect(),
            bytes,
        });
    }
    // Two quantizations that would share a name, as one tag in two folders, go by their paths.
    let mut uses: BTreeMap<String, usize> = BTreeMap::new();
    for q in &out {
        *uses.entry(q.name.to_ascii_lowercase()).or_default() += 1;
    }
    for (q, path) in out.iter_mut().zip(paths) {
        if uses
            .get(&q.name.to_ascii_lowercase())
            .is_some_and(|&n| n > 1)
        {
            q.name = path;
        }
    }
    // Largest first; a stable tie-break by name keeps the order reproducible.
    out.sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.name.cmp(&b.name)));
    out
}

/// The repository's MTP draft head for `quantization`: a `Q8_0` head if it offers one,
/// otherwise the head with the quantization's own tag. `shared` variants, which expect the
/// target's own embeddings, are passed over.
#[must_use]
pub fn draft_head(listing: &[ListedFile], quantization: &str) -> Option<String> {
    let heads: Vec<&str> = listing
        .iter()
        .map(|f| f.path.as_str())
        .filter(|p| is_gguf(p) && is_draft_head(p))
        .filter(|p| !file_name(p).to_ascii_lowercase().contains("-shared"))
        .collect();
    let with_tag = |want: &str| {
        heads
            .iter()
            .find(|p| tag(p).is_some_and(|t| t.eq_ignore_ascii_case(want)))
            .map(|p| (*p).to_string())
    };
    with_tag("Q8_0").or_else(|| {
        let own = quantization.trim_start_matches("UD-");
        with_tag(quantization).or_else(|| with_tag(own))
    })
}

/// Resolves a model's `files` against the repository listing. An exact `.gguf` path is
/// honoured, with the sibling shards of a shard; any other entry names a quantization, by its
/// name or the folder that holds only it, which brings all its shards and, unless a draft head
/// is named, the repository's draft head. Another file the repository has is left to the
/// engine, and so is every entry of a repository that offers no GGUF quantization. `Ok(None)`
/// when `files` names no GGUF weights.
///
/// # Errors
///
/// Returns an error when a named file is missing, a shard series is incomplete, a folder holds
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
    let mut draft = None;
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
                draft = Some(entry.to_string());
                continue;
            }
            match shard(entry) {
                Some((stem, _, n)) => {
                    let series: Vec<String> = (1..=n).map(|i| shard_path(&stem, i, n)).collect();
                    let missing: Vec<String> = series
                        .iter()
                        .filter(|p| !present.contains(p.as_str()))
                        .cloned()
                        .collect();
                    if !missing.is_empty() {
                        return Err(ResolveError::IncompleteShards {
                            path: entry.to_string(),
                            missing,
                        });
                    }
                    weights.extend(series);
                }
                None => weights.push(entry.to_string()),
            }
        } else {
            let name = entry.trim_end_matches('/');
            let named = quantizations
                .iter()
                .find(|q| q.name.eq_ignore_ascii_case(name));
            let in_folder: Vec<&Quantization> = quantizations
                .iter()
                .filter(|q| q.files.iter().all(|f| folder(f).eq_ignore_ascii_case(name)))
                .collect();
            let q = match (named, in_folder.as_slice()) {
                (Some(q), _) => q,
                (None, [q]) => *q,
                (None, []) if present.contains(entry) || quantizations.is_empty() => continue,
                (None, []) => {
                    return Err(ResolveError::UnknownQuantization {
                        name: name.to_string(),
                        available: quantizations.iter().map(|q| q.name.clone()).collect(),
                    });
                }
                (None, several) => {
                    return Err(ResolveError::AmbiguousQuantization {
                        name: name.to_string(),
                        quantizations: several.iter().map(|q| q.name.clone()).collect(),
                    });
                }
            };
            weights.extend(q.files.iter().cloned());
            named_quantization = Some(q.name.clone());
        }
    }
    weights.dedup();
    if weights.is_empty() && draft.is_none() {
        return Ok(None);
    }
    if let Some(name) = &named_quantization {
        if draft.is_none() {
            draft = draft_head(listing, name);
        }
        quantization = Some(name.clone());
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
        assert_eq!(
            resolved.draft.as_deref(),
            Some("MTP/mtp-Qwen3.8-Flash-Next-Q8_0.gguf")
        );
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
        assert_eq!(flat.draft, None);
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
        assert_eq!(resolved.draft, None);
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
        assert_eq!(
            with_head.draft.as_deref(),
            Some("MTP/mtp-Qwen3.8-Flash-Next-Q4_K_M.gguf")
        );
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
            "'gguf' holds 2 quantizations of the repository, Q8_0, Q4_K_M; name one of them"
        );
        // The quantization a budget chooses is pinned by the name it is advertised under.
        let chosen = choose(&qs, 5 * GB).expect("Q4_K_M fits");
        let pinned = resolve(&one_folder(), &[chosen.name.as_str()])
            .expect("the advertised name resolves")
            .expect("it names weights");
        assert_eq!(pinned.weights, ["gguf/Model-Q4_K_M.gguf"]);
        assert_eq!(pinned.weights, chosen.files);
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

    /// Every quantization resolves, by the name it is listed and advertised under, to exactly
    /// its own files.
    #[test]
    fn every_quantization_name_resolves_to_its_own_files() {
        let fixtures = [
            ("glm", glm()),
            ("qwen38", qwen38()),
            ("qwen3", qwen3()),
            ("one_folder", one_folder()),
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
}
