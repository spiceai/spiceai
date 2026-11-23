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
//! name or folder, or nothing; [`resolve`] turns each into the files to download, and with
//! none named [`default_quantization`] picks llama.cpp's default, which [`choose`] replaces with
//! the largest that fits when it does not.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;
use snafu::Snafu;
use spicepod::component::model::ModelFile;

pub use huggingface_hub::printable;

/// How many names a load line or an error lists. The repository decides how many quantizations
/// and shards it offers, so a list of them is bounded; repositories offer a few dozen at most.
pub const LISTED: usize = 40;

/// `items` joined with `, ` for a load line or an error, each [`printable`]. Past [`LISTED`]
/// items, the rest are counted: `a, b, c, and 3 more`.
#[must_use]
pub fn listed<I>(items: I) -> String
where
    I: IntoIterator,
    I::IntoIter: ExactSizeIterator,
    I::Item: AsRef<str>,
{
    let items = items.into_iter();
    let more = items.len().saturating_sub(LISTED);
    let named = items
        .take(LISTED)
        .map(|item| printable(item.as_ref()))
        .collect::<Vec<_>>()
        .join(", ");
    if more == 0 {
        named
    } else {
        format!("{named}, and {more} more")
    }
}

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
    /// The drafter's files, shard 1 first, which the engine opens: an MTP draft head, or a
    /// `DFlash` drafter its header tells; empty without one.
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
    #[snafu(display("the repository has no file '{}'", printable(path)))]
    MissingFile { path: String },
    /// A shard series some of whose shards the repository lacks.
    #[snafu(display(
        "'{}' is a shard of a series the repository lacks part of: {}",
        printable(path),
        listed(missing)
    ))]
    IncompleteShards { path: String, missing: Vec<String> },
    /// A name several quantizations answer to: a folder that holds several, or a tag one has
    /// and another's folder bears.
    #[snafu(display(
        "'{}' matches {} quantizations of the repository, {}; name one of them",
        printable(name),
        quantizations.len(),
        listed(quantizations)
    ))]
    AmbiguousQuantization {
        name: String,
        quantizations: Vec<String>,
    },
    /// A name that is neither a file nor a quantization of the repository.
    #[snafu(display(
        "the repository has no file or quantization '{}'; its quantizations are {}",
        printable(name),
        listed(available)
    ))]
    UnknownQuantization {
        name: String,
        available: Vec<String>,
    },
    /// A name only importance matrices or drafters answer to, which are not the model's weights
    /// though their names carry its quantization's tag.
    #[snafu(display(
        "'{}' names only {}, which Spice, like llama.cpp, takes by name for an importance matrix or a drafter, not the model's weights; to load one as the weights, name its path in `files`",
        printable(name),
        listed(files)
    ))]
    NamedSidecar { name: String, files: Vec<String> },
    /// `files` that name several sets of the model's weights, by name or by path, of which a
    /// model loads one: each a quantization, or a file or shard series no quantization holds.
    #[snafu(display(
        "`files` names {} sets of model weights, {}, and a model loads one; name one of them",
        weights.len(),
        listed(weights)
    ))]
    SeveralWeights {
        weights: Vec<String>,
        /// The file each set opens from, in the same order: its first shard, or itself.
        opening: Vec<String>,
    },
}

/// What a listing holds: the repository's files, as the Hub lists them, or only those the
/// Hugging Face cache holds, with the Hub not read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListedFrom {
    Repository,
    Cache,
}

impl ResolveError {
    /// The clause that says why, for a listing `from` holds: of the cache, it reads after
    /// "where", with the cache named, and says what is not there, never that the repository,
    /// which was not read, lacks it.
    #[must_use]
    pub fn clause(&self, from: ListedFrom) -> String {
        match (self, from) {
            (Self::MissingFile { path }, ListedFrom::Cache) => {
                format!("there is no file '{}'", printable(path))
            }
            (Self::IncompleteShards { path, missing }, ListedFrom::Cache) => format!(
                "'{}' is a shard of a series whose shards {} are not there",
                printable(path),
                listed(missing)
            ),
            (
                Self::AmbiguousQuantization {
                    name,
                    quantizations,
                },
                ListedFrom::Cache,
            ) => format!(
                "'{}' matches {} quantizations there, {}; name one of them",
                printable(name),
                quantizations.len(),
                listed(quantizations)
            ),
            (Self::UnknownQuantization { name, available }, ListedFrom::Cache) => format!(
                "there is no file or quantization '{}'; the quantizations there are {}",
                printable(name),
                listed(available)
            ),
            (Self::SeveralWeights { weights, .. }, ListedFrom::Cache) => format!(
                "`files` names {} sets of model weights there, {}, and a model loads one; name one of them",
                weights.len(),
                listed(weights)
            ),
            _ => self.to_string(),
        }
    }
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

/// A vision projector, which a text model does not load: a file whose name holds `mmproj`, as
/// llama.cpp's `-hf` tells them apart. Spice matches it in any case, where llama.cpp matches it
/// as written.
fn is_projector(path: &str) -> bool {
    file_name(path).to_ascii_lowercase().contains("mmproj")
}

/// Whether the GGUF file at `path` holds a model's weights, by its name: not a vision projector,
/// an importance matrix or a drafter, which are named beside a model's weights, so that the model
/// opens from the weights and not from whichever of them comes first.
#[must_use]
pub fn is_model_weights(path: &str) -> bool {
    !is_projector(path) && !is_sidecar(path)
}

/// The file of a model's draft files the engine drafts with: the first `-00001-of-` shard, else
/// the first, as it opens a sharded drafter from its first shard.
#[must_use]
pub fn drafting_file(draft: &[String]) -> Option<&str> {
    first_shard(&draft.iter().map(String::as_str).collect::<Vec<_>>())
}

/// The first of `files` whose file name holds `-00001-of-`, else the first, as llama.cpp's
/// engine opens a sharded model or drafter: by the file's name, not the folders above it.
fn first_shard<'a>(files: &[&'a str]) -> Option<&'a str> {
    files
        .iter()
        .find(|f| file_name(f).contains("-00001-of-"))
        .or_else(|| files.first())
        .copied()
}

/// Whether the GGUF file at `path` is the one its set of weights opens from: a lone file, or the
/// first shard of a series.
#[must_use]
pub fn opens_a_set(path: &str) -> bool {
    shard(path).is_none_or(|(_, index, _)| index == 1)
}

/// The file of `files` that an engine taking one file opens the model from. An MTP draft head
/// is set aside first, as the engine sets it aside to draft with; of the rest, the first that
/// holds the weights by name ([`is_model_weights`]), else the first shard 1 of the rest, as the
/// engine opens it, else the first of the rest, else the first.
#[must_use]
pub fn model_file(files: &[String]) -> Option<&str> {
    let trunk = || {
        files
            .iter()
            .map(String::as_str)
            .filter(|f| !is_draft_head(f))
    };
    trunk()
        .find(|f| is_model_weights(f))
        .or_else(|| first_shard(&trunk().collect::<Vec<_>>()))
        .or_else(|| files.first().map(String::as_str))
}

/// A file that is not the model's weights though it is a GGUF file: an importance matrix, or a
/// `dflash-`, `eagle3-` or `dspark-` drafter, as llama.cpp's `-hf` tells them apart by name.
/// Spice matches the names in any case, where llama.cpp matches them as written.
fn is_sidecar(path: &str) -> bool {
    let name = file_name(path).to_ascii_lowercase();
    ["imatrix", "dflash-", "eagle3-", "dspark-"]
        .iter()
        .any(|keyword| name.contains(keyword))
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

/// The model's complete quantizations in the listing, largest first. Draft heads, drafters,
/// importance matrices and projectors are not quantizations of the model; a shard series with
/// a shard missing is not complete and is left out.
#[must_use]
pub fn quantizations(listing: &[ListedFile]) -> Vec<Quantization> {
    quantizations_of(listing, false)
}

/// The complete series in the listing, named as [`quantizations`] names them: of the model's
/// weights, or, with `sidecars`, of the importance matrices and drafters alone.
fn quantizations_of(listing: &[ListedFile], sidecars: bool) -> Vec<Quantization> {
    let size: BTreeMap<&str, u64> = listing.iter().map(|f| (f.path.as_str(), f.size)).collect();
    // Files grouped by (folder, stem without the shard suffix, shard count): a series is one
    // shard count, so a partial layout of another count beside it neither hides it nor joins it.
    let mut groups: BTreeMap<(String, String, Option<u32>), Vec<&str>> = BTreeMap::new();
    for file in listing {
        let path = file.path.as_str();
        // A path `resolve` would read as another's names that other file in any pin.
        if !is_gguf(path)
            || normalized(path) != path
            || is_draft_head(path)
            || is_projector(path)
            || is_sidecar(path) != sidecars
        {
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
    // name until none holds, which its first file's path guarantees. So does a name that starts
    // with whitespace, as a folder's deeper in the path, which `resolve` would read as another
    // name; its path, which no quantization's starts with, does not.
    // How many quantizations claim each name, so that whether another claims one is a lookup
    // and not a scan of every other quantization's claims.
    let mut claimants: BTreeMap<&str, usize> = BTreeMap::new();
    for claim in claims.iter().flatten() {
        *claimants.entry(claim.as_str()).or_default() += 1;
    }
    let claimed_by_another = |i: usize, name: &str| {
        let name = name.to_ascii_lowercase();
        let own = usize::from(claims[i].contains(&name));
        claimants.get(name.as_str()).copied().unwrap_or(0) > own
    };
    for (i, (q, names)) in out.iter_mut().zip(&candidates).enumerate() {
        let mut at = 0;
        while at + 1 < names.len()
            && (is_gguf(&q.name)
                || normalized(&q.name) != q.name
                || !ModelFile::from_path(Path::new(&q.name)).is_gguf_selector()
                || claimed_by_another(i, &q.name))
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
        .filter(|p| is_gguf(p) && is_draft_head(p) && normalized(p) == *p)
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

/// The `files` entries that load what choosing `quantization` with no `files` loads, from a
/// listing `from` holds. From the repository's, its name ([`pin`]). From the cache's alone, the
/// path of its first file and of its draft head's: a name read from one cache, such as that of
/// a folder that holds one quantization there, can name another quantization in another node's
/// cache, where a path names the same file or nothing.
#[must_use]
pub fn pin_from(
    listing: &[ListedFile],
    quantization: &Quantization,
    from: ListedFrom,
) -> Vec<String> {
    match from {
        ListedFrom::Repository => pin(listing, quantization),
        ListedFrom::Cache => quantization
            .files
            .first()
            .into_iter()
            .cloned()
            // a shard of a series names its whole series
            .chain(draft_head(listing, quantization).into_iter().next())
            .collect(),
    }
}

/// `entries` as a model's `files` in YAML flow style, `{path: …}` each and comma-separated, for
/// a load line or an error to quote, so that it can be pasted into a Spicepod as it is: each path
/// written as `yaml_scalar` writes it.
#[must_use]
pub fn files_entries(entries: &[String]) -> String {
    entries
        .iter()
        .map(|entry| format!("{{path: {}}}", yaml_scalar(entry)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `text` as a YAML scalar that reads back as `text` within a flow collection, in printable
/// ASCII, so that the line quoting it stays one line and shows what it holds. It is plain when it
/// starts with an ASCII letter, holds only ASCII letters, digits, `_`, `-`, `.` and `/`, and is
/// not a word YAML reads as a boolean or null, as a quantization's name is. Any other text is
/// double-quoted, with `"` and `\` escaped and every character outside printable ASCII written
/// as its `\u` or `\U` escape.
fn yaml_scalar(text: &str) -> String {
    // the words YAML 1.1 or 1.2 reads as a boolean or null, in any case
    const NOT_STRINGS: [&str; 9] = ["true", "false", "yes", "no", "on", "off", "y", "n", "null"];
    let plain = text.starts_with(|c: char| c.is_ascii_alphabetic())
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'))
        && !NOT_STRINGS
            .iter()
            .any(|word| text.eq_ignore_ascii_case(word));
    if plain {
        return text.to_string();
    }
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for c in text.chars() {
        match c {
            '"' | '\\' => {
                quoted.push('\\');
                quoted.push(c);
            }
            ' '..='~' => quoted.push(c),
            _ => {
                let code = u32::from(c);
                let escape = if code <= 0xFFFF {
                    format!("\\u{code:04X}")
                } else {
                    format!("\\U{code:08X}")
                };
                quoted.push_str(&escape);
            }
        }
    }
    quoted.push('"');
    quoted
}

/// A `files` entry as `resolve` reads it: without the whitespace around it or a leading `./`.
fn normalized(entry: &str) -> &str {
    entry.trim().trim_start_matches("./")
}

/// Whether the `files` entry `entry` names `quantization`: the folder that holds it, or, unless
/// a trailing `/` makes the entry a folder, its name, its tag or its path stem.
fn answers_to(quantization: &Quantization, entry: &str) -> bool {
    let name = entry.trim_end_matches('/');
    quantization
        .files
        .iter()
        .all(|f| folder(f).eq_ignore_ascii_case(name))
        || (!entry.ends_with('/')
            && (quantization.name.eq_ignore_ascii_case(name)
                || aliases(quantization).any(|alias| alias.eq_ignore_ascii_case(name))))
}

/// Resolves a model's `files` against the repository listing. An exact `.gguf` path is
/// honoured, with the sibling shards of a shard, a draft head's included; any other entry names
/// a quantization, by its name, the folder that holds only it, or its tag or path stem, which
/// brings all its shards and, unless a draft head is named, the repository's draft head; a
/// drafter told by its header replaces that head through [`resolve_with_drafters`].
/// Another file the repository has is left to the engine, and so is every entry of a
/// repository that offers no GGUF quantization. `Ok(None)` when `files` names no GGUF weights.
///
/// # Errors
///
/// Returns an error when a named file is missing, a shard series is incomplete, a name matches
/// several quantizations, a name only importance matrices or drafters answer to, a name is
/// neither a file nor a quantization of the repository, or `files` name several sets of the
/// model's weights.
pub fn resolve(
    listing: &[ListedFile],
    files: &[&str],
) -> std::result::Result<Option<Resolved>, ResolveError> {
    resolve_with_drafters(listing, files, &BTreeSet::new())
}

/// [`resolve`], with the sets of weights that open from a file in `drafters` taken for drafters,
/// as the engine tells a `DFlash` drafter by its header: they are not counted as a second set of
/// weights, and they are the model's draft, after its weights, in place of an MTP head.
///
/// # Errors
///
/// As [`resolve`].
pub fn resolve_with_drafters(
    listing: &[ListedFile],
    files: &[&str],
    drafters: &BTreeSet<String>,
) -> std::result::Result<Option<Resolved>, ResolveError> {
    if files.is_empty() {
        return Ok(None);
    }
    let present: BTreeSet<&str> = listing.iter().map(|f| f.path.as_str()).collect();
    let quantizations = quantizations(listing);
    // The importance matrices and drafters, named as quantizations, for a name that answers to
    // none of the model's.
    let sidecars = std::cell::LazyCell::new(|| quantizations_of(listing, true));
    let mut weights = Vec::new();
    let mut draft = Vec::new();
    let mut quantization = None;
    let mut named_quantization = None;
    for &entry in files {
        // Every node reads an entry alike, whatever its listing holds.
        let entry = normalized(entry);
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
            // A listing of fewer files, as the cache's with the Hub out of reach, names by its
            // folder what the whole listing named by its tag, so a name more than one answers to
            // names none.
            let matched: Vec<&Quantization> = quantizations
                .iter()
                .filter(|q| answers_to(q, entry))
                .collect();
            if matched.is_empty() && !present.contains(entry) {
                // Passed over, such a name would leave a repository that also offers safetensors
                // to load those.
                let named: Vec<String> = sidecars
                    .iter()
                    .filter(|q| answers_to(q, entry))
                    .flat_map(|q| q.files.iter().cloned())
                    .collect();
                if !named.is_empty() {
                    return Err(ResolveError::NamedSidecar {
                        name: name.to_string(),
                        files: named,
                    });
                }
            }
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
            // A quantization the headers show to be a drafter loads beside the model, not as it.
            if !q.files.first().is_some_and(|f| drafters.contains(f)) {
                named_quantization = Some(q);
            }
        }
    }
    // Each entry naming a shard expands to its whole series, so naming two shards repeats it.
    let mut seen = BTreeSet::new();
    weights.retain(|w| seen.insert(w.clone()));
    draft.retain(|d| seen.insert(d.clone()));
    // A model loads one set of weights: given two, the engine would open the first and draft
    // with the second's head, while the load says the second is loaded. Each lone file and each
    // shard series is one set, whatever the listing names it, so the merged file and the shards
    // of one quantization are two. A projector, an importance matrix or a drafter named beside
    // the weights is none, and the engine does not open the model from it.
    let names: BTreeMap<&str, &str> = quantizations
        .iter()
        .flat_map(|q| q.files.iter().map(|f| (f.as_str(), q.name.as_str())))
        .collect();
    // The file a set opens from: its first shard, or itself.
    let opening =
        |w: &str| shard(w).map_or_else(|| w.to_string(), |(stem, _, n)| shard_path(&stem, 1, n));
    let mut sets: BTreeMap<(String, Option<u32>), (String, String)> = BTreeMap::new();
    for w in weights
        .iter()
        .filter(|w| is_model_weights(w) && !drafters.contains(&opening(w)))
    {
        let key = shard(w).map_or_else(|| (w.clone(), None), |(stem, _, n)| (stem, Some(n)));
        sets.entry(key).or_insert_with(|| {
            let name = names
                .get(w.as_str())
                .map_or_else(|| w.clone(), |n| (*n).to_string());
            (name, opening(w))
        });
    }
    if sets.len() > 1 {
        let (weights, opening) = sets.into_values().unzip();
        return Err(ResolveError::SeveralWeights { weights, opening });
    }
    // A drafter the headers tell drafts beside the weights, as the engine drafts with it, in
    // place of the MTP head a named quantization would bring. With no weights named beside it, as
    // a drafter named alone, what is named is loaded as named.
    let (mut weights, mut drafted): (Vec<String>, Vec<String>) = weights
        .into_iter()
        .partition(|w| !drafters.contains(&opening(w)));
    if !weights.iter().any(|w| is_model_weights(w)) {
        weights.append(&mut drafted);
    }
    draft.extend(drafted);
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

/// The quantization tags a model with no `files` loads by default, in order: those of
/// llama.cpp's `-hf`, so a repository loads the file `llama-server -hf <repo>` loads.
pub const DEFAULT_TAGS: [&str; 2] = ["Q4_K_M", "Q8_0"];

/// The quantization a model with no `files` loads by default, as llama.cpp's `-hf` picks it
/// from the Hub's listing, which lists a repository's files in path order: the first by path
/// whose first file's path carries [`DEFAULT_TAGS`]' `Q4_K_M`, else `Q8_0`, else the first by
/// path. A path carries a tag where the tag follows the path's start, `-`,
/// `.` or `/` and comes before `.` or `-`, in any case, so `Model-UD-Q4_K_M.gguf` carries
/// `Q4_K_M`, `Q4_K_M/model.gguf` does not (only its folder bears the tag), and `BF16` does not
/// carry `F16`. A merged file stands for the shards beside it (`M-Q4_K_M.gguf` and
/// `M-Q4_K_M-00001-of-00002.gguf` …), so it goes by the earlier of its path and its first
/// shard's in `listing`, where llama.cpp meets the quantization first. `None` when there is none.
#[must_use]
pub fn default_quantization<'q>(
    listing: &[ListedFile],
    quantizations: &'q [Quantization],
) -> Option<&'q Quantization> {
    // Where llama.cpp meets `q` in the listing: its first file's path, or, for a merged file,
    // the earlier of that and its first shard's.
    fn at<'a>(q: &'a Quantization, first_shards: &BTreeMap<String, &'a str>) -> &'a str {
        let first = q.files.first().map_or("", String::as_str);
        let shards = shard(first)
            .is_none()
            .then(|| first.strip_suffix(".gguf"))
            .flatten()
            .and_then(|stem| first_shards.get(stem).copied());
        shards.map_or(first, |shards| shards.min(first))
    }
    // The first shard of each series, by its stem, for a merged file that stands for one.
    let mut first_shards: BTreeMap<String, &str> = BTreeMap::new();
    for file in listing {
        if let Some((stem, 1, _)) = shard(&file.path) {
            first_shards
                .entry(stem)
                .and_modify(|first| *first = (*first).min(file.path.as_str()))
                .or_insert(file.path.as_str());
        }
    }
    let mut by_path: Vec<&Quantization> = quantizations.iter().collect();
    by_path.sort_by(|a, b| at(a, &first_shards).cmp(at(b, &first_shards)));
    DEFAULT_TAGS
        .iter()
        .find_map(|tag| by_path.iter().copied().find(|q| carries(q, tag)))
        .or_else(|| by_path.first().copied())
}

/// Whether `quantization`'s first file's path carries the quantization tag `tag` as a whole
/// token, as llama.cpp matches it: after the path's start, `-`, `.` or `/`, and before `.` or
/// `-`, in any case.
#[must_use]
pub fn carries(quantization: &Quantization, tag: &str) -> bool {
    let path = quantization
        .files
        .first()
        .map_or_else(String::new, |path| path.to_ascii_lowercase());
    let tag = tag.to_ascii_lowercase();
    path.match_indices(&tag).any(|(at, _)| {
        let before = path.get(..at).and_then(|head| head.chars().next_back());
        let after = path
            .get(at + tag.len()..)
            .and_then(|tail| tail.chars().next());
        matches!(before, None | Some('-' | '.' | '/')) && matches!(after, Some('.' | '-'))
    })
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

    /// A cut of unsloth/GLM-4.7-Flash-GGUF's listing (2026-11-21): single files at the top, and
    /// `BF16` in a folder of two shards, which sorts first by path.
    fn glm47() -> Vec<ListedFile> {
        listing(&[
            (
                "BF16/GLM-4.7-Flash-BF16-00001-of-00002.gguf",
                49_910_000_000,
            ),
            (
                "BF16/GLM-4.7-Flash-BF16-00002-of-00002.gguf",
                10_000_000_000,
            ),
            ("GLM-4.7-Flash-Q3_K_M.gguf", 14_610_000_000),
            ("GLM-4.7-Flash-Q4_K_M.gguf", 18_310_000_000),
            ("GLM-4.7-Flash-Q8_0.gguf", 31_840_000_000),
            ("GLM-4.7-Flash-UD-Q4_K_XL.gguf", 17_520_000_000),
            ("GLM-4.7-Flash-UD-TQ1_0.gguf", 8_330_000_000),
            ("README.md", 10_000),
        ])
    }

    /// llama.cpp meets a quantization offered both merged and split at its first shard, which
    /// sorts before the merged file's path, so the merged file that stands for it goes by the
    /// earlier of the two, and an `-alt` file between them is not the default.
    #[test]
    fn a_merged_file_goes_by_its_first_shard_for_the_default() {
        let merged = listing(&[
            ("M-Q4_K_M-00001-of-00002.gguf", GB),
            ("M-Q4_K_M-00002-of-00002.gguf", GB),
            ("M-Q4_K_M-alt.gguf", 2 * GB),
            ("M-Q4_K_M.gguf", 2 * GB),
        ]);
        let quantizations = quantizations(&merged);
        assert_eq!(
            default_quantization(&merged, &quantizations).map(|q| q.files.clone()),
            Some(vec!["M-Q4_K_M.gguf".to_string()])
        );
    }

    fn default_name(listing: &[ListedFile]) -> Option<String> {
        default_quantization(listing, &quantizations(listing)).map(|q| q.name.clone())
    }

    #[test]
    fn the_default_is_the_quantization_llama_cpp_loads() {
        // Q4_K_M where the repository offers it, whatever larger ones fit.
        assert_eq!(default_name(&glm47()).as_deref(), Some("Q4_K_M"));
        assert_eq!(default_name(&qwen3()).as_deref(), Some("Q4_K_M"));
        // Else Q8_0: GLM-5.3's complete quantizations are UD-Q2_K_XL and Q8_0, whose shards'
        // file names carry the tag.
        assert_eq!(default_name(&glm()).as_deref(), Some("Q8_0"));
        // Else the first by path: Qwen3.8's UD-Q2_K_XL/ before UD-Q3_K_XL/. Its MTP heads carry
        // Q4_K_M and Q8_0 but are not quantizations of the model.
        assert_eq!(default_name(&qwen38()).as_deref(), Some("UD-Q2_K_XL"));
        assert_eq!(default_quantization(&[], &[]), None);
    }

    #[test]
    fn a_tag_is_carried_as_a_whole_token_of_the_path() {
        let q = |path: &str| Quantization {
            name: String::new(),
            files: vec![path.to_string()],
            bytes: 0,
        };
        for (path, tag, carried) in [
            ("Model-Q4_K_M.gguf", "Q4_K_M", true),
            ("model-q4_k_m.gguf", "Q4_K_M", true),
            ("Model-UD-Q4_K_M.gguf", "Q4_K_M", true),
            ("Q4_K_M/Model-Q4_K_M-00001-of-00002.gguf", "Q4_K_M", true),
            // A folder's name is followed by `/`, so a tag only a folder bears is not carried,
            // as llama.cpp's `[.-]` after the tag does not match it.
            ("Q4_K_M/model-00001-of-00002.gguf", "Q4_K_M", false),
            ("Q4_K_M.gguf", "Q4_K_M", true),
            ("Model.Q8_0.gguf", "Q8_0", true),
            ("Model-BF16.gguf", "F16", false),
            ("Model-F16.gguf", "F16", true),
            ("Model-IQ4_K_M.gguf", "Q4_K_M", false),
            ("Model-Q4_K_M_L.gguf", "Q4_K_M", false),
            ("Model-Q8_0_XL.gguf", "Q8_0", false),
        ] {
            assert_eq!(carries(&q(path), tag), carried, "{path} carries {tag}");
        }
        // A repository whose only Q4_K_M is unsloth's UD- one loads it, as llama.cpp does.
        let ud = listing(&[
            ("Model-Q8_0.gguf", 8 * GB),
            ("Model-UD-Q4_K_M.gguf", 4 * GB),
        ]);
        assert_eq!(default_name(&ud).as_deref(), Some("UD-Q4_K_M"));
    }

    #[test]
    fn importance_matrices_and_drafters_are_not_quantizations() {
        let with_sidecars = listing(&[
            ("DeepSeek-V4.1-Flash-Q2_K.gguf", 90 * GB),
            ("dspark-DeepSeek-V4.1-Flash-Q8_0.gguf", 2 * GB),
            ("DFlash/dflash-DeepSeek-V4.1-Flash-Q8_0.gguf", 2 * GB),
            ("eagle3-DeepSeek-V4.1-Flash-Q8_0.gguf", GB),
            ("imatrix_unsloth.gguf", 10_000_000),
            ("mmproj-F16.gguf", GB),
        ]);
        assert_eq!(names(&quantizations(&with_sidecars)), vec!["Q2_K"]);
        // So a drafter's Q8_0 is not taken for the model's.
        assert_eq!(default_name(&with_sidecars).as_deref(), Some("Q2_K"));
    }

    /// A name that answers only to an importance matrix or a drafter, which the resolver, as
    /// llama.cpp does, does not take for the model's weights, is refused with the way to load the
    /// file anyway. Passed over, it would leave a repository that also offers safetensors to load
    /// those instead of the weights `files` names.
    #[test]
    fn a_name_only_an_importance_matrix_or_a_drafter_answers_to_is_refused() {
        let imatrix_named = listing(&[
            ("Acme-imatrix-7B-Q4_K_M.gguf", GB),
            ("model.safetensors", GB),
        ]);
        assert_eq!(
            resolve(&imatrix_named, &["Q4_K_M"]).map_err(|e| e.to_string()),
            Err("'Q4_K_M' names only Acme-imatrix-7B-Q4_K_M.gguf, which Spice, like llama.cpp, takes by name for an importance matrix or a drafter, not the model's weights; to load one as the weights, name its path in `files`".to_string())
        );
        // its path loads it as the weights
        let by_path = resolve(&imatrix_named, &["Acme-imatrix-7B-Q4_K_M.gguf"])
            .expect("the path resolves")
            .expect("the path names weights");
        assert_eq!(by_path.weights, ["Acme-imatrix-7B-Q4_K_M.gguf"]);
        // beside a quantization of the model that the name does not answer to, a drafter's tag
        let drafter = listing(&[
            ("Model-Q4_K_M.gguf", GB),
            ("dspark-Model-Q8_0.gguf", GB / 5),
        ]);
        assert_eq!(
            resolve(&drafter, &["Q8_0"]).map_err(|e| e.to_string()),
            Err("'Q8_0' names only dspark-Model-Q8_0.gguf, which Spice, like llama.cpp, takes by name for an importance matrix or a drafter, not the model's weights; to load one as the weights, name its path in `files`".to_string())
        );
        // a name a quantization of the model answers to loads it, though a drafter carries it too
        let both = listing(&[("Model-Q8_0.gguf", GB), ("dspark-Model-Q8_0.gguf", GB / 5)]);
        let q8 = resolve(&both, &["Q8_0"])
            .expect("Q8_0 resolves")
            .expect("Q8_0 names weights");
        assert_eq!(q8.weights, ["Model-Q8_0.gguf"]);
    }

    /// A model loads one set of weights, so `files` that name two, by name or by path, fail
    /// rather than load the first's weights as the second's with the second's draft head.
    #[test]
    fn files_that_name_two_quantizations_are_refused() {
        let two = listing(&[
            ("Model-Q4_K_M.gguf", GB),
            ("Model-Q8_0.gguf", GB),
            ("mtp-Model-Q8_0.gguf", GB / 10),
            ("mmproj-F16.gguf", GB / 2),
        ]);
        let refused = Err(
            "`files` names 2 sets of model weights, Q4_K_M, Q8_0, and a model loads one; name one of them"
                .to_string(),
        );
        for files in [
            &["Q4_K_M", "Q8_0"][..],
            &["Model-Q4_K_M.gguf", "Q8_0"],
            &["Q8_0", "Model-Q4_K_M.gguf"],
            &["Model-Q4_K_M.gguf", "Model-Q8_0.gguf"],
        ] {
            assert_eq!(
                resolve(&two, files).map_err(|e| e.to_string()),
                refused,
                "files {files:?}"
            );
        }
        // one quantization named twice, or beside a file that is no quantization, still loads
        let once = resolve(&two, &["Q8_0", "Model-Q8_0.gguf"])
            .expect("Q8_0 resolves")
            .expect("Q8_0 names weights");
        assert_eq!(
            (once.weights, once.draft, once.quantization),
            (
                vec!["Model-Q8_0.gguf".to_string()],
                vec!["mtp-Model-Q8_0.gguf".to_string()],
                Some("Q8_0".to_string())
            )
        );
        let projector = resolve(&two, &["Q4_K_M", "mmproj-F16.gguf"])
            .expect("Q4_K_M resolves")
            .expect("Q4_K_M names weights");
        assert_eq!(
            (projector.weights, projector.quantization),
            (
                vec![
                    "Model-Q4_K_M.gguf".to_string(),
                    "mmproj-F16.gguf".to_string()
                ],
                Some("Q4_K_M".to_string())
            )
        );
        // a projector named as llama.cpp names some, with `mmproj` inside the name, and a
        // drafter, which the engine loads beside the model
        let llava = listing(&[
            ("llava-v1.5-7b-Q4_K_M.gguf", GB),
            ("llava-v1.5-7b-Q8_0.gguf", GB),
            ("llava-v1.5-7b-mmproj-model-f16.gguf", GB / 2),
            ("dspark-llava-v1.5-7b-Q8_0.gguf", GB / 5),
        ]);
        assert_eq!(
            quantizations(&llava)
                .iter()
                .map(|q| q.name.as_str())
                .collect::<Vec<_>>(),
            ["Q4_K_M", "Q8_0"]
        );
        let beside = resolve(
            &llava,
            &[
                "Q4_K_M",
                "llava-v1.5-7b-mmproj-model-f16.gguf",
                "dspark-llava-v1.5-7b-Q8_0.gguf",
            ],
        )
        .expect("Q4_K_M resolves")
        .expect("Q4_K_M names weights");
        assert_eq!(
            (beside.weights, beside.draft, beside.quantization),
            (
                vec![
                    "llava-v1.5-7b-Q4_K_M.gguf".to_string(),
                    "llava-v1.5-7b-mmproj-model-f16.gguf".to_string(),
                    "dspark-llava-v1.5-7b-Q8_0.gguf".to_string(),
                ],
                Vec::<String>::new(),
                Some("Q4_K_M".to_string())
            )
        );
        // the merged file and the shards of one quantization are two sets, though the
        // repository's quantizations count them as one
        let merged = listing(&[
            ("Model-Q4_K_M.gguf", 2 * GB),
            ("Model-Q4_K_M-00001-of-00002.gguf", GB),
            ("Model-Q4_K_M-00002-of-00002.gguf", GB),
            ("Model-Q8_0.gguf", GB),
            ("mtp-Model-Q8_0.gguf", GB / 10),
        ]);
        assert_eq!(
            resolve(&merged, &["Model-Q4_K_M-00001-of-00002.gguf", "Q8_0"])
                .map_err(|e| e.to_string()),
            Err("`files` names 2 sets of model weights, Model-Q4_K_M-00001-of-00002.gguf, Q8_0, and a model loads one; name one of them".to_string())
        );
        assert_eq!(
            resolve(&merged, &["Model-Q4_K_M-00001-of-00002.gguf", "Model-Q4_K_M.gguf"])
                .map_err(|e| e.to_string()),
            Err("`files` names 2 sets of model weights, Model-Q4_K_M-00001-of-00002.gguf, Q4_K_M, and a model loads one; name one of them".to_string())
        );
        let shards = resolve(&merged, &["Model-Q4_K_M-00002-of-00002.gguf"])
            .expect("the shards resolve")
            .expect("the shards name weights");
        assert_eq!(
            shards.weights,
            [
                "Model-Q4_K_M-00001-of-00002.gguf",
                "Model-Q4_K_M-00002-of-00002.gguf"
            ]
        );
    }

    /// A drafter its header tells, named beside the weights, drafts in place of the MTP head the
    /// quantization would bring, as the engine drafts with it; named alone, it is loaded as named.
    /// Its name alone does not make it one: the engine tells it by its header.
    #[test]
    fn a_drafter_named_beside_the_weights_drafts_in_place_of_the_head() {
        let repo = listing(&[
            ("Model-Q4_K_M.gguf", GB),
            ("dspark-Model-Q8_0.gguf", GB / 5),
            ("mtp-Model-Q8_0.gguf", GB / 10),
        ]);
        let by_name = resolve(&repo, &["Q4_K_M", "dspark-Model-Q8_0.gguf"])
            .expect("Q4_K_M resolves")
            .expect("Q4_K_M names weights");
        assert_eq!(
            (by_name.weights, by_name.draft),
            (
                vec![
                    "Model-Q4_K_M.gguf".to_string(),
                    "dspark-Model-Q8_0.gguf".to_string()
                ],
                vec!["mtp-Model-Q8_0.gguf".to_string()]
            )
        );
        let drafters = BTreeSet::from(["dspark-Model-Q8_0.gguf".to_string()]);
        let beside = resolve_with_drafters(&repo, &["Q4_K_M", "dspark-Model-Q8_0.gguf"], &drafters)
            .expect("Q4_K_M resolves beside its drafter")
            .expect("Q4_K_M names weights");
        assert_eq!(
            (beside.weights, beside.draft, beside.quantization),
            (
                vec!["Model-Q4_K_M.gguf".to_string()],
                vec!["dspark-Model-Q8_0.gguf".to_string()],
                Some("Q4_K_M".to_string())
            )
        );
        let alone = resolve_with_drafters(&repo, &["dspark-Model-Q8_0.gguf"], &drafters)
            .expect("the drafter's path resolves")
            .expect("the path names a file");
        assert_eq!(
            (alone.weights, alone.draft),
            (
                vec!["dspark-Model-Q8_0.gguf".to_string()],
                Vec::<String>::new()
            )
        );
    }

    /// The draft file a load names is the one the engine drafts with: the first shard 1 among
    /// them, else the first.
    #[test]
    fn the_drafting_file_is_the_one_the_engine_opens() {
        let files =
            |names: &[&str]| -> Vec<String> { names.iter().map(ToString::to_string).collect() };
        assert_eq!(
            drafting_file(&files(&[
                "mtp-Model-Q4_K_M.gguf",
                "Helper-Q8_0-00001-of-00002.gguf",
                "Helper-Q8_0-00002-of-00002.gguf",
            ])),
            Some("Helper-Q8_0-00001-of-00002.gguf")
        );
        assert_eq!(
            drafting_file(&files(&["mtp-A-Q8_0.gguf", "mtp-B-Q8_0.gguf"])),
            Some("mtp-A-Q8_0.gguf")
        );
        // a folder named like a shard does not make its files shards, as the engine reads names
        assert_eq!(
            drafting_file(&files(&[
                "mtp-Model-Q8_0.gguf",
                "d-00001-of-2/dspark-Model-Q8_0.gguf"
            ])),
            Some("mtp-Model-Q8_0.gguf")
        );
        assert_eq!(drafting_file(&[]), None);
    }

    /// A quantization named in `files` that the headers show to be a drafter is not the one the
    /// model loads: the load names the weights' quantization, and takes their draft head.
    #[test]
    fn a_drafter_named_as_a_quantization_is_not_the_one_loaded() {
        let repo = listing(&[
            ("Model-Q4_K_M.gguf", GB),
            ("Model-Q8_0.gguf", GB),
            ("mtp-Model-Q4_K_M.gguf", GB / 10),
        ]);
        let drafters = BTreeSet::from(["Model-Q8_0.gguf".to_string()]);
        for files in [&["Q4_K_M", "Q8_0"][..], &["Q8_0", "Q4_K_M"]] {
            let resolved = resolve_with_drafters(&repo, files, &drafters)
                .expect("Q4_K_M resolves beside its drafter")
                .expect("Q4_K_M names weights");
            // the drafter drafts, in place of the MTP head the quantization would bring
            assert_eq!(
                (resolved.weights, resolved.draft, resolved.quantization),
                (
                    vec!["Model-Q4_K_M.gguf".to_string()],
                    vec!["Model-Q8_0.gguf".to_string()],
                    Some("Q4_K_M".to_string())
                ),
                "files {files:?}"
            );
        }
    }

    /// An engine that opens one file opens the model's weights, not a projector, importance
    /// matrix or drafter named ahead of them; named alone, it opens that.
    #[test]
    fn the_model_file_is_the_first_of_the_weights() {
        let files =
            |names: &[&str]| -> Vec<String> { names.iter().map(ToString::to_string).collect() };
        assert_eq!(
            model_file(&files(&[
                "mmproj-F16.gguf",
                "Model-imatrix.gguf",
                "dspark-Model-Q8_0.gguf",
                "Model-Q4_K_M-00001-of-00002.gguf",
                "Model-Q4_K_M-00002-of-00002.gguf",
            ])),
            Some("Model-Q4_K_M-00001-of-00002.gguf")
        );
        assert_eq!(
            model_file(&files(&["Model-imatrix.gguf"])),
            Some("Model-imatrix.gguf")
        );
        // an MTP head drafts for the model, so it is set aside before the weights are looked for
        assert_eq!(
            model_file(&files(&[
                "Model-Q4_K_M-imatrix.gguf",
                "mtp-Model-Q8_0.gguf"
            ])),
            Some("Model-Q4_K_M-imatrix.gguf")
        );
        assert_eq!(
            model_file(&files(&["mtp-Model-Q8_0.gguf", "Model-Q4_K_M.gguf"])),
            Some("Model-Q4_K_M.gguf")
        );
        // with no weights by name, the first shard 1, as the engine opens it
        assert_eq!(
            model_file(&files(&[
                "mmproj-F16.gguf",
                "dspark-M-00001-of-00002.gguf",
                "dspark-M-00002-of-00002.gguf",
            ])),
            Some("dspark-M-00001-of-00002.gguf")
        );
        assert_eq!(model_file(&[]), None);
    }

    /// A name can answer to several quantizations in a listing of the cache alone and to one in
    /// the Hub's, so an ambiguity read from the cache is retried, not taken for a mistake in
    /// `files`: here a stray shard out of its series' range keeps `big/` from being a
    /// quantization in the Hub's listing, and the cache lacks that shard.
    #[test]
    fn a_name_can_be_ambiguous_in_the_cache_alone() {
        let hub = listing(&[
            ("Model-Q4_K_M.gguf", GB),
            ("Model-Q8_0.gguf", GB),
            ("big/Model-Q4_K_M-00001-of-00002.gguf", GB),
            ("big/Model-Q4_K_M-00002-of-00002.gguf", GB),
            ("big/Model-Q4_K_M-00003-of-00002.gguf", GB),
        ]);
        let resolved = resolve(&hub, &["Q4_K_M"])
            .expect("one quantization answers to Q4_K_M in the Hub's listing")
            .expect("Q4_K_M names weights");
        assert_eq!(resolved.weights, ["Model-Q4_K_M.gguf"]);
        let cache = listing(&[
            ("Model-Q4_K_M.gguf", GB),
            ("big/Model-Q4_K_M-00001-of-00002.gguf", GB),
            ("big/Model-Q4_K_M-00002-of-00002.gguf", GB),
        ]);
        assert_eq!(
            resolve(&cache, &["Q4_K_M"]).map_err(|e| e.clause(ListedFrom::Cache)),
            Err(
                "'Q4_K_M' matches 2 quantizations there, big, Model-Q4_K_M; name one of them"
                    .to_string()
            )
        );
    }

    /// The `files` entries a node advertises from a listing of its cache alone name, in any
    /// other node's cache of the repository, the quantization it chose or nothing there: never
    /// another. A folder that holds one quantization in this node's cache can hold another in
    /// the next node's, so a folder's name there would load the other.
    #[test]
    fn a_pin_from_one_cache_never_loads_another_quantization_from_another() {
        let mut checked = 0;
        for (fixture, files) in [
            ("one_folder", one_folder()),
            ("tags_in_one_folder", tags_in_one_folder()),
            ("tag_meets_folder", tag_meets_folder()),
            ("merged_and_split", merged_and_split()),
            ("sharded_head", sharded_head()),
            ("bare_tags", bare_tags()),
            ("qwen38", qwen38()),
            ("glm", glm()),
        ] {
            let all = quantizations(&files);
            for q in &all {
                let head = draft_head(&files, q);
                // this node's cache holds the quantization and its draft head
                let mine: Vec<ListedFile> = files
                    .iter()
                    .filter(|f| q.files.contains(&f.path) || head.contains(&f.path))
                    .cloned()
                    .collect();
                let chosen = quantizations(&mine)
                    .into_iter()
                    .find(|m| m.files == q.files)
                    .unwrap_or_else(|| panic!("{fixture}: the cache holds {:?}", q.files));
                let pin = pin_from(&mine, &chosen, ListedFrom::Cache);
                let entries: Vec<&str> = pin.iter().map(String::as_str).collect();
                let here = resolve(&mine, &entries)
                    .unwrap_or_else(|e| panic!("{fixture}: {pin:?} does not resolve: {e}"))
                    .unwrap_or_else(|| panic!("{fixture}: {pin:?} names no weights"));
                assert_eq!(here.weights, q.files, "{fixture}: {pin:?}");
                // the next node's cache holds another quantization alone
                for other in all.iter().filter(|r| r.files != q.files) {
                    let theirs: Vec<ListedFile> = files
                        .iter()
                        .filter(|f| other.files.contains(&f.path))
                        .cloned()
                        .collect();
                    if let Ok(Some(there)) = resolve(&theirs, &entries) {
                        panic!(
                            "{fixture}: {pin:?}, advertised from a cache of {}, loads {:?} from a cache of {}",
                            q.name, there.weights, other.name
                        );
                    }
                    checked += 1;
                }
            }
        }
        assert!(
            checked > 0,
            "no two quantizations of a fixture were compared"
        );
    }

    /// A cache can make a name ambiguous that a file it gains makes plain: two shard layouts of
    /// one quantization are two quantizations until the merged file stands for both, so an
    /// ambiguity read from the cache is retried.
    #[test]
    fn a_name_ambiguous_in_the_cache_is_plain_once_it_holds_the_merged_file() {
        let mut cache = listing(&[
            ("Model-Q4_K_M-00001-of-00002.gguf", 2 * GB),
            ("Model-Q4_K_M-00002-of-00002.gguf", 2 * GB),
            ("Model-Q4_K_M-00001-of-00003.gguf", GB),
            ("Model-Q4_K_M-00002-of-00003.gguf", GB),
            ("Model-Q4_K_M-00003-of-00003.gguf", 2 * GB),
        ]);
        assert!(
            matches!(
                resolve(&cache, &["Q4_K_M"]),
                Err(ResolveError::AmbiguousQuantization { ref quantizations, .. })
                    if quantizations.len() == 2
            ),
            "{:?}",
            resolve(&cache, &["Q4_K_M"])
        );
        cache.extend(listing(&[("Model-Q4_K_M.gguf", 4 * GB)]));
        let resolved = resolve(&cache, &["Q4_K_M"])
            .expect("the merged file stands for both layouts")
            .expect("Q4_K_M names weights");
        assert_eq!(resolved.weights, ["Model-Q4_K_M.gguf"]);
    }

    /// A repository file whose path `resolve` would read as another's, as one that starts with
    /// whitespace or `./`, is never a quantization or a draft head, so no pin names it: a pin
    /// that named it would load its twin on a node whose cache holds the twin instead, and two
    /// nodes would load different weights. A pin of the twin names nothing in a cache that
    /// holds only such a file.
    #[test]
    fn a_path_resolve_would_read_as_another_is_never_chosen_or_pinned() {
        for odd in [
            " gguf/Model-Q4_K_M.gguf",
            "\u{a0}gguf/Model-Q4_K_M.gguf",
            "./gguf/Model-Q4_K_M.gguf",
        ] {
            let alone = listing(&[(odd, GB), (" MTP/mtp-Model-Q8_0.gguf", GB / 10)]);
            assert_eq!(
                names(&quantizations(&alone)),
                Vec::<String>::new(),
                "{odd:?}"
            );
            let twin = listing(&[(odd, GB), ("gguf/Model-Q4_K_M.gguf", GB)]);
            let qs = quantizations(&twin);
            assert_eq!(qs.len(), 1, "{odd:?}: {:?}", names(&qs));
            assert_eq!(qs[0].files, ["gguf/Model-Q4_K_M.gguf"], "{odd:?}");
            // the twin's name names nothing there, and its path is not there
            for (from, there) in [
                (ListedFrom::Repository, Ok(None)),
                (
                    ListedFrom::Cache,
                    Err(ResolveError::MissingFile {
                        path: "gguf/Model-Q4_K_M.gguf".to_string(),
                    }),
                ),
            ] {
                let pin = pin_from(&twin, &qs[0], from);
                let entries: Vec<&str> = pin.iter().map(String::as_str).collect();
                assert_eq!(
                    resolve(&alone, &entries),
                    there,
                    "{odd:?}: {pin:?} from {from:?}"
                );
            }
        }
        // a folder deeper in the path whose name starts with a space, beside one without it
        let folders = listing(&[("x/ a/Model-Q4_K_M.gguf", GB), ("x/a/Model-Q8_0.gguf", GB)]);
        let all = quantizations(&folders);
        assert_eq!(all.len(), 2, "{:?}", names(&all));
        for q in &all {
            for from in [ListedFrom::Repository, ListedFrom::Cache] {
                let pin = pin_from(&folders, q, from);
                let entries: Vec<&str> = pin.iter().map(String::as_str).collect();
                let resolved = resolve(&folders, &entries)
                    .unwrap_or_else(|e| panic!("{pin:?} does not resolve: {e}"))
                    .unwrap_or_else(|| panic!("{pin:?} names no weights"));
                assert_eq!(resolved.weights, q.files, "{pin:?} from {from:?}");
            }
        }
    }

    #[test]
    fn a_name_prints_as_printable_ascii() {
        for (text, printed) in [
            (
                "UD-Q4_K_XL/GLM-4.7-Flash-UD-Q4_K_XL.gguf",
                "UD-Q4_K_XL/GLM-4.7-Flash-UD-Q4_K_XL.gguf",
            ),
            ("Q4_K_M\nINFO forged line", "Q4_K_M\\nINFO forged line"),
            ("Q4_K_M\u{1b}[31m", "Q4_K_M\\u{1b}[31m"),
            (
                "Q4_K_M\u{2028}INFO forged line",
                "Q4_K_M\\u{2028}INFO forged line",
            ),
            ("\u{202e}M_K_4Q", "\\u{202e}M_K_4Q"),
            ("Q4_K_M\\n", "Q4_K_M\\\\n"),
            ("Modèle Q8_0", "Mod\\u{e8}le Q8_0"),
        ] {
            assert_eq!(printable(text), printed, "{text:?}");
        }
        // A repository's names in a resolution error print the same way.
        let listing = listing(&[("Q4_K_M\nINFO forged.gguf", GB)]);
        let error = resolve(&listing, &["Q9_K"]).expect_err("no such quantization");
        assert!(!error.to_string().contains('\n'), "{error}");
    }

    /// The repository decides how many quantizations and shards it offers, so an error names
    /// the first 40 and counts the rest.
    #[test]
    fn an_error_names_forty_and_counts_the_rest() {
        let names =
            |count: usize| -> Vec<String> { (1..=count).map(|i| format!("Q{i}")).collect() };
        let first_forty = names(40).join(", ");

        assert_eq!(
            ResolveError::UnknownQuantization {
                name: "Q99".to_string(),
                available: names(43),
            }
            .to_string(),
            format!(
                "the repository has no file or quantization 'Q99'; its quantizations are {first_forty}, and 3 more"
            )
        );
        assert_eq!(
            ResolveError::AmbiguousQuantization {
                name: "Q".to_string(),
                quantizations: names(41),
            }
            .to_string(),
            format!(
                "'Q' matches 41 quantizations of the repository, {first_forty}, and 1 more; name one of them"
            )
        );
        assert_eq!(
            ResolveError::IncompleteShards {
                path: "Q1".to_string(),
                missing: names(40),
            }
            .to_string(),
            format!("'Q1' is a shard of a series the repository lacks part of: {first_forty}")
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

    /// The model a Spicepod declares with `files: [{files}]`, read as `spiced` reads a Spicepod.
    fn declared_model(files: &str) -> spicepod::component::model::Model {
        let text = format!(
            "version: v1\nkind: Spicepod\nname: app\nmodels:\n  - from: huggingface:huggingface.co/org/Model-GGUF\n    name: model\n    files: [{files}]\n"
        );
        let value: yaml::Value =
            yaml::from_str(&text).unwrap_or_else(|e| panic!("{text} is not YAML: {e}"));
        let definition = spicepod::parse_definition_value(value)
            .unwrap_or_else(|e| panic!("{text} is not a Spicepod: {e}"));
        match definition.models.into_iter().next() {
            Some(spicepod::component::ComponentOrReference::Component(model)) => model,
            other => panic!("{text} declares no model: {other:?}"),
        }
    }

    /// A `files` entry a load line or an error prints reads back, pasted into a Spicepod, as the
    /// name it pins, whatever characters the repository gave the name, and prints as one line of
    /// printable ASCII.
    #[test]
    fn a_printed_files_entry_reads_back_as_the_name_it_pins() {
        let reads_back = |name: &str| {
            let printed = files_entries(&[name.to_string()]);
            assert!(
                printed.chars().all(|c| matches!(c, ' '..='~')),
                "{name:?} prints as {printed}"
            );
            let model = declared_model(&printed);
            let paths: Vec<&str> = model.files.iter().map(|f| f.path.as_str()).collect();
            assert_eq!(paths, [name], "{name:?} prints as {printed}");
        };
        for (name, printed) in [
            ("Q4_K_M", "{path: Q4_K_M}"),
            (
                "UD-Q4_K_XL/GLM-4.7-Flash-UD-Q4_K_XL-00001-of-00002.gguf",
                "{path: UD-Q4_K_XL/GLM-4.7-Flash-UD-Q4_K_XL-00001-of-00002.gguf}",
            ),
            ("Modèle Q8_0", r#"{path: "Mod\u00E8le Q8_0"}"#),
            ("😀Q8_0", r#"{path: "\U0001F600Q8_0"}"#),
            (
                "Q4_K_M\nINFO forged line",
                r#"{path: "Q4_K_M\u000AINFO forged line"}"#,
            ),
            ("say \"Q8\" \\ here", r#"{path: "say \"Q8\" \\ here"}"#),
            ("true", r#"{path: "true"}"#),
            ("4bit", r#"{path: "4bit"}"#),
            ("", r#"{path: ""}"#),
        ] {
            reads_back(name);
            assert_eq!(files_entries(&[name.to_string()]), printed, "{name:?}");
        }
        for name in [
            "Q4_K_M\u{1b}[31m",
            "\u{202e}M_K_4Q",
            "Q4_K_M\u{2028}INFO forged line",
            "Q8_0\t",
            "Q8_0\u{7f}",
            " Q8_0 ",
            "True",
            "NULL",
            "No",
            "on",
            "y",
            "1.0",
            "0x10",
            "1e5",
            "-Q8_0",
            ".inf",
            "~",
            "Q8_0: x",
            "Q8 #x",
            "Q8,Q4",
            "[Q8]",
            "{Q8}",
            "*Q8",
            "&Q8",
            "!Q8",
            "%Q8",
            "@Q8",
            "`Q8",
            "|Q8",
            ">Q8",
            "'Q8'",
            "?Q8",
            "Q8\\",
        ] {
            reads_back(name);
        }
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
                // the pin as a load line prints it, pasted into a Spicepod, carries it to
                // resolution
                let model = declared_model(&files_entries(&pin));
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
