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

//! The files of a Hugging Face model repository mistral.rs loads, downloaded by Spice so the
//! engine reads a local directory and never the network.
//!
//! mistral.rs reads a local model directory by its top-level file names: `tokenizer.json` or
//! `tekken.json`, `config.json` or `params.json`, the chat template files, the processor and
//! generation configs, and the weights. The weights are chosen as it chooses them —
//! `model.safetensors.index.json` and the shards it names, else every `model-NNNNN-of-NNNNN`,
//! `model` or `consolidated` safetensors file, else sharded `pytorch_model-…` pickles — and
//! every top-level JSON or Jinja file comes along, so a config the engine reads by name is
//! there whichever it is.
//!
//! A GGUF model loads the files its `files` resolve to ([`gguf_files::resolve`]): exact
//! files with their sibling shards, or a quantization by name with the repository's draft
//! head. A GGUF repository named with no `files` loads the quantization llama.cpp's `-hf`
//! loads ([`gguf_files::default_quantization`]: `Q4_K_M`, else `Q8_0`, else its first), or, when
//! the engine reports less memory than its weights need, the largest complete quantization that
//! fits.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use huggingface_hub::{DownloadError, Requester, Snapshot};
use regex::Regex;

use crate::chat::gguf_files::{self, ListedFile, ListedFrom, Quantization};
use crate::hf::HubParams;

use crate::hf::SAFETENSORS_INDEX as SAFETENSOR_INDEX;
const DOCS_URL: &str = "https://spiceai.org/docs/components/models/huggingface";

/// What a model loads, once downloaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Downloaded {
    /// The snapshot directory the files are in.
    pub dir: PathBuf,
    /// The GGUF files the model loads, as repository paths in load order: the weights, shard 1
    /// first, then a draft head. Empty for a model mistral.rs loads from safetensors.
    pub gguf: Vec<String>,
    /// The quantization chosen among what the Hugging Face cache holds, when `files` named none
    /// and the Hub's listing was not read. `None` when the Hub listed the repository or `files`
    /// named what the model loads.
    pub chosen_from_cache: Option<ChosenFromCache>,
}

/// A quantization a model with no `files` chose among what the Hugging Face cache holds, the
/// Hub's listing not read. The Hub, or another machine's cache, can give another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChosenFromCache {
    /// The quantization's name, as printable ASCII.
    pub quantization: String,
    /// The `files` entries that load it, as [`gguf_files::files_entries`] prints them.
    pub pinned: String,
    /// Why only the cache was read, as a clause ([`Snapshot::cache_only_reason`]).
    pub reason: String,
    /// Whether `HF_HUB_OFFLINE` or `TRANSFORMERS_OFFLINE` is set ([`Snapshot::is_offline`]),
    /// which only a restart without
    /// it clears; with the Hub out of reach instead, a later load can read the Hub's listing.
    pub offline: bool,
}

/// What a model that names no GGUF files chooses its quantization by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Budget {
    /// The memory free for the model, in bytes: the default quantization when its weights fit
    /// it with room for the context, or else the largest that does.
    Free(u64),
    /// No engine measures the memory: the default quantization.
    Unmeasured,
    /// The model is split across machines, which must all load the same files: the default
    /// quantization, which depends on the repository's listing alone, never on one machine's
    /// memory. With the Hub out of reach, a machine lists its own cache instead.
    Split,
}

/// Downloads the files the model `model_id` (`org/name[:revision]`) loads from, for the model
/// named `name`: the GGUF files `files` resolve to, or, when `files` names none, the GGUF
/// quantization `budget` chooses or the files mistral.rs loads from a safetensors repository.
///
/// # Errors
///
/// Returns an error if the repository or revision cannot be read, `files` names something it
/// lacks, no quantization fits, a file the engine needs is missing, or a download fails.
pub async fn download_model(
    model_id: &str,
    files: &[&str],
    budget: Budget,
    hub: HubParams<'_>,
    name: &str,
) -> Result<Downloaded, DownloadError> {
    let snapshot = open(model_id, hub, name).await?;
    let (gguf, chosen_from_cache) = resolve_gguf(
        &snapshot,
        files,
        budget,
        name,
        repo_of(model_id),
        Purpose::Load,
    )
    .await?;
    let paths: Vec<&str> = gguf.iter().map(String::as_str).collect();
    let dir = download(&snapshot, &paths).await?;
    Ok(Downloaded {
        dir,
        gguf,
        chosen_from_cache,
    })
}

/// What a model loads, resolved from the Hub without downloading the weights.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    /// The GGUF files the model loads, as [`Downloaded::gguf`]: empty for a model mistral.rs
    /// loads from safetensors.
    pub gguf: Vec<String>,
    /// The architecture the first shard's header names; `None` when no header names one.
    pub architecture: Option<String>,
}

/// The GGUF files the model `model_id` (`org/name[:revision]`) loads, resolved from `files` and
/// `budget` as [`download_model`] resolves them, and the architecture their first shard's
/// header names, both read from the Hub without downloading the weights. A node that holds a
/// share of a model split across nodes decides from them which engine serves the model, as the
/// node that downloads the weights does. It logs what it resolves at debug, not as what the
/// model loads: the head loads the weights and says what they are, and with the Hub out of
/// reach, each node resolving from its own cache, they need not be what this node resolved.
///
/// # Errors
///
/// Returns an error if the repository or revision cannot be read, `files` names something it
/// lacks, no quantization fits, or a read fails.
pub async fn model_architecture(
    model_id: &str,
    files: &[&str],
    budget: Budget,
    hub: HubParams<'_>,
    name: &str,
) -> Result<Resolved, DownloadError> {
    let snapshot = open(model_id, hub, name).await?;
    let (gguf, _) = resolve_gguf(
        &snapshot,
        files,
        budget,
        name,
        repo_of(model_id),
        Purpose::ReadArchitecture,
    )
    .await?;
    let paths: Vec<&str> = gguf.iter().map(String::as_str).collect();
    let architecture = super::gguf_header::model_architecture(&paths, |file: &&str, len| {
        let file = (*file).to_string();
        let snapshot = &snapshot;
        async move { snapshot.read_prefix(&file, len).await }
    })
    .await?;
    Ok(Resolved { gguf, architecture })
}

/// The repository `model_id` (`org/name[:revision]`) names.
fn repo_of(model_id: &str) -> &str {
    model_id.split_once(':').map_or(model_id, |(repo, _)| repo)
}

/// Opens the model `model_id` (`org/name[:revision]`), named `name` in the Spicepod.
async fn open(model_id: &str, hub: HubParams<'_>, name: &str) -> Result<Snapshot, DownloadError> {
    let requester = Requester {
        kind: "model",
        name: name.to_string(),
        docs_url: DOCS_URL,
        retried: true,
    };
    let (repo, revision) = match model_id.split_once(':') {
        Some((repo, revision)) => (repo, Some(revision).filter(|r| !r.is_empty())),
        None => (model_id, None),
    };
    crate::hf::open_model(requester, repo, revision, hub).await
}

/// What a node resolves a model's files for, which decides how it logs what it resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Purpose {
    /// To download and load them: the line says what the model loads.
    Load,
    /// To read their architecture only, as a node that holds a share of a model split across
    /// nodes does: the line is logged at debug ([`model_architecture`]).
    ReadArchitecture,
}

/// Logs `line`, which says what a model loads, as `purpose` decides.
fn log_resolved(purpose: Purpose, line: &str) {
    match purpose {
        Purpose::Load => tracing::info!("{line}"),
        Purpose::ReadArchitecture => tracing::debug!("{line}"),
    }
}

/// The GGUF files a model loads from `snapshot`: those `files` resolve to, or with none named,
/// the quantization `budget` chooses ([`choose_quantization`]), and its draft head. Empty for
/// a repository that offers no GGUF quantization or that mistral.rs loads from safetensors.
/// With the quantization chosen from a listing of the cache alone, also what was chosen
/// ([`ChosenFromCache`]).
async fn resolve_gguf(
    snapshot: &Snapshot,
    files: &[&str],
    budget: Budget,
    name: &str,
    repo: &str,
    purpose: Purpose,
) -> Result<(Vec<String>, Option<ChosenFromCache>), DownloadError> {
    let listing: Vec<ListedFile> = snapshot
        .files()
        .map(|file| ListedFile {
            path: file.path.clone(),
            size: file.size,
        })
        .collect();
    let from = if snapshot.cache_only_reason().is_some() {
        ListedFrom::Cache
    } else {
        ListedFrom::Repository
    };
    let resolved = match gguf_files::resolve(&listing, files) {
        // The engine tells a `DFlash` drafter by its header, so one can be named beside the
        // weights under a name that does not say it is one: read the headers of the sets of
        // weights before refusing them.
        Err(gguf_files::ResolveError::SeveralWeights { opening, .. }) => {
            let mut drafters = BTreeSet::new();
            for file in opening {
                if super::gguf_header::names_a_drafter(|len| snapshot.read_prefix(&file, len))
                    .await?
                {
                    drafters.insert(file);
                }
            }
            gguf_files::resolve_with_drafters(&listing, files, &drafters)
        }
        resolved => resolved,
    };
    if let Some(resolved) =
        resolved.map_err(|error| DownloadError::unusable(snapshot, error.clause(from)))?
    {
        if files.iter().any(|file| !is_gguf_path(file)) {
            let line = format!(
                "Model '{name}' loads {} from Hugging Face repo '{repo}'{}.",
                describe(
                    resolved
                        .quantization
                        .as_deref()
                        .map(gguf_files::printable)
                        .as_deref(),
                    resolved.weights.len()
                ),
                gguf_files::drafting_file(&resolved.draft).map_or_else(
                    String::new,
                    |draft| format!(", drafting with '{}'", gguf_files::printable(draft))
                )
            );
            log_resolved(purpose, &line);
        }
        let gguf = resolved
            .files()
            .into_iter()
            .map(ToString::to_string)
            .collect();
        return Ok((gguf, None));
    }
    let names: Vec<&str> = listing.iter().map(|file| file.path.as_str()).collect();
    let quantizations = gguf_files::quantizations(&listing);
    if quantizations.is_empty() || weights(&names).is_some() {
        return Ok((Vec::new(), None));
    }
    let chosen = choose_quantization(
        snapshot,
        &listing,
        &quantizations,
        budget,
        name,
        repo,
        purpose,
    )?;
    let mut gguf = chosen.files.clone();
    gguf.extend(gguf_files::draft_head(&listing, chosen));
    let chosen_from_cache = snapshot.cache_only_reason().map(|reason| ChosenFromCache {
        quantization: gguf_files::printable(&chosen.name),
        pinned: pinned(&listing, chosen, ListedFrom::Cache),
        reason,
        offline: snapshot.is_offline(),
    });
    Ok((gguf, chosen_from_cache))
}

/// The `files` entries that load `quantization` from a listing `from` holds
/// ([`gguf_files::pin_from`]), as a load line or an error prints them
/// ([`gguf_files::files_entries`]).
fn pinned(listing: &[ListedFile], quantization: &Quantization, from: ListedFrom) -> String {
    gguf_files::files_entries(&gguf_files::pin_from(listing, quantization, from))
}

/// The quantization a model with no `files` loads: its default, the one llama.cpp's `-hf` loads
/// ([`gguf_files::default_quantization`]), or, when the engine measures the memory and the
/// default's weights do not fit it with room for the context, the largest that does. A split
/// model and a build that cannot measure the memory take the default as it is. The line that
/// says which is logged as `purpose` decides ([`log_resolved`]).
fn choose_quantization<'q>(
    snapshot: &Snapshot,
    listing: &[ListedFile],
    quantizations: &'q [Quantization],
    budget: Budget,
    name: &str,
    repo: &str,
    purpose: Purpose,
) -> Result<&'q Quantization, DownloadError> {
    let Some(default) = gguf_files::default_quantization(listing, quantizations) else {
        return Err(DownloadError::unusable(
            snapshot,
            "it offers no complete GGUF quantization".to_string(),
        ));
    };
    let cache_only = snapshot.cache_only_reason();
    let line = LoadLine {
        model: name,
        repo,
        cache_only: cache_only.as_deref(),
    };
    let memory = match budget {
        Budget::Free(memory) if default.bytes > gguf_files::weights_budget(memory) => memory,
        Budget::Free(_) | Budget::Unmeasured | Budget::Split => {
            log_resolved(purpose, &line.default(default, quantizations));
            return Ok(default);
        }
    };
    match gguf_files::choose(quantizations, gguf_files::weights_budget(memory)) {
        Ok(chosen) => {
            log_resolved(
                purpose,
                &line.step_down(
                    chosen,
                    default,
                    quantizations.len(),
                    memory,
                    &pinned(listing, chosen, line.from()),
                ),
            );
            Ok(chosen)
        }
        Err(smallest) => Err(DownloadError::unusable(
            snapshot,
            format!(
                "none of its GGUF quantizations fits the {} of memory free for the model with room for the context{}. Name one in the model's `files` to load it anyway, across machines with `nodes` if one cannot hold it: {}",
                gibibytes(memory),
                smallest.map_or_else(String::new, |q| format!(
                    "; the smallest, {}, is {}",
                    gguf_files::printable(name_in(q, line.from())),
                    gigabytes(q.bytes)
                )),
                listed(quantizations, line.from())
            ),
        )),
    }
}

/// The log line of a model with no `files` that names the quantization it loads: `model` is
/// the model's name, `repo` its repository, and `cache_only` why the listing it chose from is
/// the cache's alone ([`Snapshot::cache_only_reason`]), so the line says the choice was made
/// among what the cache holds.
struct LoadLine<'a> {
    model: &'a str,
    repo: &'a str,
    cache_only: Option<&'a str>,
}

impl LoadLine<'_> {
    /// Where the listing the line names quantizations from came from.
    fn from(&self) -> ListedFrom {
        if self.cache_only.is_some() {
            ListedFrom::Cache
        } else {
            ListedFrom::Repository
        }
    }

    /// The line for `default`, the quantization a model with no `files` loads, of
    /// `quantizations`, the complete GGUF quantizations listed.
    fn default(&self, default: &Quantization, quantizations: &[Quantization]) -> String {
        let others = match quantizations.len() {
            1 if self.cache_only.is_some() => {
                " The cache holds no other complete GGUF quantization of the repo.".to_string()
            }
            1 => " It is the only complete GGUF quantization the repo offers.".to_string(),
            count => format!(
                " To load another of {}, name it in the model's `files`: {}.",
                offered(count, self.cache_only.is_some()),
                listed(quantizations, self.from())
            ),
        };
        format!(
            "Model '{}' loads {} from Hugging Face repo '{}', the quantization a model with no `files` loads: {}.{}{others}",
            self.model,
            describe_sized(default),
            self.repo,
            default_reason(quantizations, default),
            chosen_from(self.cache_only)
        )
    }

    /// The line for `chosen`, the largest of `count` complete GGUF quantizations whose weights
    /// fit `memory` when `default`'s do not, which `files` pins as `pinned`.
    fn step_down(
        &self,
        chosen: &Quantization,
        default: &Quantization,
        count: usize,
        memory: u64,
        pinned: &str,
    ) -> String {
        format!(
            "Model '{}' loads {} from Hugging Face repo '{}', the largest of {} whose weights fit the {} of memory free for it with room for the context; {} ({}), which a model with no `files` loads, does not.{} Pin it with `files: [{pinned}]`.",
            self.model,
            describe_sized(chosen),
            self.repo,
            offered(count, self.cache_only.is_some()),
            gibibytes(memory),
            gguf_files::printable(&default.name),
            gigabytes(default.bytes),
            chosen_from(self.cache_only)
        )
    }
}

/// `its 26 complete GGUF quantizations`, or with a listing of the cache alone, `the 3 complete
/// GGUF quantizations in the cache`. `count` is at least 2.
fn offered(count: usize, cache_only: bool) -> String {
    if cache_only {
        format!("the {count} complete GGUF quantizations in the cache")
    } else {
        format!("its {count} complete GGUF quantizations")
    }
}

/// The sentence a load line adds when the listing is the cache's alone, `cache_only` saying
/// why, as in "The Hugging Face Hub could not be reached (HTTP 503), so Spice chose among
/// what the Hugging Face cache holds, and a load that reads the Hub may choose another." Empty
/// for the Hub's listing.
fn chosen_from(cache_only: Option<&str>) -> String {
    cache_only.map_or_else(String::new, |reason| {
        let mut chars = reason.chars();
        let reason = chars.next().map_or_else(String::new, |first| {
            first.to_uppercase().chain(chars).collect::<String>()
        });
        format!(
            " {reason}, so Spice chose among what the Hugging Face cache holds, and a load that reads the Hub may choose another."
        )
    })
}

/// Why `default` is the quantization a model with no `files` loads, for the log line that
/// says so: it carries `Q4_K_M`, or `Q8_0` and no quantization carries `Q4_K_M`, or neither
/// is offered and it is the first by path.
fn default_reason(quantizations: &[Quantization], default: &Quantization) -> String {
    let [q4, q8] = gguf_files::DEFAULT_TAGS;
    if gguf_files::carries(default, q4) {
        format!("its {q4}, as llama.cpp loads it")
    } else if gguf_files::carries(default, q8)
        && !quantizations.iter().any(|q| gguf_files::carries(q, q4))
    {
        format!("its {q8}, as llama.cpp loads it when a repository has no {q4}")
    } else {
        format!(
            "its first by path, as llama.cpp loads it when a repository has neither {q4} nor {q8}"
        )
    }
}

/// `BF16 (1.2 GB), Q8_0 (0.6 GB), ...`: the quantizations a model's `files` can name, largest
/// first, as a load line or an error lists them, each as [`name_in`] names it. Past
/// [`gguf_files::LISTED`], the rest are counted: `..., and 3 more`.
fn listed(quantizations: &[Quantization], from: ListedFrom) -> String {
    gguf_files::listed(
        quantizations
            .iter()
            .map(|q| format!("{} ({})", name_in(q, from), gigabytes(q.bytes))),
    )
}

/// What a model's `files` names `quantization` by, as a load line or an error advises it: its
/// name, or, from a listing of the cache alone, its first file's path. A name read from one
/// cache can name another quantization, or several, once the Hub's listing is read, as a folder
/// that holds one quantization in the cache and two in the repository; a path names one file.
fn name_in(quantization: &Quantization, from: ListedFrom) -> &str {
    match from {
        ListedFrom::Repository => &quantization.name,
        ListedFrom::Cache => quantization
            .files
            .first()
            .map_or(quantization.name.as_str(), String::as_str),
    }
}

/// `quantization Q4_K_M (1 file, 18.3 GB)`: a quantization Spice chose, with its size.
fn describe_sized(quantization: &Quantization) -> String {
    let files = quantization.files.len();
    format!(
        "quantization {} ({files} {}, {})",
        gguf_files::printable(&quantization.name),
        if files == 1 { "file" } else { "files" },
        gigabytes(quantization.bytes)
    )
}

/// `quantization UD-Q4_K_XL (6 files)`, or `3 files` when the files are no one quantization.
fn describe(quantization: Option<&str>, files: usize) -> String {
    let files = if files == 1 {
        "1 file".to_string()
    } else {
        format!("{files} files")
    };
    quantization.map_or_else(|| files.clone(), |q| format!("quantization {q} ({files})"))
}

fn is_gguf_path(path: &str) -> bool {
    std::path::Path::new(path)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("gguf"))
}

/// Bytes as the Hub shows a file's size, in decimal gigabytes.
#[expect(clippy::cast_precision_loss, reason = "a size shown to one decimal")]
fn gigabytes(bytes: u64) -> String {
    format!("{:.1} GB", bytes as f64 / 1e9)
}

/// Bytes of memory, in binary gibibytes.
#[expect(clippy::cast_precision_loss, reason = "a size shown to one decimal")]
fn gibibytes(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / f64::from(1u32 << 30))
}

/// mistral.rs's weight file patterns (`mistralrs-core/src/pipeline/paths.rs`).
static SAFETENSORS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"model-\d+-of-\d+\.safetensors\b",
        r"model\.safetensors\b",
        r"consolidated\.safetensors\b",
    ]
    .iter()
    .filter_map(|pattern| Regex::new(pattern).ok())
    .collect()
});
static PICKLES: LazyLock<Option<Regex>> =
    LazyLock::new(|| Regex::new(r"pytorch_model-\d{5}-of-\d{5}.((pth)|(pt)|(bin))\b").ok());

/// Downloads what a model loads from: the GGUF files `gguf` names (every shard, and any MTP
/// draft head), or, when it names none, the repo's safetensors (or sharded pickle) weights;
/// and the top-level configs. Returns the snapshot directory.
///
/// # Errors
///
/// Returns an error if the repository lacks a file the engine needs, or a download fails.
pub async fn download(snapshot: &Snapshot, gguf: &[&str]) -> Result<PathBuf, DownloadError> {
    let names: Vec<&str> = snapshot.files().map(|file| file.path.as_str()).collect();
    let mut paths = configs(&names);
    if gguf.is_empty() {
        for alternatives in [
            ["tokenizer.json", "tekken.json"],
            ["config.json", "params.json"],
        ] {
            if !alternatives.iter().any(|name| names.contains(name)) {
                return Err(DownloadError::missing(
                    snapshot,
                    format!("'{}' or '{}'", alternatives[0], alternatives[1]),
                ));
            }
        }
        let weights = match weights(&names) {
            Some(Weights::Index) => crate::hf::read_shards(snapshot).await?,
            Some(Weights::Files(files)) => files,
            None => {
                return Err(DownloadError::missing(
                    snapshot,
                    "weights the model can load ('model.safetensors', 'model-00001-of-0000N.safetensors', 'consolidated.safetensors' or sharded 'pytorch_model-00001-of-0000N.bin')",
                ));
            }
        };
        paths.extend(weights);
    } else {
        paths.extend(gguf.iter().map(ToString::to_string));
    }
    paths.sort();
    paths.dedup();
    let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
    snapshot.download(&paths).await?;
    Ok(snapshot.dir())
}

/// The repository's top-level JSON and Jinja files: every config, tokenizer and chat template
/// the engine reads by name.
fn configs(names: &[&str]) -> Vec<String> {
    names
        .iter()
        .filter(|name| !name.contains('/'))
        .filter(|name| {
            std::path::Path::new(name)
                .extension()
                .is_some_and(|extension| extension == "json" || extension == "jinja")
        })
        .map(ToString::to_string)
        .collect()
}

/// The file mistral.rs reads a downloaded repository's chat template from: its
/// `chat_template.jinja`, else its `tokenizer_config.json`. Every top-level JSON and Jinja file
/// of the repository is downloaded with the model, so a file missing from `dir` is missing from
/// the repository.
pub async fn chat_template_file(dir: &Path) -> Option<PathBuf> {
    for name in ["chat_template.jinja", "tokenizer_config.json"] {
        let path = dir.join(name);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return Some(path);
        }
    }
    None
}

/// Where the weights mistral.rs loads are named.
#[derive(Debug, PartialEq, Eq)]
enum Weights {
    /// In `model.safetensors.index.json`, which must be read first.
    Index,
    /// These files.
    Files(Vec<String>),
}

/// The weights mistral.rs would load from a directory holding `names`, as its
/// `get_model_paths` chooses them: the safetensors index, else the safetensors files, else the
/// sharded pickles. `None` when there are none.
fn weights(names: &[&str]) -> Option<Weights> {
    let top_level: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| !name.contains('/'))
        .collect();
    if top_level.contains(&SAFETENSOR_INDEX) {
        return Some(Weights::Index);
    }
    let safetensors: Vec<String> = top_level
        .iter()
        .filter(|name| SAFETENSORS.iter().any(|pattern| pattern.is_match(name)))
        .map(ToString::to_string)
        .collect();
    if !safetensors.is_empty() {
        return Some(Weights::Files(safetensors));
    }
    let pickles: Vec<String> = top_level
        .iter()
        .filter(|name| {
            PICKLES
                .as_ref()
                .is_some_and(|pattern| pattern.is_match(name))
                && [".pth", ".pt", ".bin"]
                    .iter()
                    .any(|extension| name.ends_with(extension))
        })
        .map(ToString::to_string)
        .collect();
    (!pickles.is_empty()).then_some(Weights::Files(pickles))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(names: &[&str]) -> Option<Weights> {
        weights(names)
    }

    fn owned(names: &[&str]) -> Vec<String> {
        names.iter().map(ToString::to_string).collect()
    }

    /// As mistral.rs does, a repository's `chat_template.jinja` is read before its
    /// `tokenizer_config.json`, and a repository with neither has no template file.
    #[tokio::test]
    async fn the_chat_template_file_is_the_jinja_file_before_tokenizer_config() {
        let dir = tempfile::tempdir().expect("a scratch directory");
        assert_eq!(chat_template_file(dir.path()).await, None);
        let config = dir.path().join("tokenizer_config.json");
        std::fs::write(&config, "{}").expect("tokenizer_config.json writes");
        assert_eq!(chat_template_file(dir.path()).await, Some(config));
        let jinja = dir.path().join("chat_template.jinja");
        std::fs::write(&jinja, "").expect("chat_template.jinja writes");
        assert_eq!(chat_template_file(dir.path()).await, Some(jinja));
    }

    #[test]
    fn weights_are_chosen_as_mistral_rs_chooses_them() {
        // The index wins over everything.
        assert_eq!(
            files(&[
                "model.safetensors.index.json",
                "model-00001-of-00002.safetensors"
            ]),
            Some(Weights::Index)
        );
        // Safetensors, sharded or not, over pickles.
        assert_eq!(
            files(&[
                "model-00001-of-00002.safetensors",
                "model-00002-of-00002.safetensors",
                "pytorch_model-00001-of-00002.bin",
                "config.json",
            ]),
            Some(Weights::Files(owned(&[
                "model-00001-of-00002.safetensors",
                "model-00002-of-00002.safetensors"
            ])))
        );
        assert_eq!(
            files(&["model.safetensors", "consolidated.safetensors"]),
            Some(Weights::Files(owned(&[
                "model.safetensors",
                "consolidated.safetensors"
            ])))
        );
        // Only sharded pickles: mistral.rs does not load a lone `pytorch_model.bin`.
        assert_eq!(
            files(&[
                "pytorch_model-00001-of-00002.bin",
                "pytorch_model-00002-of-00002.bin"
            ]),
            Some(Weights::Files(owned(&[
                "pytorch_model-00001-of-00002.bin",
                "pytorch_model-00002-of-00002.bin"
            ])))
        );
        assert_eq!(files(&["pytorch_model.bin", "config.json"]), None);
        // A local directory is read by its top-level names: weights in a folder are not
        // the model's.
        assert_eq!(files(&["onnx/model.safetensors", "config.json"]), None);
    }

    /// The repository decides how many quantizations it offers, so a load line or an error
    /// names the first 40, largest first, and counts the rest.
    #[test]
    fn a_list_of_quantizations_names_forty_and_counts_the_rest() {
        let quantizations = |count: u64| -> Vec<Quantization> {
            (1..=count)
                .rev()
                .map(|i| Quantization {
                    name: format!("Q{i}"),
                    files: vec![format!("model-Q{i}.gguf")],
                    bytes: i * 100_000_000,
                })
                .collect()
        };

        assert_eq!(
            listed(&quantizations(3), ListedFrom::Repository),
            "Q3 (0.3 GB), Q2 (0.2 GB), Q1 (0.1 GB)"
        );

        let forty = listed(&quantizations(40), ListedFrom::Repository);
        assert_eq!(forty.matches(" GB)").count(), 40);
        assert!(forty.starts_with("Q40 (4.0 GB), Q39 (3.9 GB), "), "{forty}");
        assert!(forty.ends_with(", Q2 (0.2 GB), Q1 (0.1 GB)"), "{forty}");

        let many = listed(&quantizations(43), ListedFrom::Repository);
        assert_eq!(many.matches(" GB)").count(), 40);
        assert!(many.starts_with("Q43 (4.3 GB), Q42 (4.2 GB), "), "{many}");
        assert!(
            many.ends_with(", Q5 (0.5 GB), Q4 (0.4 GB), and 3 more"),
            "{many}"
        );
    }

    /// The load line of a model with no `files` names what it loads, from what, and how to
    /// load another: of the repository's quantizations, by name, or, with the Hub not read, of
    /// the cache's, by path, which a load that reads the Hub may choose otherwise from; and says
    /// when there is no other.
    #[test]
    fn a_load_line_says_what_it_chose_among() {
        let quantization = |name: &str, bytes: u64| Quantization {
            name: name.to_string(),
            files: vec![format!("Model-{name}.gguf")],
            bytes,
        };
        let q8 = quantization("Q8_0", 600_000_000);
        let q4 = quantization("Q4_K_M", 400_000_000);
        let q2 = quantization("Q2_K", 300_000_000);
        let line = |cache_only| LoadLine {
            model: "qwen3",
            repo: "o/m",
            cache_only,
        };
        let offline = Some("`HF_HUB_OFFLINE` is set");
        let outage = Some("the Hugging Face Hub could not be reached (connection failed)");
        let start = "Model 'qwen3' loads quantization";

        assert_eq!(
            line(None).default(&q4, &[q8.clone(), q4.clone(), q2.clone()]),
            format!(
                "{start} Q4_K_M (1 file, 0.4 GB) from Hugging Face repo 'o/m', the quantization a model with no `files` loads: its Q4_K_M, as llama.cpp loads it. To load another of its 3 complete GGUF quantizations, name it in the model's `files`: Q8_0 (0.6 GB), Q4_K_M (0.4 GB), Q2_K (0.3 GB)."
            )
        );
        assert_eq!(
            line(None).default(&q8, std::slice::from_ref(&q8)),
            format!(
                "{start} Q8_0 (1 file, 0.6 GB) from Hugging Face repo 'o/m', the quantization a model with no `files` loads: its Q8_0, as llama.cpp loads it when a repository has no Q4_K_M. It is the only complete GGUF quantization the repo offers."
            )
        );
        assert_eq!(
            line(offline).default(&q8, &[q8.clone(), q2.clone()]),
            format!(
                "{start} Q8_0 (1 file, 0.6 GB) from Hugging Face repo 'o/m', the quantization a model with no `files` loads: its Q8_0, as llama.cpp loads it when a repository has no Q4_K_M. `HF_HUB_OFFLINE` is set, so Spice chose among what the Hugging Face cache holds, and a load that reads the Hub may choose another. To load another of the 2 complete GGUF quantizations in the cache, name it in the model's `files`: Model-Q8_0.gguf (0.6 GB), Model-Q2_K.gguf (0.3 GB)."
            )
        );
        assert_eq!(
            line(outage).default(&q4, std::slice::from_ref(&q4)),
            format!(
                "{start} Q4_K_M (1 file, 0.4 GB) from Hugging Face repo 'o/m', the quantization a model with no `files` loads: its Q4_K_M, as llama.cpp loads it. The Hugging Face Hub could not be reached (connection failed), so Spice chose among what the Hugging Face cache holds, and a load that reads the Hub may choose another. The cache holds no other complete GGUF quantization of the repo."
            )
        );
        assert_eq!(
            line(None).step_down(&q2, &q4, 3, 1 << 30, "{path: Q2_K}"),
            format!(
                "{start} Q2_K (1 file, 0.3 GB) from Hugging Face repo 'o/m', the largest of its 3 complete GGUF quantizations whose weights fit the 1.0 GiB of memory free for it with room for the context; Q4_K_M (0.4 GB), which a model with no `files` loads, does not. Pin it with `files: [{{path: Q2_K}}]`."
            )
        );
        assert_eq!(
            line(offline).step_down(&q2, &q4, 3, 1 << 30, "{path: Q2_K}"),
            format!(
                "{start} Q2_K (1 file, 0.3 GB) from Hugging Face repo 'o/m', the largest of the 3 complete GGUF quantizations in the cache whose weights fit the 1.0 GiB of memory free for it with room for the context; Q4_K_M (0.4 GB), which a model with no `files` loads, does not. `HF_HUB_OFFLINE` is set, so Spice chose among what the Hugging Face cache holds, and a load that reads the Hub may choose another. Pin it with `files: [{{path: Q2_K}}]`."
            )
        );
    }

    #[test]
    fn configs_are_every_top_level_json_and_jinja_file() {
        assert_eq!(
            configs(&[
                "config.json",
                "tokenizer.json",
                "chat_template.jinja",
                "generation_config.json",
                "README.md",
                "model.safetensors",
                "1_Pooling/config.json",
            ]),
            owned(&[
                "config.json",
                "tokenizer.json",
                "chat_template.jinja",
                "generation_config.json"
            ])
        );
    }
}
