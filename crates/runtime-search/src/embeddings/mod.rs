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

#![allow(clippy::missing_errors_doc)]

pub mod common;
pub mod execution_plan;
pub mod table;
#[cfg(any(feature = "s3_vectors", feature = "elasticsearch"))]
pub mod warm_index;

use std::sync::Arc;

use chunking::{Chunker, ChunkingConfig};
use llms::embeddings::{Embed, Error as EmbedError};
use runtime_acceleration::acceleration::{Acceleration, RefreshMode, ZeroResultsAction};
use std::collections::HashMap;
use tokio::sync::RwLock;

pub type EmbeddingModelStore = HashMap<String, Arc<dyn Embed>>;

/// The read behavior a warm search tier should be built with for a table with this
/// `acceleration`, as `warm_index::with_memory_warm_index` takes it.
///
/// A warm tier starts empty on every process start and is only ever filled by the
/// acceleration write path, and it is the compound index's *primary* read tier. It is
/// therefore only sound when the accelerator also starts empty, so that repopulating the
/// accelerator necessarily carries every row past the tier. `None` — no warm tier at all —
/// whenever that does not hold:
///
/// - The table has no enabled acceleration, so nothing hydrates the tier at all (#12101).
/// - The accelerator keeps its rows across a restart (#12102) — which is a question for the
///   engine and the mode together, not the mode alone, since `PostgreSQL` keeps them in a server
///   that outlives the process whatever the mode says. The refresh that follows then
///   loads only what the accelerator is missing — for `append` and `changes` a delta, and for
///   `full` possibly nothing at all, since a checkpointed dataset with no
///   `refresh_check_interval` skips its startup refresh outright. The accelerator and the
///   vector engine still hold everything, so the tier settles at a strict subset and never
///   catches up — and because it is the primary, a search answers from that subset. Neither
///   read mode rescues it: `ReturnEmpty` never consults the engine index, and `UseSource`
///   falls back only when the tier returns *exactly zero* rows, which a partly-filled tier
///   does not.
/// - The accelerator is a cache (`refresh_mode: caching`). It is filled one cache miss at a
///   time, and with neither `refresh_on_startup: always` nor a `refresh_check_interval` it
///   schedules no refresh at all, so the tier holds only the rows read so far — partial by
///   design rather than by timing, which is why this applies to every accelerator mode.
///
/// Declining the tier costs a dataset the in-memory read path, not any results: the vector
/// engine index holds the whole dataset and serves the search directly. The warm tier is an
/// optimization, so where it cannot be complete it is not installed.
#[must_use]
pub fn warm_index_on_zero_results(
    acceleration: Option<&Acceleration>,
) -> Option<&ZeroResultsAction> {
    // Both declines log their own reason here, where it is known: the consumer only sees
    // `None` and cannot tell the two apart.
    let Some(acceleration) = acceleration.filter(|acceleration| acceleration.enabled) else {
        tracing::debug!(
            "Not adding an in-memory warm vector index: the table has no enabled acceleration, so nothing would populate the warm tier. Searches will be served by the vector engine directly."
        );
        return None;
    };

    if acceleration.retains_data_across_restarts() {
        tracing::debug!(
            "Not adding an in-memory warm vector index: the accelerator keeps its rows across a restart, so the warm tier would hold only the rows refreshed since startup. Searches will be served by the vector engine directly."
        );
        return None;
    }

    if matches!(acceleration.refresh_mode, Some(RefreshMode::Caching)) {
        tracing::debug!(
            "Not adding an in-memory warm vector index: a caching accelerator is populated per cache miss, so the warm tier would hold only the rows read so far. Searches will be served by the vector engine directly."
        );
        return None;
    }

    Some(&acceleration.on_zero_results)
}

pub async fn construct_chunker(
    model_name: &str,
    chunk_config: &ChunkingConfig<'_>,
    embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
) -> Result<Arc<dyn Chunker>, EmbedError> {
    let embedding_models_guard = embedding_models.read().await;
    let Some(embed_model) = embedding_models_guard.get(model_name) else {
        return Err(EmbedError::ModelDoesNotExist {
            model_name: model_name.to_string(),
        });
    };
    embed_model.chunker(chunk_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_acceleration::Engine;
    use runtime_acceleration::acceleration::Mode;

    /// An enabled acceleration in `mode`, whose `on_zero_results` is distinguishable from the
    /// default so a passed-through value is visible in the assertion.
    fn accelerated(mode: Mode) -> Acceleration {
        Acceleration {
            enabled: true,
            mode,
            on_zero_results: ZeroResultsAction::UseSource,
            ..Acceleration::default()
        }
    }

    /// Regression test for #12101: the acceleration write path is the only thing that fills a
    /// warm search tier, so an absent or disabled acceleration must yield no warm tier at all.
    #[test]
    fn on_zero_results_is_none_without_an_enabled_acceleration() {
        assert_eq!(
            warm_index_on_zero_results(None),
            None,
            "a table with no acceleration must get no warm tier"
        );

        let disabled = Acceleration {
            enabled: false,
            on_zero_results: ZeroResultsAction::UseSource,
            ..Acceleration::default()
        };
        assert_eq!(
            warm_index_on_zero_results(Some(&disabled)),
            None,
            "a disabled acceleration must get no warm tier"
        );

        let enabled = Acceleration {
            enabled: true,
            on_zero_results: ZeroResultsAction::UseSource,
            ..Acceleration::default()
        };
        assert_eq!(
            warm_index_on_zero_results(Some(&enabled)),
            Some(&ZeroResultsAction::UseSource),
            "an enabled acceleration passes its on_zero_results through"
        );
    }

    /// Regression test for #12102: an accelerator that survives a restart is only ever refreshed
    /// with what it is missing, so a warm tier — which does *not* survive — would hold that
    /// remainder alone while serving as the primary read tier. It must be declined instead.
    ///
    /// This holds for every refresh mode, `full` included: a checkpointed dataset with no
    /// `refresh_check_interval` skips its startup refresh entirely, so nothing at all reaches the
    /// tier.
    #[test]
    fn on_zero_results_is_none_for_an_accelerator_that_survives_a_restart() {
        for mode in [Mode::File, Mode::FileUpdate] {
            assert_eq!(
                warm_index_on_zero_results(Some(&accelerated(mode))),
                None,
                "{mode:?} keeps its rows across a restart, so the warm tier would hold only the \
                 rows refreshed since startup"
            );
        }
    }

    /// An accelerator that starts empty is necessarily reloaded in full, so every row passes
    /// through the warm tier and it stays sound. This is the default `mode: memory`
    /// configuration, which must keep its warm tier.
    #[test]
    fn an_ephemeral_accelerator_keeps_its_warm_tier() {
        for mode in [Mode::Memory, Mode::FileCreate] {
            assert_eq!(
                warm_index_on_zero_results(Some(&accelerated(mode))),
                Some(&ZeroResultsAction::UseSource),
                "{mode:?} starts empty, so repopulating it carries every row past the warm tier"
            );
        }
    }

    /// `PostgreSQL` keeps its rows in a server that outlives the process, and its accelerator never
    /// reads the mode when opening the table. So the two modes every other engine starts empty in
    /// are, for `PostgreSQL`, still populated after a restart — reproducing #12102 through the
    /// default `mode: memory` if the tier were decided by the mode alone.
    #[test]
    fn on_zero_results_is_none_for_postgres_in_an_otherwise_ephemeral_mode() {
        for mode in [Mode::Memory, Mode::FileCreate] {
            let postgres = Acceleration {
                engine: Engine::PostgreSQL,
                ..accelerated(mode)
            };
            assert_eq!(
                warm_index_on_zero_results(Some(&postgres)),
                None,
                "postgres in {mode:?} keeps its rows in an external server across a restart, so \
                 the warm tier would hold only the rows refreshed since startup"
            );
        }
    }

    /// A caching accelerator is filled one cache miss at a time, and with neither
    /// `refresh_on_startup: always` nor a `refresh_check_interval` it schedules no refresh at all,
    /// so the warm tier would answer as primary from whatever subset has been read so far. That
    /// holds for every accelerator mode, including the ephemeral ones that are otherwise sound.
    #[test]
    fn on_zero_results_is_none_for_a_caching_accelerator() {
        for mode in [Mode::Memory, Mode::FileCreate, Mode::File, Mode::FileUpdate] {
            let caching = Acceleration {
                refresh_mode: Some(RefreshMode::Caching),
                ..accelerated(mode)
            };
            assert_eq!(
                warm_index_on_zero_results(Some(&caching)),
                None,
                "{mode:?} with refresh_mode: caching is populated per cache miss, so the warm \
                 tier would hold only the rows read so far"
            );
        }
    }

    /// The modes that survive a restart are exactly the file modes that open an existing file.
    #[test]
    fn only_the_reopening_file_modes_retain_data_across_restarts() {
        assert!(!Mode::Memory.retains_data_across_restarts());
        assert!(!Mode::FileCreate.retains_data_across_restarts());
        assert!(Mode::File.retains_data_across_restarts());
        assert!(Mode::FileUpdate.retains_data_across_restarts());
    }

    /// The engine decides it too: a file-backed engine follows its mode, while `PostgreSQL` keeps
    /// its rows in an external server in every mode.
    #[test]
    fn postgres_retains_data_in_every_mode_unlike_a_file_backed_engine() {
        for mode in [Mode::Memory, Mode::FileCreate, Mode::File, Mode::FileUpdate] {
            let postgres = Acceleration {
                engine: Engine::PostgreSQL,
                ..accelerated(mode)
            };
            let duckdb = Acceleration {
                engine: Engine::DuckDB,
                ..accelerated(mode)
            };
            assert!(
                postgres.retains_data_across_restarts(),
                "postgres keeps its rows across a restart in {mode:?}"
            );
            assert_eq!(
                duckdb.retains_data_across_restarts(),
                mode.retains_data_across_restarts(),
                "a file-backed engine follows its mode in {mode:?}"
            );
        }
    }
}
