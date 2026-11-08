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

//! The `duckdb` vector engine, removed in v2.5.
//!
//! A dataset or view that still selects it fails to load with
//! [`removed_vector_engine_message`]. Spice never substitutes another engine for it:
//! Cayenne indexes embedding columns for `vector_search` itself, so the fix is to drop
//! the `vectors` block, which the message says.

use spicepod::{semantic::Column, vector::VectorStore};

/// The section of the Cayenne migration guide that covers the `duckdb` vector engine.
pub const DUCKDB_VECTOR_ENGINE_MIGRATION_DOCS: &str = "https://spiceai.org/docs/components/data-accelerators/cayenne/migration#migrating-the-duckdb-vector-engine";

/// The removed engine's name, as `vectors.engine` and a column's `embeddings[].engine`
/// spell it.
const REMOVED_VECTOR_ENGINE: &str = "duckdb";

/// The cause reported when a dataset or view selects the removed `duckdb` vector engine.
/// The runtime prefixes it with the component that failed to load.
#[must_use]
pub fn removed_vector_engine_message() -> String {
    format!(
        "the vector engine `duckdb` is no longer supported as of v2.5 and cannot be used. Remove the `vectors` block and use `acceleration.engine: cayenne`, which indexes embedding columns for `vector_search` itself. See: {DUCKDB_VECTOR_ENGINE_MIGRATION_DOCS}"
    )
}

/// Whether a dataset or view selects the removed `duckdb` vector engine, either through
/// an enabled `vectors` block or through a column's `embeddings[].engine` override. A
/// `vectors` block with `enabled: false` selects no engine, so its `engine` is ignored, as
/// it always has been.
#[must_use]
pub fn uses_removed_vector_engine(vectors: Option<&VectorStore>, columns: &[Column]) -> bool {
    if let Some(vectors) = vectors {
        if !vectors.enabled {
            return false;
        }
        if vectors.engine.as_deref() == Some(REMOVED_VECTOR_ENGINE) {
            return true;
        }
    }

    columns
        .iter()
        .flat_map(|column| &column.embeddings)
        .any(|embedding| embedding.engine.as_deref() == Some(REMOVED_VECTOR_ENGINE))
}

#[cfg(test)]
mod tests {
    use super::*;
    use spicepod::semantic::ColumnLevelEmbeddingConfig;

    fn vectors(enabled: bool, engine: Option<&str>) -> VectorStore {
        VectorStore {
            enabled,
            engine: engine.map(str::to_string),
            ..Default::default()
        }
    }

    fn embedded_column(engine: Option<&str>) -> Column {
        let mut embedding = ColumnLevelEmbeddingConfig::model("potion");
        embedding.engine = engine.map(str::to_string);
        Column::new("body").with_embedding(embedding)
    }

    #[test]
    fn the_removed_vector_engine_message_names_the_cayenne_fix() {
        insta::assert_snapshot!(
            "removed_vector_engine_message",
            removed_vector_engine_message()
        );
    }

    #[test]
    fn only_a_selected_duckdb_vector_engine_is_removed() {
        let duckdb = vectors(true, Some("duckdb"));
        let disabled_duckdb = vectors(false, Some("duckdb"));
        let s3 = vectors(true, Some("s3_vectors"));
        let plain = [embedded_column(None)];
        let duckdb_column = [embedded_column(Some("duckdb"))];
        let s3_column = [embedded_column(Some("s3_vectors"))];

        assert!(uses_removed_vector_engine(Some(&duckdb), &plain));
        assert!(uses_removed_vector_engine(None, &duckdb_column));
        assert!(uses_removed_vector_engine(Some(&s3), &duckdb_column));

        assert!(!uses_removed_vector_engine(Some(&disabled_duckdb), &plain));
        assert!(!uses_removed_vector_engine(
            Some(&disabled_duckdb),
            &duckdb_column
        ));
        assert!(!uses_removed_vector_engine(Some(&s3), &s3_column));
        assert!(!uses_removed_vector_engine(None, &plain));
    }
}
