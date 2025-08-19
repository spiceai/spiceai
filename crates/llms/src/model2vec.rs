/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::embeddings::Error::{FailedToInstantiateEmbeddingModel, UnsupportedEmbeddingInput};
use crate::embeddings::Embed;
use async_openai::types::EmbeddingInput;
use async_trait::async_trait;
use model2vec_rs::model::StaticModel;
use snafu::ResultExt;
use std::fmt::{Debug, Formatter};

/// A wrapper around the `model2vec` library for generating text embeddings.
///
/// Model2Vec is a technique that distills embeddings from
/// transformer models into static word embeddings.
pub struct Model2Vec {
    pub name: String,
    model: StaticModel
}

impl Model2Vec {
    pub fn from_name(name: &str) -> Result<Self, super::embeddings::Error> {
        let model = StaticModel::from_pretrained(
            &name,
            None,
            None,
            None
        ).map_err(|e| FailedToInstantiateEmbeddingModel { source: e.into()})?;

        tracing::trace!("Model2Vec::from_name {}", name);
        Ok(Self { name: name.to_string(), model })
    }
}

impl Debug for Model2Vec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Model2Vec {}", self.name)
    }
}

#[async_trait]
impl Embed for Model2Vec {
    async fn embed(&self, input: EmbeddingInput) -> Result<Vec<Vec<f32>>, super::embeddings::Error> {
        match input {
            EmbeddingInput::String(s) =>
                Ok(vec![self.model.encode_single(&s)]),
            EmbeddingInput::StringArray(sentences) =>
                Ok(self.model.encode(&sentences)),
            _ => Err(
                UnsupportedEmbeddingInput { model: self.name.clone(), message: "Model2Vec models only support strings or vectors of strings".to_string() }
            ),
        }
    }

    fn size(&self) -> i32 {
        -1
    }
}

#[cfg(test)]
mod tests {
    use async_openai::types::EmbeddingInput;
    use crate::embeddings::Embed;
    use crate::model2vec::Model2Vec;

    #[tokio::test]
    async fn test_embed() {
        // This embedding is dim 256
        let model = Model2Vec::from_name("minishlab/potion-base-8M").expect("Must instantiate");

        let embed_sentence = model
            .embed(EmbeddingInput::String("hello world".to_string()))
            .await;

        assert!(embed_sentence.is_ok());

        let embed_sentence = embed_sentence.unwrap();
        assert_eq!(embed_sentence.len(), 1);
        assert_eq!(embed_sentence[0].len(), 256);

        let embed_sentences = model
            .embed(EmbeddingInput::StringArray(vec![
                "i can eat glass".to_string(),
                "for it does not hurt me".to_string(),
            ]))
            .await;

        assert!(embed_sentences.is_ok());

        let embed_sentences = embed_sentences.unwrap();
        assert_eq!(embed_sentences.len(), 2);
        for embedded_sentence in embed_sentences {
            assert_eq!(embedded_sentence.len(), 256);
        }

        let embed_ints = model
            .embed(EmbeddingInput::IntegerArray(vec![1]))
            .await;

        assert!(embed_ints.is_err());

        let embed_2d_int = model
            .embed(EmbeddingInput::ArrayOfIntegerArray(vec![vec![1]]))
            .await;

        assert!(embed_2d_int.is_err());
    }
}