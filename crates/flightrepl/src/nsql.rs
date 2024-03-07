use candle_examples::token_output_stream::TokenOutputStream;
use candle_transformers::{generation::LogitsProcessor, models::quantized_llama::ModelWeights};
use snafu::prelude::*;
use std::{collections::HashMap, path::Path};
use tokenizers::{Encoding, Tokenizer};

use arrow::datatypes::SchemaRef;
use candle_core::{quantized::ggml_file, Tensor};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to run the NSQL model"))]
    FailedToRunModel { e: Box<dyn std::error::Error>  },

    #[snafu(display("Local model not found"))]
    LocalModelNotFound { },

    #[snafu(display("Failed to load model from file {e}"))]
    FailedToLoadModel { e: candle_core::Error },

    #[snafu(display("Failed to load model tokenizer"))]
    FailedToLoadTokenizer { e: Box<dyn std::error::Error> },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn load_model() -> Result<Option<String>> {
    return Ok(Some("Hello".to_string()))
}

pub fn run_model(prompt: String) -> Result<Option<String>> {
    let model_path = "/Users/jeadie/.spice/DuckDB-NSQL-7B-v0.1-q8_0.gguf";
    let mut file = std::fs::File::open(model_path).map_err(|_| Error::LocalModelNotFound {  })?;
    println!("GMMGL");
    let model_content = ggml_file::Content::read(&mut file, &candle_core::Device::Cpu)
                .map_err(|e| e.with_path(model_path)).map_err(|e| Error::FailedToLoadModel{e})?;
    let model = ModelWeights::from_ggml(model_content, 1).map_err(|e| Error::FailedToLoadModel { e })?;

    let oken = Tokenizer::from_file("/Users/jeadie/.spice/llama2_tokenizer.json").map_err(|e| Error::FailedToLoadTokenizer { e })?;
    let tos = TokenOutputStream::new(oken);
    _run_model(prompt.to_string(), tos, model).map_err(|e| Error::FailedToRunModel{e} )
}

pub fn _run_model(prompt_str: String, mut tos: TokenOutputStream, mut model: ModelWeights) -> std::result::Result<Option<String>, Box<dyn std::error::Error>> {

    let tokens = tos
        .tokenizer()
        .encode(prompt_str, true).unwrap(); // TODO: help
    
    let prompt_tokens = [tokens.get_ids(),].concat();
    let to_sample = 1000; // args.sample_len.saturating_sub(1);
    let max_seq_len = 4096;
    let repeat_last_n = 64;
    let repeat_penalty = 1.1;
    let device  = candle_core::Device::Cpu;
    let seed= 299792458;
    let temperature =0.8;
    let prompt_tokens = if prompt_tokens.len() + to_sample > max_seq_len - 10 {
        let to_remove = prompt_tokens.len() + to_sample + 10 - max_seq_len;
        prompt_tokens[prompt_tokens.len().saturating_sub(to_remove)..].to_vec()
    } else {
        prompt_tokens
    };
    let mut all_tokens = vec![];
    let mut logits_processor = LogitsProcessor::new(seed, Some(temperature), None);

    let mut next_token = {
        let mut next_token = 0;
        for (pos, token) in prompt_tokens.iter().enumerate() {
            let input = Tensor::new(&[*token], &device)?.unsqueeze(0)?;
            let logits = model.forward(&input, pos)?;
            let logits = logits.squeeze(0)?;
            next_token = logits_processor.sample(&logits)?
        }
        next_token
    };
    all_tokens.push(next_token);
    if let Some(t) = tos.next_token(next_token)? {
        print!("{t}");
    }

    let eos_token = "</s>";
    let eos_token = *tos.tokenizer().get_vocab(true).get(eos_token).unwrap();
    let mut sampled = 0;
    for index in 0..to_sample {
        let input = Tensor::new(&[next_token], &device)?.unsqueeze(0)?;
        let logits = model.forward(&input, prompt_tokens.len() + index)?;
        let logits = logits.squeeze(0)?;
        let logits = if repeat_penalty == 1. {
            logits
        } else {
            let start_at = all_tokens.len().saturating_sub(repeat_last_n);
            candle_transformers::utils::apply_repeat_penalty(
                &logits,
                repeat_penalty,
                &all_tokens[start_at..],
            )?
        };
        next_token = logits_processor.sample(&logits)?;
        all_tokens.push(next_token);
        if let Some(t) = tos.next_token(next_token)? {
            print!("{t}");
        }
        sampled += 1;
        if next_token == eos_token {
            break;
        };
    }
    tos.decode_rest().map_err(|e| e.into())

}

pub async fn run_nsql(
    user_query: String,
    model: String,
    schemas: &HashMap<String, SchemaRef>,
) -> Result<String, Box<dyn std::error::Error>> {
    let create_tables: String = schemas.iter().map(|(t, s)| {
        format!("CREATE TABLE {} ({})", t, s.fields.iter().map(|f| {
            println!("Field: {} {}", f.name(), f.data_type().to_string());
                format!("{} {}", f.name(), f.data_type().to_string())
            }).collect::<Vec<_>>()
            .join(", ")
        )

    }).collect::<Vec<_>>()
    .join(" ");
    let prompt = format!("{} -- Using valid SQL, answer the following questions for the tables provided above. -- {}", create_tables, user_query);
    run_model(prompt).map(|x: Option<String>| x.unwrap_or_default()).map_err(|e| e.into())
}
