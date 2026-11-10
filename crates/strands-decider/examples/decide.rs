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

//! Asks a Strands decider checkpoint questions about a state on llama.cpp, with the flags and
//! output of upstream's `strands-decider ask`:
//!
//! ```text
//! cargo run -p strands-decider --example decide --features metal -- \
//!   --base Qwen3.5-2B-Base-BF16.gguf --lora decider-lora-F32.gguf --checkpoint <decider dir> \
//!   --state "Help! My payouts have been failing for 3 days! " \
//!   --choice "Which team should handle this?=billing,sales,retail" \
//!   --noul "Does this convey urgency?" \
//!   --score "How frustrated is the writer?=calm,frustrated,depressed"
//! ```
//!
//! `--checkpoint` is the Hugging Face checkpoint directory (config, `head.safetensors`,
//! tokenizer); the base model and the adapter are GGUFs converted from it with llama.cpp's
//! `convert_hf_to_gguf.py` and `convert_lora_to_gguf.py`.
#![expect(clippy::print_stdout, reason = "an example prints its answers")]

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use llama_cpp::{Backend, LoraAdapter, Model, ModelParams};
use strands_decider::llama::LlamaHiddenStates;
use strands_decider::{
    Answer, ChoiceOptions, Content, Decider, NoulCriteria, Question, ScoreLevels,
};

#[derive(Parser)]
struct Args {
    /// The base model's GGUF.
    #[arg(long)]
    base: PathBuf,
    /// The adapter's GGUF; without it, the base model runs alone.
    #[arg(long)]
    lora: Option<PathBuf>,
    /// The checkpoint directory.
    #[arg(long)]
    checkpoint: PathBuf,
    #[arg(long, short)]
    state: String,
    /// A yes/no question; repeat.
    #[arg(long)]
    noul: Vec<String>,
    /// `question?=opt1,opt2,opt3`; repeat.
    #[arg(long)]
    choice: Vec<String>,
    /// `question?=low,mid,high` (ascending); repeat.
    #[arg(long)]
    score: Vec<String>,
    /// Layers offloaded to the GPU; all when unset.
    #[arg(long)]
    gpu_layers: Option<u32>,
}

/// `spec` split at its first `=` into the question and its comma-separated options, each
/// stripped, empty ones dropped, as upstream's CLI splits them.
fn split_spec(spec: &str) -> Result<(String, Vec<String>), String> {
    let (question, options) = spec
        .split_once('=')
        .ok_or_else(|| format!("needs 'question=opt1,opt2': {spec:?}"))?;
    let options = options
        .split(',')
        .map(str::trim)
        .filter(|o| !o.is_empty())
        .map(str::to_string)
        .collect();
    Ok((question.to_string(), options))
}

fn questions(args: &Args) -> Result<Vec<(String, Question)>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    for (i, q) in args.noul.iter().enumerate() {
        out.push((
            format!("noul_{i}"),
            Question::Noul {
                instructions: Content::from(q.as_str()),
                criteria: NoulCriteria::default(),
            },
        ));
    }
    for (i, spec) in args.choice.iter().enumerate() {
        let (q, options) = split_spec(spec)?;
        // Upstream builds a dict, so a repeated option is listed once, where it first appears.
        let mut unique: Vec<(String, Option<Content>)> = Vec::new();
        for o in options {
            if !unique.iter().any(|(name, _)| *name == o) {
                unique.push((o, Some(Content::from(""))));
            }
        }
        out.push((
            format!("choice_{i}"),
            Question::Choice {
                instructions: Content::from(q),
                options: ChoiceOptions::new(unique)?,
            },
        ));
    }
    for (i, spec) in args.score.iter().enumerate() {
        let (q, levels) = split_spec(spec)?;
        out.push((
            format!("score_{i}"),
            Question::Score {
                instructions: Content::from(q),
                levels: ScoreLevels::new(levels)?,
            },
        ));
    }
    Ok(out)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let questions = questions(&args)?;
    let decider = Decider::from_checkpoint_dir(&args.checkpoint)?;

    let backend = Backend::get();
    let mut params = ModelParams::default();
    if let Some(layers) = args.gpu_layers {
        params = params.with_gpu_layers(layers);
    }
    let model = Model::load(backend, &args.base, &params)?;
    let adapter = match &args.lora {
        Some(path) => Some(Arc::new(LoraAdapter::load(&model, path)?)),
        None => None,
    };
    let n_ctx = u32::try_from(decider.config().max_length)?;
    let mut hidden = LlamaHiddenStates::new(backend, &model, adapter, n_ctx, 512)?;

    let started = std::time::Instant::now();
    let evaluation =
        decider.evaluate(&mut hidden, &Content::from(args.state.as_str()), &questions)?;
    let elapsed = started.elapsed();

    for (name, answer) in &evaluation.answers {
        match answer {
            Answer::Noul { noul } => println!("{name} noul = {noul:.3}"),
            Answer::Choice {
                choice,
                probabilities,
                confidence,
            } => {
                println!("{name} -> {choice} (confidence {confidence:.3})");
                let mut sorted = probabilities.clone();
                sorted.sort_by(|a, b| b.1.total_cmp(&a.1));
                for (option, p) in sorted {
                    println!("    {option:<24} {p:.3}");
                }
            }
            Answer::Score {
                score,
                legend,
                probabilities,
                confidence,
            } => {
                println!("{name} score = {score:.2} (confidence {confidence:.3})");
                for ((level, p), (_, text)) in probabilities.iter().zip(legend) {
                    println!("    {level}: {text:<40} {p:.3}");
                }
            }
        }
    }
    println!("answers (rounded to 4 places): {:?}", evaluation.answers);
    println!(
        "{} input tokens in {:.2?}",
        evaluation.input_tokens, elapsed
    );
    Ok(())
}
