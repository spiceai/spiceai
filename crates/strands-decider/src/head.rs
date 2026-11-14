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
//! The pointer head: one attention score per option, query from the `<answer>` token and
//! key from the option's last token (upstream's `modeling.PointerHead`).
//!
//! A released "soup" of N checkpoints stores its N heads stacked into one head of N times the
//! pointer width, with each head's norm folded into its projections and the keys scaled so
//! that its logit is the mean of theirs. So a soup's head is evaluated as any other head.

use safetensors::{Dtype, SafeTensors};
use snafu::ensure;

use crate::{HeadSnafu, Result, as_f64};

/// `nn.LayerNorm`'s default epsilon, which the head is built with.
const LAYER_NORM_EPS: f64 = 1e-5;

#[derive(Debug, Clone)]
pub struct PointerHead {
    hidden: usize,
    dim: usize,
    norm_weight: Vec<f32>,
    norm_bias: Vec<f32>,
    /// `[dim, hidden]`, row-major, as `nn.Linear` stores it.
    q_weight: Vec<f32>,
    q_bias: Vec<f32>,
    k_weight: Vec<f32>,
    k_bias: Vec<f32>,
}

impl PointerHead {
    /// Reads `head.safetensors`: `norm.{weight,bias}` `[hidden]`, and `q`/`k`
    /// `.weight` `[dim, hidden]` and `.bias` `[dim]`, in f32. `pointer_dim` is the
    /// checkpoint's configured width, which the tensors must have.
    pub fn from_safetensors(bytes: &[u8], pointer_dim: usize) -> Result<Self> {
        let tensors = SafeTensors::deserialize(bytes).map_err(|e| {
            HeadSnafu {
                message: e.to_string(),
            }
            .build()
        })?;
        let read = |name: &str, shape: &[usize]| -> Result<Vec<f32>> {
            let view = tensors.tensor(name).map_err(|e| {
                HeadSnafu {
                    message: format!("{name}: {e}"),
                }
                .build()
            })?;
            ensure!(
                view.dtype() == Dtype::F32,
                HeadSnafu {
                    message: format!("{name} is {:?}, not F32", view.dtype()),
                }
            );
            ensure!(
                view.shape() == shape,
                HeadSnafu {
                    message: format!("{name} has shape {:?}, not {shape:?}", view.shape()),
                }
            );
            Ok(view
                .data()
                .as_chunks::<4>()
                .0
                .iter()
                .map(|&b| f32::from_le_bytes(b))
                .collect())
        };
        let hidden = tensors
            .tensor("norm.weight")
            .ok()
            .and_then(|view| view.shape().first().copied())
            .unwrap_or(0);
        ensure!(
            hidden > 0,
            HeadSnafu {
                message: "norm.weight is missing or empty",
            }
        );
        let dim = pointer_dim;
        Ok(Self {
            hidden,
            dim,
            norm_weight: read("norm.weight", &[hidden])?,
            norm_bias: read("norm.bias", &[hidden])?,
            q_weight: read("q.weight", &[dim, hidden])?,
            q_bias: read("q.bias", &[dim])?,
            k_weight: read("k.weight", &[dim, hidden])?,
            k_bias: read("k.bias", &[dim])?,
        })
    }

    /// The hidden width the head reads.
    #[must_use]
    pub fn hidden_size(&self) -> usize {
        self.hidden
    }

    fn norm(&self, x: &[f32]) -> Vec<f64> {
        let n = as_f64(x.len());
        let mean = x.iter().map(|&v| f64::from(v)).sum::<f64>() / n;
        let var = x
            .iter()
            .map(|&v| (f64::from(v) - mean).powi(2))
            .sum::<f64>()
            / n;
        let inv = 1.0 / (var + LAYER_NORM_EPS).sqrt();
        x.iter()
            .zip(&self.norm_weight)
            .zip(&self.norm_bias)
            .map(|((&v, &w), &b)| (f64::from(v) - mean) * inv * f64::from(w) + f64::from(b))
            .collect()
    }

    fn project(&self, x: &[f64], weight: &[f32], bias: &[f32]) -> Vec<f64> {
        weight
            .chunks_exact(self.hidden)
            .zip(bias)
            .map(|(row, &b)| {
                row.iter()
                    .zip(x)
                    .map(|(&w, &v)| f64::from(w) * v)
                    .sum::<f64>()
                    + f64::from(b)
            })
            .collect()
    }

    /// One logit per option: `k(norm(option)) · q(norm(decide)) / sqrt(dim)`. `decide` is the
    /// hidden state at `<answer>`, and each option the one at that option's last token. The
    /// caller checks each holds [`PointerHead::hidden_size`] values.
    #[must_use]
    pub fn logits(&self, decide: &[f32], options: &[&[f32]]) -> Vec<f64> {
        let scale = 1.0 / as_f64(self.dim).sqrt();
        let q = self.project(&self.norm(decide), &self.q_weight, &self.q_bias);
        options
            .iter()
            .map(|option| {
                let k = self.project(&self.norm(option), &self.k_weight, &self.k_bias);
                k.iter().zip(&q).map(|(a, b)| a * b).sum::<f64>() * scale
            })
            .collect()
    }
}

/// `softmax(logits / temperature)`, the temperature floored at 1e-6 as upstream floors it.
#[must_use]
pub fn softmax(logits: &[f64], temperature: f64) -> Vec<f64> {
    let t = temperature.max(1e-6);
    let max = logits.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let exp: Vec<f64> = logits.iter().map(|&l| ((l - max) / t).exp()).collect();
    let total: f64 = exp.iter().sum();
    exp.iter().map(|e| e / total).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A head file with `hidden` 2 and `dim` 1, from little-endian f32 tensors.
    fn head_file(tensors: &[(&str, Vec<usize>, Vec<f32>)]) -> Vec<u8> {
        let mut header = serde_json::Map::new();
        let mut data = Vec::new();
        for (name, shape, values) in tensors {
            let start = data.len();
            for v in values {
                data.extend_from_slice(&v.to_le_bytes());
            }
            header.insert(
                (*name).to_string(),
                serde_json::json!({"dtype": "F32", "shape": shape, "data_offsets": [start, data.len()]}),
            );
        }
        let header = serde_json::Value::Object(header).to_string();
        let mut out = (header.len() as u64).to_le_bytes().to_vec();
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(&data);
        out
    }

    #[test]
    fn logits_match_a_hand_computed_head() {
        let file = head_file(&[
            ("norm.weight", vec![2], vec![1.0, 1.0]),
            ("norm.bias", vec![2], vec![0.0, 0.0]),
            ("q.weight", vec![1, 2], vec![1.0, 0.0]),
            ("q.bias", vec![1], vec![0.5]),
            ("k.weight", vec![1, 2], vec![0.0, 2.0]),
            ("k.bias", vec![1], vec![0.0]),
        ]);
        let head = PointerHead::from_safetensors(&file, 1).unwrap_or_else(|e| panic!("{e}"));
        // LayerNorm of [3, 1] is [1, -1] / sqrt(1 + eps), so q = 1 / sqrt(1 + eps) + 0.5.
        // Option [0, 4] normalises to [-2, 2] / sqrt(4 + eps), so k = 4 / sqrt(4 + eps).
        // Option [2, 2] is all mean: k = 0. The scale is 1 / sqrt(1).
        let logits = head.logits(&[3.0, 1.0], &[&[0.0, 4.0], &[2.0, 2.0]]);
        let q = 1.0 / (1.0 + LAYER_NORM_EPS).sqrt() + 0.5;
        let k = 4.0 / (4.0 + LAYER_NORM_EPS).sqrt();
        assert!((logits[0] - q * k).abs() < 1e-12);
        assert!(logits[1].abs() < 1e-12);
        // A wrong pointer width is refused.
        assert!(matches!(
            PointerHead::from_safetensors(&file, 2),
            Err(crate::Error::Head { .. })
        ));
    }

    #[test]
    fn softmax_applies_the_temperature() {
        let p = softmax(&[0.0, 2.0f64.ln()], 1.0);
        assert!((p[0] - 1.0 / 3.0).abs() < 1e-12);
        let p = softmax(&[0.0, 2.0f64.ln()], 0.5);
        assert!((p[1] - 0.8).abs() < 1e-12);
    }
}
