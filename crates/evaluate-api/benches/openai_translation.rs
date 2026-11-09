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

//! What a `/v1/decisions` request answered by an `OpenAI` decision model costs to
//! translate, without the network: the request into System One, the System One request
//! into the request `OpenAI` receives, `OpenAI`'s answer back into System One, and that
//! into the caller's response.
//!
//! Three requests: a typical one (a predicate, a four-level score, a choice of five
//! strings and a choice of `true` and `false`), and the widest choice the Decisions API
//! takes, 255 options, once with strings only and once with a boolean and a string that
//! share its text. Each answer is built from the request the model receives, so it names
//! the options by the values they were sent with.

#![expect(clippy::expect_used, reason = "benchmark setup")]

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use evaluate_api::openai::{
    DecisionRequest, DecisionResponse, decision_response_to_system_one,
    system_one_to_decision_request,
};
use serde_json::{Value, json};

fn typical() -> Value {
    json!({
        "model": "luna",
        "input": "The package arrived with a broken screen, and the refund went through twice.",
        "questions": [
            {"type": "predicate", "name": "damaged", "instructions": "Does the customer report a damaged item?"},
            {"type": "score", "name": "severity", "instructions": "How severe?", "levels": [
                {"label": "Cosmetic"}, {"label": "Minor"}, {"label": "Major"},
                {"label": "Blocked", "description": "Cannot proceed"}
            ]},
            {"type": "choice", "name": "team", "instructions": "Which team?", "choices": [
                {"value": "billing", "description": "Payments and payouts"}, {"value": "technical"},
                {"value": "shipping"}, {"value": "sales"}, {"value": "legal"}
            ]},
            {"type": "choice", "name": "valid", "instructions": "Is the claim valid?", "choices": [
                {"value": true}, {"value": false}
            ]}
        ]
    })
}

fn widest(shared_text: bool) -> Value {
    let mut choices: Vec<Value> = (0..253)
        .map(|i| json!({"value": format!("option-{i:03}")}))
        .collect();
    if shared_text {
        choices.extend([json!({"value": true}), json!({"value": "true"})]);
    } else {
        choices.extend([
            json!({"value": "option-253"}),
            json!({"value": "option-254"}),
        ]);
    }
    json!({
        "model": "luna",
        "input": "Which option fits?",
        "questions": [{"type": "choice", "name": "pick", "instructions": "Pick one.", "choices": choices}]
    })
}

/// An answer to every question `upstream` asks, naming each option by the value it is
/// sent with: the first option chosen, and an equal probability for every option.
fn answer(upstream: &DecisionRequest) -> DecisionResponse {
    let asked = serde_json::to_value(upstream).expect("serializes");
    let answers: Vec<Value> = asked["questions"]
        .as_array()
        .expect("questions")
        .iter()
        .map(|question| {
            let name = &question["name"];
            match question["type"].as_str().expect("type") {
                "predicate" => json!({"type": "predicate", "name": name, "probability": 0.75}),
                "score" => {
                    let levels = question["levels"].as_array().expect("levels");
                    #[expect(clippy::cast_precision_loss, reason = "at most 10 levels")]
                    let share = 1.0 / levels.len() as f64;
                    let probabilities: Vec<Value> = levels
                        .iter()
                        .enumerate()
                        .map(|(i, level)| {
                            json!({"value": i, "label": level["label"], "probability": share})
                        })
                        .collect();
                    json!({"type": "score", "name": name, "score": 1.5, "probabilities": probabilities, "confidence": 0.5})
                }
                _ => {
                    let choices = question["choices"].as_array().expect("choices");
                    #[expect(clippy::cast_precision_loss, reason = "at most 255 options")]
                    let share = 1.0 / choices.len() as f64;
                    let probabilities: Vec<Value> = choices
                        .iter()
                        .map(|choice| json!({"value": choice["value"], "probability": share}))
                        .collect();
                    json!({"type": "choice", "name": name, "choice": choices[0]["value"], "probabilities": probabilities, "confidence": 0.0})
                }
            }
        })
        .collect();
    serde_json::from_value(json!({"model": "gpt-6-luna", "answers": answers})).expect("response")
}

fn translation(c: &mut Criterion) {
    for (name, body) in [
        ("typical", typical()),
        ("widest_choice", widest(false)),
        ("widest_choice_shared_text", widest(true)),
    ] {
        let request: DecisionRequest = serde_json::from_value(body).expect("decision request");
        let translated = request.to_system_one().expect("translates");
        let upstream =
            system_one_to_decision_request(&translated.request, "gpt-6-luna").expect("builds");
        let response = answer(&upstream);
        // Every step must succeed, or the loop would time an early error.
        let answered =
            decision_response_to_system_one(&translated.request, response.clone()).expect("reads");
        translated.decision_response(answered).expect("maps back");

        c.bench_function(&format!("decision_translation/{name}"), |b| {
            b.iter_batched(
                || response.clone(),
                |response| {
                    let translated = request.to_system_one().expect("translates");
                    let upstream =
                        system_one_to_decision_request(&translated.request, "gpt-6-luna")
                            .expect("builds");
                    let answered = decision_response_to_system_one(&translated.request, response)
                        .expect("reads");
                    (
                        upstream,
                        translated.decision_response(answered).expect("maps back"),
                    )
                },
                BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(benches, translation);
criterion_main!(benches);
