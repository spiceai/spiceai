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

//! Checks against a released checkpoint's tokenizer. Set `STRANDS_DECIDER_CHECKPOINT` to its
//! directory, such as a download of `StrandsAgents/strands-decider-2B-qwen3.5-v1-2610`, and
//! run with `--ignored`.

use std::path::PathBuf;

use strands_decider::{
    ChoiceOptions, Content, Decider, Question, option_token_index, render_question, render_state,
};

fn checkpoint() -> PathBuf {
    PathBuf::from(
        std::env::var("STRANDS_DECIDER_CHECKPOINT")
            .unwrap_or_else(|_| panic!("set STRANDS_DECIDER_CHECKPOINT to a checkpoint directory")),
    )
}

/// The head reads each option from the last token inside its line, so that token must end
/// with the option's name, and the question's last token must be `<answer>`'s last.
#[test]
#[ignore = "needs a downloaded checkpoint"]
fn option_rows_land_on_each_options_last_token() {
    let decider = Decider::from_checkpoint_dir(&checkpoint()).unwrap_or_else(|e| panic!("{e}"));
    let question = render_question(&Question::Choice {
        instructions: "Which team should handle this?".into(),
        options: ChoiceOptions::new(vec![
            ("billing".into(), None),
            ("sales".into(), None),
            ("retail \u{2014} shops".into(), None),
        ])
        .unwrap_or_else(|e| panic!("{e}")),
    });
    let state = render_state(&Content::from(
        "Help! My payouts have been failing for 3 days! ",
    ));
    let fitted = decider
        .tokenizer()
        .fit(&state, &[question.text.as_str()], 4096)
        .unwrap_or_else(|e| panic!("{e}"));
    let rows = option_token_index(&fitted.offsets[0], &question.option_spans)
        .unwrap_or_else(|e| panic!("{e}"));
    let text = &question.text;
    for (row, name) in rows
        .iter()
        .zip(["billing", "sales", "retail \u{2014} shops"])
    {
        let (_, end) = fitted.offsets[0][*row];
        assert!(
            text[..end].ends_with(name),
            "row {row} ends {:?}",
            &text[..end]
        );
    }
    let (_, last) = *fitted.offsets[0]
        .last()
        .unwrap_or_else(|| panic!("no tokens"));
    assert_eq!(last, text.len());
    assert!(text.ends_with("<answer>"));
    // Qwen3.5's tokenizer adds no BOS, and the checkpoint does not force one.
    let first = decider.tokenizer().inner().id_to_token(fitted.state[0]);
    assert_eq!(first.as_deref(), Some("<"));
}
