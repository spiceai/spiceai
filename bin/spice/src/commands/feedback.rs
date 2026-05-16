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

//! Feedback command - opens the Spice.ai community Slack in the user's browser.

use crate::error::Result;
use clap::Args;

const FEEDBACK_URL: &str = "https://spice.ai/slack";

/// Arguments for the feedback command.
#[derive(Args, Debug)]
#[command(
    about = "Open the Spice.ai community Slack to share feedback",
    long_about = r#"Open the Spice.ai community Slack to share feedback

Examples:
  spice feedback

See more at: https://spiceai.org/docs/"#
)]
pub struct FeedbackArgs {}

/// Execute the feedback command.
pub fn execute(_args: &FeedbackArgs) -> Result<()> {
    println!("Opening Spice.ai community Slack in your default browser:");
    println!("\n  {FEEDBACK_URL}\n");
    println!("If the browser does not open, visit the URL above manually.");

    let _ = open::that(FEEDBACK_URL);

    Ok(())
}
