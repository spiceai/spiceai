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

//! Asserting on what a test's code prints to stdout.
//!
//! Stable Rust cannot capture `println!` output in-process, so [`printed_by`]
//! re-runs the calling test in a child copy of this test binary. In the child it
//! runs the printing code between two markers and exits; in the parent it
//! returns exactly what the child printed between them.

use std::io::Write as _;
use std::process::Command;

/// Set in the child to the name of the test whose output it is producing.
const CHILD_ENV: &str = "TESTOPERATOR_PRINTED_BY_TEST";
const BEGIN: &str = "<<<printed-by:begin>>>";
const END: &str = "<<<printed-by:end>>>";

/// Everything `print` writes to stdout.
///
/// `test_path` names the calling test: pass
/// `concat!(module_path!(), "::<test fn name>")`. The test is re-run in a child
/// process, where this call prints and exits, so the caller's assertions only
/// ever run in the parent.
pub(crate) fn printed_by(test_path: &str, print: impl FnOnce()) -> String {
    // libtest names a test by its path below the crate root.
    let test_name = test_path
        .split_once("::")
        .map_or(test_path, |(_, below_crate)| below_crate);

    if std::env::var(CHILD_ENV).is_ok_and(|name| name == test_name) {
        println!("{BEGIN}");
        print();
        println!("{END}");
        std::io::stdout().flush().expect("flush the printed output");
        std::process::exit(0);
    }

    let output = Command::new(std::env::current_exe().expect("the test binary's path"))
        .args([test_name, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, test_name)
        .output()
        .expect("re-run the test in a child process");
    let stdout = String::from_utf8(output.stdout).expect("the child prints UTF-8");
    assert!(
        output.status.success(),
        "the child run of {test_name} failed ({}): {stdout}{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let begin = format!("{BEGIN}\n");
    let (_, after_begin) = stdout.split_once(&begin).unwrap_or_else(|| {
        panic!("the child run of {test_name} printed no begin marker: {stdout}")
    });
    let (printed, _) = after_begin
        .split_once(END)
        .unwrap_or_else(|| panic!("the child run of {test_name} printed no end marker: {stdout}"));
    printed.to_string()
}
