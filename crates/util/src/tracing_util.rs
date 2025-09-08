/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use tracing::subscriber;

pub fn in_tracing_context<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_ansi(true)
        .finish();
    subscriber::with_default(subscriber, f)
}
