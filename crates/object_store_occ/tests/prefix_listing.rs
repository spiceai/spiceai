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

use std::sync::Arc;

use object_store_occ::{InsertResult, LocalConditionalPut, ObjectState};

#[tokio::test]
async fn listed_keys_include_state_written_with_a_leading_slash_prefix() {
    for prefix in ["scheduler/meta/", "/scheduler/meta/"] {
        let directory = tempfile::tempdir().expect("state directory");
        let store = Arc::new(LocalConditionalPut::new(directory.path()).expect("local store"));
        let state = ObjectState::<u64>::new(store).with_prefix(prefix);
        assert_eq!(
            state.insert("job-42", &42).await.expect("insert"),
            InsertResult::Ok
        );
        assert_eq!(state.get("job-42").await.expect("read"), Some(42));
        let keys = state.list_keys().await.expect("list");
        assert_eq!(keys, vec!["job-42"], "prefix={prefix:?}");
    }
}
