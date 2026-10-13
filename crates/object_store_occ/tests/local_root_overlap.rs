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

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStoreExt, UpdateVersion};
use object_store_occ::{ConditionalWriteError, Expected, LocalConditionalPut, conditional_put};

#[tokio::test]
async fn conditional_writes_to_the_same_file_have_one_winner() {
    for nested_root in [false, true] {
        let directory = tempfile::tempdir().expect("state directory");
        let nested = directory.path().join("cluster");
        std::fs::create_dir(&nested).expect("feature directory");
        let first = LocalConditionalPut::new(directory.path()).expect("outer state store");
        let second = LocalConditionalPut::new(if nested_root {
            nested.as_path()
        } else {
            directory.path()
        })
        .expect("second state store");
        let left_bytes = Bytes::from(vec![b'A'; 8 * 1024 * 1024]);
        let right_bytes = Bytes::from(vec![b'B'; 8 * 1024 * 1024]);

        for round in 0..8 {
            let outer_key = Path::from(format!("cluster/state-{round}.json"));
            let second_key = if nested_root {
                Path::from(format!("state-{round}.json"))
            } else {
                outer_key.clone()
            };
            let initial = conditional_put(&first, &outer_key, "initial".into(), &Expected::Absent)
                .await
                .expect("create initial state");
            let expected = Expected::at_version(UpdateVersion::from(initial)).expect("local ETag");
            let observed = second
                .head(&second_key)
                .await
                .expect("same physical object");
            assert_eq!(
                expected,
                Expected::Version(UpdateVersion {
                    e_tag: observed.e_tag,
                    version: observed.version,
                }),
                "both roots must address the same version"
            );

            let (left, right) = tokio::join!(
                conditional_put(&first, &outer_key, left_bytes.clone().into(), &expected),
                conditional_put(&second, &second_key, right_bytes.clone().into(), &expected),
            );
            let stored = first
                .get(&outer_key)
                .await
                .expect("read final state")
                .bytes()
                .await
                .expect("read final bytes");
            let winners = usize::from(left.is_ok()) + usize::from(right.is_ok());
            eprintln!(
                "nested_root={nested_root} round={round} winners={winners} bytes={} first_byte={:?} left={left:?} right={right:?}",
                stored.len(),
                stored.first()
            );
            assert_eq!(winners, 1, "conditional writes must have exactly one winner");
            match (left, right) {
                (Ok(_), Err(ConditionalWriteError::Conflict { .. })) => {
                    assert_eq!(stored, left_bytes);
                }
                (Err(ConditionalWriteError::Conflict { .. }), Ok(_)) => {
                    assert_eq!(stored, right_bytes);
                }
                other => panic!("expected one applied write and one conflict: {other:?}"),
            }
        }
    }
}
