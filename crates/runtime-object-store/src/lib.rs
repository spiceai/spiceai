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

pub mod builder;
pub mod registry;
pub mod store;

pub use builder::{build_azure_object_store, build_gcs_object_store};

/// Whether `path` addresses an S3 Express One Zone bucket.
///
/// Those buckets are named `{base}--{zone-id}--x-s3`, which is the only way to tell one
/// from a standard S3 path. The convention is checked against the **bucket** alone: the
/// suffix is legal inside an object key, so a standard bucket with a
/// `prefix--x-s3/` folder must not be taken for a directory bucket — it would be sent
/// down the S3-Express write path and past the standard-S3 rejection in
/// `validate_file_path`.
///
/// Lives here rather than with a single caller because both the Cayenne engine and the
/// runtime's query planner branch on it, and a second copy of the convention could drift
/// from the first.
#[must_use]
pub fn is_s3_express_path(path: &str) -> bool {
    let Some(rest) = path.strip_prefix("s3://") else {
        return false;
    };
    let bucket = match rest.split_once('/') {
        Some((bucket, _key)) => bucket,
        None => rest,
    };
    // Both halves are required: AWS names every directory bucket `{base}--{az}--x-s3`,
    // so a bucket merely ending in the suffix is not one.
    let Some(base_and_zone) = bucket.strip_suffix("--x-s3") else {
        return false;
    };
    matches!(
        base_and_zone.rsplit_once("--"),
        Some((base, zone)) if !base.is_empty() && !zone.is_empty()
    )
}

#[cfg(test)]
mod s3_express_path_tests {
    use super::is_s3_express_path;

    #[test]
    fn only_the_zone_suffixed_naming_convention_counts() {
        assert!(is_s3_express_path("s3://mybucket--usw2-az1--x-s3/prefix/"));
        assert!(is_s3_express_path("s3://data-bucket--use1-az4--x-s3/"));
        assert!(is_s3_express_path(
            "s3://my-bucket-name--euw1-az2--x-s3/some/nested/path/"
        ));

        // Standard S3, including the shapes that look close: dashes in the bucket name
        // and a partial `--` that never reaches the zone suffix.
        assert!(!is_s3_express_path("s3://mybucket/prefix/"));
        assert!(!is_s3_express_path("s3://mybucket-with-dashes/prefix/"));
        assert!(!is_s3_express_path("s3://mybucket--partial/prefix/"));

        // Not S3 at all, and the suffix without a scheme.
        assert!(!is_s3_express_path("/local/path/"));
        assert!(!is_s3_express_path("mybucket--usw2-az1--x-s3"));

        // The suffix is legal inside an object key, and only the bucket names the
        // convention. A standard bucket with such a prefix is standard S3.
        assert!(!is_s3_express_path("s3://regular-bucket/prefix--x-s3/data"));
        assert!(!is_s3_express_path(
            "s3://regular-bucket/nested--usw2-az1--x-s3/data"
        ));
        // ...and a directory bucket stays one however its keys are named.
        assert!(is_s3_express_path(
            "s3://mybucket--usw2-az1--x-s3/prefix--x-s3/data"
        ));

        // The suffix alone is not the convention: AWS names every directory bucket
        // `{base}--{zone-id}--x-s3`.
        assert!(!is_s3_express_path("s3://mybucket--x-s3/prefix/"));
        assert!(!is_s3_express_path("s3://--x-s3/"));
    }
}
