#!/usr/bin/env python3
# Copyright 2026 Spice AI, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate a GitHub Actions matrix for parallel feature checking.

This script reads features from Cargo.toml and splits them into groups
for parallel CI jobs.

Usage:
    python generate_feature_matrix.py [--num-groups N] [--exclude FEATURE,...]

Output (to GITHUB_OUTPUT if set, otherwise stdout):
    matrix={"include":[...]}
    num_groups=N
"""

import argparse
import json
import os
import subprocess
import sys

# Features excluded from all checks (heavy deps, special builds, etc.)
DEFAULT_EXCLUDED_FEATURES = [
    "bedrock",
    "cuda",
    "default",
    "extended_tests",
    "iceberg-write",
    "kafka",
    "mcp",
    "metal",
    "models",
    "nfs",
    "odbc",
    "release",
    "s3_vectors",
    "snapshots",
    "spark",
    "tpc-extension",
]


def get_spiced_features() -> list[str]:
    """Get all features from the spiced package using cargo metadata."""
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        capture_output=True,
        text=True,
        check=True,
    )
    metadata = json.loads(result.stdout)

    for package in metadata["packages"]:
        if package["name"] == "spiced":
            return sorted(package["features"].keys())

    msg = "Could not find spiced package in cargo metadata"
    raise ValueError(msg)


def generate_matrix(
    features: list[str],
    num_groups: int,
    excluded_features: list[str],
) -> dict:
    """Generate the GitHub Actions matrix JSON."""
    # Filter out excluded features
    excluded_set = set(excluded_features)
    checkable_features = [f for f in features if f not in excluded_set]

    # Split into groups
    total = len(checkable_features)
    group_size = (total + num_groups - 1) // num_groups

    matrix_includes = []
    for g in range(num_groups):
        start = g * group_size
        end = min(start + group_size, total)

        # Features for this group
        group_features = checkable_features[start:end]

        # Features to exclude (other groups' features + always-excluded)
        other_features = checkable_features[:start] + checkable_features[end:]
        exclude_features = other_features + excluded_features

        matrix_includes.append({
            "group": g + 1,
            "features": ",".join(group_features),
            "exclude_features": ",".join(exclude_features),
            "check_openapi": g == 0,  # First group checks openapi
        })

    return {"include": matrix_includes}


def write_output(name: str, value: str) -> None:
    """Write output to GITHUB_OUTPUT or stdout."""
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"{name}={value}\n")
    else:
        print(f"{name}={value}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate GitHub Actions matrix for feature checking"
    )
    parser.add_argument(
        "--num-groups",
        type=int,
        default=2,
        help="Number of parallel groups (default: 2)",
    )
    parser.add_argument(
        "--exclude",
        type=str,
        default="",
        help="Additional comma-separated features to exclude",
    )
    args = parser.parse_args()

    # Get excluded features
    excluded = DEFAULT_EXCLUDED_FEATURES.copy()
    if args.exclude:
        excluded.extend(args.exclude.split(","))

    # Get features and generate matrix
    features = get_spiced_features()
    matrix = generate_matrix(features, args.num_groups, excluded)

    # Output
    write_output("matrix", json.dumps(matrix))
    write_output("num_groups", str(args.num_groups))

    # Also print for debugging
    if os.environ.get("GITHUB_OUTPUT"):
        print("Generated matrix:")
        print(json.dumps(matrix, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
