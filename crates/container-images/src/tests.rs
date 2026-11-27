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

use super::{Error, Pins, Reference, pins};

const POSTGRES: &str = "sha256:c699303802144847d238196173655dbbecf7a96aeca1565f2b02eb671a1f0b6b";
const OTHER: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

fn fixture() -> Pins {
    Pins::parse(&format!(
        r#"{{"description": "x", "images": {{
            "docker.io/library/postgres:18": "{POSTGRES}",
            "docker.io/clickhouse/clickhouse-server:24.8": "{OTHER}",
            "mcr.microsoft.com/mssql/server:2022-CU28-ubuntu-22.04": "{OTHER}"
        }}}}"#
    ))
    .expect("valid fixture")
}

fn key(image: &str) -> Result<String, Error> {
    Reference::parse(image)?.key(image)
}

#[test]
fn reads_a_reference_the_way_docker_does() {
    for (image, expected) in [
        ("postgres:18", "docker.io/library/postgres:18"),
        ("library/postgres:18", "docker.io/library/postgres:18"),
        (
            "docker.io/library/postgres:18",
            "docker.io/library/postgres:18",
        ),
        (
            "index.docker.io/library/postgres:18",
            "docker.io/library/postgres:18",
        ),
        (
            "clickhouse/clickhouse-server:24.8",
            "docker.io/clickhouse/clickhouse-server:24.8",
        ),
        (
            "mcr.microsoft.com/mssql/server:2022-CU28-ubuntu-22.04",
            "mcr.microsoft.com/mssql/server:2022-CU28-ubuntu-22.04",
        ),
        ("localhost:5000/team/app:1.2", "localhost:5000/team/app:1.2"),
        (
            "docker.redpanda.com/redpandadata/redpanda:v26.1.6",
            "docker.redpanda.com/redpandadata/redpanda:v26.1.6",
        ),
    ] {
        assert_eq!(key(image).as_deref(), Ok(expected), "{image}");
    }
}

#[test]
fn a_pinned_image_runs_by_its_digest_whatever_spelling_names_it() {
    let pins = fixture();
    let expected = format!("docker.io/library/postgres:18@{POSTGRES}");
    for image in [
        "postgres:18",
        "docker.io/library/postgres:18",
        &format!("postgres:18@{POSTGRES}"),
        &expected,
    ] {
        assert_eq!(
            pins.pinned(image).as_deref(),
            Ok(expected.as_str()),
            "{image}"
        );
    }
    assert_eq!(
        pins.pinned("clickhouse/clickhouse-server:24.8").as_deref(),
        Ok(format!("docker.io/clickhouse/clickhouse-server:24.8@{OTHER}").as_str())
    );
}

#[test]
fn an_image_not_pinned_is_refused_with_how_to_pin_it() {
    let pins = fixture();
    let refused = pins.pinned("postgres:17").expect_err("not pinned");
    assert_eq!(
        refused,
        Error::NotPinned {
            image: "postgres:17".into(),
            key: "docker.io/library/postgres:17".into(),
        }
    );
    assert_eq!(
        refused.to_string(),
        "postgres:17 is not pinned: test/container_images.json records no digest for docker.io/library/postgres:17. A test starts a container only from an image pinned there, so that a tag moving cannot change what it runs. Record the digest with `.github/scripts/pin_container_images.py --write docker.io/library/postgres:17`, and review it"
    );
    // The same repository on another registry is another image.
    assert!(matches!(
        pins.pinned("ghcr.io/library/postgres:18"),
        Err(Error::NotPinned { .. })
    ));
}

#[test]
fn a_moving_or_missing_tag_is_refused_even_if_listed() {
    let pins = fixture();
    for (image, tag) in [
        ("postgres:latest", "latest"),
        ("rustfs/rustfs:latest", "latest"),
        ("mcr.microsoft.com/mssql/server:2022-latest", "2022-latest"),
        ("mcr.microsoft.com/azure-storage/azurite:latest", "latest"),
    ] {
        assert_eq!(
            pins.pinned(image),
            Err(Error::MovingTag {
                image: image.into(),
                tag: tag.into()
            }),
            "{image}"
        );
    }
    assert_eq!(
        pins.pinned("postgres"),
        Err(Error::NoTag {
            image: "postgres".into()
        })
    );
    assert_eq!(
        pins.pinned(&format!("postgres@{POSTGRES}")),
        Err(Error::NoTag {
            image: format!("postgres@{POSTGRES}")
        })
    );
}

#[test]
fn a_digest_other_than_the_pinned_one_is_refused() {
    assert_eq!(
        fixture().pinned(&format!("postgres:18@{OTHER}")),
        Err(Error::OtherDigest {
            image: format!("postgres:18@{OTHER}"),
            key: "docker.io/library/postgres:18".into(),
            pinned: POSTGRES.into(),
        })
    );
}

#[test]
fn text_that_is_not_a_reference_is_refused() {
    let pins = fixture();
    for image in [
        "",
        "Postgres:18",
        "postgres:18 ",
        "postgres:-18",
        "postgres:18@sha256:abc",
        "postgres:18@md5:00",
        "/postgres:18",
        "docker.io//postgres:18",
        "-bad.example/x:1",
    ] {
        assert_eq!(
            pins.pinned(image),
            Err(Error::NotAReference {
                image: image.into()
            }),
            "{image:?}"
        );
    }
}

#[test]
fn a_malformed_pin_file_is_refused() {
    let digest = POSTGRES;
    for (images, reason) in [
        (
            format!(r#""postgres:18": "{digest}""#),
            "the key postgres:18 is not in canonical form; write it as docker.io/library/postgres:18",
        ),
        (
            format!(r#""docker.io/rustfs/rustfs:latest": "{digest}""#),
            "docker.io/rustfs/rustfs:latest names `latest`, a tag that moves to each new release. Name the release to run instead, and pin it in test/container_images.json with `.github/scripts/pin_container_images.py --write <image>`",
        ),
        (
            r#""docker.io/library/postgres:18": "sha256:C699""#.to_string(),
            "docker.io/library/postgres:18 is pinned to `sha256:C699`, which is not a sha256: digest of 64 lowercase hex digits",
        ),
        (
            format!(r#""docker.io/library/postgres:18@{digest}": "{digest}""#),
            &format!(
                "the key docker.io/library/postgres:18@{digest} carries a digest; give the digest as its value"
            ),
        ),
    ] {
        assert_eq!(
            Pins::parse(&format!(
                r#"{{"description": "x", "images": {{{images}}}}}"#
            ))
            .map(|_| ()),
            Err(Error::InvalidPins {
                reason: reason.into()
            }),
            "{images}"
        );
    }
    assert!(matches!(
        Pins::parse(r#"{"description": "x", "images": {}, "extra": 1}"#),
        Err(Error::InvalidPins { .. })
    ));
}

#[test]
fn the_checked_in_pins_are_valid() {
    let pins = pins().expect("test/container_images.json is valid");
    assert!(pins.iter().count() > 0);
}
