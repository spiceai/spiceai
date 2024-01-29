#syntax=docker/dockerfile:1.2
ARG RUST_VERSION=1.75
FROM rust:${RUST_VERSION}-slim-bookworm as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update \
    && apt install --yes pkg-config libssl-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY . /build

WORKDIR /build

RUN \
  --mount=type=cache,id=spiceai_registry,sharing=locked,target=/usr/local/cargo/registry \
  --mount=type=cache,id=spiceai_target,sharing=locked,target=/spiceai/target \
  cargo build --target-dir /spiceai/target --release

FROM debian:bookworm-slim

RUN apt update \
    && apt install --yes ca-certificates libssl3 --no-install-recommends \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY --from=build /spiceai/target/release/spiced /usr/local/bin/spiced

EXPOSE 3000 50051

ENTRYPOINT ["/usr/local/bin/spiced"]