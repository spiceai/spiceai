#syntax=docker/dockerfile:1.2
ARG RUST_VERSION=1.78
FROM rust:${RUST_VERSION}-slim-bookworm as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update \
    && apt install --yes pkg-config libssl-dev build-essential libsqlite3-dev cmake protobuf-compiler unixodbc-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY . /build
WORKDIR /build

ARG CARGO_FEATURES
ARG CARGO_INCREMENTAL=yes
ARG CARGO_NET_GIT_FETCH_WITH_CLI=false
ENV CARGO_FEATURES=$CARGO_FEATURES \
    CARGO_INCREMENTAL=$CARGO_INCREMENTAL \
    CARGO_NET_GIT_FETCH_WITH_CLI=$CARGO_NET_GIT_FETCH_WITH_CLI

RUN \
  --mount=type=cache,id=spiceai_registry,sharing=locked,target=/usr/local/cargo/registry \
  --mount=type=cache,id=spiceai_git,sharing=locked,target=/usr/local/cargo/git \
  --mount=type=cache,id=spiceai_target,sharing=locked,target=/build/target \
  cargo build --release --features ${CARGO_FEATURES:-default} && \
  cp /build/target/release/spiced /root/spiced

FROM debian:bookworm-slim

ARG CARGO_FEATURES

RUN apt update \
    && apt install --yes ca-certificates libssl3 --no-install-recommends \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

RUN if echo "$CARGO_FEATURES" | grep -q "odbc"; then \
    apt update \
    && apt install --yes unixodbc --no-install-recommends \
    && rm -rf /var/lib/{apt,dpkg,cache,log}; \
fi

COPY --from=build /root/spiced /usr/local/bin/spiced

EXPOSE 3000 50051

ENTRYPOINT ["/usr/local/bin/spiced"]