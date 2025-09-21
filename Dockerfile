#syntax=docker/dockerfile:1.2
ARG RUST_VERSION=1.88
FROM rust:${RUST_VERSION}-slim-bookworm as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update \
    && apt install --yes pkg-config libssl-dev build-essential libsqlite3-dev cmake protobuf-compiler unixodbc-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY . /build
WORKDIR /build

ARG CARGO_FEATURES
ARG RUST_PROFILE
ARG CARGO_INCREMENTAL=yes
ARG CARGO_NET_GIT_FETCH_WITH_CLI=false
ENV CARGO_FEATURES=$CARGO_FEATURES \
    CARGO_INCREMENTAL=$CARGO_INCREMENTAL \
    CARGO_NET_GIT_FETCH_WITH_CLI=$CARGO_NET_GIT_FETCH_WITH_CLI \
    RUST_PROFILE=$RUST_PROFILE

RUN \
    --mount=type=cache,id=spiceai_registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=spiceai_git,sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=spiceai_target,sharing=locked,target=/build/target \
    cargo build --profile ${RUST_PROFILE} --features ${CARGO_FEATURES:-default} && \
    cp /build/target/${RUST_PROFILE}/spiced /root/spiced

FROM debian:bookworm-slim as sandbox-setup

ARG CARGO_FEATURES

ARG INSTALL_ORACLE_ODPIC=false

# Install required packages
RUN apt update \
    && apt install --yes ca-certificates libssl3 findutils --no-install-recommends \
    && if echo "$CARGO_FEATURES" | grep -q "odbc"; then \
    apt install --yes unixodbc --no-install-recommends; \
    fi \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

# Layout a tiny filesystem in /spice_sandbox
RUN mkdir -p /spice_sandbox/bin && \
    mkdir -p /spice_sandbox/lib && \
    mkdir -p /spice_sandbox/usr/lib && \
    mkdir -p /spice_sandbox/usr/local/bin && \
    mkdir -p /spice_sandbox/etc && \
    mkdir -p /spice_sandbox/etc/ssl && \
    mkdir -p /spice_sandbox/dev && \
    mkdir -p /spice_sandbox/app


# Copy the binary
COPY --from=build /root/spiced /spice_sandbox/usr/local/bin/

# Copy CA certificates
RUN cp -r /etc/ssl/certs /spice_sandbox/etc/ssl/certs

# Copy every dependent library reported by ldd
RUN ldd /spice_sandbox/usr/local/bin/spiced | grep -o '/[^ ]*' | xargs -I '{}' sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"'

# Copy additional required libraries
RUN find /lib /usr/lib -name 'libpthread.so.0' -exec sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"' \;
RUN find /lib /usr/lib -name 'librt.so.1' -exec sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"' \;
RUN find /lib /usr/lib -name 'libdl.so.2' -exec sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"' \;


# Preinstall Oracle ODPI-C (if enabled)
RUN if [ "$INSTALL_ORACLE_ODPIC" = "true" ]; then \
    apt-get update && apt-get install -y --no-install-recommends libaio1 unzip curl; \
    ARCH=$(dpkg --print-architecture); \
    if [ "$ARCH" = "amd64" ]; then \
    curl -sSL https://download.oracle.com/otn_software/linux/instantclient/2380000/instantclient-basiclite-linux.x64-23.8.0.25.04.zip -o basic.zip; \
    elif [ "$ARCH" = "arm64" ]; then \
    curl -sSL https://download.oracle.com/otn_software/linux/instantclient/2380000/instantclient-basiclite-linux.arm64-23.8.0.25.04.zip -o basic.zip; \
    fi; \
    unzip basic.zip && \
    cp -v \
    instantclient_*/libclntsh.so.23.1 \
    instantclient_*/libclntshcore.so.23.1 \
    instantclient_*/libnnz.so \
    instantclient_*/libociicus.so \
    instantclient_*/fips.so \
    instantclient_*/legacy.so \
    /spice_sandbox/usr/lib && \
    ln -s libclntsh.so.23.1 /spice_sandbox/usr/lib/libclntsh.so && \
    ln -s libclntshcore.so.23.1 /spice_sandbox/usr/lib/libclntshcore.so && \
    cp "$(find /usr/lib /lib -name 'libaio.so.1' | head -n 1)" /spice_sandbox/usr/lib && \
    cp "$(find /usr/lib /lib -name 'libresolv.so.2' | head -n 1)" /spice_sandbox/usr/lib; \
    fi

# Minimal passwd & group for the nobody user
RUN echo 'nobody:x:65534:65534:nobody:/nonexistent:/usr/sbin/nologin' > /spice_sandbox/etc/passwd && \
    echo 'nogroup:x:65534:' > /spice_sandbox/etc/group

# Create DuckDB directory in sandbox
RUN mkdir -p /spice_sandbox/.duckdb
RUN chmod 755 /spice_sandbox/.duckdb

# Give the nobody user ownership of app dir
RUN chown -R 65534:65534 /spice_sandbox/app

# Create HuggingFace cache directory in sandbox
RUN mkdir -p /spice_sandbox/.cache/huggingface/hub
RUN chown -R 65534:65534 /spice_sandbox/.cache
RUN chmod -R 755 /spice_sandbox/.cache

FROM scratch

COPY --from=sandbox-setup /spice_sandbox/ /

USER 65534:65534

EXPOSE 8090 50051

WORKDIR /app

ENV HF_HOME=/.cache/huggingface
ENV HF_HUB_CACHE=/.cache/huggingface/hub

ENTRYPOINT ["/usr/local/bin/spiced"]
