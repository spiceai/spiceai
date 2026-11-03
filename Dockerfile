#syntax=docker/dockerfile:1.2
ARG RUST_VERSION=1.98.1
FROM rust:${RUST_VERSION}-slim-trixie as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update \
    && apt install --yes pkg-config libssl-dev build-essential libsqlite3-dev cmake protobuf-compiler unixodbc-dev clang lld libclang-dev curl \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY . /build
WORKDIR /build

ARG CARGO_FEATURES=default
ARG CARGO_NO_DEFAULT_FEATURES=false
ARG RUST_PROFILE=release
ARG CARGO_INCREMENTAL=yes
ARG CARGO_NET_GIT_FETCH_WITH_CLI=false
ARG TARGETARCH
ENV CARGO_FEATURES=$CARGO_FEATURES \
    CARGO_NO_DEFAULT_FEATURES=$CARGO_NO_DEFAULT_FEATURES \
    CARGO_INCREMENTAL=$CARGO_INCREMENTAL \
    CARGO_NET_GIT_FETCH_WITH_CLI=$CARGO_NET_GIT_FETCH_WITH_CLI \
    RUST_PROFILE=$RUST_PROFILE

RUN \
    --mount=type=cache,id=spiceai_registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=spiceai_git,sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=spiceai_target,sharing=locked,target=/build/target \
    case "${TARGETARCH}" in \
      arm64) export CFLAGS="-O3 -ffunction-sections -fdata-sections -fPIC" ;; \
      amd64) export CFLAGS="-O3 -ffunction-sections -fdata-sections -fPIC -march=x86-64" ;; \
      *) export CFLAGS="-O3 -ffunction-sections -fdata-sections -fPIC" ;; \
    esac && \
    if [ "${CARGO_NO_DEFAULT_FEATURES}" = "true" ]; then \
      cargo build --profile "${RUST_PROFILE}" --no-default-features ${CARGO_FEATURES:+--features ${CARGO_FEATURES}}; \
    else \
      cargo build --profile ${RUST_PROFILE} --features ${CARGO_FEATURES:-default}; \
    fi && \
    cp /build/target/${RUST_PROFILE}/spiced /root/spiced && \
    cp "$(find /build/target -name libpdfium.so -print -quit)" /root/libpdfium.so

# `duckdb:` and `ducklake:` datasets read through the DuckDB library installed
# on the host, loaded by name at runtime. Keep DUCKDB_VERSION at the newest
# release test/duckdb_versions.json lists.
ARG DUCKDB_VERSION=2.0.0
ARG DUCKDB_SHA256_AMD64=993ad89e7dfca66cd6da994c2cad090c2fc160ddf9f550498a098ab4bb13cfb1
ARG DUCKDB_SHA256_ARM64=c60eecf91143ca6bbac55c4f4d40dc22520488b0ac257c86e0fe9a2b451cf2b7
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64|"") duckdb_platform=linux-amd64; duckdb_sha256="${DUCKDB_SHA256_AMD64}" ;; \
      arm64) duckdb_platform=linux-arm64; duckdb_sha256="${DUCKDB_SHA256_ARM64}" ;; \
      *) echo "Unsupported TARGETARCH for libduckdb: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fSLo /tmp/libduckdb.tar.gz "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb-shared-libs-${duckdb_platform}.tar.gz"; \
    echo "${duckdb_sha256}  /tmp/libduckdb.tar.gz" | sha256sum -c -; \
    tar -C /root -xzf /tmp/libduckdb.tar.gz libduckdb.so; \
    rm -f /tmp/libduckdb.tar.gz

FROM debian:trixie-slim as sandbox-setup

ARG CARGO_FEATURES

ARG INSTALL_ORACLE_ODPIC=false
ARG ORACLE_INSTANTCLIENT_SHA256_AMD64=208bc7a9372efae098ab743d6e76aeb66e6f42579dcdbb7a6a8438412481b02e
ARG ORACLE_INSTANTCLIENT_SHA256_ARM64=390999bc623c39c065f1236b8b1331b342431564a7a5e36e4c645fe8d159fc5e

# Install required packages
RUN apt update \
    && apt install --yes ca-certificates libssl3 findutils tzdata --no-install-recommends \
    && if echo "$CARGO_FEATURES" | grep -q "odbc"; then \
    apt install --yes unixodbc --no-install-recommends; \
    fi \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

# Layout a tiny filesystem in /spice_sandbox
RUN mkdir -p /spice_sandbox/bin && \
    mkdir -p /spice_sandbox/lib && \
    mkdir -p /spice_sandbox/usr/lib && \
    mkdir -p /spice_sandbox/usr/local/bin && \
    mkdir -p /spice_sandbox/usr/share && \
    mkdir -p /spice_sandbox/etc && \
    mkdir -p /spice_sandbox/etc/ssl && \
    mkdir -p /spice_sandbox/dev && \
    mkdir -p /spice_sandbox/app


# Copy the binary
COPY --from=build /root/spiced /spice_sandbox/usr/local/bin/

# Copy PDFium (used for PDF document parsing). liteparse dlopen's it at runtime,
# so it is not linked into spiced and is not reported by ldd; ship it next to the
# binary where the loader's "next to the executable" search path finds it.
COPY --from=build /root/libpdfium.so /spice_sandbox/usr/local/bin/

# Copy CA certificates
RUN cp -r /etc/ssl/certs /spice_sandbox/etc/ssl/certs

# Copy timezone database (IANA tzdb) - used by jiff, chrono, libc, and other timezone-aware libraries
RUN cp -r /usr/share/zoneinfo /spice_sandbox/usr/share/zoneinfo

# Copy every dependent library reported by ldd
RUN ldd /spice_sandbox/usr/local/bin/spiced | grep -o '/[^ ]*' | xargs -I '{}' sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"'

# Copy PDFium's own shared-library dependencies (they are a subset of spiced's,
# but sweep explicitly since ldd on spiced does not see the dlopen'd PDFium)
RUN ldd /spice_sandbox/usr/local/bin/libpdfium.so | grep -o '/[^ ]*' | xargs -I '{}' sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"'

# Copy libduckdb, loaded by name at runtime for `duckdb:` and `ducklake:`, onto
# the default library path, with its own shared-library dependencies (ldd on
# spiced does not see it)
COPY --from=build /root/libduckdb.so /spice_sandbox/usr/lib/libduckdb.so
RUN ldd /spice_sandbox/usr/lib/libduckdb.so | grep -o '/[^ ]*' | sed 's/[:)]$//' | grep -v '^/spice_sandbox' | sort -u | xargs -I '{}' sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"'

# Copy additional required libraries
RUN find /lib /usr/lib -name 'libpthread.so.0' -exec sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"' \;
RUN find /lib /usr/lib -name 'librt.so.1' -exec sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"' \;
RUN find /lib /usr/lib -name 'libdl.so.2' -exec sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"' \;


# Preinstall Oracle ODPI-C (if enabled)
RUN if [ "$INSTALL_ORACLE_ODPIC" = "true" ]; then \
    set -euo pipefail; \
    apt-get update && apt-get install -y --no-install-recommends libaio1 unzip curl; \
    ARCH=$(dpkg --print-architecture); \
    if [ "$ARCH" = "amd64" ]; then \
    : "${ORACLE_INSTANTCLIENT_SHA256_AMD64:?ORACLE_INSTANTCLIENT_SHA256_AMD64 must be set to the expected SHA256 checksum}"; \
    curl -fsSLo basic.zip https://download.oracle.com/otn_software/linux/instantclient/2380000/instantclient-basiclite-linux.x64-23.8.0.25.04.zip; \
    echo "${ORACLE_INSTANTCLIENT_SHA256_AMD64}  basic.zip" | sha256sum -c -; \
    elif [ "$ARCH" = "arm64" ]; then \
    : "${ORACLE_INSTANTCLIENT_SHA256_ARM64:?ORACLE_INSTANTCLIENT_SHA256_ARM64 must be set to the expected SHA256 checksum}"; \
    curl -fsSLo basic.zip https://download.oracle.com/otn_software/linux/instantclient/2380000/instantclient-basiclite-linux.arm64-23.8.0.25.04.zip; \
    echo "${ORACLE_INSTANTCLIENT_SHA256_ARM64}  basic.zip" | sha256sum -c -; \
    else \
    echo "Unsupported architecture: $ARCH" >&2; exit 1; \
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
    cp "$(find /usr/lib /lib -name 'libresolv.so.2' | head -n 1)" /spice_sandbox/usr/lib && \
    rm -f basic.zip; \
    fi

# Minimal passwd & group for the nobody user
RUN echo 'nobody:x:65534:65534:nobody:/app:/usr/sbin/nologin' > /spice_sandbox/etc/passwd && \
    echo 'nogroup:x:65534:' > /spice_sandbox/etc/group

# Create DuckDB directory in sandbox
RUN mkdir -p /spice_sandbox/.duckdb
RUN chmod 755 /spice_sandbox/.duckdb

# Create a world-writable /tmp for temporary and spill files
RUN mkdir -p /spice_sandbox/tmp
RUN chmod 1777 /spice_sandbox/tmp

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

ENV HOME=/app
ENV HF_HOME=/.cache/huggingface
ENV HF_HUB_CACHE=/.cache/huggingface/hub
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_DIR=/etc/ssl/certs

ENTRYPOINT ["/usr/local/bin/spiced"]
