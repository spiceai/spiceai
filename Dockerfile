ARG RUST_VERSION=1.75
FROM rust:${RUST_VERSION}-slim-bookworm as build

RUN apt update \
    && apt install --yes pkg-config libssl-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY . /build

WORKDIR /build

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt update \
    && apt install --yes ca-certificates libssl3 --no-install-recommends \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY --from=build /build/target/release/spiced /usr/local/bin/spiced

EXPOSE 3000 50051

ENTRYPOINT ["/usr/local/bin/spiced"]