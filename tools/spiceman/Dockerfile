#syntax=docker/dockerfile:1.7

ARG BASE_IMAGE=debian:trixie-slim

FROM ${BASE_IMAGE}

ARG SPICEMAN_BIN=spiceman

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY --chmod=0755 ${SPICEMAN_BIN} /usr/local/bin/spiceman

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/spiceman"]
