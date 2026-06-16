#syntax=docker/dockerfile:1.7

ARG BASE_IMAGE=debian:trixie-slim

FROM ${BASE_IMAGE}

ARG SPIDAPTER_BIN=spidapter

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY --chmod=0755 ${SPIDAPTER_BIN} /usr/local/bin/spidapter

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/spidapter"]
