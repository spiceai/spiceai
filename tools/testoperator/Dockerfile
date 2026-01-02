#syntax=docker/dockerfile:1.7

ARG BASE_IMAGE=debian:bookworm-slim

FROM ${BASE_IMAGE}

ARG TESTOPERATOR_BIN=testoperator

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY --chmod=0755 ${TESTOPERATOR_BIN} /usr/local/bin/testoperator

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/testoperator"]
