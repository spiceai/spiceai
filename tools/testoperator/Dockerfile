#syntax=docker/dockerfile:1.7

ARG BASE_IMAGE=debian:bookworm-slim

FROM ${BASE_IMAGE}

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY testoperator /usr/local/bin/testoperator
RUN chmod +x /usr/local/bin/testoperator

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/testoperator"]
