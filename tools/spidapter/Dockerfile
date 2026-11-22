#syntax=docker/dockerfile:1.7.1@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

ARG SPIDAPTER_BIN=spidapter

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY --chmod=0755 ${SPIDAPTER_BIN} /usr/local/bin/spidapter
COPY scenarios /app/scenarios

WORKDIR /app

ENV SPIDAPTER_SCENARIO_BASE_PATH=/app/scenarios

ENTRYPOINT ["/usr/local/bin/spidapter"]
