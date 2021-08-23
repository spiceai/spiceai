FROM golang AS build

ARG REL_VERSION
ENV REL_VERSION=${REL_VERSION:-edge}

WORKDIR /build

COPY go.mod go.mod
COPY go.sum go.sum
COPY cmd cmd/
COPY pkg pkg/

WORKDIR /build/cmd/spiced

RUN make

FROM python:3.8

### App code
WORKDIR /app

RUN groupadd -r spice --gid=1000 && \
  useradd -r -g spice --uid=1000 --home-dir=/home/spice --shell=/bin/bash spice && \
  mkdir -p /home/spice && \
  chown -R spice:spice /home/spice

RUN mkdir -p /.spice/log && chown -R spice:spice /.spice
RUN chown -R spice:spice /app

USER spice

ENV HOME=/app/home
ENV SPICE_ENVIRONMENT=docker

COPY ai/src/requirements/common.txt ai/src/requirements/production.txt ./
RUN python3 -m pip install -r production.txt

COPY ai/src/*.py ./ai/
COPY ai/src/connector/ ./ai/connector/
COPY ai/src/algorithms/ ./ai/algorithms/
COPY ai/src/proto/ ./ai/proto/
COPY --from=build /build/cmd/spiced/spiced /app/spiced

WORKDIR /userapp

ENTRYPOINT ["/app/spiced"]
