FROM node as dashboard

WORKDIR /build-dashboard

COPY dashboard dashboard/

WORKDIR /build-dashboard/dashboard

RUN npm install --global npm
RUN npm install --global yarn --force
RUN yarn install
RUN yarn build

FROM golang AS build

ARG REL_VERSION
ENV REL_VERSION=${REL_VERSION:-local}

WORKDIR /build

COPY --from=dashboard /build-dashboard/dashboard/build /build/pkg/dashboard/build

COPY go.mod go.mod
COPY go.sum go.sum
COPY cmd cmd/
COPY pkg pkg/

WORKDIR /build/cmd/spiced

RUN go mod tidy
RUN make

FROM python:3.8

### App code
WORKDIR /app

# RUN groupadd -r spice --gid=1000 && \
#   useradd -r -g spice --uid=1000 --home-dir=/home/spice --shell=/bin/bash spice && \
#   mkdir -p /home/spice && \
#   chown -R spice:spice /home/spice

# RUN mkdir -p /.spice/log && chown -R spice:spice /.spice
# RUN chown -R spice:spice /app

# USER spice

ENV HOME=/app/home

COPY ai/src/requirements/common.txt ai/src/requirements/production.txt ./
RUN python3 -m pip install -r production.txt

COPY ai/src/*.py ./ai/
COPY ai/src/connector/ ./ai/connector/
COPY ai/src/algorithms/ ./ai/algorithms/
COPY ai/src/proto/ ./ai/proto/
COPY --from=build /build/cmd/spiced/spiced /app/spiced

WORKDIR /userapp

ENTRYPOINT ["/app/spiced", "--context", "docker"]
