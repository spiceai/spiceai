FROM node:16 as dashboard

WORKDIR /build-dashboard

COPY dashboard dashboard/
COPY version.txt /build-dashboard/version.txt

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

RUN go mod tidy -compat=1.17
RUN make

FROM python:3.8.12-slim

### App code
WORKDIR /app

ENV HOME=/app/home

COPY ai/src/requirements/common.txt ai/src/requirements/production.txt ./
RUN python3 -m pip install -r production.txt

COPY ai/src/*.py ./ai/
COPY ai/src/connector/ ./ai/connector/
COPY ai/src/algorithms/ ./ai/algorithms/
COPY ai/src/data_manager/ ./ai/data_manager/
COPY ai/src/proto/ ./ai/proto/
COPY --from=build /build/cmd/spiced/spiced /app/spiced

WORKDIR /userapp

ENTRYPOINT ["/app/spiced", "--context", "docker"]
