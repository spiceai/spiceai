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

ARG GH_USERNAME
ARG GH_PAT

RUN --mount=type=secret,id=gh_pat echo "machine github.com login $GH_USERNAME password $(cat /run/secrets/gh_pat)" > ~/.netrc
RUN GONOSUMDB=github.com/spiceai/data-components-contrib go get github.com/spiceai/data-components-contrib@trunk

COPY go.mod go.mod
COPY go.sum go.sum
COPY cmd cmd/
COPY pkg pkg/

WORKDIR /build/cmd/spiced

RUN go mod tidy -compat=1.17
RUN make

RUN rm ~/.netrc

FROM python:3.8.11-alpine

### App code
WORKDIR /app

ENV HOME=/app/home

RUN apk add --update-cache gcc-c++
RUN apk add --update-cache libatomic
RUN apk add --update-cache musl-dev
RUN apk add --update-cache python3-devel

COPY ai/src/requirements/common.txt ai/src/requirements/production.txt ./
RUN python3 -m pip install -r production.txt

COPY ai/src/*.py ./ai/
COPY ai/src/connector/ ./ai/connector/
COPY ai/src/algorithms/ ./ai/algorithms/
COPY ai/src/proto/ ./ai/proto/
COPY --from=build /build/cmd/spiced/spiced /app/spiced

WORKDIR /userapp

ENTRYPOINT ["/app/spiced", "--context", "docker"]
