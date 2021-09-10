SHELL := /bin/bash
PROTOC ?=protoc

################################################################################
# Target: all                                                                 #
################################################################################
.PHONY: all
all: build docker

.PHONY: build
build:
	pushd dashboard && yarn build && cp -rf build ../pkg/dashboard/ && popd
	pushd cmd/spice && go build . && popd
	pushd cmd/spiced && go build . && popd

.PHONY: lint
lint:
	pushd dashboard && yarn lint && popd
	black --check --extend-exclude proto ai/src
	go vet ./...
	golangci-lint run	

.PHONY: test
test: build
	pushd dashboard && yarn test-ci && popd
	pushd ai/src && make test && popd
	go vet ./...
	go test ./...
	pushd test/e2e && go test -v . -e2e && popd

.PHONY: docker
docker:
	docker build -t ghcr.io/spiceai/spiceai:local -f docker/Dockerfile .

################################################################################
# Target: modtidy                                                              #
################################################################################
.PHONY: modtidy
modtidy:
	go mod tidy -compat=1.17

################################################################################
# Target: init-proto                                                           #
################################################################################
.PHONY: init-proto
init-proto:
	go get google.golang.org/protobuf/cmd/protoc-gen-go google.golang.org/grpc/cmd/protoc-gen-go-grpc

################################################################################
# Target: gen-proto                                                            #
################################################################################
GRPC_PROTOS:=aiengine runtime
PROTO_PREFIX:=github.com/spiceai/spiceai

define genProtoc
.PHONY: gen-proto-$(1)
gen-proto-$(1):
	$(PROTOC) --go_out=. --go_opt=module=$(PROTO_PREFIX) --go-grpc_out=. --go-grpc_opt=require_unimplemented_servers=false,module=$(PROTO_PREFIX) ./proto/$(1)/v1/*.proto
endef

$(foreach ITEM,$(GRPC_PROTOS),$(eval $(call genProtoc,$(ITEM))))

GEN_PROTOS:=$(foreach ITEM,$(GRPC_PROTOS),gen-proto-$(ITEM))

.PHONY: gen-proto
gen-proto: $(GEN_PROTOS) modtidy
	cd ai/src && make gen-proto

################################################################################
# Target: check-proto-diff                                                           #
################################################################################
.PHONY: check-proto-diff
check-proto-diff:
	git diff --exit-code ./pkg/proto/aiengine_pb/aiengine.pb.go # check no changes
	git diff --exit-code ./pkg/proto/aiengine_pb/aiengine_grpc.pb.go # check no changes
	git diff --exit-code ./pkg/proto/runtime_pb/runtime.pb.go # check no changes
	git diff --exit-code ./ai/src/proto/aiengine/v1/aiengine_pb2.py # check no changes
	git diff --exit-code ./ai/src/proto/aiengine/v1/aiengine_pb2_grpc.py # check no changes
	git diff --exit-code ./ai/src/proto/runtime/v1/runtime_pb2.py # check no changes
	git diff --exit-code ./ai/src/proto/runtime/v1/runtime_pb2_grpc.py # check no changes