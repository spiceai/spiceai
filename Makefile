SHELL := /bin/bash
PROTOC ?=protoc
UNAME := $(shell uname)


################################################################################
# Target: all                                                                 #
################################################################################
.PHONY: all
all: build docker

.PHONY: build
build:
	pushd dashboard && yarn install && yarn build && cp -rf build ../pkg/dashboard/ && popd
	pushd cmd/spice && go build . && popd
	pushd cmd/spiced && go build . && popd

.PHONY: lint
lint:
	pushd dashboard && yarn lint && popd
	pushd ai/src && make lint && popd
	go vet ./...
	golangci-lint run

.PHONY: test-pkg
test-pkg:
	pushd pkg && go test ./... -count=3 -shuffle=on

.PHONY: update-pkg-snapshots
update-pkg-snapshots:
	pushd pkg && UPDATE_SNAPSHOTS=true go test ./... -count=5

.PHONY: test-e2e
test-e2e:
	pushd test/e2e && go test -v -e2e -context metal -shuffle=on -count=2 ./...

.PHONY: test
test: build test-pkg test-e2e
	pushd dashboard && yarn test-ci && popd
	pushd ai/src && make test && popd
	go vet ./...

.PHONY: docker
docker:
	docker build -t ghcr.io/spiceai/spiceai:local -f docker/Dockerfile .

.PHONY: metal-symlinks
metal-symlinks:
	mkdir -p ~/.spice/bin
	if [[ -e "${HOME}/.spice/bin/ai" ]]; then rm -rf "${HOME}/.spice/bin/ai"; fi
	ln -s $(shell pwd)/ai/src ${HOME}/.spice/bin/ai
	if [[ -e "${HOME}/.spice/bin/spice" ]]; then rm -rf "${HOME}/.spice/bin/spice"; fi
	if [[ -e "${HOME}/.spice/bin/spiced" ]]; then rm -rf "${HOME}/.spice/bin/spiced"; fi
	if [[ -f "$(shell pwd)/cmd/spice/spice" ]]; then ln -s "$(shell pwd)/cmd/spice/spice" ${HOME}/.spice/bin/spice; fi
	if [[ -f "$(shell pwd)/cmd/spiced/spiced" ]]; then ln -s "$(shell pwd)/cmd/spiced/spiced" ${HOME}/.spice/bin/spiced; fi
	ls -la ~/.spice/bin

################################################################################
# Target: modtidy                                                              #
################################################################################
.PHONY: modtidy
modtidy:
	go mod tidy

################################################################################
# Target: init-proto                                                           #
################################################################################
.PHONY: init-proto
init-proto:
	go get google.golang.org/grpc/cmd/protoc-gen-go-grpc
	go install google.golang.org/protobuf/cmd/protoc-gen-go
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

################################################################################
# Target: gen-proto                                                            #
################################################################################
GRPC_PROTOS:=common aiengine runtime
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

################################################################################
# Target: generate-acknowledgements                                            #
################################################################################
ACKNOWLEDGEMENTS_PATH := dashboard/src/content/acknowledgements.md

.PHONY: generate-acknowledgements
generate-acknowledgements:
	echo -e "# Open Source Acknowledgements\n\nSpice.ai acknowledges the following open source projects for making this project possible:\n\n## Python Packages\n" > $(ACKNOWLEDGEMENTS_PATH)

# Python Packages
	python3 -m venv venv_acknowledgments
	venv_acknowledgments/bin/pip install -r ai/src/requirements/production.txt
	venv_acknowledgments/bin/pip install -r ai/src/requirements/development.txt
	venv_acknowledgments/bin/pip install -r ai/src/requirements/common.txt
	venv_acknowledgments/bin/pip install pip-licenses
	venv_acknowledgments/bin/pip-licenses -f csv --with-authors --with-urls 2>/dev/null >> $(ACKNOWLEDGEMENTS_PATH)
	rm -rf venv_acknowledgments

# Go Modules
	echo -e "\n## Go Modules\n" >> $(ACKNOWLEDGEMENTS_PATH)
	go get github.com/google/go-licenses
	pushd cmd/spice && go-licenses csv . 2>/dev/null >> ../../$(ACKNOWLEDGEMENTS_PATH) && popd
	pushd cmd/spiced && go-licenses csv . 2>/dev/null >> ../../$(ACKNOWLEDGEMENTS_PATH) && popd

# Node Packages
	echo -e "\n## Node Packages\n" >> $(ACKNOWLEDGEMENTS_PATH)
	pushd dashboard && yarn install && npx license-checker --csv 2>/dev/null >> ../$(ACKNOWLEDGEMENTS_PATH) && popd

# Apply Formatting
	@if [[ "$(UNAME)" -eq "Darwin" ]]; then\
		sed -i '' 's/\"//g' $(ACKNOWLEDGEMENTS_PATH); \
		sed -i '' 's/,/, /g' $(ACKNOWLEDGEMENTS_PATH); \
		sed -i '' 's/,  /, /g' $(ACKNOWLEDGEMENTS_PATH); \
	else\
		sed -i 's/\"//g' $(ACKNOWLEDGEMENTS_PATH); \
		sed -i 's/,/, /g' $(ACKNOWLEDGEMENTS_PATH); \
		sed -i 's/,  /, /g' $(ACKNOWLEDGEMENTS_PATH); \
	fi
