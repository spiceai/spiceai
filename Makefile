SHELL := /bin/bash
PROTOC ?=protoc

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
	black --check --extend-exclude proto ai/src
	go vet ./...
	golangci-lint run	

.PHONY: test
test: build
	pushd dashboard && yarn test-ci && popd
	pushd ai/src && make test && popd
	go vet ./...
	go test ./... -shuffle=on
	pushd test/e2e && go test -v . -e2e && popd

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
	echo -e "# Open Source Acknowledgements\n\nSpice.ai would like to acknowledge the following open source projects for making this project possible:\n\n## Python Packages\n" > $(ACKNOWLEDGEMENTS_PATH)
	
	# Python Packages
	python -m venv venv-acknowledgments
	source venv-acknowledgments/bin/activate
	venv-acknowledgments/bin/pip install -r ai/src/requirements/production.txt
	venv-acknowledgments/bin/pip install -r ai/src/requirements/development.txt
	venv-acknowledgments/bin/pip install -r ai/src/requirements/common.txt
	venv-acknowledgments/bin/pip install pip-licenses
	venv-acknowledgments/bin/pip-licenses -f csv --with-authors --with-urls 2>/dev/null >> $(ACKNOWLEDGEMENTS_PATH)
	rm -rf venv-acknowledgments

	# Go Modules
	echo -e "\n## Go Modules\n" >> $(ACKNOWLEDGEMENTS_PATH)
	go get github.com/google/go-licenses
	pushd cmd/spice && go-licenses csv . 2>/dev/null >> ../../$(ACKNOWLEDGEMENTS_PATH) && popd
	pushd cmd/spiced && go-licenses csv . 2>/dev/null >> ../../$(ACKNOWLEDGEMENTS_PATH) && popd

	# Node Packages
	echo -e "\n## Node Packages\n" >> $(ACKNOWLEDGEMENTS_PATH)
	pushd dashboard && yarn install && npx license-checker --csv 2>/dev/null >> ../$(ACKNOWLEDGEMENTS_PATH) && popd

	# Apply Formatting
	sed -i 's/\"//g' $(ACKNOWLEDGEMENTS_PATH)
	sed -i 's/,/, /g' $(ACKNOWLEDGEMENTS_PATH)
	sed -i 's/,  /, /g' $(ACKNOWLEDGEMENTS_PATH)
