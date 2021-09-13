.PHONY: test
test: venv
	SPICE_DEEPRL_ALGORITHM="vpg" venv/bin/python3 -m pytest --timeout 10
	SPICE_DEEPRL_ALGORITHM="dql" venv/bin/python3 -m pytest --timeout 10

venv:
	python3 -m venv venv
	venv/bin/pip3 install --upgrade pip
	venv/bin/pip3 install -r requirements/production.txt

.PHONY: venv-dev
venv-dev:
	python3 -m venv venv
	venv/bin/pip3 install --upgrade pip
	venv/bin/pip3 install -r requirements/development.txt

.PHONY: venv-gen-proto
venv-gen-proto:
	python3 -m venv venv
	venv/bin/pip3 install --upgrade pip
	venv/bin/pip3 install grpcio-tools

################################################################################
# Target: gen-proto                                                            #
################################################################################
GRPC_PROTOS:=common aiengine runtime

define genProtoc
.PHONY: gen-proto-$(1)
gen-proto-$(1):
	venv/bin/python3 -m grpc_tools.protoc --proto_path=../.. --python_out=. --grpc_python_out=. proto/$(1)/v1/$(1).proto
endef

$(foreach ITEM,$(GRPC_PROTOS),$(eval $(call genProtoc,$(ITEM))))

GEN_PROTOS:=$(foreach ITEM,$(GRPC_PROTOS),gen-proto-$(ITEM))

.PHONY: gen-proto
gen-proto: $(GEN_PROTOS)
