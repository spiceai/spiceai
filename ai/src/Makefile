.PHONY: test
test: venv
	ALGORITHM="dql" venv/bin/python3 -m pytest --timeout 10
	ALGORITHM="vpg" venv/bin/python3 -m pytest --timeout 10

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

.PHONY: install-codespaces
install-codespaces:
	sudo apt update
	sudo apt-get install libbz2-dev -y
	cp /usr/lib/python3.8/lib-dynload/_bz2.cpython-38-x86_64-linux-gnu.so  venv/lib/python3.8/site-packages

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
