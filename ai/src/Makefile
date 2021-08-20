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

.PHONY: gen-proto
gen-proto:
	venv/bin/python3 -m grpc_tools.protoc --proto_path=../.. --python_out=. --grpc_python_out=. proto/aiengine/v1/aiengine.proto
