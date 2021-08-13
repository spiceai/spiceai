.PHONY: test
test: venv
	SPICE_AIALGORITHM="vpg" venv/bin/python3 -m pytest --timeout 10
	SPICE_AIALGORITHM="dql" venv/bin/python3 -m pytest --timeout 10

venv:
	python3 -m venv venv
	venv/bin/pip3 install -r requirements.txt