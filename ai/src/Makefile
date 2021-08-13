.PHONY: test
test: venv
	ALGORITHM="vpg" venv/bin/python3 -m pytest --timeout 10
	ALGORITHM="dql" venv/bin/python3 -m pytest --timeout 10

venv:
	python3 -m venv venv
	venv/bin/pip3 install -r requirements.txt