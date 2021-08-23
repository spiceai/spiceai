.PHONY: all
all: clean build

build:
	yarn install
	yarn build
	cp -r build ../pkg/dashboard/

.PHONY: clean
clean:
	rm -r -f node_modules
	rm -r -f build
	rm -r -f ../pkg/dashboard/build
