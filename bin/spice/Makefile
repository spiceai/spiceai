BASE_PACKAGE_NAME := github.com/spiceai/spiceai

GIT_SHA := $(shell git rev-parse --short HEAD)
FILE_VERSION := $(shell cat ../../version.txt)

TAGS := $(if $(SPICE_CLI_FEATURES),--tags $(SPICE_CLI_FEATURES),)

ifdef REL_VERSION
	SPICE_VERSION := $(REL_VERSION)
	SPICED_FEATURES := --features release
else
	SPICE_VERSION := $(if $(FILE_VERSION),$(FILE_VERSION)-build.$(GIT_SHA),local)
endif

ifdef DEV
	SPICE_VERSION := $(if $(FILE_VERSION),$(FILE_VERSION)-build.$(GIT_SHA)-dev,local-dev)
endif

LDFLAGS:="-X $(BASE_PACKAGE_NAME)/bin/spice/pkg/version.version=$(SPICE_VERSION)"

.PHONY: all
all:
ifeq ($(OS),Windows_NT)
	go build -v -ldflags=$(LDFLAGS) -o "..\..\target\release\spice.exe"
else
	mkdir -p ../../target/release 2> /dev/null || true
	go build -v -ldflags=$(LDFLAGS) -o ../../target/release/spice $(TAGS)
endif
