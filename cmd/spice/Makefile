BASE_PACKAGE_NAME := github.com/spiceai/spiceai

ifdef REL_VERSION
	SPICE_VERSION := $(REL_VERSION)
else
	SPICE_VERSION := local
endif

LDFLAGS:="-X $(BASE_PACKAGE_NAME)/pkg/version.version=$(SPICE_VERSION)"

.PHONY: all
all:
	go build -v -ldflags=$(LDFLAGS)