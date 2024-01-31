BASE_PACKAGE_NAME := github.com/spiceai/spiceai

ifdef REL_VERSION
	SPICE_VERSION := $(REL_VERSION)
	SPICED_FEATURES := --features release
else
	SPICE_VERSION := local
endif

ifdef SPICED_TARGET_DIR
	TARGET_DIR := $(SPICED_TARGET_DIR)
else
	TARGET_DIR := ../../target
endif

LDFLAGS:="-X $(BASE_PACKAGE_NAME)/pkg/version.version=$(SPICE_VERSION)"

.PHONY: all
all:
	mkdir -p ../../target/release 2> /dev/null|| true
	go build -v -ldflags=$(LDFLAGS) -o ../../target/release/spice
	cargo build --release $(SPICED_FEATURES) --target-dir $(TARGET_DIR)