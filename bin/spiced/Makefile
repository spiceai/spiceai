ifdef REL_VERSION
	SPICED_FEATURES := --features release
endif

ifdef DEV
	SPICED_FEATURES := --features dev
endif

ifdef SPICED_TARGET_DIR
	TARGET_DIR := $(SPICED_TARGET_DIR)
else
	TARGET_DIR := ../../target
endif

.PHONY: all
all:
	cargo build --release $(SPICED_FEATURES) --target-dir $(TARGET_DIR)