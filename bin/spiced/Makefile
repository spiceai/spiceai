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

ifdef SPICED_CUSTOM_FEATURES
	CUSTOM_FEATURES := --no-default-features --features "$(SPICED_CUSTOM_FEATURES)"
endif

ifdef SPICED_WINDOWS_STATIC
	CUSTOM_FEATURES := --features "windows-static"
endif

.PHONY: all
all:
	cargo build --release $(SPICED_FEATURES) --target-dir $(TARGET_DIR) $(CUSTOM_FEATURES)
