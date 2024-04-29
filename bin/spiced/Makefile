OPT_LEVEL = --release

ifdef REL_VERSION
	SPICED_FEATURES := --features release
endif

ifdef DEV
	SPICED_FEATURES := --features dev
	OPT_LEVEL = --profile dev
endif

ifdef SPICED_TARGET_DIR
	TARGET_DIR := $(SPICED_TARGET_DIR)
else
	TARGET_DIR := ../../target
endif

ifdef SPICED_CUSTOM_FEATURES
	CUSTOM_FEATURES := --no-default-features --features "$(SPICED_CUSTOM_FEATURES)"
endif

.PHONY: all
all:
	cargo build ${OPT_LEVEL} $(SPICED_FEATURES) --target-dir $(TARGET_DIR) $(CUSTOM_FEATURES)
