OPT_LEVEL = --release
SPICED_FEATURES := --features $(SPICED_NON_DEFAULT_FEATURES)

ifdef REL_VERSION
	SPICED_FEATURES := --features release $(SPICED_NON_DEFAULT_FEATURES)
endif

ifdef DEV
	SPICED_FEATURES := --features dev $(SPICED_NON_DEFAULT_FEATURES)
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
