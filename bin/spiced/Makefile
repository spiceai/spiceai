# Default optimization level (used when RUST_PROFILE or DEV is not set)
OPT_LEVEL = --release
SPICED_NON_DEFAULT_FEATURES_WITH_FLAG := $(if $(SPICED_NON_DEFAULT_FEATURES),--features $(SPICED_NON_DEFAULT_FEATURES),)
SPICED_FEATURES := $(SPICED_NON_DEFAULT_FEATURES_WITH_FLAG)

ifdef REL_VERSION
	SPICED_FEATURES := --features release $(SPICED_NON_DEFAULT_FEATURES_WITH_FLAG)
endif

ifdef DEV
    SPICED_FEATURES := --features dev $(SPICED_NON_DEFAULT_FEATURES_WITH_FLAG)
    OPT_LEVEL = --profile dev
endif

# Override OPT_LEVEL with RUST_PROFILE if set
ifdef RUST_PROFILE
    OPT_LEVEL = --profile $(RUST_PROFILE)
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
