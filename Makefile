################################################################################
# Target: all                                                                 #
################################################################################
.PHONY: all
all: build

.PHONY: build-cli
build-cli:
	cargo build --release -p spice

.PHONY: build-cli-dev
build-cli-dev:
	cargo build -p spice

.PHONY: build-runtime
build-runtime:
	make -C bin/spiced

.PHONY: build-validator
build-validator:
	cargo build --release -p spicepod-validator

.PHONY: build
build: build-cli build-runtime

.PHONY: build-dev
build-dev:
	cargo build -p spice
	export DEV=true; make -C bin/spiced

.PHONY: build-testoperator-dev
build-testoperator-dev:
	cargo build -p testoperator --all-features

.PHONY: build-testoperator
build-testoperator:
	cargo build --release -p testoperator --all-features

.PHONY: ci
ci:
	make -C bin/spice
	make -C bin/spiced

.PHONY: test
test:
	@cargo test --all --lib

ifdef RUST_PROFILE
    CARGO_PROFILE := --profile $(RUST_PROFILE)
	NEXTEST_CARGO_PROFILE := --cargo-profile $(RUST_PROFILE)
else
	CARGO_PROFILE := --profile dev
	NEXTEST_CARGO_PROFILE := --cargo-profile dev
endif

.PHONY: nextest
nextest:
	@cargo nextest run --all --lib $(NEXTEST_CARGO_PROFILE) $(NEXTEST_FLAG)

# Also update .github/workflows/integration.yml with changes to this target
.PHONY: test-integration
test-integration:
	# Test if .env file exists, and login to Spice if not
	@test -f .env || (`spice login`)
	@cargo test -p runtime --test integration --features postgres,mysql,delta_lake,duckdb,sqlite,turso -- --nocapture

.PHONY: test-integration-without-spiceai-dataset
test-integration-without-spiceai-dataset:
	@cargo test -p runtime --test integration --features postgres,mysql,delta_lake,duckdb,sqlite,turso -- --nocapture --skip spiceai_integration_test

.PHONY: test-integration-models
test-integration-models:
	@cargo test -p runtime --test integration_models --features models -- --nocapture

.PHONY: test-integration-models-without-openai
test-integration-models-without-openai:
	@cargo test -p runtime --test integration_models --features models -- --nocapture --skip openai_test

.PHONY: test-bench
test-bench:
	@cargo bench -p runtime --features postgres,spark,mysql

.PHONY: lint lint-rust
lint: lint-rust

lint-rust:
	cargo fmt --all -- --check
	## All except metal, cuda
	CLIPPY_CONF_DIR=".ci" cargo clippy $(CARGO_PROFILE) --lib --bins --features aws-secrets-manager,keyring-secret-store,models,odbc,release,mcp --workspace -- \
		-Dwarnings \
		-Dclippy::pedantic \
		-Dclippy::unwrap_used \
		-Dclippy::expect_used \
		-Dclippy::clone_on_ref_ptr \
		-Aclippy::module_name_repetitions \
		-Aclippy::large_futures \
		-Aclippy::too_many_lines \
		-Dclippy::equatable_if_let \
		-Dclippy::needless_collect \
		-Dclippy::redundant_clone \
		-Dclippy::todo \
		-Dclippy::assertions_on_result_states \
		-Dclippy::allow_attributes
	cargo clippy $(CARGO_PROFILE) --tests --features aws-secrets-manager,keyring-secret-store,models,odbc,release,mcp --workspace -- \
		-Dwarnings \
		-Dclippy::pedantic \
		-Dclippy::unwrap_used \
		-Aclippy::expect_used \
		-Dclippy::clone_on_ref_ptr \
		-Aclippy::module_name_repetitions \
		-Aclippy::large_futures \
		-Aclippy::too_many_lines \
		-Dclippy::equatable_if_let \
		-Dclippy::needless_collect \
		-Dclippy::redundant_clone \
		-Dclippy::todo \
		-Dclippy::assertions_on_result_states \
		-Dclippy::allow_attributes \
		-Aunfulfilled_lint_expectations

lint-rust-fix:
	cargo fmt --all
	## All except metal, cuda
	CLIPPY_CONF_DIR=".ci" cargo clippy $(CARGO_PROFILE) --lib --bins --fix --allow-dirty --features aws-secrets-manager,keyring-secret-store,models,odbc,release,mcp --workspace -- \
		-Dwarnings \
		-Dclippy::pedantic \
		-Dclippy::unwrap_used \
		-Dclippy::expect_used \
		-Dclippy::clone_on_ref_ptr \
		-Aclippy::module_name_repetitions \
		-Aclippy::large_futures \
		-Aclippy::too_many_lines \
		-Dclippy::equatable_if_let \
		-Dclippy::needless_collect \
		-Dclippy::redundant_clone \
		-Dclippy::todo \
		-Dclippy::assertions_on_result_states \
		-Dclippy::allow_attributes
	cargo clippy $(CARGO_PROFILE) --fix --allow-dirty --tests --features aws-secrets-manager,keyring-secret-store,models,odbc,release,mcp --workspace -- \
		-Dwarnings \
		-Dclippy::pedantic \
		-Dclippy::unwrap_used \
		-Aclippy::expect_used \
		-Dclippy::clone_on_ref_ptr \
		-Aclippy::module_name_repetitions \
		-Aclippy::large_futures \
		-Aclippy::too_many_lines \
		-Dclippy::equatable_if_let \
		-Dclippy::needless_collect \
		-Dclippy::redundant_clone \
		-Dclippy::todo \
		-Dclippy::assertions_on_result_states \
		-Dclippy::allow_attributes \
		-Aunfulfilled_lint_expectations

check-rust-features:
	cargo check $(CARGO_PROFILE) --no-default-features
	cargo check $(CARGO_PROFILE) --no-default-features --features duckdb
	cargo check $(CARGO_PROFILE) --no-default-features --features postgres
	cargo check $(CARGO_PROFILE) --no-default-features --features sqlite
	cargo check $(CARGO_PROFILE) --no-default-features --features mysql
	cargo check $(CARGO_PROFILE) --no-default-features --features keyring-secret-store
	cargo check $(CARGO_PROFILE) --no-default-features --features flightsql
	cargo check $(CARGO_PROFILE) --no-default-features --features aws-secrets-manager
	cargo check $(CARGO_PROFILE) --no-default-features --features databricks
	cargo check $(CARGO_PROFILE) --no-default-features --features delta_lake
	cargo check $(CARGO_PROFILE) --no-default-features --features dremio
	cargo check $(CARGO_PROFILE) --no-default-features --features clickhouse
	cargo check $(CARGO_PROFILE) --no-default-features --features debezium
	cargo check $(CARGO_PROFILE) --no-default-features --features runtime/openapi
	cargo check $(CARGO_PROFILE) --no-default-features --features dynamodb
	cargo check $(CARGO_PROFILE) --no-default-features --features oracle
	cargo check $(CARGO_PROFILE) --no-default-features --features mongodb

.PHONY: fmt-toml
fmt-toml:
	taplo fmt

.PHONY: run
run:
	~/.spice/bin/spiced

.PHONY: docker
docker:
	docker buildx build --build-arg RUST_PROFILE=release -t spiceai-rust:local-dev .

.PHONY: docker-run
docker-run:
	docker stop spiceai && docker rm spiceai || true
	docker run --name spiceai -p 8090:8090 -p 50051:50051 spiceai-rust:local-dev

.PHONY: docker-local
docker-local:
	cp ~/.spice/bin/spiced .spiced-local-tmp
	docker build -f Dockerfile.local -t spiceai.org/spiceai:local .
	rm .spiced-local-tmp

.PHONY: deps-licenses
dep-licenses:
	@cargo install cargo-license --quiet
	@cargo license -d

.PHONY: display-deps
display-deps:
	@cargo install cargo-license --quiet
	@cargo license -d  --tsv --direct-deps-only --all-features | grep -v "github.com/spiceai"


################################################################################
# Target: install                                                              #
################################################################################
# Default install includes models. Use -data suffix variants to build without models.
# Data-only features (default features minus models)
# Note: postgres-accel enables the PostgreSQL data accelerator (separate from postgres connector)
SPICED_DATA_FEATURES := duckdb,postgres,postgres-accel,sqlite,mysql,flightsql,delta_lake,databricks,dremio,clickhouse,sharepoint,snapshots,snowflake,spark,ftp,sftp,debezium,kafka,anonymous_telemetry,mssql,dynamodb,imap,alloc-snmalloc,oracle,runtime/s3_vectors,mongodb,iceberg-write,turso,smb,pingora,scylladb

.PHONY: install
install: build
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spice ~/.spice/bin/spice
	install -m 755 target/release/spiced ~/.spice/bin/spiced

.PHONY: install-dev
install-dev: build-dev
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spice ~/.spice/bin/spice
	install -m 755 target/debug/spiced ~/.spice/bin/spiced

# Data-only variants (without models)
.PHONY: install-data-only
install-data-only:
	make install SPICED_CUSTOM_FEATURES="$(SPICED_DATA_FEATURES)"

.PHONY: install-data-only-dev
install-data-only-dev:
	make install-dev SPICED_CUSTOM_FEATURES="$(SPICED_DATA_FEATURES)"

# Metal variants (with GPU acceleration)
.PHONY: install-metal
install-metal:
	make install SPICED_NON_DEFAULT_FEATURES="metal"

.PHONY: install-metal-dev
install-metal-dev:
	make install-dev SPICED_NON_DEFAULT_FEATURES="metal"

.PHONY: install-data-only-metal
install-data-only-metal:
	make install SPICED_CUSTOM_FEATURES="$(SPICED_DATA_FEATURES),metal"

.PHONY: install-data-only-metal-dev
install-data-only-metal-dev:
	make install-dev SPICED_CUSTOM_FEATURES="$(SPICED_DATA_FEATURES),metal"

# CUDA variants
.PHONY: install-cuda
install-cuda:
	make install SPICED_NON_DEFAULT_FEATURES="cuda"

.PHONY: install-data-only-cuda
install-data-only-cuda:
	make install SPICED_CUSTOM_FEATURES="$(SPICED_DATA_FEATURES),cuda"

# ODBC variants
.PHONY: install-odbc
install-odbc:
	make install SPICED_NON_DEFAULT_FEATURES="odbc"

# NFS variants
.PHONY: install-nfs
install-nfs:
	make install SPICED_NON_DEFAULT_FEATURES="nfs"

.PHONY: install-testoperator-dev
install-testoperator-dev: build-testoperator-dev
	mkdir -p ~/.spice/bin
	install -m 755 target/debug/testoperator ~/.spice/bin/testoperator

.PHONY: install-testoperator
install-testoperator: build-testoperator
	mkdir -p ~/.spice/bin
	install -m 755 target/release/testoperator ~/.spice/bin/testoperator

.PHONY: install-cli
install-cli: build-cli
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spice ~/.spice/bin/spice

.PHONY: install-runtime
install-runtime: build-runtime
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spiced ~/.spice/bin/spiced

.PHONY: install-cli-dev
install-cli-dev: build-cli-dev
	mkdir -p ~/.spice/bin
	install -m 755 target/debug/spice ~/.spice/bin/spice

################################################################################
# Target: distributed                                                          #
################################################################################
.PHONY: distributed
distributed:
	make install SPICED_NON_DEFAULT_FEATURES="vortex"
	./scripts/distributed.sh

.PHONY: distributed-dev
distributed-dev:
	make install-dev SPICED_NON_DEFAULT_FEATURES="vortex"
	./scripts/distributed.sh

################################################################################
# Target: generate-acknowledgements                                            #
################################################################################
ACKNOWLEDGEMENTS_PATH := acknowledgements.md

.PHONY: generate-acknowledgements
generate-acknowledgements:
	echo "# Open Source Acknowledgements\n\nSpice.ai acknowledges the following open source projects for making this project possible:\n\n" > $(ACKNOWLEDGEMENTS_PATH)
	make generate-acknowledgements-rust
	make generate-acknowledgements-formatting

.PHONY: generate-acknowledgements-rust
generate-acknowledgements-rust:
	@echo "\n## Rust Crates\n" >> "$(ACKNOWLEDGEMENTS_PATH)"
	@make display-deps 2>/dev/null | awk -F'\t' 'NR>1 {printf "- %s %s, %s %s\n  <br/>%s\n\n", $$1, $$2, $$5, $$6, $$4}' >> "$(ACKNOWLEDGEMENTS_PATH)"


.PHONY: generate-acknowledgements-formatting
generate-acknowledgements-formatting:
	@if [[ "$(UNAME)" -eq "Darwin" ]]; then\
		sed -i '' 's/\"//g' $(ACKNOWLEDGEMENTS_PATH); \
		sed -i '' 's/,/, /g' $(ACKNOWLEDGEMENTS_PATH); \
		sed -i '' 's/,  /, /g' $(ACKNOWLEDGEMENTS_PATH); \
	else\
		sed -i 's/\"//g' $(ACKNOWLEDGEMENTS_PATH); \
		sed -i 's/,/, /g' $(ACKNOWLEDGEMENTS_PATH); \
		sed -i 's/,  /, /g' $(ACKNOWLEDGEMENTS_PATH); \
	fi

-include Makefile.local
