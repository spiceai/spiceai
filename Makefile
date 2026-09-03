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
	cargo build $(CARGO_PROFILE) -p spice

.PHONY: build-spiced
build-spiced:
	make -C bin/spiced

.PHONY: build-spiced-dev
build-spiced-dev:
	cargo build -p spiced

.PHONY: build-validator
build-validator:
	cargo build --release -p spicepod-validator

.PHONY: build
build: build-cli build-spiced

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

.PHONY: build-spidapter-dev
build-spidapter-dev:
	cargo build -p spidapter --all-features

.PHONY: build-spidapter
build-spidapter:
	cargo build --release -p spidapter --all-features

.PHONY: build-cayenne-flightsql-dev
build-cayenne-flightsql-dev:
	cargo build -p cayenne-flightsql --all-features

.PHONY: build-cayenne-flightsql
build-cayenne-flightsql:
	cargo build --release -p cayenne-flightsql --all-features

.PHONY: ci
ci:
	make -C bin/spice
	make -C bin/spiced

# Local CI attestation ("developer sign-off"). Skips Rust lint/build/tests when
# the branch has no Rust-affecting files vs trunk (.rs, Cargo.toml/lock,
# rust-toolchain*, .cargo/*); otherwise target-lints changed crates, then full
# lint + unit tests. Posts a `signoff` commit status on HEAD so the PR can enter
# the merge queue. See scripts/signoff and docs/dev/ci_signoff.md.
.PHONY: signoff
signoff:
	@./scripts/signoff

# Remote sign-off for the current branch: probe lab SSH hosts (192.168.1.100,
# 192.168.1.101) for a Git checkout at $HOME/dev/spice2 and run scripts/signoff
# there when available; otherwise dispatch the self-hosted GitHub Actions
# signoff.yml workflow. Supports Git and JJ via scripts/signoff remote. Skips
# Rust lint/build/tests when the branch has no Rust-affecting files vs trunk
# (same as local signoff).
.PHONY: signoff-remote
signoff-remote:
	@./scripts/signoff remote

.PHONY: test
test:
	@cargo test --all --lib

# Indent these with spaces, never a tab: this block follows the `test` recipe, so
# a tab-indented line is parsed as another command in that recipe instead of an
# assignment — which both breaks `make test` and silently empties the variable.
ifdef RUST_PROFILE
    CARGO_PROFILE := --profile $(RUST_PROFILE)
    NEXTEST_CARGO_PROFILE := --cargo-profile $(RUST_PROFILE)
else
    CARGO_PROFILE := --profile dev
    NEXTEST_CARGO_PROFILE := --cargo-profile dev
endif

# `libnfs` binds a system library, and on a modern glibc its generated bindings
# carry a layout assertion for a type the headers only forward-declare, so the
# crate fails to compile at all (spiceai/spiceai#12130). `lint-rust` already
# excludes it via `_LINT_WORKSPACE_FLAGS`; excluding it here too keeps both halves
# of the gate agreeing on which crates need system libraries, so a sign-off on
# such a host fails only for reasons in the branch under test. The crate has no
# unit tests of its own.
#
# One cargo invocation, not one per test group: under `resolver = "2"` the
# resolved feature set of every crate is a function of which packages are
# selected, so `--all`, `-p cayenne` and `-p runtime` each resolve a different
# dependency graph and none of them can reuse another's artifacts
# (spiceai/spiceai#12337). One selection resolves once, and a nextest filterset
# decides which of the built tests actually run.
#
# `--tests` rather than `--lib` is what brings cayenne's integration tests and
# the `metrics` binary into that one selection. It builds every test target
# in the workspace, including ones the filterset never runs. Naming just the
# wanted targets with `--test <glob>` would build fewer, but a new test file that
# didn't match the glob would silently stop being covered — cayenne already has
# a test target that doesn't follow the `*_test` convention its other 55 do.
#
# `metrics` is a `tests/` binary rather than a `--lib` test because it needs its
# own process to control the OTel meter-provider install order. Every metrics
# test lives in that one binary, so selecting it by name here covers all of them:
# naming individual binaries is what left two of them built but never run.
#
# `kind(=proc-macro)` is the other half of what `--lib` used to select: nextest
# labels a proc-macro crate's unit tests `proc-macro`, not `lib`, so leaving it
# out would silently drop runtime-parameters-derive's tests from the gate.
# `--tests` also builds the 14 bin targets as unit-test harnesses; `--lib` never
# ran those, and nothing here selects `kind(=bin)`, so it still doesn't.
#
# Running cayenne's integration tests under the workspace resolve rather than
# `-p cayenne` enables its `turso` feature, which puts 304 `*_turso` variants in
# the gate alongside their SQLite siblings. They need more stack than the 2 MiB
# std gives a thread; `test_with_backends!` reserves it (see
# `crates/cayenne/tests/common/mod.rs`), so no name needs excluding here.
#
# Shared so `nextest` and `verify-cli` cannot drift onto different selections:
# a different selection resolves different features, which would make verify-cli
# recompile instead of reading the build nextest just did.
#
# `runtime-cloud-connect`'s integration tests are selected for the same reason
# cayenne's are: they stand their whole control plane up in-process — a TLS
# enroll mock and a tonic gateway on ephemeral ports — so unlike the suites in
# `integration*.yml` they need no credentials and no external service. They are
# also the only coverage of the enrollment, reconnect, command-dispatch and
# heartbeat wire paths; `--lib` cannot reach a running client at all.
#
# `runtime`'s `result_correctness` binary is named rather than taking all of
# `package(=runtime) & kind(=test)`: it is the accelerator-vs-standalone-oracle
# parity gate and needs no credentials, while `runtime`'s other integration
# binaries do and stay in the nightly suites.
#
# The credential-free Spice CLI integration binaries exercise the shipped
# command surface. `cloud_integration` needs live credentials and remains in
# the nightly gate, so select the other two binaries by name rather than every
# integration test in the `spice` package.
# `--features` here, not a per-crate default, because the result-correctness
# lanes are the only reason the gate links an engine at all. Cargo skips a test
# target whose `required-features` are unmet *without saying so*, so before this
# the filterset below selected `result_correctness_vs_duckdb_test` and cargo
# silently never built it — selection looked complete while three lanes never
# ran. `runtime/duckdb,runtime/sqlite` likewise unlock `runtime`'s accelerator
# parity binary. See `crates/cayenne/tests/correctness/README.md`.
NEXTEST_SELECTION := --all --exclude libnfs \
	--features cayenne/result-correctness-duckdb,runtime/duckdb,runtime/sqlite
NEXTEST_FILTER := kind(=lib) + kind(=proc-macro) + (package(=cayenne) & kind(=test)) + (package(=runtime) & binary(=result_correctness)) + (package(=runtime-cloud-connect) & kind(=test)) + (package(=spice) & binary(=cli_integration)) + (package(=spice) & binary(=connect_service_cli)) + binary(=metrics)
# Extra narrowing for callers that can't run everything (CI lacks credentials
# for some tests). It has to *intersect* the expression above rather than sit
# beside it: nextest unions repeated `-E` flags, so a second `-E 'not (…)'` would
# match everything the first one excluded and widen the run instead.
ifneq ($(strip $(NEXTEST_FILTER_EXTRA)),)
_NEXTEST_FILTER := ($(NEXTEST_FILTER)) & ($(NEXTEST_FILTER_EXTRA))
else
_NEXTEST_FILTER := $(NEXTEST_FILTER)
endif
# A filterset smuggled in through NEXTEST_FLAG would silently widen the run for
# the reason above, and a gate that runs more than it was asked to reads as green.
ifneq (,$(findstring -E,$(NEXTEST_FLAG))$(findstring --filterset,$(NEXTEST_FLAG)))
$(error NEXTEST_FLAG carries a nextest filterset — pass it as NEXTEST_FILTER_EXTRA instead, which intersects the gate's own filterset rather than being unioned with it)
endif
.PHONY: nextest
nextest:
	@cargo nextest run $(NEXTEST_SELECTION) --tests $(NEXTEST_CARGO_PROFILE) $(NEXTEST_FLAG) -E '$(_NEXTEST_FILTER)'

# Unit tests for named packages — the fail-fast pre-check scripts/signoff runs on
# the crates a branch touched, before the full workspace gate. Same lib-only
# scope and profile as `nextest`, so its test binaries carry into that run.
# Callers must filter out packages without a library target: `--lib` is a fatal
# `no library targets found` on bin-only crates.
# --no-tests=pass because a scoped selection legitimately covers crates with no
# unit tests (29 workspace libraries have none). nextest exits 4 on "no tests to
# run" by default, which would abort the sign-off for a branch that only touched
# one of them; the full `nextest` run still gates the workspace.
# The gate does not build the `spice` CLI on its own, because `nextest`'s `--tests`
# build already emits it: cargo builds a package's bins alongside that package's
# integration tests, and `spice` has three. A CLI link error therefore fails
# `nextest` itself, and a separate `cargo build -p spice` only re-resolved the
# whole graph at a selection no other phase in the gate shares.
#
# That is an assumption about cargo's target selection, and it is the quiet kind
# to lose: removing `spice`'s `tests/` targets would stop its bin from being built,
# and the gate would drop CLI coverage without a single failure. So ask cargo
# whether the bin is in that build graph, rather than looking for the file — a warm
# `target/` would still hold a stale binary from an earlier build, so a file check
# would pass at exactly the moment coverage was lost. Same selection as `nextest`,
# so after it this is a fingerprint scan (measured ~2s), not a build.
.PHONY: verify-cli
verify-cli:
	@out="$(TARGET_DIR)/verify-cli-artifacts.json"; \
	mkdir -p "$(TARGET_DIR)"; \
	cargo test --no-run --message-format json $(CARGO_PROFILE) --tests \
	  $(NEXTEST_SELECTION) $(NEXTEST_FLAG) > "$$out" || exit $$?; \
	$(PYTHON) scripts/verify_cli_build.py "$$out" version.txt

.PHONY: nextest-packages
nextest-packages:
	@test -n "$(strip $(PACKAGES))" || { echo 'nextest-packages requires PACKAGES="crate1 crate2"' >&2; exit 1; }
	@cargo nextest run --no-tests=pass $(_LINT_PKG_FLAGS) --lib $(_FEATURES_FLAGS) $(NEXTEST_CARGO_PROFILE) $(NEXTEST_FLAG)

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
	@cargo test -p runtime --test integration_models --features models,duckdb -- --nocapture

.PHONY: test-integration-models-without-openai
test-integration-models-without-openai:
	@cargo test -p runtime --test integration_models --features models,duckdb -- --nocapture --skip openai_test

.PHONY: test-bench
test-bench:
	@cargo bench -p runtime --features postgres,spark,mysql

## Optional: PACKAGES="pkg1 pkg2" to lint specific packages instead of the whole workspace
## Optional: FEATURES="feat1,feat2" to override features
## Feature defaults: when FEATURES is unset, uses the full release feature set for
## workspace-wide linting (unless PACKAGES is set, then uses package defaults —
## workspace features like `models` are not valid on every crate).
## Example: make lint-rust PACKAGES="runtime data_components"
## Example: make lint-rust-fix PACKAGES="runtime data_components" FEATURES="duckdb,postgres"
PACKAGES ?=
FEATURES ?=
# Use strip non-empty checks (not bare ifdef): PACKAGES/FEATURES are always
# assigned via ?=, and empty command-line overrides (PACKAGES= FEATURES=) must
# fall through to workspace defaults — not emit `-p`/`--features` with no value.
ifneq ($(strip $(PACKAGES)),)
_LINT_PKG_FLAGS := $(foreach p,$(PACKAGES),-p $(p))
_LINT_WORKSPACE_FLAGS := $(_LINT_PKG_FLAGS)
_FMT_FLAGS := $(_LINT_PKG_FLAGS)
# Scoped runs rely on cargo's default target selection (the package's lib
# and/or bins — the same set --lib --bins names): an explicit --lib is a fatal
# `no library targets found` on bin-only packages (e.g. testoperator), which
# breaks the targeted pre-lint that scripts/signoff derives for such branches.
_LINT_TARGET_FLAGS :=
else
_LINT_WORKSPACE_FLAGS := --workspace --exclude libnfs --exclude lopdf --exclude ttf-parser --exclude pdf-extract
_FMT_FLAGS := --all
_LINT_TARGET_FLAGS := --lib --bins
endif
# Apply FEATURES if provided, otherwise default to hardcoded features only for workspace-wide linting
ifneq ($(strip $(FEATURES)),)
_FEATURES_FLAGS := --features $(FEATURES)
else ifneq ($(strip $(PACKAGES)),)
_FEATURES_FLAGS :=
else
_FEATURES_FLAGS := --features adbc,aws-secrets-manager,keyring-secret-store,models,odbc,release,mcp,snapshots,elasticsearch,http-functions,wasm-functions,rate-control,spicebench
endif

## The guard scripts below need Python 3.11+ (stdlib `tomllib`). The sign-off
## runners carry more than one interpreter and bare `python3` does not resolve to
## the same one on every run: github-runner-02 passed this recipe at 03:36 and
## failed it at 06:58 the same morning on `found 3.9`, which fails the whole
## sign-off in ~9s with nothing wrong in the branch. Resolve an interpreter that
## is actually new enough instead of trusting PATH order. Falls back to plain
## `python3` so a host with nothing newer still gets check_crate_layers.py's own
## "needs Python 3.11+" message rather than a missing-command error.
## Override with `make lint-rust PYTHON=python3.12` to pin a specific one.
ifeq ($(strip $(PYTHON)),)
PYTHON := $(shell for p in python3.14 python3.13 python3.12 python3.11 python3; do \
		command -v $$p >/dev/null 2>&1 || continue; \
		$$p -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null && { echo $$p; exit 0; }; \
	done; echo python3)
endif

.PHONY: lint lint-rust
lint: lint-rust

# Full workspace lint (default), or scoped via PACKAGES=… for a fast fail-first pass.
lint-rust:
	cargo fmt $(_FMT_FLAGS) -- --check
	## Crate-layering guard (fast, no compile): no crate may depend on a higher tier. See docs/dev/crate_layering.md
	$(PYTHON) scripts/check_crate_layers.py
	## Table-layer guard (fast, no compile): a provider-wrapping TableProvider silently stops every layer walk. See docs/dev/crate_layering.md
	$(PYTHON) scripts/check_table_layers.py
	## Rust-gate path-list guard (fast, no compile): the sign-off, Attestation, and merge-queue path lists must agree. See docs/dev/ci_signoff.md
	## Its derivation is exercised first: the live-tree scan only covers the paths today's workspace happens to contain, so a derivation that stopped working would pass unnoticed on a clean tree
	$(PYTHON) scripts/test_check_rust_gate_paths.py
	$(PYTHON) scripts/check_rust_gate_paths.py
	## Unreachable-module guard (fast, no compile): every file under a crate's src/ must be reachable from its crate root, or nothing compiles it
	## Its parser is exercised first: the live-tree scan only covers the shapes today's workspace happens to contain, so a parser regression for any other shape would pass unnoticed
	$(PYTHON) scripts/test_check_module_reachability.py
	$(PYTHON) scripts/check_module_reachability.py
	## Fork-pin guard (fast, no compile): a moved fork pin must come with a re-audit of that fork's patches. See docs/dev/fork_patches.md
	## Its parsers are exercised first: with both sides empty the guard would report agreement, so a regex regression would pass unnoticed
	$(PYTHON) scripts/test_check_fork_patches.py
	$(PYTHON) scripts/check_fork_patches.py
	## All except metal, cuda, nfs (nfs requires system libnfs library)
	CLIPPY_CONF_DIR=".ci" cargo clippy $(CARGO_PROFILE) --keep-going $(_LINT_TARGET_FLAGS) $(_FEATURES_FLAGS) $(_LINT_WORKSPACE_FLAGS) -- \
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
	cargo clippy $(CARGO_PROFILE) --keep-going --tests $(_FEATURES_FLAGS) $(_LINT_WORKSPACE_FLAGS) -- \
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
	cargo fmt $(_FMT_FLAGS)
	## All except metal, cuda, nfs (nfs requires system libnfs library)
	CLIPPY_CONF_DIR=".ci" cargo clippy $(CARGO_PROFILE) $(_LINT_TARGET_FLAGS) --fix --allow-dirty $(_FEATURES_FLAGS) $(_LINT_WORKSPACE_FLAGS) -- \
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
	cargo clippy $(CARGO_PROFILE) --fix --allow-dirty --tests $(_FEATURES_FLAGS) $(_LINT_WORKSPACE_FLAGS) -- \
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
	cargo check $(CARGO_PROFILE) --no-default-features --features adbc
	cargo check $(CARGO_PROFILE) --no-default-features --features duckdb
	cargo check $(CARGO_PROFILE) --no-default-features --features postgres
	cargo check $(CARGO_PROFILE) --no-default-features --features sqlite
	cargo check $(CARGO_PROFILE) --no-default-features --features mysql
	cargo check $(CARGO_PROFILE) --no-default-features --features keyring-secret-store
	cargo check $(CARGO_PROFILE) --no-default-features --features flightsql
	cargo check $(CARGO_PROFILE) --no-default-features --features http-functions
	cargo check $(CARGO_PROFILE) --no-default-features --features wasm-functions
	cargo check $(CARGO_PROFILE) --no-default-features --features wasm-functions-compile
	cargo check $(CARGO_PROFILE) --no-default-features --features aws-secrets-manager
	cargo check $(CARGO_PROFILE) --no-default-features --features databricks
	cargo check $(CARGO_PROFILE) --no-default-features --features delta_lake
	cargo check $(CARGO_PROFILE) --no-default-features --features dremio
	cargo check $(CARGO_PROFILE) --no-default-features --features clickhouse
	cargo check $(CARGO_PROFILE) --no-default-features --features cosmosdb
	cargo check $(CARGO_PROFILE) --no-default-features --features debezium
	cargo check $(CARGO_PROFILE) --no-default-features --features runtime/openapi
	cargo check $(CARGO_PROFILE) --no-default-features --features dynamodb
	cargo check $(CARGO_PROFILE) --no-default-features --features oracle
	cargo check $(CARGO_PROFILE) --no-default-features --features mongodb
	cargo check $(CARGO_PROFILE) --no-default-features --features snapshots

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
# Honour CARGO_TARGET_DIR if set (e.g. for custom build directories / sccache setups),
# otherwise fall back to the default Cargo output directory.
TARGET_DIR := $(or $(CARGO_TARGET_DIR),target)

# Default install includes models. Use -data suffix variants to build without models.
#
# The feature set the -data variants build with. It is NOT derived from bin/spiced's `default`,
# and it is not `default` minus `models`: it is an independent list maintained by hand right here,
# so a feature added to `default` does not reach `make install-data-only` until it is added below
# too, and no check reports the omission. The two currently differ in both directions, which is why
# reading this list as "default, less the model bits" is wrong: -data is also an ADBC-less build
# without any of the three feature-gated secret stores `default` turns on (aws-secrets-manager,
# azure-keyvault and keyring-secret-store) — the env and Kubernetes stores are not feature-gated
# at all, so a -data build still reads secrets from both of those — and it carries the PostgreSQL
# accelerator and acceleration snapshots, which `default` does not. Recompute the difference
# before relying on it rather than trusting a comment, from this list and the
# `default = [...]` array in bin/spiced/Cargo.toml.
#
# Note: postgres-accel enables the PostgreSQL data accelerator (separate from postgres connector)
SPICED_DATA_FEATURES := duckdb,postgres,postgres-accel,sqlite,mysql,flightsql,delta_lake,databricks,dremio,clickhouse,cosmosdb,sharepoint,snapshots,snowflake,spark,ftp,sftp,debezium,kafka,anonymous_telemetry,mssql,dynamodb,imap,alloc-snmalloc,oracle,runtime/s3_vectors,mongodb,iceberg-write,turso,smb

.PHONY: install
install: build
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/release/spice ~/.spice/bin/spice
	install -m 755 $(TARGET_DIR)/release/spiced ~/.spice/bin/spiced

.PHONY: install-dev
install-dev: build-dev
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/debug/spice ~/.spice/bin/spice
	install -m 755 $(TARGET_DIR)/debug/spiced ~/.spice/bin/spiced

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

# ScyllaDB variants
.PHONY: install-scylladb
install-scylladb:
	make install SPICED_NON_DEFAULT_FEATURES="scylladb"

# Install from a CI build artifact (branch or commit SHA)
# Usage:
#   make install-build              # Latest build from trunk
#   make install-build REF=<branch> # Latest build from a branch
#   make install-build REF=<sha>    # Build for a specific commit
REF ?= trunk

.PHONY: install-build
install-build:
	./install/install-build.sh $(REF)

.PHONY: install-testoperator-dev
install-testoperator-dev: build-testoperator-dev
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/debug/testoperator ~/.spice/bin/testoperator

.PHONY: install-testoperator
install-testoperator: build-testoperator
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/release/testoperator ~/.spice/bin/testoperator

.PHONY: install-spidapter-dev
install-spidapter-dev: build-spidapter-dev
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/debug/spidapter ~/.spice/bin/spidapter

.PHONY: install-spidapter
install-spidapter: build-spidapter
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/release/spidapter ~/.spice/bin/spidapter

.PHONY: install-cayenne-flightsql-dev
install-cayenne-flightsql-dev: build-cayenne-flightsql-dev
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/debug/cayenne-flightsql ~/.spice/bin/cayenne-flightsql

.PHONY: install-cayenne-flightsql
install-cayenne-flightsql: build-cayenne-flightsql
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/release/cayenne-flightsql ~/.spice/bin/cayenne-flightsql

.PHONY: install-cli
install-cli: build-cli
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/release/spice ~/.spice/bin/spice

.PHONY: install-spiced
install-spiced: build-spiced
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/release/spiced ~/.spice/bin/spiced

.PHONY: install-cli-dev
install-cli-dev: build-cli-dev
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/debug/spice ~/.spice/bin/spice

.PHONY: install-spiced-dev
install-spiced-dev: build-spiced-dev
	mkdir -p ~/.spice/bin
	install -m 755 $(TARGET_DIR)/debug/spiced ~/.spice/bin/spiced

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
