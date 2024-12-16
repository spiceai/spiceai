################################################################################
# Target: all                                                                 #
################################################################################
.PHONY: all
all: build

.PHONY: build-cli
build-cli:
	make -C bin/spice

.PHONY: build-cli-dev
build-cli-dev:
	export DEV=true; make -C bin/spice

.PHONY: build-runtime
build-runtime:
	make -C bin/spiced

.PHONY: build
build: build-cli build-runtime

.PHONY: build-dev
build-dev:
	export DEV=true; make -C bin/spice
	export DEV=true; make -C bin/spiced

.PHONY: ci
ci:
	make -C bin/spice
	make -C bin/spiced

.PHONY: test
test:
	@cargo test --all --lib

.PHONY: nextest
nextest:
	@cargo nextest run --all

# Also update .github/workflows/integration.yml with changes to this target
.PHONY: test-integration
test-integration:
	# Test if .env file exists, and login to Spice if not
	@test -f .env || (`spice login`)
	@cargo test -p runtime --test integration --features postgres,mysql,delta_lake,duckdb,sqlite -- --nocapture

.PHONY: test-integration-without-spiceai-dataset
test-integration-without-spiceai-dataset:
	@cargo test -p runtime --test integration --features postgres,mysql,delta_lake,duckdb,sqlite -- --nocapture --skip spiceai_integration_test

.PHONY: test-integration-models
test-integration-models:
	@cargo test -p runtime --test integration_models --features models -- --nocapture

.PHONY: test-integration-models-without-openai
test-integration-models-without-openai:
	@cargo test -p runtime --test integration_models --features models -- --nocapture --skip openai_test

.PHONY: test-bench
test-bench:
	@cargo bench -p runtime --features postgres,spark,mysql

PHONY: test-bench-vector-search
test-bench-vector-search:
	@cargo bench -p runtime --bench vector_search --features models

PHONY: test-bench-vector-search-with-metal
test-bench-vector-search-with-metal:
	@cargo bench -p runtime --bench vector_search --features models,metal

.PHONY: lint lint-go lint-rust
lint: lint-go lint-rust

lint-rust:
	cargo fmt --all -- --check
	## All except metal, cuda
	cargo clippy --all-targets --features aws-secrets-manager,keyring-secret-store,models,odbc,release --workspace -- \
		-Dwarnings \
		-Dclippy::pedantic \
		-Dclippy::unwrap_used \
		-Dclippy::expect_used \
		-Dclippy::clone_on_ref_ptr \
		-Aclippy::module_name_repetitions

lint-go:
	go vet ./...
	golangci-lint run

.PHONY: fmt-toml
fmt-toml:
	taplo fmt

.PHONY: run
run:
	~/.spice/bin/spiced

.PHONY: docker
docker:
	docker buildx build -t spiceai-rust:local-dev .

.PHONY: docker-run
docker-run:
	docker stop spiceai && docker rm spiceai || true
	docker run --name spiceai -p 8090:8090 -p 50051:50051 spiceai-rust:local-dev

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
.PHONY: install
install: build
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spice ~/.spice/bin/spice
	install -m 755 target/release/spiced ~/.spice/bin/spiced

.PHONY: install-with-models
install-with-models:
	make install SPICED_NON_DEFAULT_FEATURES="models"

.PHONY: install-with-models-dev
install-with-models-dev:
	make install-dev SPICED_NON_DEFAULT_FEATURES="models"

.PHONY: install-with-models-metal-dev
install-with-models-metal-dev:
	make install-dev SPICED_NON_DEFAULT_FEATURES="models,metal"

install-with-models-metal:
	make install SPICED_NON_DEFAULT_FEATURES="models,metal"

install-with-models-cuda:
	make install SPICED_NON_DEFAULT_FEATURES="models,cuda"

.PHONY: install-with-odbc
install-with-odbc:
	make install SPICED_NON_DEFAULT_FEATURES="odbc"

.PHONY: install-cli
install-cli: build-cli
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spice ~/.spice/bin/spice

.PHONY: install-runtime
install-runtime: build-runtime
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spiced ~/.spice/bin/spiced

################################################################################
# Target: install-dev                                                          #
################################################################################
.PHONY: install-dev
install-dev: build-dev
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spice ~/.spice/bin/spice
	install -m 755 target/debug/spiced ~/.spice/bin/spiced

.PHONY: install-cli-dev
install-cli-dev: build-cli-dev
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spice ~/.spice/bin/spice

################################################################################
# Target: modtidy                                                              #
################################################################################
.PHONY: modtidy
modtidy:
	go mod tidy




################################################################################
# Target: generate-acknowledgements                                            #
################################################################################
ACKNOWLEDGEMENTS_PATH := acknowledgements.md

.PHONY: generate-acknowledgements
generate-acknowledgements:
	echo "# Open Source Acknowledgements\n\nSpice.ai acknowledges the following open source projects for making this project possible:\n\n" > $(ACKNOWLEDGEMENTS_PATH)
	make generate-acknowledgements-go
	make generate-acknowledgements-rust
	make generate-acknowledgements-formatting

.PHONY: generate-acknowledgements-go
generate-acknowledgements-go:
	echo "\n## Go Modules\n" >> $(ACKNOWLEDGEMENTS_PATH)
	go get github.com/google/go-licenses
	go install github.com/google/go-licenses
	cd bin/spice && go-licenses report --ignore github.com/spiceai/spiceai . 2>/dev/null >> ../../$(ACKNOWLEDGEMENTS_PATH) && cd ../../

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
