################################################################################
# Target: all                                                                 #
################################################################################
.PHONY: all
all: build

.PHONY: build-cli
build-cli:
	make -C bin/spice

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
	export SPICED_TARGET_DIR=/workspace/spiceai/target; make -C bin/spiced

.PHONY: test
test:
	@cargo test --all

.PHONY: test
nextest:
	@cargo nextest run --all

.PHONY: lint
lint:
	go vet ./...
	golangci-lint run
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features --workspace -- \
		-Dwarnings \
		-Dclippy::pedantic \
		-Dclippy::unwrap_used \
		-Dclippy::expect_used \
		-Dclippy::clone_on_ref_ptr

.PHONY: run
run:
	~/.spice/bin/spiced

.PHONY: docker
docker:
	docker buildx build -t spiceai-rust:local-dev .

.PHONY: docker-run
docker-run:
	docker stop spiceai && docker rm spiceai || true
	docker run --name spiceai -p 3000:3000 -p 50051:50051 spiceai-rust:local-dev

.PHONY: deps-licenses
dep-licenses:
	@cargo install cargo-license --quiet
	@cargo license -d

.PHONY: display-deps
display-deps:
	@cargo install cargo-license --quiet
	@cargo license -d  --tsv --direct-deps-only | grep -v "github.com/spiceai"


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

.PHONY: install-with-federation
install-with-federation:
	make install SPICED_NON_DEFAULT_FEATURES="federation-experimental"

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
