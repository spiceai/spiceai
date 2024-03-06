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

.PHONY: lint
lint:
	go vet ./...
	golangci-lint run
	cargo fmt --all -- --check
	cargo clippy --all-targets --workspace -- \
		-Dwarnings \
		-Dclippy::pedantic \
		-Dclippy::unwrap_used \
		-Dclippy::expect_used

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
	@cargo license -d  --tsv --direct-deps-only


################################################################################
# Target: install                                                              #
################################################################################
.PHONY: install
install: build
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spice ~/.spice/bin/spice
	install -m 755 target/release/spiced ~/.spice/bin/spiced

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
	install -m 755 target/release/spiced ~/.spice/bin/spiced

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
	go install github.com/google/go-licenses
	pushd bin/spice && go-licenses csv . 2>/dev/null >> ../../$(ACKNOWLEDGEMENTS_PATH) && popd

.PHONY: generate-acknowledgements-rust
generate-acknowledgements-rust:
	@echo "\n## Rust Crates\n" >> "$(ACKNOWLEDGEMENTS_PATH)"
	@make display-deps 2>/dev/null | awk 'BEGIN { \
		FS="\t"; \
		print "| name | version | authors | repository | license | license_file | description |"; \
		print "|------|---------|---------|------------|---------|--------------|-------------|"; \
	} \
	{ \
		printf("| %s | %s | %s | %s | %s | %s | %s |\n", $$1, $$2, $$3, $$4, $$5, $$6, $$7); \
	}' >> "$(ACKNOWLEDGEMENTS_PATH)"


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