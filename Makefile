################################################################################
# Target: all                                                                 #
################################################################################
.PHONY: all
all: build

.PHONY: build
build:
	make -C bin/spice
	make -C bin/spiced

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

.PHONY: check-deps-licenses
dep-licenses:
	cargo install cargo-license
	cargo license -d

################################################################################
# Target: install                                                              #
################################################################################
.PHONY: install
install: build
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spice ~/.spice/bin/spice
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
