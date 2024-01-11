################################################################################
# Target: all                                                                 #
################################################################################
.PHONY: all
all: build

.PHONY: build
build:
	make -C bin/spice
	cargo build --release

.PHONY: lint
lint:
	go vet ./...
	golangci-lint run
	cargo fmt --all -- --check

################################################################################
# Target: install                                                              #
################################################################################
.PHONY: install
install: build
	mkdir -p ~/.spice/bin
	install -m 755 target/release/spice ~/.spice/bin/spice
	install -m 755 target/release/spiced ~/.spice/bin/spiced

################################################################################
# Target: modtidy                                                              #
################################################################################
.PHONY: modtidy
modtidy:
	go mod tidy
