################################################################################
# Target: all                                                                 #
################################################################################
.PHONY: all
all:
	docker build -t ghcr.io/spiceai/spiced:dev -f docker/Dockerfile .

.PHONY: push
push:
	docker push ghcr.io/spiceai/spiced:dev

.PHONY: test
test:
	go vet ./...
	go test ./...
	pushd ai/src  && make test && popd

.PHONY: build
build:
	pushd cmd/spice && go build . && popd
	pushd cmd/spiced && go build . && popd

.PHONY: lint
lint:
	go vet ./...
	black --check ai/src
	golangci-lint run
