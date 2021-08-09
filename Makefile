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
	source ai/src/venv/bin/activate && python -m unittest discover -s ai/src/tests && deactivate