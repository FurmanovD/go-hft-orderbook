lint:
	golangci-lint run ./...

test:
	go run gotest.tools/gotestsum@latest

.PHONY: lint test