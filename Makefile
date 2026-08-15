# ==============================================================================
# MailBaby Go Client Makefile
# ==============================================================================

PROTO_SRC := pb/mailbaby.proto
PB_DIR    := pb

.PHONY: proto build vet test fmt clean help

## proto: Regenerate protobuf and gRPC Go stubs from the MailBaby proto definition
proto:
	@echo "==> Generating protobuf stubs..."
	protoc \
	  --go_out=. --go_opt=paths=source_relative \
	  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	  $(PROTO_SRC)
	@echo "==> Protobuf code generation complete."

## build: Compile all packages
build:
	@echo "==> Building all packages..."
	go build ./...

## vet: Run static analysis
vet:
	@echo "==> Running go vet..."
	go vet ./...

## test: Run all unit tests
test:
	@echo "==> Running all test suites..."
	go test ./...

## fmt: Format all Go source files
fmt:
	@echo "==> Formatting source files..."
	gofmt -l -w .

## clean: Remove generated protobuf artifacts
clean:
	@echo "==> Cleaning generated stubs..."
	rm -rf $(PB_DIR)
	@echo "==> Clean complete."

## help: Display available Makefile targets
help:
	@echo "MailBaby Go Client"
	@echo
	@echo "Usage:"
	@echo "  make <target>"
	@echo
	@echo "Targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed -e 's/## //g' | awk 'BEGIN {FS = ": "}; {printf "  %-12s %s\n", $$1, $$2}'
