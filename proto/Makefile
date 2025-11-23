.PHONY: generate generate-messages generate-interfaces clean help generate-docker

# Extract Go version from go.mod (e.g., "1.25.4" -> "1.25")
GO_VERSION := $(shell grep '^go ' go.mod | awk '{print $$2}' | cut -d. -f1,2)
GO_IMAGE := golang:$(GO_VERSION)-alpine

# Use Docker if PROTOC_DOCKER is set, otherwise use local protoc
PROTOC := $(if $(PROTOC_DOCKER),docker run --rm -v $(PWD):/workspace -w /workspace $(PROTOC_DOCKER),protoc)

# Directories
MESSAGES_DIR := messages
INTERFACES_DIR := interfaces
GEN_DIR := gen
OUT_DIR := $(GEN_DIR)

# Message proto files (only messages, no services)
MESSAGE_PROTO_FILES := $(wildcard $(MESSAGES_DIR)/*.proto)

# Interface proto files (contain service definitions)
INTERFACE_PROTO_FILES := $(wildcard $(INTERFACES_DIR)/*.proto)

generate: generate-messages generate-interfaces
	@echo "All generation complete!"

generate-messages:
	@echo "Generating Go code from message proto files..."
	@mkdir -p $(GEN_DIR)
	@if [ -z "$(MESSAGE_PROTO_FILES)" ]; then \
		echo "No message proto files found in $(MESSAGES_DIR)/"; \
	else \
		for proto in $(MESSAGE_PROTO_FILES); do \
			echo "Generating from $$proto..."; \
			$(PROTOC) --go_out=$(OUT_DIR) \
				-I. -I=$(MESSAGES_DIR) $$proto; \
		done; \
		echo "Message generation complete!"; \
	fi

generate-interfaces:
	@echo "Generating Go code from interface proto files (with gRPC)..."
	@mkdir -p $(GEN_DIR)
	@if [ -z "$(INTERFACE_PROTO_FILES)" ]; then \
		echo "No interface proto files found in $(INTERFACES_DIR)/"; \
	else \
		for proto in $(INTERFACE_PROTO_FILES); do \
			echo "Generating from $$proto..."; \
			$(PROTOC) --go_out=$(OUT_DIR) \
				--go-grpc_out=$(OUT_DIR) \
				-I. -I=$(INTERFACES_DIR) -I=$(MESSAGES_DIR) $$proto; \
		done; \
		echo "Interface generation complete!"; \
	fi

# Docker-based generation (no need to install protoc locally)
generate-docker:
	@echo "Generating using Docker (no local protoc required)..."
	@echo "Using Go version $(GO_VERSION) from go.mod"
	@docker run --rm --platform linux/amd64 \
		-v $(PWD):/workspace \
		-w /workspace \
		$(GO_IMAGE) \
		sh -c "apk add --no-cache protobuf protobuf-dev && \
		mkdir -p gen && \
		go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
		go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && \
		protoc --go_out=gen -I. -I=messages messages/*.proto && \
		protoc --go_out=gen --go-grpc_out=gen -I. -I=interfaces -I=messages interfaces/*.proto"
	@echo "Docker generation complete!"

clean:
	@echo "Cleaning generated files..."
	@rm -rf $(GEN_DIR)
	@echo "Clean complete!"

help:
	@echo "Available targets:"
	@echo "  generate            - Generate Go code from all proto files (messages + interfaces)"
	@echo "  generate-messages   - Generate Go code from message proto files only"
	@echo "  generate-interfaces - Generate Go code from interface proto files (with gRPC)"
	@echo "  generate-docker     - Generate using Docker (no local protoc required)"
	@echo "  clean               - Remove all generated Go files"
	@echo "  help                - Show this help message"
	@echo ""
	@echo "Note: You need either:"
	@echo "  - protoc installed locally (brew install protobuf), OR"
	@echo "  - Docker installed (use 'make generate-docker')"

