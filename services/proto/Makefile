.PHONY: generate generate-messages generate-interfaces clean help generate-docker

# Extract Go version from go.mod (e.g., "1.25.4" -> "1.25")
GO_VERSION := $(shell grep '^go ' go.mod | awk '{print $$2}' | cut -d. -f1,2)
GO_IMAGE := golang:$(GO_VERSION)-alpine

# Use Docker if PROTOC_DOCKER is set, otherwise use local protoc
PROTOC := $(if $(PROTOC_DOCKER),docker run --rm -v $(PWD):/workspace -w /workspace $(PROTOC_DOCKER),protoc)

# Directories
MODEL_DIR := model
INTERFACES_DIR := interfaces
GEN_DIR := gen
OUT_DIR := .

# Model proto files (data models, enums, messages, events)
MODEL_PROTO_FILES := $(wildcard $(MODEL_DIR)/*.proto)

# Interface proto files (contain service definitions)
INTERFACE_PROTO_FILES := $(wildcard $(INTERFACES_DIR)/*.proto)

generate: generate-models generate-interfaces
	@echo "All generation complete!"

generate-models:
	@echo "Generating Go code from model proto files..."
	@mkdir -p $(GEN_DIR)
	@if [ -z "$(MODEL_PROTO_FILES)" ]; then \
		echo "No model proto files found in $(MODEL_DIR)/"; \
	else \
		for proto in $(MODEL_PROTO_FILES); do \
			echo "Generating from $$proto..."; \
			$(PROTOC) --go_out=$(OUT_DIR) \
				--go_opt=module=github.com/robertodantas/lnpay/proto \
				-I. -I=$(MODEL_DIR) $$proto; \
		done; \
		echo "Model generation complete!"; \
	fi

generate-interfaces:
	@echo "Generating Go code from interface proto files (gRPC only)..."
	@mkdir -p $(GEN_DIR)
	@if [ -z "$(INTERFACE_PROTO_FILES)" ]; then \
		echo "No interface proto files found in $(INTERFACES_DIR)/"; \
	else \
		for proto in $(INTERFACE_PROTO_FILES); do \
			echo "Generating from $$proto..."; \
			$(PROTOC) --go-grpc_out=$(OUT_DIR) \
				--go-grpc_opt=module=github.com/robertodantas/lnpay/proto \
				-I. -I=$(INTERFACES_DIR) -I=$(MODEL_DIR) $$proto; \
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
		protoc --go_out=. --go_opt=module=github.com/robertodantas/lnpay/proto -I. -I=model model/*.proto && \
		protoc --go-grpc_out=. --go-grpc_opt=module=github.com/robertodantas/lnpay/proto -I. -I=interfaces -I=model interfaces/*.proto"
	@echo "Docker generation complete!"

clean:
	@echo "Cleaning generated files..."
	@find $(GEN_DIR) -type f -name "*.pb.go" -delete
	@find $(GEN_DIR) -type f -name "*_grpc.pb.go" -delete
	@find $(GEN_DIR) -type d -empty -delete
	@echo "Clean complete! (preserved json_marshal.go files)"

help:
	@echo "Available targets:"
	@echo "  generate            - Generate Go code from all proto files (models + interfaces)"
	@echo "  generate-models     - Generate Go code from model proto files only"
	@echo "  generate-interfaces - Generate Go code from interface proto files (with gRPC)"
	@echo "  generate-docker     - Generate using Docker (no local protoc required)"
	@echo "  clean               - Remove all generated Go files"
	@echo "  help                - Show this help message"
	@echo ""
	@echo "Note: You need either:"
	@echo "  - protoc installed locally (brew install protobuf), OR"
	@echo "  - Docker installed (use 'make generate-docker')"

