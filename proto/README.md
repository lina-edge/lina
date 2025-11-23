# RPC Proto Definitions

This directory contains Protocol Buffer definitions for the LNPay services, organized into:
- **`messages/`** - Message definitions (enums, messages, events)
- **`interfaces/`** - Service definitions (gRPC services)

## Prerequisites

You have two options:

### Option 1: Install protoc locally (faster, recommended for development)

1. **Install Protocol Buffers compiler (protoc)**
   - macOS: `brew install protobuf`
   - Linux: `sudo apt-get install protobuf-compiler` (or equivalent)
   - Or download from: https://github.com/protocolbuffers/protobuf/releases

2. **Install Go plugins**
   ```bash
   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
   ```

3. **Ensure Go bin is in your PATH**
   ```bash
   export PATH="$PATH:$(go env GOPATH)/bin"
   ```

### Option 2: Use Docker (no local installation needed)

Just have Docker installed - no need to install protoc or plugins locally!

## Generating Go Code

### Using Makefile (Recommended)

**If you installed protoc locally:**
```bash
# Generate all (messages + interfaces)
make generate

# Or generate separately:
make generate-messages   # Only message definitions
make generate-interfaces # Only service definitions (with gRPC)
```

**If using Docker (no local protoc needed):**
```bash
make generate-docker
```

### Manual Generation

**Messages (no gRPC):**
```bash
protoc --go_out=gen -I. -I=messages messages/*.proto
```

**Interfaces (with gRPC):**
```bash
protoc --go_out=gen --go-grpc_out=gen -I. -I=interfaces -I=messages interfaces/*.proto
```

## Generated Files

The generation will create `*.pb.go` files in the `gen/` directory, organized by the proto package structure:
- **Messages**: `gen/iot/payperuse/edge/model/*/` - Go structs for message definitions
- **Interfaces**: `gen/iot/payperuse/edge/interfaces/sync/*/` - Go structs and gRPC service code

Files are generated based on the `go_package` option in each proto file, maintaining the package hierarchy.

## Version Control

**Recommended: Commit generated files**

Yes, you should commit the generated `*.pb.go` and `*_grpc.pb.go` files to version control. This ensures:
- All team members use the same generated code
- No need for everyone to install protoc
- Simpler CI/CD pipelines
- Consistency across environments

**Best practices:**
1. Always regenerate files after modifying `.proto` files: `make generate`
2. Commit both the `.proto` changes and the generated `.pb.go` files together
3. If you see conflicts in generated files, regenerate them: `make clean && make generate`

## Cleaning Generated Files

```bash
make clean
```

