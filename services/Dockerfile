# Build Stage
FROM golang:1.25-alpine AS builder

    # ARG for service folder (e.g. consumption, registry, ledger)
    ARG SERVICE
    WORKDIR /workspace

    # Copy proto module to match project structure
    # Proto is now in services/proto, but we copy it to ./proto/ in container
    # so that ../proto from services/${SERVICE} resolves correctly
    COPY services/proto/go.mod ./services/proto/
    COPY services/proto/gen ./services/proto/gen
    
    # Copy internal module to match project structure
    # Internal is now in services/internal, but we copy it to ./internal/ in container
    # so that ../internal from services/${SERVICE} resolves correctly
    COPY services/internal/go.mod ./services/internal/
    COPY services/internal/*.go ./services/internal/
    # If your internal directory has subdirectories to include, copy as needed:
    COPY services/internal/ ./services/internal/

    # Copy service files to services/ directory to match project structure
    COPY services/${SERVICE}/go.mod services/${SERVICE}/go.sum ./services/${SERVICE}/
    WORKDIR /workspace/services/${SERVICE}
    RUN go mod download

    # Copy the rest of the application source code
    COPY services/${SERVICE} ./

    # Build the Go application
    RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o myapp .

# Final Stage
FROM alpine:latest

    ARG SERVICE
    WORKDIR /app

    COPY --from=builder /workspace/services/${SERVICE}/myapp .

    EXPOSE 8080

    CMD ["./myapp"]