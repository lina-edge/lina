# Build Stage
FROM golang:1.25-alpine AS builder

    # ARG for service folder (e.g. consumption, registry, ledger)
    ARG SERVICE
    WORKDIR /workspace

    # Copy proto module to match project structure
    COPY proto/go.mod ./proto/
    COPY proto/gen ./proto/gen
    
    # Copy library module to match project structure
    COPY library/go.mod ./library/
    COPY library/*.go ./library/
    # If your library directory has subdirectories to include, copy as needed:
    COPY library/ ./library/

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