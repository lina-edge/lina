# Build Stage
FROM golang:1.25-alpine AS builder

    # ARG for service folder (e.g. consumption, device, ledger)
    ARG SERVICE
    WORKDIR /workspace

    # Copy common proto3 definition files
    COPY services/proto/go.mod ./services/proto/
    COPY services/proto/gen ./services/proto/gen
    
    # Copy shared library for common utilities
    COPY services/internal/go.mod ./services/internal/
    COPY services/internal/*.go ./services/internal/

    # Copy target service files
    COPY services/${SERVICE}/go.mod services/${SERVICE}/go.sum ./services/${SERVICE}/

    WORKDIR /workspace/services/${SERVICE}

    RUN --mount=type=cache,target=/go/pkg/mod,id=gomodcache \
    --mount=type=cache,target=/root/.cache/go-build,id=gobuildcache \
    go mod download

    # Copy target service source code
    COPY services/${SERVICE} ./

    # Build the target service
    RUN --mount=type=cache,target=/go/pkg/mod,id=gomodcache \
    --mount=type=cache,target=/root/.cache/go-build,id=gobuildcache \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o ${SERVICE} .

# Final Stage
FROM alpine:3.23

    ARG SERVICE
    ENV APP=${SERVICE}
    WORKDIR /app

    COPY --from=builder /workspace/services/${SERVICE}/${SERVICE} .

    # Create data directory for SQLite databases
    RUN mkdir -p /data

    EXPOSE 8080

    CMD ["sh", "-c", "./${APP}"]