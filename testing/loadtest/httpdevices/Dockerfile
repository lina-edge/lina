# Build Stage
FROM golang:1.25-alpine AS builder

    WORKDIR /workspace

    # Copy common proto3 definition files
    COPY services/proto/go.mod ./services/proto/
    COPY services/proto/gen ./services/proto/gen

    # Copy internal module (needed by device package)
    COPY services/internal/go.mod services/internal/go.sum ./services/internal/
    COPY services/internal/*.go ./services/internal/

    # Copy device package
    COPY testing/device/go.mod testing/device/go.sum ./testing/device/
    COPY testing/device/*.go ./testing/device/

    # Copy httpdevices files
    COPY testing/loadtest/httpdevices/go.mod testing/loadtest/httpdevices/go.sum ./testing/loadtest/httpdevices/
    WORKDIR /workspace/testing/loadtest/httpdevices
    
    RUN --mount=type=cache,target=/go/pkg/mod,id=gomodcache \
    --mount=type=cache,target=/root/.cache/go-build,id=gobuildcache \
    go mod download

    # Copy httpdevices source code
    COPY testing/loadtest/httpdevices/*.go ./

    # Build the httpdevices application
    RUN --mount=type=cache,target=/go/pkg/mod,id=gomodcache \
    --mount=type=cache,target=/root/.cache/go-build,id=gobuildcache \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o httpdevices .

# Final Stage
FROM alpine:3.23

    ENV APP=httpdevices
    WORKDIR /app

    COPY --from=builder /workspace/testing/loadtest/httpdevices/httpdevices .

    EXPOSE 8080

    CMD ["./httpdevices"]

