# Build Stage
FROM golang:1.25-alpine AS builder

    # ARG for service folder (e.g. consumption, registry, ledger)
    ARG SERVICE
    WORKDIR /workspace

    # Copy common proto3 definition files
    COPY services/proto/go.mod ./services/proto/
    COPY services/proto/gen ./services/proto/gen
    
    # Copy a shared library for common utilities
    COPY services/internal/go.mod ./services/internal/
    COPY services/internal/*.go ./services/internal/

    # Copy target service files
    COPY services/${SERVICE}/go.mod services/${SERVICE}/go.sum ./services/${SERVICE}/
    WORKDIR /workspace/services/${SERVICE}
    RUN go mod download

    # Copy target service source code
    COPY services/${SERVICE} ./

    # Build the target service
    RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o ${SERVICE} .

# Final Stage
FROM alpine:latest

    ARG SERVICE
    ENV APP=${SERVICE}
    WORKDIR /app

    COPY --from=builder /workspace/services/${SERVICE}/${SERVICE} .

    EXPOSE 8080

    CMD ./${APP}