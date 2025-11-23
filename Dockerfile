# Build Stage
FROM golang:1.25-alpine AS builder

    # ARG for service folder (e.g. consumption, registry, ledger)
    ARG SERVICE
    WORKDIR /workspace

    # Copy only the proto module files needed for the replace directive
    # We need go.mod for the module and gen/ for the generated files
    COPY proto/go.mod ./proto/
    COPY proto/gen ./proto/gen

    # Copy go.mod and go.sum to the service directory to maintain relative paths
    COPY ${SERVICE}/go.mod ${SERVICE}/go.sum ./${SERVICE}/
    WORKDIR /workspace/${SERVICE}
    RUN go mod download

    # Copy the rest of the application source code
    COPY ${SERVICE} ./

    # Build the Go application
    RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o myapp .

# Final Stage
FROM alpine:latest

    ARG SERVICE
    WORKDIR /app

    COPY --from=builder /workspace/${SERVICE}/myapp .

    EXPOSE 8080

    CMD ["./myapp"]