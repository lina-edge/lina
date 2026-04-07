# Build Smart Meter UI
FROM node:22-alpine AS ui-build
    WORKDIR /app/ui
    COPY testing/smartmeter/ui/package.json testing/smartmeter/ui/package-lock.json* ./
    RUN --mount=type=cache,target=/root/.npm npm install
    COPY testing/smartmeter/ui .
    RUN npm run build

# Build Smart Meter Core
FROM golang:1.25-alpine AS core-builder

    WORKDIR /workspace

    # Copy common proto3 definition files
    COPY services/proto/go.mod ./services/proto/
    COPY services/proto/gen ./services/proto/gen

    # Copy shared library for common utilities
    COPY services/internal/go.mod ./services/internal/
    COPY services/internal/ *.go ./services/internal/

    COPY testing/device/go.mod testing/device/go.sum ./testing/device/
    COPY testing/device/*.go ./testing/device/

    # Copy Smart Meter Core files
    COPY testing/smartmeter/core/go.mod testing/smartmeter/core/go.sum ./testing/smartmeter/core/
    WORKDIR /workspace/testing/smartmeter/core
    RUN --mount=type=cache,target=/go/pkg/mod go mod download

    # Copy Smart Meter Core source code
    COPY testing/smartmeter/core/*.go ./

    # Build Smart Meter Core application
    RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o smartmeter .

# Smart Meter Runtime
FROM alpine:3.23
    RUN apk --no-cache add ca-certificates
    WORKDIR /app

    # Copy Smart Meter Core binary
    COPY --from=core-builder /workspace/testing/smartmeter/core/smartmeter .

    # Copy Smart Meter UI static files
    COPY --from=ui-build /app/ui/out ./public

    # Expose single port for both Smart Meter UI and WebSocket
    EXPOSE 8080

    # Start the application (serves UI and WebSocket on same port)
    CMD ["./smartmeter"]