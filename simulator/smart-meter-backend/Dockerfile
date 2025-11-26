# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /workspace

# Copy proto module
COPY services/proto/go.mod ./services/proto/
COPY services/proto/gen ./services/proto/gen

# Copy smart-meter-backend files
COPY simulator/smart-meter-backend/go.mod simulator/smart-meter-backend/go.sum ./simulator/smart-meter-backend/
WORKDIR /workspace/simulator/smart-meter-backend
RUN go mod download

# Copy source code
COPY simulator/smart-meter-backend/*.go ./

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o smart-meter-backend .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /workspace/simulator/smart-meter-backend/smart-meter-backend .

# Expose port
EXPOSE 8080

# Set default environment variables
ENV PORT=8080
ENV DEVICE_ID=smart-meter-001
ENV MQTT_BROKER_URL=tcp://mqtt:1883

# Run the application
CMD ["./smart-meter-backend"]
