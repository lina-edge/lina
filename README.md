# LINA (Lightning Integrated Node Architecture)

A Lightning Network payment system for smart meters and IoT devices. This system enables devices to request Lightning invoices, track consumption, and manage balances through a microservices architecture.

## Overview

LINA is a distributed system that connects IoT devices (like smart meters) to the Lightning Network for real-time micropayments. Devices publish consumption data via MQTT, and the system manages device balances, creates Lightning invoices, and tracks energy consumption.

## Architecture

The system consists of several microservices communicating via:
- **MQTT** (southbound): Device-to-system communication
- **gRPC** (east-west): Inter-service communication
- **HTTP/gRPC** (northbound): External API access
- **Redis Streams**: Event-driven communication between services

#### Services

- **Device Service** (`services/device/`): Manages device registration, MQTT communication, and routes device events to other services
- **Ledger Service** (`services/ledger/`): Manages device balances, credits, and debits. Provides gRPC API for invoice creation.
- **Lightning Service** (`services/lightning/`): Interfaces with LND (Lightning Network Daemon) to create and manage invoices
- **Consumption Service** (`services/consumption/`): Tracks and aggregates energy consumption data from devices
- **Autopay Service** (`services/autopay/`): Automatically pays invoices during load testing (not for production use)

#### Infrastructure

- **Caddy** (`infrastructure/caddy/`): Reverse proxy and API gateway for northbound HTTP/gRPC access
- **Mosquitto** (`infrastructure/mosquitto/`): MQTT broker with TLS for secure device communication
- **Redis** (`infrastructure/redis/`): Used for caching and Redis Streams for event-driven communication
- **Certs** (`infrastructure/certs/`): Certificate generation for MQTT TLS

#### Testing Tools

- **Smart Meter Simulator** (`testing/smartmeter/`): Simulates a smart meter device with web UI for testing
- **Load Testing** (`testing/loadtest/`): K6-based load testing tools
- **Measurement** (`testing/measurement/`): Performance monitoring and metrics collection

## Project Structure

```
lina/
├── services/                    # Core business services
│   ├── consumption/           # Consumption tracking service
│   ├── device/                 # Device management service
│   ├── ledger/                 # Balance/ledger service
│   ├── lightning/              # Lightning Network integration
│   ├── autopay/                # Auto-payment service (testing only)
│   ├── internal/              # Shared internal libraries
│   └── proto/                  # Protocol buffer definitions
│
├── infrastructure/             # Infrastructure components
│   ├── caddy/                  # Reverse proxy/API gateway
│   ├── mosquitto/              # MQTT broker
│   ├── redis/                  # Cache/database
│   └── certs/                  # Certificate generation
│
├── testing/                     # Testing and simulation tools
│   ├── smartmeter/             # Smart meter simulator
│   ├── loadtest/               # Load testing tools (k6)
│   └── measurement/            # Performance measurement tools
│
└── deployment/                 # Deployment configuration
    ├── docker-compose.*.yml    # Docker Compose files
    └── scripts/                 # Deployment scripts
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Go 1.25+ (for local development)
- Node.js 22+ (for smartmeter UI development)
- LND node (for Lightning Network integration)

### Quick Start

1. **Generate certificates** (required for MQTT TLS):

```bash
cd infrastructure/certs
./generate-certs.sh
```

2. **Configure environment**:

Create a `.env` file at the project root (or copy from `deployment/.env.example`). You'll need:
- LND connection details (`LND_HOST`, `LND_TLS_CERT_HEX`, `LND_MACAROON_HEX`)
- Redis configuration
- MQTT broker configuration
- Service-specific settings

3. **Start the system**:

```bash
# Development environment
docker-compose -f deployment/docker-compose.dev.yml up

# Or start specific services
docker-compose -f deployment/docker-compose.dev.yml up device ledger lightning consumption
```

4. **Start the smart meter simulator** (in another terminal):

```bash
docker-compose -f deployment/docker-compose.simulators.yml up
```

The smart meter UI will be available at `http://localhost:3001`

### Service Endpoints

Once running, services are accessible via Caddy (port 8080):

- **Services**: `http://localhost:8080`

## Development

### Running Services Locally

Each service can be run independently for development:

```bash
# Example: Run device service
cd services/device
go run main.go
```

Services require:
- Redis connection (configured via `REDIS_HOST` env var)
- Access to shared proto definitions in `services/proto/`
- Access to internal libraries in `services/internal/`

### Building Services

All services share a common Dockerfile pattern:

```bash
# Build a specific service
docker build -f services/Dockerfile --build-arg SERVICE=device -t lina-device .
```

### Protocol Buffers

Protocol definitions are in `services/proto/`:
- **Model protos** (`model/`): Domain models and events
- **Interface protos** (`interfaces/`): Service-to-service gRPC APIs

To regenerate Go code from protos:

```bash
cd services/proto
make
```

### Testing

#### Smart Meter Simulator

The smart meter simulator provides a web UI to simulate device behavior:

```bash
docker-compose -f deployment/docker-compose.simulators.yml up
```

Access the UI at `http://localhost:3001`

#### Load Testing

```bash
cd testing/loadtest
make run
```

#### Performance Measurement

```bash
cd testing/measurement
python monitor.py
```

## Key Concepts

### Device Flow

1. **Device Registration**: Device connects via MQTT with credentials
2. **Usage Reporting**: Device publishes consumption data via MQTT
3. **Balance Management**: Device service routes events to ledger service
4. **Invoice Creation**: When balance is low, lightning service creates invoice
5. **Payment**: User pays invoice, balance is credited

### Event-Driven Architecture

Services communicate via Redis Streams:
- Device events → Device Service
- Balance updates → Ledger Service
- Invoice events → Lightning Service
- Consumption data → Consumption Service

### MQTT Topics

- `device/{device_id}/usage`: Device publishes usage data
- `device/{device_id}/invoice/request`: Device requests invoice
- `device/{device_id}/invoice/response`: System responds with invoice

### gRPC APIs

- **Ledger Service**: `CreateOrGetAuthorization()`
- **Lightning Service**: `CreateInvoice()`

## Deployment

### Local Deployment

```bash
./deployment/scripts/deploy.sh local
```

### Remote Deployment

```bash
./deployment/scripts/deploy.sh remote user@hostname
```

For detailed deployment instructions, see `deployment/scripts/DEPLOYMENT.md`

### Production

Use the production compose file which pulls pre-built images:

```bash
docker-compose -f deployment/docker-compose.prod.yml up -d
```

### Building and Pushing Docker Images

To build and push Docker images to a registry (for use in production or across multiple machines):

1. **Login to your Docker registry**:
   ```bash
   docker login
   # or for private registries:
   docker login your-registry.com
   ```

2. **Build and push all images**:
   ```bash
   # Make the script executable (if needed)
   chmod +x deployment/scripts/build-and-push.sh
   
   # Build and push to Docker Hub (replace 'username' with your Docker Hub username)
   ./deployment/scripts/build-and-push.sh docker.io/username/lina latest
   
   # Or use a specific tag
   ./deployment/scripts/build-and-push.sh docker.io/username/lina v1.0.0
   
   # For GitHub Container Registry
   ./deployment/scripts/build-and-push.sh ghcr.io/username/lina latest
   
   # For a private registry
   ./deployment/scripts/build-and-push.sh registry.example.com/lina latest
   ```

The script builds multi-architecture images (amd64 and arm64) by default. To build for specific platforms:

```bash
# Build only for amd64
./deployment/scripts/build-and-push.sh docker.io/username/lina latest linux/amd64

# Build for specific platforms
./deployment/scripts/build-and-push.sh docker.io/username/lina latest linux/amd64,linux/arm64
```

For detailed instructions, see `deployment/scripts/DOCKER_PUBLISH.md`

## Configuration

### Environment Variables

Key environment variables (see individual service READMEs for complete lists):

- `LND_HOST`: LND gRPC endpoint
- `LND_TLS_CERT_HEX`: LND TLS certificate (hex-encoded)
- `LND_MACAROON_HEX`: LND macaroon (hex-encoded)
- `REDIS_HOST`: Redis connection string
- `MQTT_BROKER`: MQTT broker address
- `MQTT_USERNAME` / `MQTT_PASSWORD`: MQTT credentials
- `DB_PATH`: SQLite database path (per service)

### Docker Compose Paths

All docker-compose files are in `deployment/` and use relative paths:
- Build contexts: `../services/`, `../infrastructure/`, `../testing/`
- Volumes: `../infrastructure/certs/`
- Environment: `../.env`

Always run docker-compose from project root with `-f deployment/docker-compose.*.yml`

## Troubleshooting

### Services won't start

- Check that Redis is running and accessible
- Verify certificates are generated (`infrastructure/certs/`)
- Check `.env` file exists and has required variables
- Review service logs: `docker-compose -f deployment/docker-compose.dev.yml logs <service>`

### MQTT connection issues

- Verify certificates are mounted correctly
- Check MQTT broker is healthy: `docker-compose -f deployment/docker-compose.dev.yml ps mosquitto`
- Verify device credentials in MQTT broker

### LND connection issues

- Verify LND node is running and accessible
- Check TLS certificate and macaroon are correctly hex-encoded
- Test LND connection: `lncli --lnddir=<path> getinfo`

## Additional Documentation

- **Service Documentation**: See individual `README.md` files in each service directory
- **Deployment Guide**: `deployment/scripts/DEPLOYMENT.md`
- **Docker Publishing**: `deployment/scripts/DOCKER_PUBLISH.md`
- **Protocol Definitions**: `services/proto/README.md`
- **Smart Meter UI**: `testing/smartmeter/ui/README.md`

## Contributing

When adding new services:
1. Create service directory in `services/`
2. Add service to `services/Dockerfile` build args
3. Update docker-compose files with new service
4. Add service to Caddy configuration if it needs northbound API

When adding infrastructure:
1. Create component directory in `infrastructure/`
2. Add Dockerfile and configuration
3. Update docker-compose files
