# Native Deployment Instructions (No Docker)

## Objective

Generate an Ansible-based deployment that installs and runs the full LINA edge stack **natively on a Linux machine** (e.g., Raspberry Pi or Debian-based system), without Docker.

All services must run as **systemd units**, and observability must be implemented using Prometheus-compatible exporters.

---

## Target Environment

- OS: Debian-based (e.g., Raspberry Pi OS 64-bit, Debian 12+)
- Architecture: ARM64 or AMD64
- Privileges: sudo/root available
- Network: outbound internet access for package installation
- Deployment style: single-node edge device

---

## High-Level Requirements

The deployment must:

1. Install all required dependencies
2. Deploy Go binaries
3. Configure Redis and MQTT broker
4. Configure and run all services via systemd
5. Install and configure monitoring exporters
6. Ensure services start on boot
7. Be fully reproducible via Ansible

---

## Services to Deploy

### Core Application Services (Go binaries)

Each service must:

- Run as a dedicated Linux user
- Have its own systemd unit
- Restart automatically on failure
- Log via journald

Services:

- device-service
- ledger-service
- consumption-service
- lightning-service

---

### Infrastructure Dependencies

Install via package manager (apt):

- Redis (redis-server)
- MQTT broker (nanomq)

Ensure:

- Services are enabled and running
- Config files are placed under `/etc/`
- Proper ports are open and listening

---

## Directory Structure

Use the following structure:

- Binaries: `/opt/lina/bin/`
- Configs: `/etc/lina/`
- Logs: handled by journald
- Data:
  - Redis: default
  - SQLite (if used): `/var/lib/lina/`

---

## Systemd Requirements

Each service must have:

- `Restart=always`
- `RestartSec=5`
- `User=<service_user>`
- `WorkingDirectory=/opt/lina/`
- `ExecStart=/opt/lina/bin/<service_binary>`
- Environment file support (`/etc/lina/<service>.env`)

Define dependencies where appropriate:

- ledger-service → after redis
- device-service → after nanomq
- consumption-service → after ledger-service
- lightning-service → after network

---

## Monitoring and Observability

### 1. Node Exporter

Install Prometheus node_exporter:

- Run as systemd service
- Listen port: `9463` (aligned with `docker-compose.evaluation.edge.yml` host mapping and `prometheus.template.yml`)
- Collect:
  - CPU
  - Memory
  - Disk
  - Network

---

### 2. Process Exporter

Install process-exporter:

- Group processes by name:
  - device-service
  - ledger-service
  - consumption-service
  - lightning-service
  - nanomq
  - redis-server

- Config file: `/etc/process-exporter/config.yml`
- Run as systemd service

---

### 3. Systemd Exporter (optional but recommended)

- Expose metrics per systemd unit
- Track:
  - service state
  - restarts
  - resource usage

---

### 4. Redis Exporter

- Connect to local Redis instance
- Expose Redis metrics (including Streams)

---

### 5. Application Metrics

All Go services must expose Prometheus metrics endpoints:

- `/metrics`
- HTTP port configurable per service

Metrics already implemented should remain unchanged.

---

## Networking

Ensure the following ports are exposed:

- MQTT: 1883 / 8883 (if TLS)
- Redis: 6379
- Node exporter: 9463
- Redis exporter: 9461
- Process exporter: 9256
- Systemd exporter: 9558 (optional; Linux + host D-Bus)
- Service metrics: configurable per service

---

## Security Considerations

- Create dedicated Linux users per service
- No services should run as root
- Restrict file permissions:
  - `/opt/lina/bin` → root-owned, executable
  - `/etc/lina` → readable by service users only
- Optional:
  - Enable TLS for MQTT
  - Use firewall rules (ufw)

---

## Ansible Requirements

The generated Ansible project must include:

### Structure

- `inventory/`
- `playbooks/`
- `roles/`
  - common/
  - lina-services/
  - monitoring/
  - redis/
  - nanomq/

---

### Tasks to Implement

#### System Preparation

- Update apt cache
- Install base packages:
  - curl
  - wget
  - ca-certificates
  - unzip

---

#### Install Dependencies

- Install redis-server
- Install nanomq

---

#### Deploy Binaries

- Copy Go binaries to `/opt/lina/bin/`
- Set executable permissions

---

#### Configure Services

- Copy config files to `/etc/lina/`
- Create environment files if needed

---

#### Create Systemd Units

For each service:

- Create `.service` file
- Reload systemd daemon
- Enable service
- Start service

---

#### Monitoring Setup

- Install node_exporter (binary or package)
- Install process-exporter
- Install redis_exporter
- (Optional) install systemd_exporter

- Create systemd units for each exporter
- Enable and start all exporters

---

## Expected Outcome

After running the Ansible playbook:

- All services are running via systemd
- System survives reboot with all services restored
- Prometheus can scrape:
  - node metrics
  - process metrics
  - Redis metrics
  - application metrics
- No Docker or container runtime is used

---

## Optional Enhancements

- Add resource limits via systemd (CPUQuota, MemoryMax)
- Add log rotation policies
- Add health check endpoints
- Add automated updates for binaries
- Add TLS configuration for MQTT and APIs

---

## Important Notes

- Do NOT use Docker, Docker Compose, or any container runtime
- Prefer native Linux packages and static binaries
- Keep the setup lightweight and suitable for constrained edge devices

---

## Deliverable

Generate:

- Complete Ansible project
- All required roles and playbooks
- Templates for systemd unit files
- Templates for exporter configs
- Example inventory file

The result must allow a single command such as:
