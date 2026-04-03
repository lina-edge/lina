# Native edge deployment (Ansible)

Installs the LINA edge stack on a Debian-based host **without Docker**: `redis-server`, `mosquitto`, four Go services under systemd, and Prometheus-style exporters (node, process, redis, optional systemd).

Layout matches `ansible.md` at the repo root (`/opt/lina/bin`, `/etc/lina`, `/var/lib/lina`, journald logging).

## Prerequisites

- Control machine: Ansible 2.14+ (`ansible-playbook`).
- Target: Debian 12+ or Raspberry Pi OS (64-bit), sudo/root over SSH.
- Built Linux binaries for the target `GOARCH` named exactly: `device`, `ledger`, `consumption`, `lightning` (output of `go build` from each service module).

On **Apple Silicon**, a local **Ubuntu arm64 Multipass** VM is a convenient target (same arch as 64-bit Pi). See `deployment/multipass/README.md` and `deployment/multipass/create-vm.sh`.

## Configure

1. Edit `inventory/hosts` (or copy from `inventory/hosts.example`): set `ansible_host`, `ansible_user`, and **`lina_binaries_dir`** — an absolute path on the **control** machine where those four binaries live.
2. Edit `group_vars/all.yml` for MQTT credentials, `SERVICE_TOKEN`, and **Lightning** (`lina_lnd_*` hex values). Use `ansible-vault` for production secrets.

Ports used on a single host are split to avoid collisions (REST, gRPC, and `METRICS_ADDR`); adjust variables under “Single-host ports” in `group_vars/all.yml` if needed.

## Run

From this directory (`deployment/ansible`):

```bash
ansible-playbook playbooks/site.yml
```

Ansible loads `ansible.cfg` here, including `inventory/hosts` and `roles/`.

## After deploy

- Application metrics: device `9466`, ledger `9460`, consumption `9465` (defaults; overridable via `METRICS_ADDR` in each env file template).
- Node exporter (package): port **9100**.
- Process exporter: **9256** (`lina_process_exporter_listen`).
- Redis exporter: **9121** (`lina_redis_exporter_listen`).
- Systemd exporter (optional): **9558**.

Plain MQTT is enabled on **1883** with `allow_anonymous true` by default for lab use; enable TLS via `lina_mosquitto_tls_enable` and cert paths in `group_vars/all.yml`, then tighten anonymous access in `roles/mosquitto/templates/mosquitto.conf.j2`.

If Mosquitto fails with a duplicate `listener` error, the distro may already define port 1883 in `/etc/mosquitto/mosquitto.conf`; remove or comment that block so only `conf.d/99-lina.conf` defines listeners (or merge settings into one file).

## Roles

| Role           | Purpose                                                |
|----------------|--------------------------------------------------------|
| `common`       | apt update, base packages                              |
| `redis`        | `redis-server`, loopback bind, optional password       |
| `mosquitto`    | broker + `/etc/mosquitto/conf.d/99-lina.conf`          |
| `lina-services`| users, binaries, `/etc/lina/*.env`, systemd units      |
| `monitoring`   | `prometheus-node-exporter` (apt), process/redis/systemd exporters |
