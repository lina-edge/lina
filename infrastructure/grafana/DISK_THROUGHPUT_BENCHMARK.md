# Disk throughput benchmark (node_disk_read_bytes_total)

Use this when you want to see how much read throughput a node can reach, and compare it to `node_disk_read_bytes_total` in Grafana (for example on `monitoring-systemd.json`).

## What the metric means

- `node_disk_read_bytes_total` is a **counter** (cumulative bytes read per block device).
- In Grafana/Prometheus you must use **`rate()`** or **`irate()`** to get bytes per second.

## Reduce interference from your services

1. Stop application workloads (examples — adjust to your units):

   ```bash
   sudo systemctl stop your-app.service
   sudo systemctl stop your-worker.service
   ```

2. Optionally stop container stacks if acceptable:

   ```bash
   sudo systemctl stop docker
   ```

3. **Keep** `node_exporter` (and Prometheus scraping it) running if you want live graphs during the test.

4. Avoid running backups, large `rsync`, or other disk-heavy jobs during the benchmark.

For a “clean room” hardware ceiling, boot a minimal/live image and run the same `fio` tests there (no app stack at all).

## Find the right `device` label

Block devices in node_exporter often include `loop`, `ram`, etc. Start with:

```promql
sum by (device) (rate(node_disk_read_bytes_total{instance="$instance"}[1m]))
```

Then exclude obvious virtual devices:

```promql
sum by (device) (
  rate(node_disk_read_bytes_total{
    instance="$instance",
    device!~"loop.*|ram.*|fd.*"
  }[30s])
)
```

Adjust `device` filters if you use LVM/RAID (`dm-*`) and need to match what `fio` is actually exercising.

## Install fio (if needed)

- Debian/Ubuntu: `sudo apt install fio`
- RHEL/Fedora: `sudo dnf install fio`

## Prepare a test file (once)

Use a path on the filesystem you want to measure (same disk/volume as production if that is the goal):

```bash
sudo mkdir -p /var/lib/disk-bench
sudo fallocate -l 8G /var/lib/disk-bench/testfile
```

Increase size if the disk is fast and the run finishes too quickly (e.g. 32G).

## Controlled sequential read (good baseline)

`--direct=1` reduces page-cache effects so reads show up more clearly at the block layer.

```bash
sudo fio --name=readtest \
  --filename=/var/lib/disk-bench/testfile \
  --rw=read \
  --bs=1M \
  --iodepth=32 \
  --ioengine=libaio \
  --direct=1 \
  --numjobs=1 \
  --runtime=120 \
  --time_based \
  --group_reporting
```

Watch Grafana: peak `rate(node_disk_read_bytes_total[30s])` on the matching `device` should align roughly with `fio`’s reported bandwidth (allow some overhead and aggregation differences).

## Test matrix (optional)

Run each for 60–120 seconds and note the peak on the dashboard vs `fio` output.

| Profile | Purpose |
|--------|---------|
| Seq read, `bs=1M`, `numjobs=1`, high `iodepth` | Streaming read, single queue depth exploration |
| Seq read, `numjobs=4` (NVMe) | Often higher throughput on parallel queues |
| Rand read, `bs=4k`, `iodepth=64` | Random read IOPS / mixed workloads |

Example random read:

```bash
sudo fio --name=randread \
  --filename=/var/lib/disk-bench/testfile \
  --rw=randread \
  --bs=4k \
  --iodepth=64 \
  --ioengine=libaio \
  --direct=1 \
  --numjobs=4 \
  --runtime=120 \
  --time_based \
  --group_reporting
```

Example parallel sequential read:

```bash
sudo fio --name=seqread-parallel \
  --filename=/var/lib/disk-bench/testfile \
  --rw=read \
  --bs=1M \
  --iodepth=16 \
  --ioengine=libaio \
  --direct=1 \
  --numjobs=4 \
  --runtime=120 \
  --time_based \
  --group_reporting
```

## Grafana / PromQL snippets

Per device (bytes/sec):

```promql
sum by (device) (
  rate(node_disk_read_bytes_total{
    instance="$instance",
    device!~"loop.*|ram.*|fd.*"
  }[30s])
)
```

Max across devices on one instance (bytes/sec):

```promql
max by (instance) (
  sum by (instance, device) (
    rate(node_disk_read_bytes_total{
      instance="$instance",
      device!~"loop.*|ram.*|fd.*"
    }[30s])
  )
)
```

In Grafana, use a **unit** of bytes/sec or add a **Math** override (divide by `1024^2` for MiB/s).

## Caveats

- **Page cache**: without `--direct=1`, workloads may be largely served from RAM; the counter may not move as you expect.
- **Device vs filesystem**: you are measuring the block device node_exporter reports; RAID, LVM, encryption, and cloud volume types change what “max” means.
- **Shared storage / VMs**: the hypervisor or storage backend may cap throughput regardless of local tuning.
- **Writes**: for write ceiling, use `node_disk_written_bytes_total` with the same `rate()` pattern and `fio --rw=write` / `randwrite` (writes are destructive — use a dedicated test file or disk).

## Cleanup

```bash
sudo rm -f /var/lib/disk-bench/testfile
# sudo rmdir /var/lib/disk-bench   # if empty
```

Restart any services you stopped earlier.
