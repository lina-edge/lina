# Execution Environment

Running Architecture in Edge Node and External Machine

## Network

Edge Node at 192.168.0.170
External Machine at 192.168.0.166

## Running the infrastructure on edge node 

```bash
docker compose -f deployment/docker-compose.evaluation.edge.yml up -d
```

That runs:
- caddy (192.168.0.170:8080)
- mosquitto (192.168.0.170:8883)
- redis
- device service
- ledger service
- consumption service
- lightning service
- redis exporter (prometheus metrics) 
- cadvisor exporter (prometheus metrics)
- node exporter (prometheus metrics)

<!-- Then, the edge was configured to collect the service logs to a file so that we can observe them later.

```bash
docker compose -f deployment/docker-compose.evaluation.edge.yml logs -f -t --no-color device ledger lightning consumption |& tee load-evaluation-edge-node.log
``` -->

## Running the LND node on External Machine

On Polar Lightning software, a new network was created with a bitcoin core in regtest mode.

Two LND nodes (v0.19.1-beta), alice and bob, connected to the single bitcoin core (v29.0)

6 payment channels created from bob to alice each with 15,000,000 sats of capacity to have enoguh liquditiy for both functional tests and load testing.

LND Node A (Alice / LINA): 192.168.0.166:10001
LND Node B (User / Payer): 192.168.0.166:10002


## Running the infrastructure on external machine

```bash
docker compose -f deployment/docker-compose.evaluation.external.yml up -d
```

That runs:
- autopay service
- smart meter device
- http devices
- prometheus server
- grafana 

For load tests the auto pay was enabled and the payment is made automatically upon invoice created on LND.

The external machine was configured to also collect smart meter logs.

```bash
docker compose -f deployment/docker-compose.evaluation.external.yml logs -f -t --no-color httpdevices |& tee load-evaluation-external-machine.log
```

## Device Setup

To support the provisioning of hundreds of devices, a new endpoint was added in device service Northbound Interface `/devices/batch` to create devices in a range. This endpoint supports a device identifier pattern,  k6_device_{id} whereas the id start in 1 to N. To simplify the register process, the \ac{MQTT} access control is simplified to use a wildcard to accept any device, the credentials uses the device identifier as username and append `_password` suffix to the password.

The goal is to exercise the load of usage reports, so the security simplifcaiton does not affect the evaluation. 

The k6 load testing script is responsible for registering the devices in the Northbound Interface, then it incrementally increases the number of active devices publishing usage reports to evaluate the load.

Each device connects to MQTT and perform the initial authorization request which is rejected due to insufficient funds. This causes the \textit{HTTP Device} to auto initiate the funding process.

For the loading test, \textttt{250,000 msats} is going to be added to each device.

The device was configured to emit usage reports at a fixed 1 report/s at a unit price of 100 msat and to request authorization slots of 10000 msat. 

The load test device should perform a new authorization once the current expires or exausths, it should also add funds if it receives a STOP command due to insufficient funds. The auto pay service will listen for new invoices and auto pay them so that each device can have funds available for the usage reporting load testing.

The devices were configured to emit a random value between 0.1 to 1.0 kWh. Under this configuration, that means each device will consume some value between 10 msat to 100 msat per second. Given that authorization slot of 10.000 msat, in worst case scenario, each authorization will accomodate 100 usage reports, and each device would be able to continuously consume every second during a 41 minutes period (2500 seconds) without having to add funds again, having enough funds through the load testing.

In summary, each virtual device should:
- Create invoice for 250 sats (250.000 msats).
- Detect invoice payment and new device balance (250.000 msats)
- Submit an authorization request for initial operation (10.000 msat) 
- Submit usage report every second from 0.1 kWh to 1.0 kWh

### Systematic Load Testing Procedure

#### Fix per-device reporting rate.
In all experiments, each simulated device publishes one usage report per second ($f = 1$\,report/s/device). This rate remains constant across all load levels.

#### Select load levels.
The number of simulated devices $N$ is increased in increments of 25 devices per
load level:
\[
    N \in \{0, 25, 50, 75, 100, 125, 150\}.
\]
Additional load levels are executed only if saturation is not observed at
$N = 150$.


Each level should take 180s 
(60s to increment 25 devices and stabilize + 120s of total devices reporting every second).

#### Warm-up and initialization.
When a new load level starts, the corresponding number of High-Load Test Device
Simulator instances is launched. During a 60-second warm-up period, each new
device:
\begin{itemize}
    \item requests a funding invoice,
    \item has the invoice automatically paid by the Auto-Pay Service,
    \item requests an authorization with a sufficiently large allowance so that
        it does not expire during the measurement window.
\end{itemize}
Usage reporting is started only after funding and authorization succeed.

#### Measurement window.
After warm-up, metrics are collected over a 120-second interval for the current
load level. During this period, each device publishes exactly one usage report
per second, and the edge node processes the resulting debits.

#### Recorded metrics.
For each load level, the following metrics are recorded:

##### Service-level metrics

- Usage reports recorded per second (consumption_events_total)
- Authorization debits per second (ledger_authorizations_total)
- Distribution end to end usage to debit latency (ledger_debit_latency_seconds_bucket)
- MQTT Messages received per second (device_mqtt_messages_received_total)
- MQTT Messages processed per second (device_mqtt_messages_processed_total)
- Redis Stream Consumer Lag (redis_stream_group_lag)
- Redis Stream Consumer Pending Messages (redis_stream_group_consumer_messages_pending)

##### System-level metrics
- Container memory usage (container_memory_working_set_bytes)
- Container CPU usage (container_cpu_usage_seconds_total)
- Disk read usage (container_fs_reads_bytes_total)
- Disk write usage (container_fs_writes_bytes_total) 
- Network receive throughput (container_network_receive_bytes_total)
- Network transimit throughput (container_network_transmit_bytes_total)

##### Load-generator metrics (k6)

- Number of VU's (Virtual Users / Devices) (k6_vus)

#### Saturation criteria and stopping condition

A load level is considered saturated if any of the following hold during the
measurement window:
\begin{itemize}
    \item CPU usage on the Raspberry Pi exceeds 95\% for more than 5 consecutive seconds,
    \item the Redis backlog gauge (redis_stream_group_lag) grows monotonically without recovery,
    \item the measured debit-processing rate falls below the incoming usage rate ($N$ debits per second),
    \item the latency distribution of authorization debits hits a threshold (e.g., 5s) for a significant fraction of requests.
\end{itemize}

Once saturation is observed at a given $N$, higher load levels are not executed.

## Running the k6 Load Test Generator

```bash
API_BASE_URL=http://192.168.0.170:8080 K6_PROMETHEUS_RW_SERVER_URL=http://localhost:9090/api/v1/write k6 run -o experimental-prometheus-rw  testing/loadtest/loadtest.js
```

## Collecting metrics

```json
{"from":"2026-01-08T18:25:40.655Z","to":"2026-01-08T18:46:33.820Z"}
```

```
python3 export_metrics.py --from "2026-01-08T18:25:40.655Z" --to "2026-01-08T18:46:33.820Z" --prometheus-url http://localhost:9090
```

```bash
python3 plot_exported_metrics.py
```


# Evaluation and Observations

