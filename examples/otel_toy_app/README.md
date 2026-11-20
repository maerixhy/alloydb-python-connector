# OpenTelemetry toy app

Opens connections to a real AlloyDB instance through the connector and shows
all six connector metrics: dial count, dial duration, open connections, bytes
sent, bytes received and refresh count. Names and attributes match the Go
connector's `METRICS.md`.

The app configures OpenTelemetry once, the way any application would, and
never calls a connector telemetry API. The metrics show up because the
connector records to the global meter provider.

## Setup

From the repo root:

```
pip install -e .
pip install -r examples/otel_toy_app/requirements.txt
gcloud auth application-default login
```

Your account needs `roles/alloydb.client` and
`roles/serviceusage.serviceUsageConsumer` on the instance's project. For
`--exporter otlp` it also needs `roles/monitoring.metricWriter`, and the
project needs the Telemetry API enabled
(`gcloud services enable telemetry.googleapis.com`).

Set the same variables the system tests use:

```
export ALLOYDB_INSTANCE_URI="projects/<PROJECT>/locations/<REGION>/clusters/<CLUSTER>/instances/<INSTANCE>"
export ALLOYDB_USER="<your db user>"
export ALLOYDB_PASS="..."
export ALLOYDB_DB="postgres"
export ALLOYDB_IP_TYPE="PUBLIC"   # PUBLIC from a laptop, PRIVATE from a VM in the VPC
```

## Run

All commands run from `examples/otel_toy_app/`.

**Console only.** Prints a table per metric every 5 seconds:

```
python toy_app.py --dials 10 --fail-every 4
```

**Cloud Monitoring and console together** (the demo setup). `--workers`
keeps several connections open at once, `--hold` keeps each one open for a
few seconds, and `--rows` makes each query return more data, so open
connections and bytes have something to show:

```
python toy_app.py --exporter otlp,console --dials 300 --interval 2 \
    --workers 5 --hold 8 --queries 5 --rows 1000 --fail-every 10
```

In Metrics Explorer, search for `alloydb.connector`. With OTLP the metrics
appear under `prometheus.googleapis.com/`, for example
`prometheus.googleapis.com/alloydb.connector.dial.count/counter`. Group
`dial.count` by `alloydb.status` or `alloydb.cache.hit`. Exports run every 10
seconds, so allow a minute for points to appear. Counters chart as a rate
(`/s`) by default.

**Prometheus**:

```
python toy_app.py --exporter prometheus --dials 30 --keep-running
curl -s localhost:9464/metrics | grep alloydb_connector
```

Prometheus turns dots into underscores and adds suffixes, for example
`alloydb_connector_dial_count_total`. These match the Go connector, so the Go
team's Grafana queries work for Python too.

## What each metric shows

| Metric | What drives it here |
| --- | --- |
| `dial.count` | every dial. The first is a cache miss, later ones are cache hits. `--fail-every` adds `user_error` dials |
| `dial.duration` | successful dials. The first one is slowest because it fetches certificates |
| `connections.open` | connections currently held. Raise `--workers` and `--hold` |
| `bytes.received` / `bytes.sent` | query traffic. Raise `--queries` and `--rows` |
| `refresh.count` | one refresh at startup, plus one after each injected failure |

`--fail-every N` makes every Nth dial ask for a Private Service Connect
address. The instance has none, so the dial fails with `user_error` and keeps
its instance labels, and the connector drops its cached connection info, so
the next dial is a cache miss with a fresh refresh. Use an instance without
PSC enabled.

Bytes and open connections are counted by the socket the connector hands to
the driver, which built-in telemetry installs. The app turns built-in
telemetry on. `--no-builtin` turns it off, and then only dial and refresh
metrics are recorded.

## Options

| Flag | Default | |
| --- | --- | --- |
| `--exporter` | `console` | comma separated: `console`, `otlp`, `gcp` (deprecated), `prometheus` |
| `--dials` | 10 | connections to open |
| `--interval` | 1.0 | seconds between dials |
| `--workers` | 1 | dials running at the same time |
| `--queries` | 1 | queries per connection |
| `--rows` | 1 | rows each query returns |
| `--hold` | 0 | seconds to keep each connection open |
| `--fail-every` | 0 | make every Nth dial fail (0 = never) |
| `--refresh-strategy` | `background` | `background` or `lazy` |
| `--no-builtin` | off | turn built-in telemetry off |
| `--export-interval` | 5 | seconds between exports (10 minimum for `otlp` and `gcp`) |
| `--project` | from credentials | project for `--exporter otlp` or `gcp` |
| `--region` | `us-central1` | the app's location for `--exporter otlp` |
| `--keep-running` | off | keep serving after the dials finish |
