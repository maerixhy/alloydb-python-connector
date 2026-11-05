# OpenTelemetry toy app

Opens connections to a real AlloyDB instance through the connector and shows
the connector's metrics: `alloydb.connector.dial.count`,
`alloydb.connector.dial.duration` and `alloydb.connector.refresh.count`. Names
and attributes match the Go connector's `METRICS.md`.

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
`--exporter otlp` or `gcp` it also needs `roles/monitoring.metricWriter`, and
the project needs the Telemetry API enabled for `otlp`
(`gcloud services enable telemetry.googleapis.com`).

Set the same variables the system tests use:

```
export ALLOYDB_INSTANCE_URI="projects/<PROJECT>/locations/<REGION>/clusters/<CLUSTER>/instances/<INSTANCE>"
export ALLOYDB_USER="postgres"
export ALLOYDB_PASS="..."
export ALLOYDB_DB="postgres"
export ALLOYDB_IP_TYPE="PUBLIC"   # PUBLIC from a laptop, PRIVATE from a VM in the VPC
```

## Run

All commands run from `examples/otel_toy_app/`.

**Console** (default). Prints a table per metric every 5 seconds and once more
at exit:

```
python toy_app.py --dials 10 --fail-every 4
```

```
alloydb.connector.dial.count
  instance_id  auth.type  cache.hit  refresh.type   status       value
  -            built_in   false      refresh_ahead  cache_error      2
  my-inst      built_in   false      refresh_ahead  success          1
  my-inst      built_in   true       refresh_ahead  success          7
```

The first dial to an instance is a cache miss, which also triggers a refresh.
Later dials are cache hits. `--fail-every N` makes every Nth dial use an
invalid instance URI, so you can see failed dials (`cache_error`) next to the
successful ones.

**Cloud Monitoring (recommended: OTLP)**. Uses the standard OpenTelemetry OTLP
exporter, sending to Google Cloud's Telemetry API (`telemetry.googleapis.com`).
This is Google's supported path
([migration guide](https://github.com/GoogleCloudPlatform/opentelemetry-operations-python/blob/main/MIGRATION.md)):

```
python toy_app.py --exporter otlp --dials 300 --interval 2 --fail-every 10
```

In Metrics Explorer, search for `dial.count`. With OTLP the full name is
`prometheus.googleapis.com/alloydb.connector.dial.count/counter`. Set the
aggregation to Sum by `alloydb.status` or `alloydb.cache.hit`. Exports run every 10 seconds, so allow a minute
for points to appear. Counters chart as a rate (`/s`) by default. Set the
alignment function to delta to see counts.

**Cloud Monitoring (legacy exporter)**. `--exporter gcp` uses
`CloudMonitoringMetricsExporter`, which Google has deprecated. Metrics appear
as `workload.googleapis.com/alloydb.connector.dial.count`.

**Prometheus**:

```
python toy_app.py --exporter prometheus --dials 30 --keep-running
curl -s localhost:9464/metrics | grep alloydb_connector
```

Prometheus turns dots into underscores and adds suffixes:
`alloydb_connector_dial_count_total`, `alloydb_connector_refresh_count_total`
and `alloydb_connector_dial_duration_milliseconds_bucket`. These match the Go
connector, so the Go team's Grafana queries work for Python too.

## Options

| Flag | Default | |
| --- | --- | --- |
| `--exporter` | `console` | `console`, `otlp`, `gcp` (deprecated) or `prometheus` |
| `--dials` | 10 | connections to open |
| `--interval` | 1.0 | seconds between dials |
| `--fail-every` | 0 | make every Nth dial fail (0 = never) |
| `--refresh-strategy` | `background` | `background` or `lazy` (sets `refresh_type`) |
| `--export-interval` | 5 | seconds between exports (10 minimum for `otlp` and `gcp`) |
| `--project` | from credentials | project for `--exporter otlp` or `gcp` |
| `--region` | `us-central1` | the app's location for `--exporter otlp` (required by the Telemetry API) |
| `--keep-running` | off | keep serving after the dials finish |