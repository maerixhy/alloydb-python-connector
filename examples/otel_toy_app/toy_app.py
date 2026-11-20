# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Toy app: connector metrics from a real AlloyDB instance.

The app sets up OpenTelemetry the way any application would (steps 1 and 3
below). It never touches a connector telemetry API. The connector's metrics
show up because the connector records to the global meter provider.

See README.md for setup.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import inspect
import os
import sys
import threading
import time
from typing import Any

from console_table import ConsoleTableExporter
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricExporter
from opentelemetry.sdk.metrics.export import MetricReader
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from google.cloud.alloydbconnector import Connector

EXPORTERS = ("console", "otlp", "gcp", "prometheus")

# --fail-every asks for an IP type the instance does not have. The connector
# fetches the instance's connection info, finds no PSC address and fails
# with user_error. The failed dial keeps its instance labels, and since the
# connector drops its cached connection info, the next dial is a cache miss
# with a fresh refresh. Use an instance without Private Service Connect.
FAIL_IP_TYPE = "PSC"


def google_project(args: argparse.Namespace) -> str:
    """The project to send metrics to: --project, else the one from
    Application Default Credentials."""
    if args.project:
        return args.project
    import google.auth

    _, project = google.auth.default()
    if not project:
        sys.exit("No project found. Pass --project <PROJECT_ID>.")
    return project


def otlp_to_google_cloud() -> MetricExporter:
    """Standard OTLP exporter sending to Google Cloud's Telemetry API.

    This is the supported replacement for CloudMonitoringMetricsExporter. See
    https://github.com/GoogleCloudPlatform/opentelemetry-operations-python/blob/main/MIGRATION.md
    """
    import grpc
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
        OTLPMetricExporter,
    )

    import google.auth
    from google.auth.transport.grpc import AuthMetadataPlugin
    import google.auth.transport.requests

    credentials, _ = google.auth.default()
    auth = AuthMetadataPlugin(
        credentials=credentials, request=google.auth.transport.requests.Request()
    )
    channel_credentials = grpc.composite_channel_credentials(
        grpc.ssl_channel_credentials(), grpc.metadata_call_credentials(auth)
    )
    return OTLPMetricExporter(
        endpoint="telemetry.googleapis.com", credentials=channel_credentials
    )


def build_reader(name: str, args: argparse.Namespace) -> MetricReader:
    # Cloud Monitoring accepts at most one point per series every 5s.
    cloud_interval_ms = max(args.export_interval, 10) * 1000
    if name == "console":
        return PeriodicExportingMetricReader(
            ConsoleTableExporter(), export_interval_millis=args.export_interval * 1000
        )
    if name == "otlp":
        return PeriodicExportingMetricReader(
            otlp_to_google_cloud(), export_interval_millis=cloud_interval_ms
        )
    if name == "gcp":
        from opentelemetry.exporter.cloud_monitoring import (
            CloudMonitoringMetricsExporter,
        )

        return PeriodicExportingMetricReader(
            CloudMonitoringMetricsExporter(project_id=args.project),
            export_interval_millis=cloud_interval_ms,
        )
    if name == "prometheus":
        from opentelemetry.exporter.prometheus import PrometheusMetricReader
        from prometheus_client import start_http_server

        start_http_server(args.prometheus_port)
        print(f"Prometheus metrics at http://localhost:{args.prometheus_port}/metrics")
        return PrometheusMetricReader()
    raise ValueError(f"unknown exporter {name}")


def parse_exporters(value: str) -> list[str]:
    names = [n.strip() for n in value.split(",") if n.strip()]
    unknown = [n for n in names if n not in EXPORTERS]
    if not names or unknown:
        raise argparse.ArgumentTypeError(
            f"choose from {', '.join(EXPORTERS)}, comma separated"
        )
    return names


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(f"Set {name} (see README.md)")
    return value


def new_connector(ip_type: str, args: argparse.Namespace) -> Connector:
    """Create the Connector with built-in telemetry on.

    Built-in telemetry is what hands the driver an instrumented socket, which
    is how bytes and open connections get counted on both paths. Releases
    with the public option take it directly. Before that option exists, the
    private flag is set the same way the connector's own tests do.
    """
    kwargs: dict[str, Any] = {
        "ip_type": ip_type,
        "refresh_strategy": args.refresh_strategy,
    }
    supports_option = (
        "enable_builtin_telemetry" in inspect.signature(Connector).parameters
    )
    if supports_option:
        kwargs["enable_builtin_telemetry"] = not args.no_builtin
    connector = Connector(**kwargs)
    if not supports_option and not args.no_builtin:
        connector._enable_builtin_telemetry = True
    return connector


class Stats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.ok = 0
        self.failed = 0

    def add(self, ok: bool) -> None:
        with self._lock:
            if ok:
                self.ok += 1
            else:
                self.failed += 1


def one_dial(
    connector: Connector,
    n: int,
    args: argparse.Namespace,
    instance_uri: str,
    creds: dict[str, str],
    stats: Stats,
) -> None:
    """Open a connection, run queries on it, hold it, then close it."""
    kwargs: dict[str, Any] = dict(creds)
    if args.fail_every and n % args.fail_every == 0:
        kwargs["ip_type"] = FAIL_IP_TYPE
    try:
        conn = connector.connect(instance_uri, "pg8000", **kwargs)
        try:
            cursor = conn.cursor()
            for _ in range(args.queries):
                cursor.execute("SELECT generate_series(1, %s)", (args.rows,))
                cursor.fetchall()
            time.sleep(args.hold)
        finally:
            conn.close()
        stats.add(True)
        print(f"dial {n}/{args.dials}: ok")
    except Exception as e:
        stats.add(False)
        reason = str(e).splitlines()[0][:80] if str(e) else ""
        print(f"dial {n}/{args.dials}: failed ({type(e).__name__}: {reason})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--exporter",
        type=parse_exporters,
        default=["console"],
        help="comma separated: console, otlp (Google Cloud via OTLP), "
        "gcp (deprecated Cloud Monitoring exporter), prometheus. "
        "Example: --exporter otlp,console",
    )
    parser.add_argument("--dials", type=int, default=10, help="connections to open")
    parser.add_argument(
        "--interval", type=float, default=1.0, help="seconds between dials"
    )
    parser.add_argument(
        "--workers", type=int, default=1, help="dials running at the same time"
    )
    parser.add_argument("--queries", type=int, default=1, help="queries per connection")
    parser.add_argument(
        "--rows", type=int, default=1, help="rows each query returns (more bytes)"
    )
    parser.add_argument(
        "--hold", type=float, default=0.0, help="seconds to keep each connection open"
    )
    parser.add_argument(
        "--fail-every",
        type=int,
        default=0,
        help="make every Nth dial fail on purpose (0 = never)",
    )
    parser.add_argument(
        "--refresh-strategy", choices=["background", "lazy"], default="background"
    )
    parser.add_argument(
        "--no-builtin",
        action="store_true",
        help="turn built-in telemetry off (no bytes or open connections)",
    )
    parser.add_argument(
        "--export-interval", type=int, default=5, help="seconds between exports"
    )
    parser.add_argument(
        "--project", help="GCP project for --exporter otlp or gcp (default: ADC)"
    )
    parser.add_argument(
        "--region",
        default="us-central1",
        help="GCP region recorded as the app's location for --exporter otlp",
    )
    parser.add_argument("--prometheus-port", type=int, default=9464)
    parser.add_argument(
        "--keep-running",
        action="store_true",
        help="keep serving after the dials finish (useful for Prometheus)",
    )
    args = parser.parse_args()

    instance_uri = require_env("ALLOYDB_INSTANCE_URI")
    creds = {
        "user": require_env("ALLOYDB_USER"),
        "password": require_env("ALLOYDB_PASS"),
        "db": require_env("ALLOYDB_DB"),
    }
    ip_type = os.environ.get("ALLOYDB_IP_TYPE", "PUBLIC")

    # 1. Application OTel setup. This is the only telemetry code in the app.
    resource_attrs = {"service.name": "alloydb-otel-toy-app"}
    if "otlp" in args.exporter:
        # The Telemetry API reads the destination project and the location
        # from the resource. On GCP the resource detector fills in the
        # region, but on a laptop nothing does and every point is rejected.
        resource_attrs["gcp.project_id"] = google_project(args)
        resource_attrs["cloud.region"] = args.region
    provider = MeterProvider(
        metric_readers=[build_reader(name, args) for name in args.exporter],
        resource=Resource.create(resource_attrs),
    )
    metrics.set_meter_provider(provider)

    # 2. Use the connector as usual.
    stats = Stats()
    with new_connector(ip_type, args) as connector:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for n in range(1, args.dials + 1):
                pool.submit(one_dial, connector, n, args, instance_uri, creds, stats)
                time.sleep(args.interval)

        print(f"\n{stats.ok} succeeded, {stats.failed} failed")
        if args.keep_running:
            print("Still running so exporters can be scraped. Ctrl-C to stop.")
            try:
                while True:
                    time.sleep(60)
            except KeyboardInterrupt:
                pass

    # 3. The app, not the connector, flushes and shuts down OTel.
    provider.shutdown()


if __name__ == "__main__":
    main()
