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

"""Tests for connector metrics on the global OpenTelemetry provider."""

import subprocess
import sys
import textwrap
from typing import Any
from typing import Iterator
from unittest.mock import patch

from mocks import FakeAlloyDBClient
from mocks import FakeCredentials
from mocks import FakeInstance
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
import pytest

from google.cloud.alloydbconnector import Connector
from google.cloud.alloydbconnector import metrics as connector_metrics
from google.cloud.alloydbconnector.metrics import FanOutMetricRecorder
from google.cloud.alloydbconnector.metrics import GlobalMetricRecorder
from google.cloud.alloydbconnector.metrics import new_global_metric_recorder
from google.cloud.alloydbconnector.telemetry import NullMetricRecorder
from google.cloud.alloydbconnector.telemetry import TelemetryAttributes

INSTANCE_URI = (
    "projects/test-project/locations/test-region/"
    "clusters/test-cluster/instances/test-instance"
)
TARGET = {
    "alloydb.project_id": "test-project",
    "alloydb.location": "test-region",
    "alloydb.cluster_id": "test-cluster",
    "alloydb.instance_id": "test-instance",
    "alloydb.connector.type": "python",
}


@pytest.fixture
def reader() -> Iterator[InMemoryMetricReader]:
    """An in-memory SDK reader standing in for the application's provider.

    The global provider can only be set once per process, so the meter the
    module asks for is routed to a per-test provider instead, and the
    process-wide instruments are rebuilt around it.
    """
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    connector_metrics._reset_instruments()
    with patch.object(metrics, "get_meter", side_effect=provider.get_meter):
        yield reader
    connector_metrics._reset_instruments()


def collect(reader: InMemoryMetricReader) -> dict[str, Any]:
    data = reader.get_metrics_data()
    out: dict[str, Any] = {}
    if data is None:
        return out
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                out[m.name] = (sm.scope.name, m)
    return out


def points(reader: InMemoryMetricReader, name: str) -> list[Any]:
    found = collect(reader).get(name)
    return list(found[1].data.data_points) if found else []


def recorder() -> GlobalMetricRecorder:
    return GlobalMetricRecorder(
        "test-project", "test-region", "test-cluster", "test-instance"
    )


def test_contract_matches_go(reader: InMemoryMetricReader) -> None:
    """Names, units and scope from the Go connector's METRICS.md."""
    r = recorder()
    attrs = TelemetryAttributes(dial_status="success", refresh_status="success")
    r.record_dial_count(attrs)
    r.record_dial_latency(1.0)
    r.record_open_connection(attrs)
    r.record_bytes_rx(1)
    r.record_bytes_tx(1)
    r.record_refresh_count(attrs)

    found = collect(reader)
    assert {scope for scope, _ in found.values()} == {"google.cloud.alloydbconnector"}
    assert {name: m.unit for name, (_, m) in found.items()} == {
        "alloydb.connector.dial.count": "{dial}",
        "alloydb.connector.dial.duration": "ms",
        "alloydb.connector.connections.open": "{connection}",
        "alloydb.connector.bytes.sent": "By",
        "alloydb.connector.bytes.received": "By",
        "alloydb.connector.refresh.count": "{refresh}",
    }


def test_attributes_match_go(reader: InMemoryMetricReader) -> None:
    r = recorder()
    attrs = TelemetryAttributes(
        iam_authn=True,
        cache_hit=True,
        dial_status="tcp_error",
        refresh_status="failure",
        refresh_type="lazy",
    )
    r.record_dial_count(attrs)
    r.record_dial_latency(12.5)
    r.record_open_connection(attrs)
    r.record_refresh_count(attrs)
    r.record_bytes_rx(7)

    (dial,) = points(reader, "alloydb.connector.dial.count")
    assert dict(dial.attributes) == {
        **TARGET,
        "alloydb.auth.type": "iam",
        "alloydb.cache.hit": True,
        "alloydb.status": "tcp_error",
    }
    (latency,) = points(reader, "alloydb.connector.dial.duration")
    assert dict(latency.attributes) == TARGET
    assert latency.sum == 12.5
    (open_conns,) = points(reader, "alloydb.connector.connections.open")
    assert dict(open_conns.attributes) == {**TARGET, "alloydb.auth.type": "iam"}
    (refresh,) = points(reader, "alloydb.connector.refresh.count")
    assert dict(refresh.attributes) == {
        **TARGET,
        "alloydb.status": "failure",
        "alloydb.refresh.type": "lazy",
    }
    (rx,) = points(reader, "alloydb.connector.bytes.received")
    assert dict(rx.attributes) == TARGET


def test_no_client_uid_exported(reader: InMemoryMetricReader) -> None:
    recorder().record_dial_count(TelemetryAttributes(dial_status="success"))
    (dial,) = points(reader, "alloydb.connector.dial.count")
    assert not any("client_uid" in key for key in dial.attributes)


def test_open_connections_goes_up_and_down(reader: InMemoryMetricReader) -> None:
    r = recorder()
    attrs = TelemetryAttributes()
    r.record_open_connection(attrs)
    r.record_open_connection(attrs)
    r.record_closed_connection(attrs)
    (point,) = points(reader, "alloydb.connector.connections.open")
    assert point.value == 1


def test_byte_totals_are_shared_per_instance(reader: InMemoryMetricReader) -> None:
    """Two recorders for one instance (e.g. two Connectors) add to a single
    total, and the total survives a recorder being dropped."""
    first, second = recorder(), recorder()
    first.record_bytes_rx(100)
    second.record_bytes_rx(50)
    first.record_bytes_tx(10)
    del first
    (rx,) = points(reader, "alloydb.connector.bytes.received")
    (tx,) = points(reader, "alloydb.connector.bytes.sent")
    assert rx.value == 150
    assert tx.value == 10


def test_instances_are_distinguished(reader: InMemoryMetricReader) -> None:
    recorder().record_bytes_rx(1)
    other = GlobalMetricRecorder("test-project", "test-region", "test-cluster", "other")
    other.record_bytes_rx(5)
    by_instance = {
        p.attributes["alloydb.instance_id"]: p.value
        for p in points(reader, "alloydb.connector.bytes.received")
    }
    assert by_instance == {"test-instance": 1, "other": 5}


class _Spy(NullMetricRecorder):
    def __init__(self) -> None:
        self.calls: list[str] = []

    def record_dial_count(self, attrs: TelemetryAttributes) -> None:
        self.calls.append("dial_count")

    def record_bytes_rx(self, count: int) -> None:
        self.calls.append("bytes_rx")


class _Broken(NullMetricRecorder):
    def record_dial_count(self, attrs: TelemetryAttributes) -> None:
        raise RuntimeError("boom")

    def record_bytes_rx(self, count: int) -> None:
        raise RuntimeError("boom")


def test_fan_out_delivers_to_every_recorder() -> None:
    a, b = _Spy(), _Spy()
    fan_out = FanOutMetricRecorder([a, b])
    fan_out.record_dial_count(TelemetryAttributes())
    fan_out.record_bytes_rx(1)
    assert a.calls == b.calls == ["dial_count", "bytes_rx"]


def test_fan_out_isolates_a_failing_recorder() -> None:
    spy = _Spy()
    fan_out = FanOutMetricRecorder([_Broken(), spy])
    fan_out.record_dial_count(TelemetryAttributes())
    fan_out.record_bytes_rx(1)
    assert spy.calls == ["dial_count", "bytes_rx"]


def test_new_global_metric_recorder_never_raises() -> None:
    connector_metrics._reset_instruments()
    with patch.object(metrics, "get_meter", side_effect=RuntimeError("boom")):
        assert new_global_metric_recorder("p", "l", "c", "i") is None
    connector_metrics._reset_instruments()


def test_provider_set_after_connector_is_created() -> None:
    """A recorder created before the application configures OTel still
    exports, including the observable byte counters. Runs in a subprocess
    because the global provider can only be set once per process."""
    code = textwrap.dedent(
        """
        from opentelemetry import metrics
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader
        from google.cloud.alloydbconnector.metrics import GlobalMetricRecorder
        from google.cloud.alloydbconnector.telemetry import TelemetryAttributes

        r = GlobalMetricRecorder("p", "l", "c", "i")
        reader = InMemoryMetricReader()
        metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
        r.record_dial_count(TelemetryAttributes(dial_status="success"))
        r.record_bytes_rx(42)
        names = {m.name: m for rm in reader.get_metrics_data().resource_metrics
                 for sm in rm.scope_metrics for m in sm.metrics}
        assert "alloydb.connector.dial.count" in names, names
        rx = names["alloydb.connector.bytes.received"].data.data_points[0]
        assert rx.value == 42, rx
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_no_provider_is_a_noop() -> None:
    """Without an application-configured provider, recording is dropped."""
    code = textwrap.dedent(
        """
        from google.cloud.alloydbconnector.metrics import GlobalMetricRecorder
        from google.cloud.alloydbconnector.telemetry import TelemetryAttributes

        r = GlobalMetricRecorder("p", "l", "c", "i")
        a = TelemetryAttributes(dial_status="success")
        r.record_dial_count(a)
        r.record_dial_latency(1.0)
        r.record_open_connection(a)
        r.record_closed_connection(a)
        r.record_bytes_rx(1)
        r.record_bytes_tx(1)
        r.record_refresh_count(a)
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


# ---------- through the Connector ----------


def _connect(connector: Connector, driver_connect: Any) -> None:
    with patch("google.cloud.alloydbconnector.pg8000.connect", driver_connect):
        connector.connect(INSTANCE_URI, "pg8000", user="u", password="p", db="d")


@pytest.mark.usefixtures("proxy_server")
def test_connector_reports_to_global_provider(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    """With built-in telemetry off, the default today, dial and refresh
    metrics still reach the application's provider."""
    with Connector(credentials) as connector:
        connector._client = fake_client
        _connect(connector, lambda sock, **kw: True)
        _connect(connector, lambda sock, **kw: True)

    dials = {
        p.attributes["alloydb.cache.hit"]: p.value
        for p in points(reader, "alloydb.connector.dial.count")
    }
    assert dials == {False: 1, True: 1}
    (latency,) = points(reader, "alloydb.connector.dial.duration")
    assert latency.count == 2
    (refresh,) = points(reader, "alloydb.connector.refresh.count")
    assert refresh.attributes["alloydb.status"] == "success"
    assert refresh.attributes["alloydb.refresh.type"] == "refresh_ahead"


@pytest.mark.usefixtures("proxy_server")
def test_connector_reports_bytes_and_open_connections(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    """When the driver gets an InstrumentedSocket (built-in telemetry on),
    bytes and open connections reach the global path too. The fake server
    sends the instance name once the metadata exchange is done."""

    def driver(sock: Any, **kwargs: Any) -> Any:
        assert sock.recv(1024) == b"test-instance"
        return sock  # stands in for the driver's connection

    with Connector(credentials) as connector:
        connector._enable_builtin_telemetry = True
        connector._client = fake_client
        with patch("google.cloud.alloydbconnector.pg8000.connect", driver):
            conn = connector.connect(INSTANCE_URI, "pg8000", user="u", db="d")

        (rx,) = points(reader, "alloydb.connector.bytes.received")
        assert rx.value == len(b"test-instance")
        (open_conns,) = points(reader, "alloydb.connector.connections.open")
        assert open_conns.value == 1

        conn.close()
        (open_conns,) = points(reader, "alloydb.connector.connections.open")
        assert open_conns.value == 0


@pytest.mark.usefixtures("proxy_server")
def test_connector_close_does_not_touch_app_provider(
    credentials: FakeCredentials, fake_client: FakeAlloyDBClient
) -> None:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    connector_metrics._reset_instruments()
    with (
        patch.object(metrics, "get_meter", side_effect=provider.get_meter),
        patch.object(provider, "shutdown") as shutdown,
        patch.object(provider, "force_flush") as flush,
    ):
        with Connector(credentials) as connector:
            connector._client = fake_client
            _connect(connector, lambda sock, **kw: True)
        shutdown.assert_not_called()
        flush.assert_not_called()
    connector_metrics._reset_instruments()
    assert points(reader, "alloydb.connector.dial.count")


@pytest.mark.usefixtures("proxy_server")
def test_connector_failure_status_reaches_global_provider(
    credentials: FakeCredentials, reader: InMemoryMetricReader
) -> None:
    from google.cloud.alloydbconnector.exceptions import IPTypeNotFoundError

    # its own instance: the shared fake_instance fixture is session scoped
    client = FakeAlloyDBClient(FakeInstance(ip_addrs={"PRIVATE": "127.0.0.1"}))
    with Connector(credentials) as connector:
        connector._client = client
        with pytest.raises(IPTypeNotFoundError):
            connector.connect(INSTANCE_URI, "pg8000", ip_type="psc")
    (dial,) = points(reader, "alloydb.connector.dial.count")
    assert dial.attributes["alloydb.status"] == "user_error"
    assert dial.attributes["alloydb.instance_id"] == "test-instance"


async def test_async_connector_reports_to_global_provider(
    credentials: FakeCredentials, reader: InMemoryMetricReader
) -> None:
    """AsyncConnector feeds the global path through the same recorder:
    dials, latency and open connections. asyncpg has no hook for bytes."""
    from mocks import FakeAsyncConnection

    from google.cloud.alloydbconnector import AsyncConnector

    with patch("google.cloud.alloydbconnector.asyncpg.connect") as connect:
        fake_connection = FakeAsyncConnection()
        connect.return_value = fake_connection
        async with AsyncConnector(credentials) as connector:
            connector._client = FakeAlloyDBClient()
            await connector.connect(INSTANCE_URI, "asyncpg", user="u", db="d")

            (dial,) = points(reader, "alloydb.connector.dial.count")
            assert dial.attributes["alloydb.status"] == "success"
            (latency,) = points(reader, "alloydb.connector.dial.duration")
            assert latency.count == 1
            (open_conns,) = points(reader, "alloydb.connector.connections.open")
            assert open_conns.value == 1

            await fake_connection.close()
            (open_conns,) = points(reader, "alloydb.connector.connections.open")
            assert open_conns.value == 0
