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

"""Tests that the connector records dial and refresh metrics, end to end
through OpenTelemetryMetricRecorder into an in-memory SDK reader."""

import asyncio
import socket
import ssl
import subprocess
import sys
import textwrap
import threading
from typing import Any
from typing import Iterator
from unittest.mock import patch

from mock import AsyncMock
from mocks import FakeAlloyDBClient
from mocks import FakeCredentials
from mocks import FakeInstance
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
import pytest

from google.cloud.alloydbconnector import AsyncConnector
from google.cloud.alloydbconnector import Connector
from google.cloud.alloydbconnector.async_connector import _classify_driver_error
from google.cloud.alloydbconnector.exceptions import IPTypeNotFoundError
from google.cloud.alloydbconnector.metric_events import DialTracker
from google.cloud.alloydbconnector.metric_events import build_metric_recorder
from google.cloud.alloydbconnector.metric_events import instance_attributes_from_uri
from google.cloud.alloydbconnector.metrics import FanOutMetricRecorder
from google.cloud.alloydbconnector.metrics import NullMetricRecorder
from google.cloud.alloydbconnector.metrics import OpenTelemetryMetricRecorder

INSTANCE_URI = (
    "projects/test-project/locations/test-region/"
    "clusters/test-cluster/instances/test-instance"
)
INSTANCE_ATTRS = {
    "project": "test-project",
    "location": "test-region",
    "cluster": "test-cluster",
    "instance": "test-instance",
}
CONNECT_KWARGS = {"user": "u", "password": "p", "db": "d"}


# ---------- helpers ----------


@pytest.fixture
def reader() -> Iterator[InMemoryMetricReader]:
    """An in-memory SDK reader behind the meter the recorder asks for.

    OpenTelemetryMetricRecorder calls opentelemetry.metrics.get_meter, which
    normally returns a meter from the global provider. Routing that call to a
    per-test provider keeps tests independent, since the real global
    provider can only be set once per process.
    """
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    with patch.object(metrics, "get_meter", side_effect=provider.get_meter):
        yield reader


def points(reader: InMemoryMetricReader, name: str) -> list[Any]:
    data = reader.get_metrics_data()
    if data is None:
        return []
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                if m.name == name:
                    return list(m.data.data_points)
    return []


def counts_by_status(reader: InMemoryMetricReader) -> dict[str, int]:
    out: dict[str, int] = {}
    for p in points(reader, "dial_count"):
        status = p.attributes["status"]
        out[status] = out.get(status, 0) + p.value
    return out


def _connector(
    credentials: FakeCredentials, client: FakeAlloyDBClient, **kwargs: Any
) -> Connector:
    connector = Connector(credentials, **kwargs)
    connector._client = client
    return connector


# ---------- attributes and recorder construction ----------


def test_instance_attributes_from_uri() -> None:
    assert instance_attributes_from_uri(INSTANCE_URI) == INSTANCE_ATTRS


def test_instance_attributes_from_domain_scoped_project() -> None:
    attrs = instance_attributes_from_uri(
        "projects/example.com:proj/locations/r/clusters/c/instances/i"
    )
    assert attrs["project"] == "example.com:proj"
    assert attrs["instance"] == "i"


def test_instance_attributes_from_invalid_uri() -> None:
    assert instance_attributes_from_uri("not/a/uri") == {
        "project": "",
        "location": "",
        "cluster": "",
        "instance": "",
    }


def test_build_recorder_default_fans_out() -> None:
    recorder = build_metric_recorder()
    assert isinstance(recorder, FanOutMetricRecorder)
    assert isinstance(recorder._recorders[0], OpenTelemetryMetricRecorder)


def test_build_recorder_opt_out() -> None:
    assert isinstance(build_metric_recorder(False), NullMetricRecorder)


def test_build_recorder_never_raises() -> None:
    with patch.object(metrics, "get_meter", side_effect=RuntimeError("boom")):
        assert isinstance(build_metric_recorder(), NullMetricRecorder)


class _Broken:
    def __getattr__(self, name: str) -> Any:
        def fail(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("boom")

        return fail


def test_dial_tracker_never_raises() -> None:
    dial = DialTracker(_Broken(), INSTANCE_URI, False, "lazy")  # type: ignore
    dial.mark_success()
    dial.finish()


# ---------- global provider ----------


def test_connector_created_before_app_sets_global_provider() -> None:
    """A connector created before the app configures OTel still exports,
    via OpenTelemetry's proxy meter. Runs in a subprocess because the
    global provider can only be set once per process."""
    code = textwrap.dedent(
        """
        from opentelemetry import metrics
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader
        from google.cloud.alloydbconnector import metric_events

        dial = metric_events.DialTracker(
            metric_events.build_metric_recorder(), "bad/uri", False, "lazy")
        reader = InMemoryMetricReader()
        metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
        dial.finish()
        names = [m.name for rm in reader.get_metrics_data().resource_metrics
                 for sm in rm.scope_metrics for m in sm.metrics]
        assert names == ["dial_count"], names
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


# ---------- sync connector ----------


@pytest.mark.usefixtures("proxy_server")
def test_connect_success_records_dial_latency_and_refresh(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    with _connector(credentials, fake_client) as connector:
        with patch("google.cloud.alloydbconnector.pg8000.connect", return_value=True):
            connector.connect(INSTANCE_URI, "pg8000", **CONNECT_KWARGS)
            connector.connect(INSTANCE_URI, "pg8000", **CONNECT_KWARGS)

    dials = points(reader, "dial_count")
    by_hit = {p.attributes["is_cache_hit"]: p for p in dials}
    assert by_hit["false"].value == 1 and by_hit["true"].value == 1
    assert dict(by_hit["false"].attributes) == {
        **INSTANCE_ATTRS,
        "connector_type": "python",
        "auth_type": "built_in",
        "is_cache_hit": "false",
        "status": "success",
        "refresh_type": "refresh_ahead",
    }

    latency = points(reader, "dial_latencies")[0]
    assert latency.count == 2 and latency.sum > 0
    assert dict(latency.attributes) == {**INSTANCE_ATTRS, "connector_type": "python"}

    refresh = points(reader, "refresh_count")[0]
    assert dict(refresh.attributes) == {
        **INSTANCE_ATTRS,
        "connector_type": "python",
        "status": "success",
        "refresh_type": "refresh_ahead",
    }


@pytest.mark.usefixtures("proxy_server")
def test_lazy_refresh_type(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    with _connector(credentials, fake_client, refresh_strategy="lazy") as connector:
        with patch("google.cloud.alloydbconnector.pg8000.connect", return_value=True):
            connector.connect(INSTANCE_URI, "pg8000", **CONNECT_KWARGS)
    assert points(reader, "dial_count")[0].attributes["refresh_type"] == "lazy"
    refresh = points(reader, "refresh_count")[0]
    assert refresh.attributes["refresh_type"] == "lazy"
    assert refresh.attributes["status"] == "success"


@pytest.mark.usefixtures("proxy_server")
def test_multiple_instances_are_distinguished(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    other = INSTANCE_URI.replace("test-instance", "other-instance")
    with _connector(credentials, fake_client) as connector:
        with patch("google.cloud.alloydbconnector.pg8000.connect", return_value=True):
            connector.connect(INSTANCE_URI, "pg8000", **CONNECT_KWARGS)
            connector.connect(other, "pg8000", **CONNECT_KWARGS)
            connector.connect(other, "pg8000", **CONNECT_KWARGS)

    totals: dict[str, int] = {}
    for p in points(reader, "dial_count"):
        name = p.attributes["instance"]
        totals[name] = totals.get(name, 0) + p.value
    assert totals == {"test-instance": 1, "other-instance": 2}


def test_no_client_id_is_exported(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    with _connector(credentials, fake_client) as connector:
        with pytest.raises(ValueError):
            connector.connect(INSTANCE_URI, "bogus-driver")
    attrs = points(reader, "dial_count")[0].attributes
    assert "client_uid" not in attrs


def test_unsupported_driver_is_user_error(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    with _connector(credentials, fake_client) as connector:
        with pytest.raises(ValueError):
            connector.connect(INSTANCE_URI, "bogus-driver")
    assert counts_by_status(reader) == {"user_error": 1}
    assert points(reader, "dial_latencies") == []


def test_missing_ip_type_is_user_error(
    credentials: FakeCredentials, reader: InMemoryMetricReader
) -> None:
    client = FakeAlloyDBClient(FakeInstance(ip_addrs={"PRIVATE": "127.0.0.1"}))
    with _connector(credentials, client) as connector:
        with pytest.raises(IPTypeNotFoundError):
            connector.connect(INSTANCE_URI, "pg8000", ip_type="public")
    assert counts_by_status(reader) == {"user_error": 1}


def test_refresh_failure_is_cache_error(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    fake_client.get_connection_info = AsyncMock(side_effect=RuntimeError("api"))
    with _connector(credentials, fake_client, refresh_strategy="lazy") as connector:
        with pytest.raises(RuntimeError):
            connector.connect(INSTANCE_URI, "pg8000")
    assert counts_by_status(reader) == {"cache_error": 1}
    assert points(reader, "refresh_count")[0].attributes["status"] == "failure"


def test_invalid_instance_uri_is_cache_error(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    with _connector(credentials, fake_client) as connector:
        with pytest.raises(ValueError):
            connector.connect("bad/uri", "pg8000")
    assert counts_by_status(reader) == {"cache_error": 1}


@pytest.mark.usefixtures("proxy_server")
def test_tcp_failure_is_tcp_error(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    with _connector(credentials, fake_client) as connector:
        with patch(
            "google.cloud.alloydbconnector.connector.socket.create_connection",
            side_effect=ConnectionRefusedError(),
        ):
            with pytest.raises(ConnectionRefusedError):
                connector.connect(INSTANCE_URI, "pg8000")
    assert counts_by_status(reader) == {"tcp_error": 1}


def test_tls_failure_is_tls_error(credentials: FakeCredentials) -> None:
    """A server that does not speak TLS fails the handshake. Uses its own
    socket so the shared proxy server fixture is not disturbed."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]

    def serve() -> None:
        conn, _ = server.accept()
        conn.sendall(b"this is not a TLS server\r\n" * 4)
        conn.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    tracker = DialTracker(NullMetricRecorder(), INSTANCE_URI, False, "lazy")
    with Connector(credentials) as connector:
        with patch("google.cloud.alloydbconnector.connector.SERVER_PROXY_PORT", port):
            with pytest.raises(ssl.SSLError):
                connector.metadata_exchange(
                    INSTANCE_URI,
                    "127.0.0.1",
                    ssl.create_default_context(),
                    False,
                    dial=tracker,
                )
    thread.join(2)
    server.close()
    assert tracker.status == "tls_error"


@pytest.mark.usefixtures("proxy_server")
def test_metadata_exchange_failure_is_mdx_error_and_closes_socket(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    with _connector(credentials, fake_client) as connector:
        real_exchange = connector._metadata_exchange
        exchanged: list[ssl.SSLSocket] = []

        def failing_exchange(*args: Any) -> Any:
            # complete the real exchange so the shared proxy server keeps
            # serving, then fail as a bad server response would
            exchanged.append(real_exchange(*args))
            raise ValueError("Metadata Exchange request has failed")

        with patch.object(connector, "_metadata_exchange", failing_exchange):
            with pytest.raises(ValueError):
                connector.connect(INSTANCE_URI, "pg8000")
    assert counts_by_status(reader) == {"mdx_error": 1}
    # the connector still closes the socket on a failed exchange (#568)
    assert exchanged[0].fileno() == -1


def test_iam_auth_type(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    with _connector(credentials, fake_client) as connector:
        with pytest.raises(ValueError):
            connector.connect(INSTANCE_URI, "bogus", enable_iam_auth=True)
    assert points(reader, "dial_count")[0].attributes["auth_type"] == "iam"


def test_opt_out_records_nothing(
    credentials: FakeCredentials,
    fake_client: FakeAlloyDBClient,
    reader: InMemoryMetricReader,
) -> None:
    with _connector(credentials, fake_client, enable_otel_metrics=False) as connector:
        assert isinstance(connector._metric_recorder, NullMetricRecorder)
        with pytest.raises(ValueError):
            connector.connect(INSTANCE_URI, "bogus-driver")
    assert points(reader, "dial_count") == []


@pytest.mark.usefixtures("proxy_server")
def test_close_does_not_shut_down_app_provider(
    credentials: FakeCredentials, fake_client: FakeAlloyDBClient
) -> None:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    with (
        patch.object(metrics, "get_meter", side_effect=provider.get_meter),
        patch.object(provider, "shutdown") as shutdown,
        patch.object(provider, "force_flush") as flush,
    ):
        with _connector(credentials, fake_client) as connector:
            with patch(
                "google.cloud.alloydbconnector.pg8000.connect", return_value=True
            ):
                connector.connect(INSTANCE_URI, "pg8000", **CONNECT_KWARGS)
        shutdown.assert_not_called()
        flush.assert_not_called()
    # the provider still works after the connector is closed
    assert counts_by_status(reader) == {"success": 1}


# ---------- async connector ----------


async def test_async_connect_success(
    credentials: FakeCredentials, reader: InMemoryMetricReader
) -> None:
    with patch("google.cloud.alloydbconnector.asyncpg.connect") as connect:
        future: asyncio.Future = asyncio.Future()
        future.set_result(True)
        connect.return_value = future
        async with AsyncConnector(credentials) as connector:
            connector._client = FakeAlloyDBClient()
            await connector.connect(INSTANCE_URI, "asyncpg", user="u", db="d")

    assert counts_by_status(reader) == {"success": 1}
    assert points(reader, "dial_latencies")[0].count == 1
    assert points(reader, "refresh_count")[0].attributes["status"] == "success"


@pytest.mark.parametrize(
    "error, status",
    [
        (ConnectionRefusedError(), "tcp_error"),
        (ssl.SSLError(), "tls_error"),
        (ValueError("bad password"), "user_error"),
    ],
)
async def test_async_connect_failure_classified(
    credentials: FakeCredentials,
    reader: InMemoryMetricReader,
    error: Exception,
    status: str,
) -> None:
    with patch("google.cloud.alloydbconnector.asyncpg.connect", side_effect=error):
        async with AsyncConnector(credentials) as connector:
            connector._client = FakeAlloyDBClient()
            with pytest.raises(type(error)):
                await connector.connect(INSTANCE_URI, "asyncpg", user="u", db="d")
    assert counts_by_status(reader) == {status: 1}
    assert points(reader, "dial_latencies") == []


def test_classify_ssl_before_os_error() -> None:
    assert issubclass(ssl.SSLError, OSError)
    assert _classify_driver_error(ssl.SSLError()) == "tls_error"
    assert _classify_driver_error(TimeoutError()) == "tcp_error"


async def test_async_opt_out(credentials: FakeCredentials) -> None:
    async with AsyncConnector(credentials, enable_otel_metrics=False) as connector:
        assert isinstance(connector._metric_recorder, NullMetricRecorder)