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

"""Connector metrics on the application's global OpenTelemetry provider.

Built-in telemetry (telemetry.py) reports to AlloyDB's own Cloud Monitoring
prefix. This module reports the same connector events to whatever
OpenTelemetry pipeline the application has configured, through the global
MeterProvider. The connector never configures, flushes or shuts down that
provider. If the application has not set one, OpenTelemetry's no-op provider
drops every measurement, so there is nothing to opt out of.

Metric names, units, attributes and the instrumentation scope follow the Go
connector's public contract (METRICS.md), so dashboards and queries work for
both connectors.

Each instance gets a GlobalMetricRecorder, and FanOutMetricRecorder sends
every event to both it and the built-in recorder, like Go's
MultiMetricRecorder.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
import logging
import threading
from typing import TYPE_CHECKING
from typing import Any
from typing import Iterable
from typing import Optional
from typing import Protocol
from typing import Sequence

from opentelemetry import metrics

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions
    from opentelemetry.metrics import Observation

    from google.cloud.alloydbconnector.telemetry import TelemetryAttributes

logger = logging.getLogger(name=__name__)

METER_NAME = "google.cloud.alloydbconnector"

DIAL_COUNT = "alloydb.connector.dial.count"
DIAL_DURATION = "alloydb.connector.dial.duration"
CONNECTIONS_OPEN = "alloydb.connector.connections.open"
BYTES_SENT = "alloydb.connector.bytes.sent"
BYTES_RECEIVED = "alloydb.connector.bytes.received"
REFRESH_COUNT = "alloydb.connector.refresh.count"

PROJECT_ID = "alloydb.project_id"
LOCATION = "alloydb.location"
CLUSTER_ID = "alloydb.cluster_id"
INSTANCE_ID = "alloydb.instance_id"
CONNECTOR_TYPE = "alloydb.connector.type"
AUTH_TYPE = "alloydb.auth.type"
CACHE_HIT = "alloydb.cache.hit"
STATUS = "alloydb.status"
REFRESH_TYPE = "alloydb.refresh.type"

_PYTHON = "python"


class MetricRecorder(Protocol):
    """The recorder interface the connector calls (see telemetry.py)."""

    def record_dial_count(self, attrs: TelemetryAttributes) -> None: ...

    def record_dial_latency(self, latency_ms: float) -> None: ...

    def record_open_connection(self, attrs: TelemetryAttributes) -> None: ...

    def record_closed_connection(self, attrs: TelemetryAttributes) -> None: ...

    def record_bytes_rx(self, count: int) -> None: ...

    def record_bytes_tx(self, count: int) -> None: ...

    def record_refresh_count(self, attrs: TelemetryAttributes) -> None: ...


class FanOutMetricRecorder:
    """Sends every event to each recorder.

    A recorder that raises is logged and skipped, so it never stops the
    others or affects the connection.
    """

    def __init__(self, recorders: Sequence[MetricRecorder]) -> None:
        self._recorders = tuple(recorders)

    @property
    def recorders(self) -> tuple[MetricRecorder, ...]:
        return self._recorders

    def _each(self, method: str, *args: Any) -> None:
        for recorder in self._recorders:
            try:
                getattr(recorder, method)(*args)
            except Exception as e:
                logger.debug(f"Failed to record connector metric ({method}): {e}")

    def record_dial_count(self, attrs: TelemetryAttributes) -> None:
        self._each("record_dial_count", attrs)

    def record_dial_latency(self, latency_ms: float) -> None:
        self._each("record_dial_latency", latency_ms)

    def record_open_connection(self, attrs: TelemetryAttributes) -> None:
        self._each("record_open_connection", attrs)

    def record_closed_connection(self, attrs: TelemetryAttributes) -> None:
        self._each("record_closed_connection", attrs)

    def record_bytes_rx(self, count: int) -> None:
        self._each("record_bytes_rx", count)

    def record_bytes_tx(self, count: int) -> None:
        self._each("record_bytes_tx", count)

    def record_refresh_count(self, attrs: TelemetryAttributes) -> None:
        self._each("record_refresh_count", attrs)


@dataclass
class _ByteTotals:
    """Cumulative bytes for one instance, read by the observable counters.

    Shared by every recorder for the same instance, so two Connectors
    dialing one instance add to one total instead of reporting two
    conflicting ones, and the total outlives a closed Connector instead of
    dropping back to zero.
    """

    attributes: dict[str, str]
    rx: int = 0
    tx: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


class _Instruments:
    """The global path's instruments, created once per process.

    Byte counters are observable, as in telemetry.py: a socket read only
    adds to an integer, and the totals are read once per export. A
    synchronous counter add costs several microseconds, more than a local
    socket read itself.
    """

    def __init__(self) -> None:
        from google.cloud.alloydbconnector.version import __version__

        self._lock = threading.Lock()
        self._byte_totals: dict[tuple[tuple[str, str], ...], _ByteTotals] = {}
        meter = metrics.get_meter(METER_NAME, version=__version__)
        self.dial_count = meter.create_counter(
            DIAL_COUNT, unit="{dial}", description="Dial attempts."
        )
        self.dial_duration = meter.create_histogram(
            DIAL_DURATION,
            unit="ms",
            description="Duration of successful dial operations.",
        )
        self.connections_open = meter.create_up_down_counter(
            CONNECTIONS_OPEN,
            unit="{connection}",
            description="Open connector connections.",
        )
        self.refresh_count = meter.create_counter(
            REFRESH_COUNT,
            unit="{refresh}",
            description="Connection-information refresh operations.",
        )
        meter.create_observable_counter(
            BYTES_SENT,
            callbacks=[self._observe_tx],
            unit="By",
            description="Bytes sent through connector connections.",
        )
        meter.create_observable_counter(
            BYTES_RECEIVED,
            callbacks=[self._observe_rx],
            unit="By",
            description="Bytes received through connector connections.",
        )

    def byte_totals(self, attributes: dict[str, str]) -> _ByteTotals:
        key = tuple(sorted(attributes.items()))
        with self._lock:
            totals = self._byte_totals.get(key)
            if totals is None:
                totals = _ByteTotals(dict(attributes))
                self._byte_totals[key] = totals
            return totals

    def _observe(self, received: bool) -> Iterable[Observation]:
        from opentelemetry.metrics import Observation

        with self._lock:
            all_totals = list(self._byte_totals.values())
        observations = []
        for totals in all_totals:
            with totals.lock:
                value = totals.rx if received else totals.tx
            observations.append(Observation(value, totals.attributes))
        return observations

    def _observe_rx(self, options: CallbackOptions) -> Iterable[Observation]:
        return self._observe(received=True)

    def _observe_tx(self, options: CallbackOptions) -> Iterable[Observation]:
        return self._observe(received=False)


_instruments: Optional[_Instruments] = None
_instruments_lock = threading.Lock()


def _get_instruments() -> _Instruments:
    global _instruments
    with _instruments_lock:
        if _instruments is None:
            _instruments = _Instruments()
        return _instruments


def _reset_instruments() -> None:
    """Drop the process-wide instruments. For tests only."""
    global _instruments
    with _instruments_lock:
        _instruments = None


def _auth_type(iam_authn: bool) -> str:
    return "iam" if iam_authn else "built_in"


class GlobalMetricRecorder:
    """Records one instance's connector metrics on the global provider.

    Holds no per-connection state, and its hot path (byte counts) only takes
    a lock and adds to an integer.
    """

    def __init__(
        self, project_id: str, location: str, cluster: str, instance: str
    ) -> None:
        self._instruments = _get_instruments()
        self._target = {
            PROJECT_ID: project_id,
            LOCATION: location,
            CLUSTER_ID: cluster,
            INSTANCE_ID: instance,
            CONNECTOR_TYPE: _PYTHON,
        }
        self._bytes = self._instruments.byte_totals(self._target)

    def record_dial_count(self, attrs: TelemetryAttributes) -> None:
        self._instruments.dial_count.add(
            1,
            {
                **self._target,
                AUTH_TYPE: _auth_type(attrs.iam_authn),
                CACHE_HIT: attrs.cache_hit,
                STATUS: attrs.dial_status,
            },
        )

    def record_dial_latency(self, latency_ms: float) -> None:
        self._instruments.dial_duration.record(latency_ms, self._target)

    def record_open_connection(self, attrs: TelemetryAttributes) -> None:
        self._instruments.connections_open.add(
            1, {**self._target, AUTH_TYPE: _auth_type(attrs.iam_authn)}
        )

    def record_closed_connection(self, attrs: TelemetryAttributes) -> None:
        self._instruments.connections_open.add(
            -1, {**self._target, AUTH_TYPE: _auth_type(attrs.iam_authn)}
        )

    def record_bytes_rx(self, count: int) -> None:
        with self._bytes.lock:
            self._bytes.rx += count

    def record_bytes_tx(self, count: int) -> None:
        with self._bytes.lock:
            self._bytes.tx += count

    def record_refresh_count(self, attrs: TelemetryAttributes) -> None:
        self._instruments.refresh_count.add(
            1,
            {
                **self._target,
                STATUS: attrs.refresh_status,
                REFRESH_TYPE: attrs.refresh_type,
            },
        )


def new_global_metric_recorder(
    project_id: str, location: str, cluster: str, instance: str
) -> Optional[GlobalMetricRecorder]:
    """Create a recorder for the global path. Never raises."""
    try:
        return GlobalMetricRecorder(project_id, location, cluster, instance)
    except Exception as e:
        logger.debug(f"Failed to initialize OpenTelemetry metrics: {e}")
        return None
