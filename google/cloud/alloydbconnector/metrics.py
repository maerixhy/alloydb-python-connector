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

from __future__ import annotations

from typing import Protocol

from opentelemetry import metrics

# Attributes attached to a single measurement. Keys and values are always
# strings. Every measurement carries the target instance identity:
#   project, location, cluster, instance
# Individual events add their own bounded keys on top (for example
# "status" for dials and "refresh_type" for refreshes).
Attributes = dict[str, str]


class MetricRecorder(Protocol):
    """
    Receives connector events. Each recorder turns the events into
    measurements for one destination (for example the global OpenTelemetry
    MeterProvider). Implementations must be thread-safe and must never raise.
    """

    def record_dial_count(self, attrs: Attributes) -> None:
        """One dial attempt finished. attrs includes "status"."""

    def record_dial_latency(self, latency_ms: float, attrs: Attributes) -> None:
        """Time taken by a dial attempt, in milliseconds."""

    def record_open_connection(self, delta: int, attrs: Attributes) -> None:
        """A connection opened (+1) or closed (-1)."""

    def record_bytes_sent(self, num_bytes: int, attrs: Attributes) -> None:
        """Bytes written to the server over the connection."""

    def record_bytes_received(self, num_bytes: int, attrs: Attributes) -> None:
        """Bytes read from the server over the connection."""

    def record_refresh_count(self, attrs: Attributes) -> None:
        """A connection info refresh finished. attrs includes "status"."""


class OpenTelemetryMetricRecorder:
    """
    Records events as OpenTelemetry instruments on the global MeterProvider.

    The user does not pass a provider. If the application has installed an
    SDK MeterProvider (opentelemetry.metrics.set_meter_provider), measurements
    flow to it, including when it is installed after the connector is created.
    Otherwise the OpenTelemetry API's no-op provider drops them.
    """

    def __init__(self) -> None:
        meter = metrics.get_meter("alloydb.googleapis.com/client")
        self._dial_count = meter.create_counter(
            "dial_count", description="Number of dial attempts."
        )
        self._dial_latency = meter.create_histogram(
            "dial_latencies", unit="ms", description="Dial latency."
        )
        self._open_connections = meter.create_up_down_counter(
            "open_connections", description="Number of open connections."
        )
        self._bytes_sent = meter.create_counter(
            "bytes_sent", unit="By", description="Bytes sent to the server."
        )
        self._bytes_received = meter.create_counter(
            "bytes_received", unit="By", description="Bytes received from the server."
        )
        self._refresh_count = meter.create_counter(
            "refresh_count", description="Number of connection info refreshes."
        )

    def record_dial_count(self, attrs: Attributes) -> None:
        self._dial_count.add(1, attrs)

    def record_dial_latency(self, latency_ms: float, attrs: Attributes) -> None:
        self._dial_latency.record(latency_ms, attrs)

    def record_open_connection(self, delta: int, attrs: Attributes) -> None:
        self._open_connections.add(delta, attrs)

    def record_bytes_sent(self, num_bytes: int, attrs: Attributes) -> None:
        self._bytes_sent.add(num_bytes, attrs)

    def record_bytes_received(self, num_bytes: int, attrs: Attributes) -> None:
        self._bytes_received.add(num_bytes, attrs)

    def record_refresh_count(self, attrs: Attributes) -> None:
        self._refresh_count.add(1, attrs)


class NullMetricRecorder:
    """Recorder that drops every event. Used when no recorder is configured."""

    def record_dial_count(self, attrs: Attributes) -> None:
        pass

    def record_dial_latency(self, latency_ms: float, attrs: Attributes) -> None:
        pass

    def record_open_connection(self, delta: int, attrs: Attributes) -> None:
        pass

    def record_bytes_sent(self, num_bytes: int, attrs: Attributes) -> None:
        pass

    def record_bytes_received(self, num_bytes: int, attrs: Attributes) -> None:
        pass

    def record_refresh_count(self, attrs: Attributes) -> None:
        pass


class FanOutMetricRecorder:
    """
    Delivers every event to each wrapped recorder. A recorder that raises
    never stops the others, and never affects the connection.
    """

    def __init__(self, recorders: list[MetricRecorder]) -> None:
        self._recorders = list(recorders)

    def record_dial_count(self, attrs: Attributes) -> None:
        for r in self._recorders:
            try:
                r.record_dial_count(attrs)
            except Exception:
                pass

    def record_dial_latency(self, latency_ms: float, attrs: Attributes) -> None:
        for r in self._recorders:
            try:
                r.record_dial_latency(latency_ms, attrs)
            except Exception:
                pass

    def record_open_connection(self, delta: int, attrs: Attributes) -> None:
        for r in self._recorders:
            try:
                r.record_open_connection(delta, attrs)
            except Exception:
                pass

    def record_bytes_sent(self, num_bytes: int, attrs: Attributes) -> None:
        for r in self._recorders:
            try:
                r.record_bytes_sent(num_bytes, attrs)
            except Exception:
                pass

    def record_bytes_received(self, num_bytes: int, attrs: Attributes) -> None:
        for r in self._recorders:
            try:
                r.record_bytes_received(num_bytes, attrs)
            except Exception:
                pass

    def record_refresh_count(self, attrs: Attributes) -> None:
        for r in self._recorders:
            try:
                r.record_refresh_count(attrs)
            except Exception:
                pass
