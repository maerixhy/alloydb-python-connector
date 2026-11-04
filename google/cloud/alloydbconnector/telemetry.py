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

"""Connector metric recording seam.

Connector events (dials, refreshes) are sent to a ``MetricRecorder``. A
``FanOutMetricRecorder`` delivers one event to several recorders, so each
export path (the application's global OpenTelemetry provider, the built-in
Cloud Monitoring pipeline) plugs in as its own recorder.

Recording must never raise into the dial path. Recorders are expected not to
raise, and the fan-out also guards each call and logs failures at debug level.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Any
from typing import Protocol
from typing import Sequence

logger = logging.getLogger(name=__name__)

_INSTANCE_URI_REGEX = re.compile(
    r"projects/([^:]+(:[^:]+)?)/locations/([^:]+)/clusters/([^:]+)/instances/([^:]+)"
)


@dataclass(frozen=True)
class InstanceAttributes:
    """Bounded identity of the target AlloyDB instance.

    Attached to every measurement so one application can report on many
    instances. There is deliberately no per-connector client ID here: it
    would create unbounded, restart-specific time series.
    """

    project_id: str = ""
    location: str = ""
    cluster_id: str = ""
    instance_id: str = ""

    @classmethod
    def from_uri(cls, instance_uri: str) -> "InstanceAttributes":
        """Parse an instance URI. Returns empty attributes if it is invalid,
        so a malformed URI still produces a (failed) dial measurement."""
        match = _INSTANCE_URI_REGEX.fullmatch(instance_uri)
        if match is None:
            return cls()
        return cls(match.group(1), match.group(3), match.group(4), match.group(5))

    def as_dict(self) -> dict[str, str]:
        return {
            "project_id": self.project_id,
            "location": self.location,
            "cluster_id": self.cluster_id,
            "instance_id": self.instance_id,
        }


@dataclass(frozen=True)
class DialAttributes:
    """One connect attempt.

    Attributes:
        auth_type: "iam" or "built_in".
        is_cache_hit: Connection info for the instance was already cached.
        status: "success", "user_error", "cache_error", "tcp_error",
            "tls_error" or "mdx_error".
        refresh_type: "refresh_ahead" or "lazy".
    """

    instance: InstanceAttributes
    auth_type: str
    is_cache_hit: bool
    status: str
    refresh_type: str


@dataclass(frozen=True)
class RefreshAttributes:
    """One connection info refresh.

    Attributes:
        status: "success" or "failure".
        refresh_type: "refresh_ahead" or "lazy".
    """

    instance: InstanceAttributes
    status: str
    refresh_type: str


class MetricRecorder(Protocol):
    """Receives connector events. Implementations must not raise."""

    def record_dial_count(self, attrs: DialAttributes) -> None: ...

    def record_dial_latency(self, attrs: DialAttributes, latency_ms: float) -> None: ...

    def record_refresh_count(self, attrs: RefreshAttributes) -> None: ...


class NullMetricRecorder:
    """Records nothing. Used when every export path is disabled."""

    def record_dial_count(self, attrs: DialAttributes) -> None:
        pass

    def record_dial_latency(self, attrs: DialAttributes, latency_ms: float) -> None:
        pass

    def record_refresh_count(self, attrs: RefreshAttributes) -> None:
        pass


class FanOutMetricRecorder:
    """Forwards each event to every recorder. A failure in one recorder is
    logged and does not stop the others."""

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

    def record_dial_count(self, attrs: DialAttributes) -> None:
        self._each("record_dial_count", attrs)

    def record_dial_latency(self, attrs: DialAttributes, latency_ms: float) -> None:
        self._each("record_dial_latency", attrs, latency_ms)

    def record_refresh_count(self, attrs: RefreshAttributes) -> None:
        self._each("record_refresh_count", attrs)
