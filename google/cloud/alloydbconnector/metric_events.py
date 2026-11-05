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

"""Turns connector activity into ``MetricRecorder`` events.

``metrics.py`` defines the recorders. This module is the connector side: it
builds the connector's recorder, tracks each dial attempt and its status, and
builds the attribute dicts every measurement carries.

Recording must never raise into the dial path or slow it down. Every call is
guarded and failures are logged at debug level.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Optional

from google.cloud.alloydbconnector.metrics import Attributes
from google.cloud.alloydbconnector.metrics import FanOutMetricRecorder
from google.cloud.alloydbconnector.metrics import MetricRecorder
from google.cloud.alloydbconnector.metrics import NullMetricRecorder
from google.cloud.alloydbconnector.metrics import OpenTelemetryMetricRecorder

logger = logging.getLogger(name=__name__)

# Instance identity keys (see the Attributes comment in metrics.py).
PROJECT = "project"
LOCATION = "location"
CLUSTER = "cluster"
INSTANCE = "instance"

# Event attribute keys
CONNECTOR_TYPE = "connector_type"
AUTH_TYPE = "auth_type"
IS_CACHE_HIT = "is_cache_hit"
STATUS = "status"
REFRESH_TYPE = "refresh_type"

# connector_type value for this library
CONNECTOR_TYPE_PYTHON = "python"

# auth_type values
AUTH_IAM = "iam"
AUTH_BUILT_IN = "built_in"

# dial status values (same as the Go connector)
DIAL_SUCCESS = "success"
DIAL_USER_ERROR = "user_error"
DIAL_CACHE_ERROR = "cache_error"
DIAL_TCP_ERROR = "tcp_error"
DIAL_TLS_ERROR = "tls_error"
DIAL_MDX_ERROR = "mdx_error"

# refresh status values
REFRESH_SUCCESS = "success"
REFRESH_FAILURE = "failure"

# refresh_type values
REFRESH_AHEAD = "refresh_ahead"
REFRESH_LAZY = "lazy"

_INSTANCE_URI_REGEX = re.compile(
    r"projects/([^:]+(:[^:]+)?)/locations/([^:]+)/clusters/([^:]+)/instances/([^:]+)"
)


def instance_attributes(
    project: str = "", location: str = "", cluster: str = "", instance: str = ""
) -> Attributes:
    """Bounded identity of the target AlloyDB instance.

    Attached to every measurement so one application can report on many
    instances. There is deliberately no per-connector client ID: it would
    create unbounded, restart-specific time series.
    """
    return {
        PROJECT: project,
        LOCATION: location,
        CLUSTER: cluster,
        INSTANCE: instance,
    }


def instance_attributes_from_uri(instance_uri: str) -> Attributes:
    """Parse an instance URI. Returns empty values if it is invalid, so a
    malformed URI still produces a (failed) dial measurement."""
    match = _INSTANCE_URI_REGEX.fullmatch(instance_uri)
    if match is None:
        return instance_attributes()
    return instance_attributes(
        match.group(1), match.group(3), match.group(4), match.group(5)
    )


def build_metric_recorder(enable_otel_metrics: bool = True) -> MetricRecorder:
    """Build the connector's recorder from the enabled export paths.

    Never raises. A path that fails to initialize is skipped and logged.
    """
    recorders: list[MetricRecorder] = []
    if enable_otel_metrics:
        try:
            recorders.append(OpenTelemetryMetricRecorder())
        except Exception as e:
            logger.debug(f"Failed to initialize OpenTelemetry metrics: {e}")
    # The built-in Cloud Monitoring recorder will be appended here.
    if not recorders:
        return NullMetricRecorder()
    return FanOutMetricRecorder(recorders)


class DialTracker:
    """Tracks one connect attempt and records it when finished.

    Dial code sets ``status`` right before each failure path, like the Go
    connector, rather than inferring it from the exception afterward.
    ``mark_success`` records latency at the point the connection is
    established.
    """

    def __init__(
        self,
        recorder: MetricRecorder,
        instance_uri: str,
        enable_iam_auth: bool,
        refresh_type: str,
    ) -> None:
        self._recorder = recorder
        self._start = time.perf_counter()
        self._instance = instance_attributes_from_uri(instance_uri)
        self._auth_type = AUTH_IAM if enable_iam_auth else AUTH_BUILT_IN
        self._refresh_type = refresh_type
        self.is_cache_hit = False
        self.status: Optional[str] = None

    def _count_attrs(self) -> Attributes:
        return {
            **self._instance,
            CONNECTOR_TYPE: CONNECTOR_TYPE_PYTHON,
            AUTH_TYPE: self._auth_type,
            IS_CACHE_HIT: "true" if self.is_cache_hit else "false",
            STATUS: self.status or DIAL_USER_ERROR,
            REFRESH_TYPE: self._refresh_type,
        }

    def mark_success(self) -> None:
        self.status = DIAL_SUCCESS
        latency_ms = (time.perf_counter() - self._start) * 1000
        try:
            self._recorder.record_dial_latency(
                latency_ms, {**self._instance, CONNECTOR_TYPE: CONNECTOR_TYPE_PYTHON}
            )
        except Exception as e:
            logger.debug(f"Failed to record dial latency: {e}")

    def finish(self) -> None:
        """Record the attempt. An attempt that failed without a status
        set (e.g. an unsupported driver) counts as a user error."""
        try:
            self._recorder.record_dial_count(self._count_attrs())
        except Exception as e:
            logger.debug(f"Failed to record dial count: {e}")


def record_refresh(
    recorder: MetricRecorder,
    instance: Attributes,
    refresh_type: str,
    success: bool,
) -> None:
    """Record one refresh result. Never raises."""
    try:
        recorder.record_refresh_count(
            {
                **instance,
                CONNECTOR_TYPE: CONNECTOR_TYPE_PYTHON,
                STATUS: REFRESH_SUCCESS if success else REFRESH_FAILURE,
                REFRESH_TYPE: refresh_type,
            }
        )
    except Exception as e:
        logger.debug(f"Failed to record refresh count: {e}")