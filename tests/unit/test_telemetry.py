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

"""Tests for the connector metric recording seam."""

from typing import Any

from google.cloud.alloydbconnector.telemetry import DialAttributes
from google.cloud.alloydbconnector.telemetry import FanOutMetricRecorder
from google.cloud.alloydbconnector.telemetry import InstanceAttributes
from google.cloud.alloydbconnector.telemetry import MetricRecorder
from google.cloud.alloydbconnector.telemetry import NullMetricRecorder
from google.cloud.alloydbconnector.telemetry import RefreshAttributes

INSTANCE_URI = (
    "projects/test-project/locations/test-region/"
    "clusters/test-cluster/instances/test-instance"
)


def dial_attrs() -> DialAttributes:
    return DialAttributes(
        instance=InstanceAttributes.from_uri(INSTANCE_URI),
        auth_type="built_in",
        is_cache_hit=False,
        status="success",
        refresh_type="refresh_ahead",
    )


def refresh_attrs() -> RefreshAttributes:
    return RefreshAttributes(
        instance=InstanceAttributes.from_uri(INSTANCE_URI),
        status="success",
        refresh_type="lazy",
    )


class SpyRecorder:
    """Records every call it receives."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def record_dial_count(self, attrs: DialAttributes) -> None:
        self.calls.append(("record_dial_count", (attrs,)))

    def record_dial_latency(self, attrs: DialAttributes, latency_ms: float) -> None:
        self.calls.append(("record_dial_latency", (attrs, latency_ms)))

    def record_refresh_count(self, attrs: RefreshAttributes) -> None:
        self.calls.append(("record_refresh_count", (attrs,)))


class BrokenRecorder:
    def record_dial_count(self, attrs: DialAttributes) -> None:
        raise RuntimeError("boom")

    def record_dial_latency(self, attrs: DialAttributes, latency_ms: float) -> None:
        raise RuntimeError("boom")

    def record_refresh_count(self, attrs: RefreshAttributes) -> None:
        raise RuntimeError("boom")


def test_instance_attributes_from_uri() -> None:
    assert InstanceAttributes.from_uri(INSTANCE_URI).as_dict() == {
        "project_id": "test-project",
        "location": "test-region",
        "cluster_id": "test-cluster",
        "instance_id": "test-instance",
    }


def test_instance_attributes_from_domain_scoped_project() -> None:
    attrs = InstanceAttributes.from_uri(
        "projects/example.com:proj/locations/r/clusters/c/instances/i"
    )
    assert attrs.project_id == "example.com:proj"
    assert attrs.instance_id == "i"


def test_instance_attributes_from_invalid_uri() -> None:
    assert InstanceAttributes.from_uri("not/a/uri") == InstanceAttributes()


def test_instance_attributes_have_no_client_id() -> None:
    assert "client_uid" not in InstanceAttributes.from_uri(INSTANCE_URI).as_dict()


def test_null_recorder_accepts_all_events() -> None:
    recorder: MetricRecorder = NullMetricRecorder()
    recorder.record_dial_count(dial_attrs())
    recorder.record_dial_latency(dial_attrs(), 1.0)
    recorder.record_refresh_count(refresh_attrs())


def test_fan_out_delivers_to_every_recorder() -> None:
    a, b = SpyRecorder(), SpyRecorder()
    fan_out = FanOutMetricRecorder([a, b])
    fan_out.record_dial_count(dial_attrs())
    fan_out.record_dial_latency(dial_attrs(), 12.5)
    fan_out.record_refresh_count(refresh_attrs())
    expected = [
        ("record_dial_count", (dial_attrs(),)),
        ("record_dial_latency", (dial_attrs(), 12.5)),
        ("record_refresh_count", (refresh_attrs(),)),
    ]
    assert a.calls == expected
    assert b.calls == expected


def test_fan_out_isolates_failing_recorder() -> None:
    spy = SpyRecorder()
    fan_out = FanOutMetricRecorder([BrokenRecorder(), spy])
    fan_out.record_dial_count(dial_attrs())
    fan_out.record_dial_latency(dial_attrs(), 1.0)
    fan_out.record_refresh_count(refresh_attrs())
    assert [name for name, _ in spy.calls] == [
        "record_dial_count",
        "record_dial_latency",
        "record_refresh_count",
    ]


def test_fan_out_with_no_recorders() -> None:
    fan_out = FanOutMetricRecorder([])
    assert fan_out.recorders == ()
    fan_out.record_dial_count(dial_attrs())