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

from unittest import mock

from opentelemetry import metrics

from google.cloud.alloydbconnector.metrics import FanOutMetricRecorder
from google.cloud.alloydbconnector.metrics import NullMetricRecorder
from google.cloud.alloydbconnector.metrics import OpenTelemetryMetricRecorder

ATTRS = {"project": "p", "location": "l", "cluster": "c", "instance": "i"}


class FakeRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def record_dial_count(self, attrs):
        self.calls.append(("dial_count", attrs))

    def record_dial_latency(self, latency_ms, attrs):
        self.calls.append(("dial_latency", latency_ms, attrs))

    def record_open_connection(self, delta, attrs):
        self.calls.append(("open_connection", delta, attrs))

    def record_bytes_sent(self, num_bytes, attrs):
        self.calls.append(("bytes_sent", num_bytes, attrs))

    def record_bytes_received(self, num_bytes, attrs):
        self.calls.append(("bytes_received", num_bytes, attrs))

    def record_refresh_count(self, attrs):
        self.calls.append(("refresh_count", attrs))


class RaisingRecorder:
    def __getattr__(self, name):
        def fail(*args, **kwargs):
            raise RuntimeError("boom")

        return fail


def emit_all(recorder) -> None:
    recorder.record_dial_count(ATTRS)
    recorder.record_dial_latency(1.5, ATTRS)
    recorder.record_open_connection(1, ATTRS)
    recorder.record_bytes_sent(10, ATTRS)
    recorder.record_bytes_received(20, ATTRS)
    recorder.record_refresh_count(ATTRS)


def test_otel_recorder_accepts_all_events_without_sdk() -> None:
    # No SDK provider installed: the API's no-op global provider drops them.
    emit_all(OpenTelemetryMetricRecorder())


def test_otel_recorder_uses_global_meter_provider() -> None:
    meter = mock.MagicMock()
    with mock.patch.object(metrics, "get_meter", return_value=meter) as get_meter:
        recorder = OpenTelemetryMetricRecorder()
        emit_all(recorder)
    get_meter.assert_called_once()
    meter.create_counter.return_value.add.assert_any_call(1, ATTRS)
    meter.create_histogram.return_value.record.assert_called_once_with(1.5, ATTRS)
    meter.create_up_down_counter.return_value.add.assert_called_once_with(1, ATTRS)


def test_null_recorder_accepts_all_events() -> None:
    emit_all(NullMetricRecorder())


def test_fan_out_delivers_to_every_recorder() -> None:
    a, b = FakeRecorder(), FakeRecorder()
    emit_all(FanOutMetricRecorder([a, b]))
    assert len(a.calls) == 6
    assert a.calls == b.calls


def test_fan_out_isolates_failing_recorder() -> None:
    good = FakeRecorder()
    emit_all(FanOutMetricRecorder([RaisingRecorder(), good]))
    assert len(good.calls) == 6
