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

"""A console exporter that prints one table per connector metric.

The SDK's ConsoleMetricExporter prints a large JSON blob per export. This
prints only the connector's metrics, one row per combination of attribute
values, so you can watch the counts change:

    alloydb.connector.dial.count
      instance_id  auth.type  cache.hit  refresh.type   status   value
      my-inst      built_in   false      refresh_ahead  success      1
      my-inst      built_in   true       refresh_ahead  success      4
"""

from __future__ import annotations

import sys
from typing import Any
from typing import Sequence
from typing import TextIO

from opentelemetry.sdk.metrics import Counter
from opentelemetry.sdk.metrics import Histogram
from opentelemetry.sdk.metrics import ObservableCounter
from opentelemetry.sdk.metrics import ObservableGauge
from opentelemetry.sdk.metrics import ObservableUpDownCounter
from opentelemetry.sdk.metrics import UpDownCounter
from opentelemetry.sdk.metrics.export import AggregationTemporality
from opentelemetry.sdk.metrics.export import HistogramDataPoint
from opentelemetry.sdk.metrics.export import MetricExporter
from opentelemetry.sdk.metrics.export import MetricExportResult
from opentelemetry.sdk.metrics.export import MetricsData

CONNECTOR_METER = "google.cloud.alloydbconnector"

# Same for every row, so they add noise without information.
HIDDEN_KEYS = {
    "alloydb.connector.type",
    "alloydb.project_id",
    "alloydb.location",
    "alloydb.cluster_id",
}
INSTANCE_KEY = "alloydb.instance_id"


class ConsoleTableExporter(MetricExporter):
    def __init__(self, out: TextIO = sys.stdout) -> None:
        # Cumulative totals are easier to read than per-interval deltas.
        super().__init__(
            preferred_temporality={
                Counter: AggregationTemporality.CUMULATIVE,
                UpDownCounter: AggregationTemporality.CUMULATIVE,
                Histogram: AggregationTemporality.CUMULATIVE,
                ObservableCounter: AggregationTemporality.CUMULATIVE,
                ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
                ObservableGauge: AggregationTemporality.CUMULATIVE,
            }
        )
        self._out = out

    def export(
        self, metrics_data: MetricsData, timeout_millis: float = 10_000, **kwargs: Any
    ) -> MetricExportResult:
        lines: list[str] = []
        for rm in metrics_data.resource_metrics:
            for sm in rm.scope_metrics:
                if sm.scope.name != CONNECTOR_METER:
                    continue
                for metric in sm.metrics:
                    lines.extend(_table(metric.name, list(metric.data.data_points)))
                    lines.append("")
        if lines:
            self._out.write("\n".join(lines) + "\n")
            self._out.flush()
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return True

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
        pass


def _table(name: str, points: Sequence[Any]) -> list[str]:
    if not points:
        return [name, "  (no data)"]
    keys = sorted(
        {k for p in points for k in p.attributes if k not in HIDDEN_KEYS},
        key=lambda k: (k != INSTANCE_KEY, k),
    )
    is_histogram = isinstance(points[0], HistogramDataPoint)
    value_cols = ["count", "avg_ms", "max_ms"] if is_histogram else ["value"]

    rows = []
    for p in points:
        row = [_cell(p.attributes.get(k, "")) for k in keys]
        if is_histogram:
            avg = p.sum / p.count if p.count else 0.0
            row += [str(p.count), f"{avg:.1f}", f"{p.max:.1f}"]
        else:
            row.append(str(p.value))
        rows.append(row)
    rows.sort()

    # drop the shared "alloydb." prefix so the columns stay narrow
    header = [k.removeprefix("alloydb.") for k in keys] + value_cols
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(header)]
    n_text = len(keys)

    def fmt(cells: list[str]) -> str:
        parts = [
            c.ljust(w) if i < n_text else c.rjust(w)
            for i, (c, w) in enumerate(zip(cells, widths))
        ]
        return "  " + "  ".join(parts)

    return [name, fmt(header)] + [fmt(r) for r in rows]


def _cell(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value) or "-"