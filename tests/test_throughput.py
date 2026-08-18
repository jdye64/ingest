from __future__ import annotations

import time

from ingest.services.throughput import ThroughputMeter


def test_throughput_meter_aggregates_across_ingestors() -> None:
    meter = ThroughputMeter(max_age_seconds=60)
    meter.record(10, ingestor_id="a1")
    meter.record(5, ingestor_id="a2")
    meter.record(5, ingestor_id="a1")
    snap = meter.snapshot(10.0)
    assert snap.pages_in_window == 20
    assert snap.documents_in_window == 3
    assert snap.pps == 2.0
    assert [row.ingestor_id for row in snap.by_ingestor] == ["a1", "a2"]
    assert snap.by_ingestor[0].pages == 15
    assert snap.by_ingestor[0].pps == 1.5


def test_throughput_meter_prunes_old_samples() -> None:
    meter = ThroughputMeter(max_age_seconds=60)
    meter.record(100, ingestor_id="a1")
    # Force the sample into the past
    with meter._lock:
        old = meter._samples[0]
        meter._samples[0] = type(old)(ts=time.monotonic() - 30, pages=old.pages, ingestor_id=old.ingestor_id)
    snap = meter.snapshot(5.0)
    assert snap.pages_in_window == 0
    assert snap.pps == 0.0
