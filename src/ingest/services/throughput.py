from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass(frozen=True)
class ThroughputSample:
    ts: float
    pages: int
    ingestor_id: str


@dataclass(frozen=True)
class IngestorThroughput:
    ingestor_id: str
    pps: float
    pages: int
    documents: int


@dataclass(frozen=True)
class ThroughputSnapshot:
    pps: float
    window_seconds: float
    pages_in_window: int
    documents_in_window: int
    by_ingestor: list[IngestorThroughput]


class ThroughputMeter:
    """In-memory rolling window of indexed pages for realtime PPS."""

    def __init__(self, *, max_age_seconds: float = 120.0) -> None:
        self._max_age = max_age_seconds
        self._samples: deque[ThroughputSample] = deque()
        self._lock = threading.Lock()

    def record(self, pages: int, *, ingestor_id: str | None = None) -> None:
        pages = max(0, int(pages))
        if pages <= 0:
            pages = 1
        sample = ThroughputSample(
            ts=time.monotonic(),
            pages=pages,
            ingestor_id=ingestor_id or "local",
        )
        with self._lock:
            self._samples.append(sample)
            self._prune_locked(sample.ts)

    def snapshot(self, window_seconds: float = 10.0) -> ThroughputSnapshot:
        window = max(1.0, float(window_seconds))
        now = time.monotonic()
        cutoff = now - window
        with self._lock:
            self._prune_locked(now)
            recent = [s for s in self._samples if s.ts >= cutoff]

        pages = sum(s.pages for s in recent)
        docs = len(recent)
        by_ingestor_pages: dict[str, int] = defaultdict(int)
        by_ingestor_docs: dict[str, int] = defaultdict(int)
        for sample in recent:
            by_ingestor_pages[sample.ingestor_id] += sample.pages
            by_ingestor_docs[sample.ingestor_id] += 1

        ingestors = [
            IngestorThroughput(
                ingestor_id=ingestor_id,
                pps=round(ingestor_pages / window, 2),
                pages=ingestor_pages,
                documents=by_ingestor_docs[ingestor_id],
            )
            for ingestor_id, ingestor_pages in sorted(by_ingestor_pages.items(), key=lambda item: (-item[1], item[0]))
        ]
        return ThroughputSnapshot(
            pps=round(pages / window, 2),
            window_seconds=window,
            pages_in_window=pages,
            documents_in_window=docs,
            by_ingestor=ingestors,
        )

    def _prune_locked(self, now: float) -> None:
        cutoff = now - self._max_age
        while self._samples and self._samples[0].ts < cutoff:
            self._samples.popleft()


_meter: ThroughputMeter | None = None


def get_throughput_meter() -> ThroughputMeter:
    global _meter
    if _meter is None:
        _meter = ThroughputMeter()
    return _meter


def reset_throughput_meter() -> None:
    global _meter
    _meter = None
