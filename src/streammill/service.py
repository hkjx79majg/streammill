"""Core service surface for StreamMill.

The frozen baseline only reports process health; that surface stays
unchanged. On top of it this module now implements named event-time
tumbling windows with explicit watermarks:

* streams are created with a positive ``window_ms`` and a non-negative
  ``allowed_lateness_ms``;
* events carry an integer ``timestamp_ms`` and a finite numeric ``value``
  and land in the left-closed, right-open window
  ``[floor(timestamp_ms / window_ms) * window_ms, +window_ms)``;
* a per-stream watermark only moves forward; once it reaches
  ``window_end_ms + allowed_lateness_ms`` the window is final, is
  emitted exactly once, and is retained for queries;
* events with ``timestamp_ms < watermark - allowed_lateness_ms`` are
  acknowledged but dropped without touching the aggregates.

Streams may optionally enable event-id deduplication by passing a
positive ``dedup_retention_ms`` (no smaller than ``allowed_lateness_ms``)
at creation time. On such streams every event must carry a non-empty
string ``event_id``:

* the first occurrence of an id is aggregated and remembered together
  with its ``timestamp_ms`` and ``value``;
* an exact retry (same ``timestamp_ms`` and numerically equal ``value``)
  is reported as ``duplicate: true`` and never touches the aggregates —
  the dedup check runs before the lateness check, so retained retries
  are duplicates even past the lateness horizon;
* the same id with a different timestamp or value is rejected with
  ``event_id_conflict`` and leaves all state untouched;
* an unseen id that arrives too late is dropped as usual and its id is
  not remembered.

Remembered ids are evicted once the watermark advances past
``timestamp_ms + dedup_retention_ms`` (ids exactly on the boundary are
kept); evicted ids may be reused and are treated as new events.

Restart recovery, joins, persistence and automatic watermarks are
explicitly out of scope.
"""

from __future__ import annotations

import threading

from . import __version__


class StreamExistsError(Exception):
    """Raised when creating a stream that already exists."""


class StreamNotFoundError(Exception):
    """Raised when addressing a stream that does not exist."""


class WatermarkRegressionError(Exception):
    """Raised when a new watermark would move the stream backwards."""


class EventIdConflictError(Exception):
    """Raised when a retained event id is resubmitted with different data."""


class _Stream:
    """Mutable state of one named stream. Guarded by the Service lock."""

    def __init__(
        self,
        name: str,
        window_ms: int,
        allowed_lateness_ms: int,
        dedup_retention_ms: int | None = None,
    ) -> None:
        self.name = name
        self.window_ms = window_ms
        self.allowed_lateness_ms = allowed_lateness_ms
        self.dedup_retention_ms = dedup_retention_ms
        self.watermark: int | None = None
        # window_start_ms -> [count, sum]; only non-empty windows exist.
        self._windows: dict[int, list] = {}
        self._finalized: list[dict] = []
        self._finalized_starts: set[int] = set()
        # event_id -> (timestamp_ms, value) for accepted, not yet evicted ids.
        self._dedup: dict[str, tuple[int, float]] = {}

    def add_event(
        self, timestamp_ms: int, value: float, event_id: str | None = None
    ) -> dict:
        if self.dedup_retention_ms is not None:
            retained = self._dedup.get(event_id)
            if retained is not None:
                kept_ts, kept_value = retained
                if kept_ts == timestamp_ms and kept_value == value:
                    return {"dropped": False, "duplicate": True}
                raise EventIdConflictError(event_id)
            if (
                self.watermark is not None
                and timestamp_ms < self.watermark - self.allowed_lateness_ms
            ):
                # Unseen too-late ids are dropped without being remembered.
                return {"dropped": True, "duplicate": False}
            self._aggregate(timestamp_ms, value)
            self._dedup[event_id] = (timestamp_ms, value)
            return {"dropped": False, "duplicate": False}
        if (
            self.watermark is not None
            and timestamp_ms < self.watermark - self.allowed_lateness_ms
        ):
            return {"dropped": True}
        self._aggregate(timestamp_ms, value)
        return {"dropped": False}

    def _aggregate(self, timestamp_ms: int, value: float) -> None:
        start = (timestamp_ms // self.window_ms) * self.window_ms
        bucket = self._windows.setdefault(start, [0, 0])
        bucket[0] += 1
        bucket[1] += value

    def advance_watermark(self, watermark_ms: int) -> list[dict]:
        if self.watermark is not None and watermark_ms < self.watermark:
            raise WatermarkRegressionError(
                f"watermark {watermark_ms} is below current {self.watermark}"
            )
        self.watermark = watermark_ms
        newly: list[dict] = []
        for start in sorted(self._windows):
            if start in self._finalized_starts:
                continue
            end = start + self.window_ms
            if watermark_ms >= end + self.allowed_lateness_ms:
                count, total = self._windows[start]
                newly.append(
                    {
                        "stream": self.name,
                        "window_start_ms": start,
                        "window_end_ms": end,
                        "count": count,
                        "sum": total,
                    }
                )
                self._finalized_starts.add(start)
        self._finalized.extend(newly)
        if self.dedup_retention_ms is not None:
            horizon = watermark_ms - self.dedup_retention_ms
            self._dedup = {
                event_id: kept
                for event_id, kept in self._dedup.items()
                if kept[0] >= horizon
            }
        return newly

    def results(self) -> list[dict]:
        return [dict(row) for row in self._finalized]


class Service:
    """StreamMill service: health reporting plus windowed event aggregation."""

    name = "streammill"
    version = __version__

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._streams: dict[str, _Stream] = {}

    def health(self) -> dict[str, str]:
        return {"status": "ok", "service": self.name, "version": self.version}

    def create_stream(
        self,
        name: str,
        window_ms: int,
        allowed_lateness_ms: int,
        dedup_retention_ms: int | None = None,
    ) -> dict:
        with self._lock:
            if name in self._streams:
                raise StreamExistsError(name)
            self._streams[name] = _Stream(
                name, window_ms, allowed_lateness_ms, dedup_retention_ms
            )
        payload = {
            "stream": name,
            "window_ms": window_ms,
            "allowed_lateness_ms": allowed_lateness_ms,
        }
        if dedup_retention_ms is not None:
            payload["dedup_retention_ms"] = dedup_retention_ms
        return payload

    def dedup_enabled(self, name: str) -> bool | None:
        """Whether the stream deduplicates; None if the stream is unknown."""
        with self._lock:
            stream = self._streams.get(name)
            return None if stream is None else stream.dedup_retention_ms is not None

    def add_event(
        self, name: str, timestamp_ms: int, value: float, event_id: str | None = None
    ) -> dict:
        with self._lock:
            outcome = self._get(name).add_event(timestamp_ms, value, event_id)
        return {"stream": name, **outcome}

    def advance_watermark(self, name: str, watermark_ms: int) -> dict:
        with self._lock:
            finalized = self._get(name).advance_watermark(watermark_ms)
        return {"stream": name, "watermark_ms": watermark_ms, "finalized": finalized}

    def results(self, name: str) -> dict:
        with self._lock:
            rows = self._get(name).results()
        return {"stream": name, "results": rows}

    def _get(self, name: str) -> _Stream:
        try:
            return self._streams[name]
        except KeyError:
            raise StreamNotFoundError(name) from None
