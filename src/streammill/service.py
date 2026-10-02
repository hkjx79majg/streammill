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

Streams may additionally opt into event-identifier deduplication by
setting a positive ``dedup_retention_ms`` (at least
``allowed_lateness_ms``). Such streams require every event to carry a
non-empty ``event_id``:

* an unseen id is aggregated once and retained keyed by its original
  ``timestamp_ms``; the response reports ``duplicate: false``;
* an exact retry (same id, same timestamp, numerically equal value) is
  acknowledged with ``duplicate: true`` and never aggregates again,
  even once it is past the lateness horizon;
* a reused id with a different timestamp or value raises
  ``EventIdConflictError`` without touching aggregation or dedup state;
* unseen events past the lateness horizon are dropped without retaining
  their id;
* after each successful watermark advance, ids whose
  ``timestamp_ms < watermark_ms - dedup_retention_ms`` are forgotten
  (the boundary itself is retained) and may be reused.

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
    """Raised when an event_id is reused with a different payload."""


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
        # event_id -> (timestamp_ms, value); only for dedup-enabled streams.
        self._event_ids: dict[str, tuple[int, float]] = {}

    @property
    def dedup_enabled(self) -> bool:
        return self.dedup_retention_ms is not None

    def add_event(
        self, timestamp_ms: int, value: float, event_id: str | None = None
    ) -> dict:
        if self.dedup_enabled:
            assert event_id is not None
            retained = self._event_ids.get(event_id)
            if retained is not None:
                old_ts, old_value = retained
                if old_ts != timestamp_ms or old_value != value:
                    raise EventIdConflictError(event_id)
                return {"dropped": False, "duplicate": True}
            # Dedup precedes the lateness check: unseen late events are
            # dropped without recording the id.
            if (
                self.watermark is not None
                and timestamp_ms < self.watermark - self.allowed_lateness_ms
            ):
                return {"dropped": True, "duplicate": False}
            self._aggregate(timestamp_ms, value)
            self._event_ids[event_id] = (timestamp_ms, value)
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
        if self.dedup_enabled:
            # Forget ids older than the retention horizon; ids exactly on
            # the boundary are retained.
            horizon = watermark_ms - self.dedup_retention_ms
            stale = [eid for eid, (ts, _) in self._event_ids.items() if ts < horizon]
            for eid in stale:
                del self._event_ids[eid]
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

    def is_dedup_enabled(self, name: str) -> bool:
        with self._lock:
            return self._get(name).dedup_enabled

    def add_event(
        self,
        name: str,
        timestamp_ms: int,
        value: float,
        event_id: str | None = None,
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
