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

On top of the in-memory state this module also implements portable
full-state snapshots:

* :meth:`Service.snapshot` exports a consistent point-in-time JSON
  document (``format_version`` 1) holding every stream's creation
  config, watermark, still-open window aggregates, finalized results
  and retained dedup records; streams are sorted by name, windows and
  finalized rows by ``window_start_ms``, dedup records by
  ``event_id``. The document is built under the service lock, so
  concurrent creates, events and watermark advances are either fully
  inside or fully outside the snapshot.
* :meth:`Service.restore_snapshot` validates a snapshot document and
  installs it atomically on an instance that has no streams yet.
  Invalid documents raise :class:`SnapshotInvalidError` and leave the
  instance completely empty; restoring into a non-empty instance
  raises :class:`RestoreConflictError` regardless of the document.

Joins, persistence and automatic watermarks remain explicitly out of
scope; snapshots cross process boundaries only as JSON documents and
no endpoint gains on-disk side effects.
"""

from __future__ import annotations

import math
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


class SnapshotInvalidError(Exception):
    """Raised when a snapshot document fails validation."""


class RestoreConflictError(Exception):
    """Raised when restoring into an instance that already has streams."""


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


_SNAPSHOT_FORMAT_VERSION = 1
_SNAPSHOT_TOP_FIELDS = {"format_version", "streams"}
_SNAPSHOT_STREAM_FIELDS = {
    "name",
    "window_ms",
    "allowed_lateness_ms",
    "watermark_ms",
    "windows",
    "finalized",
    "dedup_retention_ms",
    "dedup",
}
_SNAPSHOT_WINDOW_FIELDS = {"window_start_ms", "count", "sum"}
_SNAPSHOT_FINALIZED_FIELDS = {"window_start_ms", "window_end_ms", "count", "sum"}
_SNAPSHOT_DEDUP_FIELDS = {"event_id", "timestamp_ms", "value"}


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _stream_snapshot(stream: _Stream) -> dict:
    """Export one stream's state; the caller holds the service lock."""
    document = {
        "name": stream.name,
        "window_ms": stream.window_ms,
        "allowed_lateness_ms": stream.allowed_lateness_ms,
        "watermark_ms": stream.watermark,
        "windows": [
            {"window_start_ms": start, "count": count, "sum": total}
            for start, (count, total) in sorted(stream._windows.items())
            if start not in stream._finalized_starts
        ],
        "finalized": [
            {
                "window_start_ms": row["window_start_ms"],
                "window_end_ms": row["window_end_ms"],
                "count": row["count"],
                "sum": row["sum"],
            }
            for row in sorted(
                stream._finalized, key=lambda row: row["window_start_ms"]
            )
        ],
    }
    if stream.dedup_retention_ms is not None:
        document["dedup_retention_ms"] = stream.dedup_retention_ms
        document["dedup"] = [
            {"event_id": event_id, "timestamp_ms": timestamp_ms, "value": value}
            for event_id, (timestamp_ms, value) in sorted(stream._dedup.items())
        ]
    return document


def _parse_snapshot(document: object) -> dict[str, "_Stream"]:
    """Validate a snapshot document and rebuild the streams it describes.

    Pure: raises :class:`SnapshotInvalidError` on any inconsistency and
    never silently repairs the input.
    """
    if not isinstance(document, dict):
        raise SnapshotInvalidError("snapshot must be a JSON object")
    extra = sorted(set(document) - _SNAPSHOT_TOP_FIELDS)
    if extra:
        raise SnapshotInvalidError(f"unexpected fields: {', '.join(extra)}")
    version = document.get("format_version")
    if not _is_int(version) or version != _SNAPSHOT_FORMAT_VERSION:
        raise SnapshotInvalidError(f"unsupported format_version: {version!r}")
    streams_data = document.get("streams")
    if not isinstance(streams_data, list):
        raise SnapshotInvalidError("streams must be an array")
    streams: dict[str, _Stream] = {}
    previous_name: str | None = None
    for entry in streams_data:
        stream = _parse_snapshot_stream(entry)
        if previous_name is not None and stream.name <= previous_name:
            raise SnapshotInvalidError(
                "streams must be unique and sorted ascending by name"
            )
        previous_name = stream.name
        streams[stream.name] = stream
    return streams


def _parse_snapshot_stream(data: object) -> "_Stream":
    if not isinstance(data, dict):
        raise SnapshotInvalidError("stream entries must be JSON objects")
    extra = sorted(set(data) - _SNAPSHOT_STREAM_FIELDS)
    if extra:
        raise SnapshotInvalidError(f"unexpected stream fields: {', '.join(extra)}")
    for field in (
        "name",
        "window_ms",
        "allowed_lateness_ms",
        "watermark_ms",
        "windows",
        "finalized",
    ):
        if field not in data:
            raise SnapshotInvalidError(f"missing stream field: {field}")
    name = data["name"]
    if not isinstance(name, str) or not name:
        raise SnapshotInvalidError("stream name must be a non-empty string")
    window_ms = data["window_ms"]
    if not _is_int(window_ms) or window_ms <= 0:
        raise SnapshotInvalidError("window_ms must be a positive integer")
    allowed_lateness_ms = data["allowed_lateness_ms"]
    if not _is_int(allowed_lateness_ms) or allowed_lateness_ms < 0:
        raise SnapshotInvalidError("allowed_lateness_ms must be a non-negative integer")
    retention = data.get("dedup_retention_ms")
    if retention is not None:
        if not _is_int(retention) or retention <= 0:
            raise SnapshotInvalidError("dedup_retention_ms must be a positive integer")
        if retention < allowed_lateness_ms:
            raise SnapshotInvalidError(
                "dedup_retention_ms must be at least allowed_lateness_ms"
            )
    watermark = data["watermark_ms"]
    if watermark is not None and not _is_int(watermark):
        raise SnapshotInvalidError("watermark_ms must be null or an integer")

    windows = _parse_snapshot_windows(data["windows"], window_ms)
    finalized = _parse_snapshot_finalized(data["finalized"], window_ms)

    open_starts = {row["window_start_ms"] for row in windows}
    final_starts = {row["window_start_ms"] for row in finalized}
    if open_starts & final_starts:
        raise SnapshotInvalidError("window listed as both open and finalized")
    for row in finalized:
        if watermark is None or watermark < row["window_end_ms"] + allowed_lateness_ms:
            raise SnapshotInvalidError(
                "finalized window inconsistent with watermark and allowed_lateness_ms"
            )
    for row in windows:
        if (
            watermark is not None
            and watermark
            >= row["window_start_ms"] + window_ms + allowed_lateness_ms
        ):
            raise SnapshotInvalidError(
                "open window would already be finalized at the given watermark"
            )

    dedup = _parse_snapshot_dedup(data.get("dedup", []), retention, watermark)

    stream = _Stream(name, window_ms, allowed_lateness_ms, retention)
    stream.watermark = watermark
    stream._windows = {
        row["window_start_ms"]: [row["count"], row["sum"]] for row in windows
    }
    stream._finalized = [
        {
            "stream": name,
            "window_start_ms": row["window_start_ms"],
            "window_end_ms": row["window_end_ms"],
            "count": row["count"],
            "sum": row["sum"],
        }
        for row in finalized
    ]
    stream._finalized_starts = final_starts
    stream._dedup = dedup
    return stream


def _parse_snapshot_windows(data: object, window_ms: int) -> list[dict]:
    if not isinstance(data, list):
        raise SnapshotInvalidError("windows must be an array")
    parsed: list[dict] = []
    previous: int | None = None
    for entry in data:
        if not isinstance(entry, dict) or set(entry) != _SNAPSHOT_WINDOW_FIELDS:
            raise SnapshotInvalidError(
                "window entries must have exactly window_start_ms, count and sum"
            )
        start = entry["window_start_ms"]
        if not _is_int(start) or start % window_ms != 0:
            raise SnapshotInvalidError("window_start_ms must align to the window grid")
        count = entry["count"]
        if not _is_int(count) or count < 1:
            raise SnapshotInvalidError("window count must be a positive integer")
        total = entry["sum"]
        if not _is_finite_number(total):
            raise SnapshotInvalidError("window sum must be a finite number")
        if previous is not None and start <= previous:
            raise SnapshotInvalidError(
                "windows must be unique and sorted ascending by window_start_ms"
            )
        previous = start
        parsed.append({"window_start_ms": start, "count": count, "sum": total})
    return parsed


def _parse_snapshot_finalized(data: object, window_ms: int) -> list[dict]:
    if not isinstance(data, list):
        raise SnapshotInvalidError("finalized must be an array")
    parsed: list[dict] = []
    previous: int | None = None
    for entry in data:
        if not isinstance(entry, dict) or set(entry) != _SNAPSHOT_FINALIZED_FIELDS:
            raise SnapshotInvalidError(
                "finalized entries must have exactly window_start_ms, "
                "window_end_ms, count and sum"
            )
        start = entry["window_start_ms"]
        if not _is_int(start) or start % window_ms != 0:
            raise SnapshotInvalidError("window_start_ms must align to the window grid")
        end = entry["window_end_ms"]
        if not _is_int(end) or end != start + window_ms:
            raise SnapshotInvalidError(
                "window_end_ms must equal window_start_ms + window_ms"
            )
        count = entry["count"]
        if not _is_int(count) or count < 1:
            raise SnapshotInvalidError("window count must be a positive integer")
        total = entry["sum"]
        if not _is_finite_number(total):
            raise SnapshotInvalidError("window sum must be a finite number")
        if previous is not None and start <= previous:
            raise SnapshotInvalidError(
                "finalized windows must be unique and sorted ascending by "
                "window_start_ms"
            )
        previous = start
        parsed.append(
            {
                "window_start_ms": start,
                "window_end_ms": end,
                "count": count,
                "sum": total,
            }
        )
    return parsed


def _parse_snapshot_dedup(
    data: object, retention: int | None, watermark: int | None
) -> dict[str, tuple[int, float]]:
    if not isinstance(data, list):
        raise SnapshotInvalidError("dedup must be an array")
    if retention is None and data:
        raise SnapshotInvalidError("dedup records require dedup_retention_ms")
    dedup: dict[str, tuple[int, float]] = {}
    previous_id: str | None = None
    for entry in data:
        if not isinstance(entry, dict) or set(entry) != _SNAPSHOT_DEDUP_FIELDS:
            raise SnapshotInvalidError(
                "dedup entries must have exactly event_id, timestamp_ms and value"
            )
        event_id = entry["event_id"]
        if not isinstance(event_id, str) or not event_id:
            raise SnapshotInvalidError("event_id must be a non-empty string")
        timestamp_ms = entry["timestamp_ms"]
        if not _is_int(timestamp_ms):
            raise SnapshotInvalidError("dedup timestamp_ms must be an integer")
        value = entry["value"]
        if not _is_finite_number(value):
            raise SnapshotInvalidError("dedup value must be a finite number")
        if previous_id is not None and event_id <= previous_id:
            raise SnapshotInvalidError(
                "dedup records must be unique and sorted ascending by event_id"
            )
        previous_id = event_id
        if watermark is not None and timestamp_ms < watermark - retention:
            raise SnapshotInvalidError(
                "dedup record older than the retention eviction horizon"
            )
        dedup[event_id] = (timestamp_ms, value)
    return dedup


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

    def snapshot(self) -> dict:
        """Export a consistent point-in-time snapshot of all stream state."""
        with self._lock:
            return {
                "format_version": _SNAPSHOT_FORMAT_VERSION,
                "streams": [
                    _stream_snapshot(self._streams[name])
                    for name in sorted(self._streams)
                ],
            }

    def restore_snapshot(self, document: object) -> int:
        """Atomically install a snapshot on a stream-less instance.

        The conflict check, validation and state swap all happen under
        the lock, so a failed restore leaves the instance completely
        empty and a successful one becomes visible all at once.
        """
        with self._lock:
            if self._streams:
                raise RestoreConflictError("instance already has streams")
            streams = _parse_snapshot(document)
            self._streams = streams
            return len(streams)

    def _get(self, name: str) -> _Stream:
        try:
            return self._streams[name]
        except KeyError:
            raise StreamNotFoundError(name) from None
