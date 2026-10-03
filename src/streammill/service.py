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

Streams may optionally be created with a positive ``slide_ms`` (no
larger than ``window_ms`` and dividing it evenly). Such streams use
fixed-step overlapping sliding windows: an accepted event at timestamp
``t`` is counted once in every window ``[start, start + window_ms)``
whose ``start`` is an integer multiple of ``slide_ms`` and satisfies
``start <= t < start + window_ms`` (negative timestamps follow the same
mathematical grid). Every window still finalizes independently once the
watermark reaches its own ``window_end_ms + allowed_lateness_ms`` and is
emitted exactly once, ordered by window start. Without ``slide_ms`` the
stream is the plain tumbling stream above and every public shape is
unchanged.

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

Streams created with a non-negative ``auto_watermark_lag_ms`` track the
maximum ``timestamp_ms`` of every successfully accepted event. After an
event is aggregated (and its id registered, when deduplicating) the
watermark advances atomically to
``max(current_watermark, max_event_timestamp - auto_watermark_lag_ms)``;
the lateness/dedup decision for that event uses the watermark as it was
before the advance. Dropped, duplicate or conflicting events never move
the maximum event timestamp or the watermark. Manual watermark posts keep
working on automatic streams and subsequent automatic advances never
regress below a manual watermark. Event responses on automatic streams
additionally carry the post-processing ``watermark_ms`` (``null`` until a
watermark exists) and the windows finalized by that response.

A portable full-state snapshot is available through
``Service.snapshot`` / ``Service.restore_snapshot``: export produces a
self-contained JSON document (``format_version`` 1) capturing config,
watermark, open windows, finalized results and retained dedup records;
automatic streams additionally carry ``auto_watermark_lag_ms`` and
``max_event_timestamp_ms``; sliding streams additionally carry
``slide_ms`` (a document without it restores as a tumbling stream).
Restore is only accepted on an instance
without any streams, validates the document strictly (never silently
repairing it) and publishes the whole state atomically.

Joins and disk persistence remain out of scope.
"""

from __future__ import annotations

import math
import threading

from . import __version__

SNAPSHOT_FORMAT_VERSION = 1


class StreamExistsError(Exception):
    """Raised when creating a stream that already exists."""


class StreamNotFoundError(Exception):
    """Raised when addressing a stream that does not exist."""


class WatermarkRegressionError(Exception):
    """Raised when a new watermark would move the stream backwards."""


class EventIdConflictError(Exception):
    """Raised when a retained event id is resubmitted with different data."""


class SnapshotError(Exception):
    """Raised when a restore document fails structural or semantic validation."""


class RestoreConflictError(Exception):
    """Raised when restore targets an instance that already has streams."""


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


class _Stream:
    """Mutable state of one named stream. Guarded by the Service lock."""

    def __init__(
        self,
        name: str,
        window_ms: int,
        allowed_lateness_ms: int,
        dedup_retention_ms: int | None = None,
        auto_watermark_lag_ms: int | None = None,
        slide_ms: int | None = None,
    ) -> None:
        self.name = name
        self.window_ms = window_ms
        self.allowed_lateness_ms = allowed_lateness_ms
        self.dedup_retention_ms = dedup_retention_ms
        self.auto_watermark_lag_ms = auto_watermark_lag_ms
        # None selects the tumbling grid (step == window_ms); otherwise the
        # stream is sliding with this fixed, evenly-dividing step.
        self.slide_ms = slide_ms
        self.watermark: int | None = None
        # Maximum timestamp_ms of a successfully accepted event; only
        # maintained on automatic streams, None until the first such event.
        self.max_event_timestamp: int | None = None
        # window_start_ms -> [count, sum]; only non-empty windows exist.
        self._windows: dict[int, list] = {}
        self._finalized: list[dict] = []
        self._finalized_starts: set[int] = set()
        # event_id -> (timestamp_ms, value) for accepted, not yet evicted ids.
        self._dedup: dict[str, tuple[int, float]] = {}

    @property
    def _step_ms(self) -> int:
        """Grid spacing: slide_ms for sliding streams, else window_ms."""
        return self.slide_ms if self.slide_ms is not None else self.window_ms

    def _window_starts(self, timestamp_ms: int) -> list[int]:
        """Starts of every grid window covering ``timestamp_ms``.

        Windows are ``[start, start + window_ms)`` with ``start`` on the
        grid of multiples of the step; the floor division keeps the same
        boundaries for negative timestamps. The list is ascending, which
        is also the finalization/results order.
        """
        step = self._step_ms
        last = (timestamp_ms // step) * step
        first = last - (self.window_ms - step)
        return list(range(first, last + 1, step))

    def add_event(
        self, timestamp_ms: int, value: float, event_id: str | None = None
    ) -> dict:
        """Process one event against the pre-event watermark.

        On automatic streams every response additionally carries the
        post-processing ``watermark_ms`` (``None`` until a watermark exists)
        and the ``finalized`` windows produced by this response; duplicate
        and dropped events leave the watermark untouched and finalize
        nothing. Accepted-event bookkeeping (maximum event timestamp plus
        the resulting monotone watermark advance) only runs after
        aggregation and dedup registration.
        """
        automatic = self.auto_watermark_lag_ms is not None
        if self.dedup_retention_ms is not None:
            retained = self._dedup.get(event_id)
            if retained is not None:
                if retained[0] == timestamp_ms and retained[1] == value:
                    outcome: dict = {"dropped": False, "duplicate": True}
                    if automatic:
                        outcome.update(watermark_ms=self.watermark, finalized=[])
                    return outcome
                raise EventIdConflictError(event_id)
            if self._is_too_late(timestamp_ms):
                # Unseen too-late ids are dropped without being remembered.
                outcome = {"dropped": True, "duplicate": False}
                if automatic:
                    outcome.update(watermark_ms=self.watermark, finalized=[])
                return outcome
            self._aggregate(timestamp_ms, value)
            self._dedup[event_id] = timestamp_ms, value
            outcome = {"dropped": False, "duplicate": False}
        else:
            if self._is_too_late(timestamp_ms):
                outcome = {"dropped": True}
                if automatic:
                    outcome.update(watermark_ms=self.watermark, finalized=[])
                return outcome
            self._aggregate(timestamp_ms, value)
            outcome = {"dropped": False}
        if automatic:
            newly = self._note_accepted_event(timestamp_ms)
            outcome.update(watermark_ms=self.watermark, finalized=newly)
        return outcome

    def _is_too_late(self, timestamp_ms: int) -> bool:
        return (
            self.watermark is not None
            and timestamp_ms < self.watermark - self.allowed_lateness_ms
        )

    def _note_accepted_event(self, timestamp_ms: int) -> list[dict]:
        """Advance the max event timestamp and then the watermark."""
        if (
            self.max_event_timestamp is None
            or timestamp_ms > self.max_event_timestamp
        ):
            self.max_event_timestamp = timestamp_ms
        target = self.max_event_timestamp - self.auto_watermark_lag_ms
        if self.watermark is None or target > self.watermark:
            # Publish through the shared path so finalization and dedup
            # eviction behave exactly like a manual advance.
            return self.advance_watermark(target)
        return []

    def _aggregate(self, timestamp_ms: int, value: float) -> None:
        for start in self._window_starts(timestamp_ms):
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

    def to_snapshot(self) -> dict:
        """Serialize this stream as one entry of a version-1 snapshot."""
        entry: dict = {
            "name": self.name,
            "window_ms": self.window_ms,
            "allowed_lateness_ms": self.allowed_lateness_ms,
            "dedup_retention_ms": self.dedup_retention_ms,
            "watermark_ms": self.watermark,
            "windows": [
                {
                    "window_start_ms": start,
                    "window_end_ms": start + self.window_ms,
                    "count": self._windows[start][0],
                    "sum": self._windows[start][1],
                }
                for start in sorted(self._windows)
                if start not in self._finalized_starts
            ],
            "finalized": sorted(
                (dict(row) for row in self._finalized),
                key=lambda row: row["window_start_ms"],
            ),
        }
        if self.auto_watermark_lag_ms is not None:
            # Automatic streams publish the pair together; manual streams
            # keep the historical document shape exactly.
            entry["auto_watermark_lag_ms"] = self.auto_watermark_lag_ms
            entry["max_event_timestamp_ms"] = self.max_event_timestamp
        if self.slide_ms is not None:
            # Sliding streams publish the step; documents without it
            # restore as tumbling streams.
            entry["slide_ms"] = self.slide_ms
        if self.dedup_retention_ms is not None:
            entry["dedup_records"] = [
                {"event_id": event_id, "timestamp_ms": kept[0], "value": kept[1]}
                for event_id, kept in sorted(self._dedup.items())
            ]
        return entry

    @classmethod
    def from_snapshot(cls, data: object) -> "_Stream":
        """Rebuild one stream from a snapshot entry or raise SnapshotError.

        Every structural and semantic rule is checked and nothing is
        repaired: callers get a fully formed stream or nothing.
        """
        if not isinstance(data, dict):
            raise SnapshotError("stream entry must be an object")
        allowed = {
            "name",
            "window_ms",
            "allowed_lateness_ms",
            "dedup_retention_ms",
            "watermark_ms",
            "windows",
            "finalized",
            "dedup_records",
            "auto_watermark_lag_ms",
            "max_event_timestamp_ms",
            "slide_ms",
        }
        extra = sorted(set(data) - allowed)
        if extra:
            raise SnapshotError(f"stream entry has unexpected field: {extra[0]}")
        required = {
            "name",
            "window_ms",
            "allowed_lateness_ms",
            "dedup_retention_ms",
            "watermark_ms",
            "windows",
            "finalized",
        }
        missing = sorted(required - set(data))
        if missing:
            raise SnapshotError(f"stream entry missing required field: {missing[0]}")

        # The automatic-watermark pair must appear together; a document
        # without it restores as a manual stream.
        has_lag = "auto_watermark_lag_ms" in data
        has_max = "max_event_timestamp_ms" in data
        if has_lag != has_max:
            raise SnapshotError(
                f"stream entry automatic-watermark fields must appear together: "
                f"auto_watermark_lag_ms and max_event_timestamp_ms"
            )
        auto_lag = data.get("auto_watermark_lag_ms") if has_lag else None
        if has_lag and (not _is_int(auto_lag) or auto_lag < 0):
            raise SnapshotError(
                f"stream {data.get('name')!r}: auto_watermark_lag_ms must be a "
                f"non-negative integer"
            )
        max_event_timestamp = data.get("max_event_timestamp_ms") if has_max else None
        if has_max and max_event_timestamp is not None and not _is_int(
            max_event_timestamp
        ):
            raise SnapshotError(
                f"stream {data.get('name')!r}: max_event_timestamp_ms must be an "
                f"integer or null"
            )

        name = data.get("name")
        if not isinstance(name, str) or not name:
            raise SnapshotError("stream name must be a non-empty string")
        window_ms = data.get("window_ms")
        if not _is_int(window_ms) or window_ms <= 0:
            raise SnapshotError(f"stream {name!r}: window_ms must be a positive integer")
        slide_ms = data.get("slide_ms") if "slide_ms" in data else None
        if "slide_ms" in data and (
            not _is_int(slide_ms)
            or slide_ms <= 0
            or slide_ms > window_ms
            or window_ms % slide_ms != 0
        ):
            raise SnapshotError(
                f"stream {name!r}: slide_ms must be a positive integer no larger "
                f"than window_ms that evenly divides it"
            )
        allowed_lateness_ms = data.get("allowed_lateness_ms")
        if not _is_int(allowed_lateness_ms) or allowed_lateness_ms < 0:
            raise SnapshotError(
                f"stream {name!r}: allowed_lateness_ms must be a non-negative integer"
            )
        retention = data.get("dedup_retention_ms")
        if retention is not None and (not _is_int(retention) or retention <= 0):
            raise SnapshotError(
                f"stream {name!r}: dedup_retention_ms must be a positive integer"
            )
        if retention is not None and retention < allowed_lateness_ms:
            raise SnapshotError(
                f"stream {name!r}: dedup_retention_ms must be at least allowed_lateness_ms"
            )
        watermark = data.get("watermark_ms")
        if watermark is not None and not _is_int(watermark):
            raise SnapshotError(f"stream {name!r}: watermark_ms must be an integer or null")
        if max_event_timestamp is not None and watermark is None:
            raise SnapshotError(
                f"stream {name!r}: non-null max_event_timestamp_ms requires a "
                f"non-null watermark"
            )

        stream = cls(
            name,
            window_ms,
            allowed_lateness_ms,
            retention,
            auto_lag,
            slide_ms,
        )
        stream.watermark = watermark
        stream.max_event_timestamp = max_event_timestamp

        finalized_rows = cls._parse_window_list(
            data.get("finalized"),
            name,
            window_ms,
            "finalized",
            with_stream=True,
            step_ms=stream._step_ms,
        )
        if finalized_rows and watermark is None:
            raise SnapshotError(
                f"stream {name!r}: finalized windows require a non-null watermark"
            )
        for row in finalized_rows:
            start = row["window_start_ms"]
            if watermark is not None and watermark < start + window_ms + allowed_lateness_ms:
                raise SnapshotError(
                    f"stream {name!r}: finalized window {start} requires a watermark "
                    f"of at least {start + window_ms + allowed_lateness_ms}"
                )
            if row["stream"] != name:
                raise SnapshotError(
                    f"stream {name!r}: finalized row labels stream {row['stream']!r}"
                )
            stream._finalized_starts.add(start)
            stream._finalized.append(dict(row))

        open_rows = cls._parse_window_list(
            data.get("windows"),
            name,
            window_ms,
            "windows",
            with_stream=False,
            step_ms=stream._step_ms,
        )
        for row in open_rows:
            start = row["window_start_ms"]
            if start in stream._finalized_starts:
                raise SnapshotError(
                    f"stream {name!r}: window {start} is both open and finalized"
                )
            if watermark is not None and watermark >= start + window_ms + allowed_lateness_ms:
                raise SnapshotError(
                    f"stream {name!r}: open window {start} should already be finalized "
                    f"at watermark {watermark}"
                )
            stream._windows[start] = [row["count"], row["sum"]]

        if retention is not None:
            records = data.get("dedup_records")
            stream._load_dedup_records(records)
        elif "dedup_records" in data:
            raise SnapshotError(
                f"stream {name!r}: dedup_records present without dedup_retention_ms"
            )

        if auto_lag is not None and max_event_timestamp is None:
            # No accepted event has ever happened, so no window can carry
            # aggregates either.
            if stream._windows or stream._finalized_starts:
                raise SnapshotError(
                    f"stream {name!r}: null max_event_timestamp_ms requires no "
                    f"open or finalized windows"
                )
        if auto_lag is not None and max_event_timestamp is not None:
            # The remembered maximum must be backed by an accepted event, so
            # every window it aggregates into has to be open or finalized
            # (one window on tumbling streams, several overlapping ones on
            # sliding streams).
            known = stream._windows.keys() | stream._finalized_starts
            for covered in stream._window_starts(max_event_timestamp):
                if covered not in known:
                    raise SnapshotError(
                        f"stream {name!r}: max_event_timestamp_ms "
                        f"{max_event_timestamp} aggregates into unknown window "
                        f"{covered}"
                    )
            # A valid automatic state always has watermark >= max event
            # timestamp minus the configured lag.
            if watermark < max_event_timestamp - auto_lag:
                raise SnapshotError(
                    f"stream {name!r}: watermark {watermark} is below "
                    f"max_event_timestamp_ms - auto_watermark_lag_ms "
                    f"({max_event_timestamp - auto_lag})"
                )
        return stream

    @staticmethod
    def _parse_window_list(
        value: object,
        name: str,
        window_ms: int,
        where: str,
        *,
        with_stream: bool,
        step_ms: int | None = None,
    ) -> list[dict]:
        if not isinstance(value, list):
            raise SnapshotError(f"stream {name!r}: {where} must be an array")
        if step_ms is None:
            step_ms = window_ms
        fields = (
            {"stream", "window_start_ms", "window_end_ms", "count", "sum"}
            if with_stream
            else {"window_start_ms", "window_end_ms", "count", "sum"}
        )
        rows: list[dict] = []
        previous: int | None = None
        for raw in value:
            if not isinstance(raw, dict):
                raise SnapshotError(f"stream {name!r}: {where} entries must be objects")
            extra = sorted(set(raw) - fields)
            if extra:
                raise SnapshotError(
                    f"stream {name!r}: {where} entry has unexpected field: {extra[0]}"
                )
            missing = sorted(fields - set(raw))
            if missing:
                raise SnapshotError(
                    f"stream {name!r}: {where} entry missing field: {missing[0]}"
                )
            start = raw["window_start_ms"]
            end = raw["window_end_ms"]
            count = raw["count"]
            total = raw["sum"]
            if not _is_int(start):
                raise SnapshotError(
                    f"stream {name!r}: {where} window_start_ms must be an integer"
                )
            if start % step_ms != 0:
                raise SnapshotError(
                    f"stream {name!r}: window {start} is not aligned to the "
                    f"window grid step {step_ms}"
                )
            if not _is_int(end) or end != start + window_ms:
                raise SnapshotError(
                    f"stream {name!r}: window {start} end must be {start + window_ms}"
                )
            if not _is_int(count) or count <= 0:
                raise SnapshotError(
                    f"stream {name!r}: window {start} count must be a positive integer"
                )
            if not _is_finite_number(total):
                raise SnapshotError(
                    f"stream {name!r}: window {start} sum must be a finite number"
                )
            if with_stream and (
                not isinstance(raw["stream"], str) or not raw["stream"]
            ):
                raise SnapshotError(
                    f"stream {name!r}: finalized row stream must be a non-empty string"
                )
            if previous is not None and start <= previous:
                raise SnapshotError(
                    f"stream {name!r}: {where} must be strictly ordered by window_start_ms"
                )
            previous = start
            rows.append(raw)
        return rows

    def _load_dedup_records(self, records: object) -> None:
        if not isinstance(records, list):
            raise SnapshotError(f"stream {self.name!r}: dedup_records must be an array")
        assert self.dedup_retention_ms is not None
        buckets = set(self._windows) | self._finalized_starts
        bucket_counts: dict[int, int] = {}
        previous: str | None = None
        for raw in records:
            if not isinstance(raw, dict):
                raise SnapshotError(
                    f"stream {self.name!r}: dedup_records entries must be objects"
                )
            fields = {"event_id", "timestamp_ms", "value"}
            extra = sorted(set(raw) - fields)
            if extra:
                raise SnapshotError(
                    f"stream {self.name!r}: dedup record has unexpected field: {extra[0]}"
                )
            missing = sorted(fields - set(raw))
            if missing:
                raise SnapshotError(
                    f"stream {self.name!r}: dedup record missing field: {missing[0]}"
                )
            event_id = raw["event_id"]
            timestamp_ms = raw["timestamp_ms"]
            value = raw["value"]
            if not isinstance(event_id, str) or not event_id:
                raise SnapshotError(
                    f"stream {self.name!r}: event_id must be a non-empty string"
                )
            if not _is_int(timestamp_ms):
                raise SnapshotError(
                    f"stream {self.name!r}: dedup record timestamp_ms must be an integer"
                )
            if not _is_finite_number(value):
                raise SnapshotError(
                    f"stream {self.name!r}: dedup record value must be a finite number"
                )
            if previous is not None and event_id <= previous:
                raise SnapshotError(
                    f"stream {self.name!r}: dedup_records must be strictly ordered by event_id"
                )
            previous = event_id
            if (
                self.watermark is not None
                and timestamp_ms < self.watermark - self.dedup_retention_ms
            ):
                raise SnapshotError(
                    f"stream {self.name!r}: dedup record {event_id!r} is past the "
                    f"retention horizon and must have been evicted"
                )
            for start in self._window_starts(timestamp_ms):
                if start not in buckets:
                    raise SnapshotError(
                        f"stream {self.name!r}: dedup record {event_id!r} aggregates "
                        f"into unknown window {start}"
                    )
                bucket_counts[start] = bucket_counts.get(start, 0) + 1
            self._dedup[event_id] = (timestamp_ms, value)
        finalized_counts = {
            row["window_start_ms"]: row["count"] for row in self._finalized
        }
        for bucket, retained in bucket_counts.items():
            if bucket in self._windows:
                total = self._windows[bucket][0]
            else:
                total = finalized_counts[bucket]
            if retained > total:
                raise SnapshotError(
                    f"stream {self.name!r}: window {bucket} has more retained ids "
                    f"({retained}) than aggregated events ({total})"
                )


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
        auto_watermark_lag_ms: int | None = None,
        slide_ms: int | None = None,
    ) -> dict:
        with self._lock:
            if name in self._streams:
                raise StreamExistsError(name)
            self._streams[name] = _Stream(
                name,
                window_ms,
                allowed_lateness_ms,
                dedup_retention_ms,
                auto_watermark_lag_ms,
                slide_ms,
            )
        payload = {
            "stream": name,
            "window_ms": window_ms,
            "allowed_lateness_ms": allowed_lateness_ms,
        }
        if dedup_retention_ms is not None:
            payload["dedup_retention_ms"] = dedup_retention_ms
        if auto_watermark_lag_ms is not None:
            payload["auto_watermark_lag_ms"] = auto_watermark_lag_ms
        if slide_ms is not None:
            payload["slide_ms"] = slide_ms
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
        """Return a consistent point-in-time, JSON-serializable snapshot.

        The whole document is assembled while holding the service lock, so
        concurrent creates, events and watermark advances are either fully
        included or fully excluded.
        """
        with self._lock:
            return {
                "format_version": SNAPSHOT_FORMAT_VERSION,
                "streams": [
                    self._streams[name].to_snapshot() for name in sorted(self._streams)
                ],
            }

    def restore_snapshot(self, document: object) -> int:
        """Replace instance state with a validated snapshot, atomically.

        Only callable on an instance without streams; otherwise raises
        RestoreConflictError, regardless of document content. Any invalid
        document raises SnapshotError and leaves the (empty) instance
        untouched. Returns the restored stream count.
        """
        with self._lock:
            # Conflict takes priority over every content check: an instance
            # with streams always gets restore_conflict.
            if self._streams:
                raise RestoreConflictError(
                    "restore is only allowed on an instance without streams"
                )
            if not isinstance(document, dict):
                raise SnapshotError("snapshot document must be a JSON object")
            if set(document) != {"format_version", "streams"}:
                extra = sorted(set(document) - {"format_version", "streams"})
                missing = sorted({"format_version", "streams"} - set(document))
                if extra:
                    raise SnapshotError(
                        f"snapshot document has unexpected field: {extra[0]}"
                    )
                raise SnapshotError(
                    f"snapshot document missing required field: {missing[0]}"
                )
            version = document["format_version"]
            if not _is_int(version):
                raise SnapshotError("format_version must be an integer")
            if version != SNAPSHOT_FORMAT_VERSION:
                raise SnapshotError(f"unsupported format_version: {version}")
            entries = document["streams"]
            if not isinstance(entries, list):
                raise SnapshotError("streams must be an array")

            # Build and validate everything before the single publishing
            # assignment, so a failure leaves no partial state and concurrent
            # requests never observe half-restored streams.
            restored: dict[str, _Stream] = {}
            previous_name: str | None = None
            for entry in entries:
                stream = _Stream.from_snapshot(entry)
                if stream.name in restored:
                    raise SnapshotError(
                        f"duplicate stream name in snapshot: {stream.name!r}"
                    )
                if previous_name is not None and stream.name <= previous_name:
                    raise SnapshotError("streams must be strictly ordered by name")
                previous_name = stream.name
                restored[stream.name] = stream
            self._streams = restored
            return len(restored)

    def _get(self, name: str) -> _Stream:
        try:
            return self._streams[name]
        except KeyError:
            raise StreamNotFoundError(name) from None
