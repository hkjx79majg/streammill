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

In-process dimension tables provide the lookup side of an optional
current-value stream-table join:

* ``POST /tables`` creates a named table; rows are written with
  ``PUT``-style upserts of non-empty string ``key``/``label`` pairs and
  report whether the stored label actually ``changed``;
* a stream may be created with ``lookup_table`` naming an existing
  table. Events on such a stream must carry a non-empty ``lookup_key``;
  after the usual dedup and lateness decisions the current label is read
  atomically (an unknown key is rejected with ``lookup_key_not_found``
  and leaves all state untouched) and the event is aggregated both into
  the plain windows and into per-window ``(lookup_key, label)`` groups.
  Later table writes only affect later events. Dedup records on joined
  streams include the ``lookup_key``, so resubmitting an id with a
  different key is an ``event_id_conflict``;
* joined groups finalize together with their window and are exposed
  through ``Service.joined_results``, ordered by window start, lookup
  key and label.

Snapshots export as ``format_version`` 2 (adding ``tables`` plus
``lookup_table``/``joined_windows``/``joined_finalized`` on joined
streams) whenever any table exists, and restore stays compatible with
version 1 documents. When the join is unused every public shape and the
version 1 snapshot are unchanged.

Streams may optionally materialize a window change feed by passing a
positive ``change_retention`` at creation time (echoed in the create
response; without it the stream behaves exactly as before). On such
streams every state change is published as a totally ordered sequence of
records, numbered by a ``seq`` that starts at 1 and never regresses:

* every successfully aggregated event appends one ``upsert`` record per
  affected base window (ascending by window start on sliding streams),
  carrying the post-event ``count``/``sum`` of that window;
* every watermark advance that finalizes windows appends one ``final``
  record per newly finalized window (ascending by window start) with the
  final aggregates — on automatic streams the upserts of the triggering
  event always precede the finals of the resulting advance, and an
  advance without newly finalized windows appends nothing;
* duplicates, too-late drops, conflicts, unknown lookup keys and
  validation failures never consume a sequence number, and joined
  streams publish only the base-window changes.

``Service.changes`` returns the retained records with ``seq`` greater
than a cursor, ascending, together with the current ``latest_seq``.
Only the newest ``change_retention`` records are kept (older ones are
trimmed after every commit, without moving ``latest_seq``); a cursor
past ``latest_seq`` raises ChangeCursorAheadError and a cursor that has
fallen behind the oldest retained record raises
ChangeCursorExpiredError.

Instances with at least one change-feed stream export snapshots as
``format_version`` 3, which always carries the ``tables`` array and, on
enabled streams, ``change_retention``/``latest_seq``/``changes``.
Restore validates the retained records strictly (sequence continuity
against ``latest_seq`` and the retention cap, ordering, window
alignment, and the last retained record of every window against the
aggregated state) and publishes the feed so the cursor and the next
sequence number continue exactly as on the uninterrupted instance.
Version 1 and 2 documents restore as before, and instances without a
change feed keep exporting their original versions and shapes.

Streams may optionally enable recoverable atomic batch ingestion by
passing a positive ``batch_retention`` at creation time (echoed in the
create response; without it the stream behaves exactly as before and the
batch entry point raises BatchIngestNotEnabledError). On such streams
``Service.add_batch`` accepts a non-empty ``batch_id`` plus 1 to 1000
events that each follow the stream's single-event rules:

* the whole batch is applied at one consistent point, in input order and
  under the service lock, reusing the per-event dedup, lateness, lookup,
  window, automatic-watermark, finalization and change-feed semantics —
  no concurrent request can observe an intermediate state, and on
  automatic streams each element sees the watermark left by the
  previous one;
* the first event-id conflict or unknown lookup key aborts the batch and
  rolls back every state the earlier elements touched (aggregates, dedup
  records, watermark, finalized results, joined groups and change
  sequence), so a failed batch leaves the stream exactly as before and
  does not occupy its identifier; too-late drops and exact duplicates
  remain successful outcomes;
* a successful batch is remembered together with its request and full
  response. Within the retention a retry carrying the same field values
  in the same event order replays the stored response without writing
  again (and without refreshing the retention order), while the same
  ``batch_id`` with different content raises BatchIdConflictError. Only
  the newest ``batch_retention`` records are kept; evicted identifiers
  may be reused, and concurrent submissions of one identifier commit at
  most once.

Instances with at least one batch-enabled stream export snapshots as
``format_version`` 4, which always carries the ``tables`` array and, on
enabled streams, ``batch_retention`` plus the retained batch records in
commit order. Restore validates the records strictly (identifier
uniqueness, the retention cap, and the request/response shapes against
the stream's features) and publishes them so replay and eviction behave
exactly as on the uninterrupted instance. Older document versions
restore as before.

Disk persistence remains out of scope.
"""

from __future__ import annotations

import copy
import math
import threading

from . import __version__

SNAPSHOT_FORMAT_VERSION = 1
SNAPSHOT_FORMAT_VERSION_JOIN = 2
SNAPSHOT_FORMAT_VERSION_CHANGES = 3
SNAPSHOT_FORMAT_VERSION_BATCHES = 4


class StreamExistsError(Exception):
    """Raised when creating a stream that already exists."""


class StreamNotFoundError(Exception):
    """Raised when addressing a stream that does not exist."""


class TableExistsError(Exception):
    """Raised when creating a table that already exists."""


class TableNotFoundError(Exception):
    """Raised when addressing a table that does not exist."""


class LookupKeyNotFoundError(Exception):
    """Raised when an event's lookup_key has no row in the table."""


class JoinNotEnabledError(Exception):
    """Raised when querying joined results on a plain stream."""


class ChangeFeedNotEnabledError(Exception):
    """Raised when reading the change feed of a stream without one."""


class ChangeCursorAheadError(Exception):
    """Raised when a change cursor is past the latest sequence number."""


class ChangeCursorExpiredError(Exception):
    """Raised when a change cursor fell behind the retained records."""


class BatchIngestNotEnabledError(Exception):
    """Raised when submitting a batch to a stream without batch ingest."""


class BatchIdConflictError(Exception):
    """Raised when a retained batch id is resubmitted with different content."""


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
        lookup_table: str | None = None,
        change_retention: int | None = None,
        batch_retention: int | None = None,
    ) -> None:
        self.name = name
        self.window_ms = window_ms
        self.allowed_lateness_ms = allowed_lateness_ms
        self.dedup_retention_ms = dedup_retention_ms
        self.auto_watermark_lag_ms = auto_watermark_lag_ms
        # None selects the tumbling grid (step == window_ms); otherwise the
        # stream is sliding with this fixed, evenly-dividing step.
        self.slide_ms = slide_ms
        # Name of the dimension table this stream joins against, if any.
        self.lookup_table = lookup_table
        # Maximum number of retained change-feed records; None disables
        # the feed entirely.
        self.change_retention = change_retention
        # Maximum number of retained batch records; None disables batch
        # ingestion entirely.
        self.batch_retention = batch_retention
        self.watermark: int | None = None
        # Maximum timestamp_ms of a successfully accepted event; only
        # maintained on automatic streams, None until the first such event.
        self.max_event_timestamp: int | None = None
        # window_start_ms -> [count, sum]; only non-empty windows exist.
        self._windows: dict[int, list] = {}
        self._finalized: list[dict] = []
        self._finalized_starts: set[int] = set()
        # event_id -> (timestamp_ms, value) for accepted, not yet evicted
        # ids; joined streams append the lookup_key to the tuple.
        self._dedup: dict[str, tuple] = {}
        # Joined streams only: window_start_ms -> (lookup_key, label) ->
        # [count, sum] for open windows, plus the finalized group rows in
        # results order (window start, lookup_key, label).
        self._joined_windows: dict[int, dict[tuple[str, str], list]] = {}
        self._joined_finalized: list[dict] = []
        # Change feed: the next record gets _latest_seq + 1; _changes
        # holds at most change_retention records, oldest first.
        self._latest_seq = 0
        self._changes: list[dict] = []
        # Retained batch records in commit order, at most batch_retention
        # of them; each is {"batch_id", "request", "response"}.
        self._batches: list[dict] = []

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
        self,
        timestamp_ms: int,
        value: float,
        event_id: str | None = None,
        lookup_key: str | None = None,
        label_of=None,
    ) -> dict:
        """Process one event against the pre-event watermark.

        On automatic streams every response additionally carries the
        post-processing ``watermark_ms`` (``None`` until a watermark exists)
        and the ``finalized`` windows produced by this response; duplicate
        and dropped events leave the watermark untouched and finalize
        nothing. Accepted-event bookkeeping (maximum event timestamp plus
        the resulting monotone watermark advance) only runs after
        aggregation and dedup registration.

        On joined streams the current label for ``lookup_key`` is read
        through ``label_of`` after the dedup and lateness decisions but
        before any state mutation, so an unknown key raises
        LookupKeyNotFoundError with all state untouched. The dedup check
        includes the lookup key: an exact retry repeats timestamp, value
        and key, while the same id with a different key conflicts.
        """
        automatic = self.auto_watermark_lag_ms is not None
        joined = self.lookup_table is not None
        if self.dedup_retention_ms is not None:
            retained = self._dedup.get(event_id)
            if retained is not None:
                same = retained[0] == timestamp_ms and retained[1] == value
                if joined:
                    same = same and retained[2] == lookup_key
                if same:
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
            label = self._resolve_label(label_of, lookup_key)
            self._aggregate(timestamp_ms, value, lookup_key, label)
            self._dedup[event_id] = (
                (timestamp_ms, value, lookup_key)
                if joined
                else (timestamp_ms, value)
            )
            outcome = {"dropped": False, "duplicate": False}
        else:
            if self._is_too_late(timestamp_ms):
                outcome = {"dropped": True}
                if automatic:
                    outcome.update(watermark_ms=self.watermark, finalized=[])
                return outcome
            label = self._resolve_label(label_of, lookup_key)
            self._aggregate(timestamp_ms, value, lookup_key, label)
            outcome = {"dropped": False}
        if automatic:
            newly = self._note_accepted_event(timestamp_ms)
            outcome.update(watermark_ms=self.watermark, finalized=newly)
        return outcome

    def apply_batch(self, events: list[dict], label_of=None) -> list[dict]:
        """Apply a whole batch atomically, in input order.

        Every element goes through the regular single-event path, so
        dedup, lateness, lookup, window, automatic-watermark,
        finalization and change-feed semantics are identical to posting
        the events one by one; each element observes the state left by
        the previous ones. The first EventIdConflictError or
        LookupKeyNotFoundError rolls every mutation back — aggregates,
        dedup records, watermark, finalized results, joined groups and
        the change sequence — so a failed batch leaves no trace.
        """
        backup = self._state_backup()
        outcomes: list[dict] = []
        try:
            for event in events:
                outcomes.append(
                    self.add_event(
                        event["timestamp_ms"],
                        event["value"],
                        event.get("event_id"),
                        event.get("lookup_key"),
                        label_of,
                    )
                )
        except Exception:
            self._state_restore(backup)
            raise
        return outcomes

    def _state_backup(self) -> tuple:
        """Deep snapshot of every mutable structure a batch can touch."""
        return (
            self.watermark,
            self.max_event_timestamp,
            copy.deepcopy(self._windows),
            copy.deepcopy(self._finalized),
            set(self._finalized_starts),
            copy.deepcopy(self._dedup),
            copy.deepcopy(self._joined_windows),
            copy.deepcopy(self._joined_finalized),
            self._latest_seq,
            copy.deepcopy(self._changes),
        )

    def _state_restore(self, backup: tuple) -> None:
        (
            self.watermark,
            self.max_event_timestamp,
            self._windows,
            self._finalized,
            self._finalized_starts,
            self._dedup,
            self._joined_windows,
            self._joined_finalized,
            self._latest_seq,
            self._changes,
        ) = backup

    def _resolve_label(self, label_of, lookup_key: str | None):
        """Read the current label for a joined event, or None on plain streams."""
        if self.lookup_table is None:
            return None
        return label_of(lookup_key)

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

    def _aggregate(
        self,
        timestamp_ms: int,
        value: float,
        lookup_key: str | None = None,
        label: str | None = None,
    ) -> None:
        for start in self._window_starts(timestamp_ms):
            bucket = self._windows.setdefault(start, [0, 0])
            bucket[0] += 1
            bucket[1] += value
            # Base-window upsert with the post-event aggregates; sliding
            # streams publish one record per window, ascending by start.
            self._append_change("upsert", start, bucket[0], bucket[1])
            if self.lookup_table is not None:
                groups = self._joined_windows.setdefault(start, {})
                group = groups.setdefault((lookup_key, label), [0, 0])
                group[0] += 1
                group[1] += value

    def _append_change(self, kind: str, start: int, count: int, total: float) -> None:
        """Publish one change-feed record and trim to the retention cap.

        Sequence numbers are handed out consecutively from 1 and never
        regress; trimming only drops the oldest retained records.
        """
        if self.change_retention is None:
            return
        self._latest_seq += 1
        self._changes.append(
            {
                "seq": self._latest_seq,
                "kind": kind,
                "window_start_ms": start,
                "window_end_ms": start + self.window_ms,
                "count": count,
                "sum": total,
            }
        )
        overflow = len(self._changes) - self.change_retention
        if overflow > 0:
            del self._changes[:overflow]

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
                # The final record of a window always follows every
                # upsert of the triggering commit.
                self._append_change("final", start, count, total)
                if self.lookup_table is not None:
                    # Joined groups finalize synchronously with their window,
                    # ordered by (lookup_key, label) within it.
                    groups = self._joined_windows.pop(start, {})
                    for key, group_label in sorted(groups):
                        group_count, group_sum = groups[(key, group_label)]
                        self._joined_finalized.append(
                            {
                                "stream": self.name,
                                "window_start_ms": start,
                                "window_end_ms": end,
                                "lookup_key": key,
                                "label": group_label,
                                "count": group_count,
                                "sum": group_sum,
                            }
                        )
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
        if self.lookup_table is not None:
            # Joined streams publish the table reference plus the grouped
            # state; only format_version 2 documents may carry these.
            entry["lookup_table"] = self.lookup_table
            joined_open = []
            for start in sorted(self._joined_windows):
                groups = self._joined_windows[start]
                for key, group_label in sorted(groups):
                    group_count, group_sum = groups[(key, group_label)]
                    joined_open.append(
                        {
                            "window_start_ms": start,
                            "window_end_ms": start + self.window_ms,
                            "lookup_key": key,
                            "label": group_label,
                            "count": group_count,
                            "sum": group_sum,
                        }
                    )
            entry["joined_windows"] = joined_open
            entry["joined_finalized"] = [dict(row) for row in self._joined_finalized]
        if self.dedup_retention_ms is not None:
            records = []
            for event_id, kept in sorted(self._dedup.items()):
                record = {
                    "event_id": event_id,
                    "timestamp_ms": kept[0],
                    "value": kept[1],
                }
                if self.lookup_table is not None:
                    record["lookup_key"] = kept[2]
                records.append(record)
            entry["dedup_records"] = records
        if self.change_retention is not None:
            # Change-feed streams publish the config, the cursor and the
            # retained records; only format_version 3 documents may carry
            # these.
            entry["change_retention"] = self.change_retention
            entry["latest_seq"] = self._latest_seq
            entry["changes"] = [dict(record) for record in self._changes]
        if self.batch_retention is not None:
            # Batch-enabled streams publish the config plus the retained
            # batch records in commit order; only format_version 4
            # documents may carry these.
            entry["batch_retention"] = self.batch_retention
            entry["batches"] = copy.deepcopy(self._batches)
        return entry

    @classmethod
    def from_snapshot(
        cls,
        data: object,
        table_names: set[str] | None = None,
        allow_change_feed: bool = False,
        allow_batches: bool = False,
    ) -> "_Stream":
        """Rebuild one stream from a snapshot entry or raise SnapshotError.

        Every structural and semantic rule is checked and nothing is
        repaired: callers get a fully formed stream or nothing. Join
        fields (``lookup_table``, ``joined_windows``, ``joined_finalized``)
        are only accepted when ``table_names`` is given (a version 2
        document); the referenced table must be one of them. Change-feed
        fields (``change_retention``, ``latest_seq``, ``changes``) are
        only accepted when ``allow_change_feed`` is set (a version 3
        document). Batch fields (``batch_retention``, ``batches``) are
        only accepted when ``allow_batches`` is set (a version 4
        document).
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
        if table_names is not None:
            allowed |= {"lookup_table", "joined_windows", "joined_finalized"}
        if allow_change_feed:
            allowed |= {"change_retention", "latest_seq", "changes"}
        if allow_batches:
            allowed |= {"batch_retention", "batches"}
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

        lookup_table = data.get("lookup_table")
        if "lookup_table" in data:
            if not isinstance(lookup_table, str) or not lookup_table:
                raise SnapshotError(
                    f"stream {name!r}: lookup_table must be a non-empty string"
                )
            if lookup_table not in table_names:
                raise SnapshotError(
                    f"stream {name!r}: unknown lookup_table {lookup_table!r}"
                )
            for field in ("joined_windows", "joined_finalized"):
                if field not in data:
                    raise SnapshotError(
                        f"stream entry missing required field: {field}"
                    )
        else:
            for field in ("joined_windows", "joined_finalized"):
                if field in data:
                    raise SnapshotError(
                        f"stream {name!r}: {field} present without lookup_table"
                    )

        # The change-feed triple must appear together; a document without
        # it restores as a stream without a change feed.
        change_retention = data.get("change_retention")
        if "change_retention" in data:
            if not _is_int(change_retention) or change_retention <= 0:
                raise SnapshotError(
                    f"stream {name!r}: change_retention must be a positive integer"
                )
            for field in ("latest_seq", "changes"):
                if field not in data:
                    raise SnapshotError(
                        f"stream entry missing required field: {field}"
                    )
        else:
            for field in ("latest_seq", "changes"):
                if field in data:
                    raise SnapshotError(
                        f"stream {name!r}: {field} present without change_retention"
                    )
        latest_seq = data.get("latest_seq")
        if change_retention is not None and (
            not _is_int(latest_seq) or latest_seq < 0
        ):
            raise SnapshotError(
                f"stream {name!r}: latest_seq must be a non-negative integer"
            )

        # The batch pair must appear together; a document without it
        # restores as a stream without batch ingestion.
        batch_retention = data.get("batch_retention")
        if "batch_retention" in data:
            if not _is_int(batch_retention) or batch_retention <= 0:
                raise SnapshotError(
                    f"stream {name!r}: batch_retention must be a positive integer"
                )
            if "batches" not in data:
                raise SnapshotError("stream entry missing required field: batches")
        elif "batches" in data:
            raise SnapshotError(
                f"stream {name!r}: batches present without batch_retention"
            )

        stream = cls(
            name,
            window_ms,
            allowed_lateness_ms,
            retention,
            auto_lag,
            slide_ms,
            lookup_table,
            change_retention,
            batch_retention,
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

        if lookup_table is not None:
            cls._load_joined_state(stream, data, name, window_ms)

        if retention is not None:
            records = data.get("dedup_records")
            stream._load_dedup_records(records, joined=lookup_table is not None)
        elif "dedup_records" in data:
            raise SnapshotError(
                f"stream {name!r}: dedup_records present without dedup_retention_ms"
            )

        if change_retention is not None:
            stream._load_change_records(data["changes"], latest_seq)

        if batch_retention is not None:
            stream._load_batch_records(data["batches"])

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

    @staticmethod
    def _parse_joined_list(
        value: object,
        name: str,
        window_ms: int,
        where: str,
        *,
        with_stream: bool,
        step_ms: int,
    ) -> list[dict]:
        """Validate one joined-group array of a version 2 snapshot entry."""
        if not isinstance(value, list):
            raise SnapshotError(f"stream {name!r}: {where} must be an array")
        fields = {
            "window_start_ms",
            "window_end_ms",
            "lookup_key",
            "label",
            "count",
            "sum",
        }
        if with_stream:
            fields |= {"stream"}
        rows: list[dict] = []
        previous: tuple | None = None
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
            lookup_key = raw["lookup_key"]
            label = raw["label"]
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
            if not isinstance(lookup_key, str) or not lookup_key:
                raise SnapshotError(
                    f"stream {name!r}: {where} lookup_key must be a non-empty string"
                )
            if not isinstance(label, str) or not label:
                raise SnapshotError(
                    f"stream {name!r}: {where} label must be a non-empty string"
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
                    f"stream {name!r}: joined_finalized row stream must be a "
                    f"non-empty string"
                )
            order_key = (start, lookup_key, label)
            if previous is not None and order_key <= previous:
                raise SnapshotError(
                    f"stream {name!r}: {where} must be strictly ordered by "
                    f"window_start_ms, lookup_key, label"
                )
            previous = order_key
            rows.append(raw)
        return rows

    @classmethod
    def _load_joined_state(
        cls, stream: "_Stream", data: dict, name: str, window_ms: int
    ) -> None:
        """Load joined groups and check them against the base windows.

        The joined groups must partition the plain window aggregates
        exactly: every open or finalized base window has groups, and the
        per-window group counts and sums add up to the base totals.
        """
        open_rows = cls._parse_joined_list(
            data["joined_windows"],
            name,
            window_ms,
            "joined_windows",
            with_stream=False,
            step_ms=stream._step_ms,
        )
        for row in open_rows:
            start = row["window_start_ms"]
            groups = stream._joined_windows.setdefault(start, {})
            groups[(row["lookup_key"], row["label"])] = [row["count"], row["sum"]]

        finalized_rows = cls._parse_joined_list(
            data["joined_finalized"],
            name,
            window_ms,
            "joined_finalized",
            with_stream=True,
            step_ms=stream._step_ms,
        )
        for row in finalized_rows:
            if row["stream"] != name:
                raise SnapshotError(
                    f"stream {name!r}: joined_finalized row labels stream "
                    f"{row['stream']!r}"
                )
            stream._joined_finalized.append(dict(row))

        if set(stream._joined_windows) != set(stream._windows):
            raise SnapshotError(
                f"stream {name!r}: joined_windows do not match the open windows"
            )
        for start, groups in stream._joined_windows.items():
            cls._check_group_totals(
                name,
                start,
                sum(group[0] for group in groups.values()),
                sum(group[1] for group in groups.values()),
                stream._windows[start],
            )

        finalized_by_start = {
            row["window_start_ms"]: row for row in stream._finalized
        }
        joined_finalized_starts = {
            row["window_start_ms"] for row in stream._joined_finalized
        }
        if joined_finalized_starts != stream._finalized_starts:
            raise SnapshotError(
                f"stream {name!r}: joined_finalized do not match the finalized "
                f"windows"
            )
        grouped: dict[int, list[dict]] = {}
        for row in stream._joined_finalized:
            grouped.setdefault(row["window_start_ms"], []).append(row)
        for start, rows in grouped.items():
            base = finalized_by_start[start]
            cls._check_group_totals(
                name,
                start,
                sum(row["count"] for row in rows),
                sum(row["sum"] for row in rows),
                (base["count"], base["sum"]),
            )

    @staticmethod
    def _check_group_totals(
        name: str, start: int, count: int, total: float, base: tuple
    ) -> None:
        base_count, base_sum = base
        if count != base_count or not math.isclose(
            total, base_sum, rel_tol=1e-9, abs_tol=1e-9
        ):
            raise SnapshotError(
                f"stream {name!r}: joined groups of window {start} do not add "
                f"up to the base window aggregates"
            )

    def _load_dedup_records(self, records: object, *, joined: bool = False) -> None:
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
            if joined:
                fields |= {"lookup_key"}
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
            if joined and (
                not isinstance(raw["lookup_key"], str) or not raw["lookup_key"]
            ):
                raise SnapshotError(
                    f"stream {self.name!r}: dedup record lookup_key must be a "
                    f"non-empty string"
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
            self._dedup[event_id] = (
                (timestamp_ms, value, raw["lookup_key"])
                if joined
                else (timestamp_ms, value)
            )
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

    def _load_change_records(self, records: object, latest_seq: int) -> None:
        """Load retained change-feed records under strict validation.

        The retained records must be exactly the tail of the sequence a
        live instance would hold: consecutive sequence numbers ending at
        ``latest_seq``, at most ``change_retention`` of them (and exactly
        that many once the sequence has grown past the cap), aligned to
        the window grid, with at most one ``final`` per window and no
        record after it, and the last retained record of every window
        matching the aggregated state of that window.
        """
        assert self.change_retention is not None
        if not isinstance(records, list):
            raise SnapshotError(f"stream {self.name!r}: changes must be an array")
        if len(records) != min(latest_seq, self.change_retention):
            raise SnapshotError(
                f"stream {self.name!r}: retained changes are inconsistent "
                f"with latest_seq {latest_seq} and change_retention "
                f"{self.change_retention}"
            )
        known_windows = set(self._windows) | self._finalized_starts
        parsed: list[dict] = []
        previous_seq: int | None = None
        final_seen: set[int] = set()
        last_by_window: dict[int, dict] = {}
        for raw in records:
            if not isinstance(raw, dict):
                raise SnapshotError(
                    f"stream {self.name!r}: changes entries must be objects"
                )
            fields = {"seq", "kind", "window_start_ms", "window_end_ms", "count", "sum"}
            extra = sorted(set(raw) - fields)
            if extra:
                raise SnapshotError(
                    f"stream {self.name!r}: change record has unexpected field: "
                    f"{extra[0]}"
                )
            missing = sorted(fields - set(raw))
            if missing:
                raise SnapshotError(
                    f"stream {self.name!r}: change record missing field: {missing[0]}"
                )
            seq = raw["seq"]
            kind = raw["kind"]
            start = raw["window_start_ms"]
            end = raw["window_end_ms"]
            count = raw["count"]
            total = raw["sum"]
            if not _is_int(seq) or seq <= 0:
                raise SnapshotError(
                    f"stream {self.name!r}: change record seq must be a "
                    f"positive integer"
                )
            if previous_seq is not None and seq != previous_seq + 1:
                raise SnapshotError(
                    f"stream {self.name!r}: changes must have consecutive "
                    f"sequence numbers"
                )
            previous_seq = seq
            if kind not in ("upsert", "final"):
                raise SnapshotError(
                    f"stream {self.name!r}: change record kind must be "
                    f"'upsert' or 'final'"
                )
            if not _is_int(start):
                raise SnapshotError(
                    f"stream {self.name!r}: change record window_start_ms must "
                    f"be an integer"
                )
            if start % self._step_ms != 0:
                raise SnapshotError(
                    f"stream {self.name!r}: window {start} is not aligned to "
                    f"the window grid step {self._step_ms}"
                )
            if not _is_int(end) or end != start + self.window_ms:
                raise SnapshotError(
                    f"stream {self.name!r}: window {start} end must be "
                    f"{start + self.window_ms}"
                )
            if not _is_int(count) or count <= 0:
                raise SnapshotError(
                    f"stream {self.name!r}: window {start} count must be a "
                    f"positive integer"
                )
            if not _is_finite_number(total):
                raise SnapshotError(
                    f"stream {self.name!r}: window {start} sum must be a "
                    f"finite number"
                )
            if start not in known_windows:
                raise SnapshotError(
                    f"stream {self.name!r}: change record references unknown "
                    f"window {start}"
                )
            if start in final_seen:
                raise SnapshotError(
                    f"stream {self.name!r}: window {start} has records after "
                    f"its final record"
                )
            if kind == "final":
                if start not in self._finalized_starts:
                    raise SnapshotError(
                        f"stream {self.name!r}: window {start} has a final "
                        f"record but is not finalized"
                    )
                final_seen.add(start)
            record = {
                "seq": seq,
                "kind": kind,
                "window_start_ms": start,
                "window_end_ms": end,
                "count": count,
                "sum": total,
            }
            parsed.append(record)
            last_by_window[start] = record
        if parsed and parsed[-1]["seq"] != latest_seq:
            raise SnapshotError(
                f"stream {self.name!r}: latest_seq {latest_seq} does not match "
                f"the newest retained record"
            )
        finalized_by_start = {
            row["window_start_ms"]: row for row in self._finalized
        }
        for start, record in last_by_window.items():
            if start in self._windows:
                expected_kind = "upsert"
                expected_count, expected_sum = self._windows[start]
            else:
                expected_kind = "final"
                row = finalized_by_start[start]
                expected_count, expected_sum = row["count"], row["sum"]
            if record["kind"] != expected_kind:
                raise SnapshotError(
                    f"stream {self.name!r}: last retained record of window "
                    f"{start} must be a {expected_kind!r}"
                )
            if record["count"] != expected_count or not math.isclose(
                record["sum"], expected_sum, rel_tol=1e-9, abs_tol=1e-9
            ):
                raise SnapshotError(
                    f"stream {self.name!r}: last retained record of window "
                    f"{start} does not match the aggregated state"
                )
        self._changes = parsed
        self._latest_seq = latest_seq

    def _load_batch_records(self, records: object) -> None:
        """Load retained batch records under strict validation.

        The records are kept in commit order with unique identifiers and
        at most ``batch_retention`` of them; every request and response
        must match the shapes the batch entry point produces on this
        stream (event fields follow the stream's dedup/join features,
        outcomes follow its dedup/automatic features).
        """
        assert self.batch_retention is not None
        if not isinstance(records, list):
            raise SnapshotError(f"stream {self.name!r}: batches must be an array")
        if len(records) > self.batch_retention:
            raise SnapshotError(
                f"stream {self.name!r}: batches exceed batch_retention "
                f"{self.batch_retention}"
            )
        seen: set[str] = set()
        parsed: list[dict] = []
        for raw in records:
            if not isinstance(raw, dict):
                raise SnapshotError(
                    f"stream {self.name!r}: batches entries must be objects"
                )
            fields = {"batch_id", "request", "response"}
            extra = sorted(set(raw) - fields)
            if extra:
                raise SnapshotError(
                    f"stream {self.name!r}: batch record has unexpected field: "
                    f"{extra[0]}"
                )
            missing = sorted(fields - set(raw))
            if missing:
                raise SnapshotError(
                    f"stream {self.name!r}: batch record missing field: "
                    f"{missing[0]}"
                )
            batch_id = raw["batch_id"]
            if not isinstance(batch_id, str) or not batch_id:
                raise SnapshotError(
                    f"stream {self.name!r}: batch_id must be a non-empty string"
                )
            if batch_id in seen:
                raise SnapshotError(
                    f"stream {self.name!r}: duplicate batch_id {batch_id!r}"
                )
            seen.add(batch_id)
            request = self._parse_batch_request(raw["request"], batch_id)
            response = self._parse_batch_response(
                raw["response"], batch_id, len(request["events"])
            )
            parsed.append(
                {"batch_id": batch_id, "request": request, "response": response}
            )
        self._batches = parsed

    def _parse_batch_request(self, raw: object, batch_id: str) -> dict:
        if not isinstance(raw, dict):
            raise SnapshotError(
                f"stream {self.name!r}: batch request must be an object"
            )
        fields = {"batch_id", "events"}
        extra = sorted(set(raw) - fields)
        if extra:
            raise SnapshotError(
                f"stream {self.name!r}: batch request has unexpected field: "
                f"{extra[0]}"
            )
        missing = sorted(fields - set(raw))
        if missing:
            raise SnapshotError(
                f"stream {self.name!r}: batch request missing field: {missing[0]}"
            )
        if raw["batch_id"] != batch_id:
            raise SnapshotError(
                f"stream {self.name!r}: batch request batch_id does not match "
                f"the record"
            )
        events = raw["events"]
        if not isinstance(events, list) or not 1 <= len(events) <= 1000:
            raise SnapshotError(
                f"stream {self.name!r}: batch events must be an array of "
                f"1 to 1000 elements"
            )
        return {
            "batch_id": batch_id,
            "events": [self._parse_batch_event(event) for event in events],
        }

    def _parse_batch_event(self, raw: object) -> dict:
        if not isinstance(raw, dict):
            raise SnapshotError(
                f"stream {self.name!r}: batch events entries must be objects"
            )
        fields = {"timestamp_ms", "value"}
        if self.dedup_retention_ms is not None:
            fields |= {"event_id"}
        if self.lookup_table is not None:
            fields |= {"lookup_key"}
        extra = sorted(set(raw) - fields)
        if extra:
            raise SnapshotError(
                f"stream {self.name!r}: batch event has unexpected field: "
                f"{extra[0]}"
            )
        missing = sorted(fields - set(raw))
        if missing:
            raise SnapshotError(
                f"stream {self.name!r}: batch event missing field: {missing[0]}"
            )
        if not _is_int(raw["timestamp_ms"]):
            raise SnapshotError(
                f"stream {self.name!r}: batch event timestamp_ms must be an integer"
            )
        if not _is_finite_number(raw["value"]):
            raise SnapshotError(
                f"stream {self.name!r}: batch event value must be a finite number"
            )
        event = {"timestamp_ms": raw["timestamp_ms"], "value": raw["value"]}
        if self.dedup_retention_ms is not None:
            if not isinstance(raw["event_id"], str) or not raw["event_id"]:
                raise SnapshotError(
                    f"stream {self.name!r}: batch event event_id must be a "
                    f"non-empty string"
                )
            event["event_id"] = raw["event_id"]
        if self.lookup_table is not None:
            if not isinstance(raw["lookup_key"], str) or not raw["lookup_key"]:
                raise SnapshotError(
                    f"stream {self.name!r}: batch event lookup_key must be a "
                    f"non-empty string"
                )
            event["lookup_key"] = raw["lookup_key"]
        return event

    def _parse_batch_response(
        self, raw: object, batch_id: str, event_count: int
    ) -> dict:
        if not isinstance(raw, dict):
            raise SnapshotError(
                f"stream {self.name!r}: batch response must be an object"
            )
        fields = {"stream", "batch_id", "outcomes"}
        extra = sorted(set(raw) - fields)
        if extra:
            raise SnapshotError(
                f"stream {self.name!r}: batch response has unexpected field: "
                f"{extra[0]}"
            )
        missing = sorted(fields - set(raw))
        if missing:
            raise SnapshotError(
                f"stream {self.name!r}: batch response missing field: {missing[0]}"
            )
        if raw["stream"] != self.name:
            raise SnapshotError(
                f"stream {self.name!r}: batch response labels stream "
                f"{raw['stream']!r}"
            )
        if raw["batch_id"] != batch_id:
            raise SnapshotError(
                f"stream {self.name!r}: batch response batch_id does not match "
                f"the record"
            )
        outcomes = raw["outcomes"]
        if not isinstance(outcomes, list) or len(outcomes) != event_count:
            raise SnapshotError(
                f"stream {self.name!r}: batch response outcomes must be an "
                f"array matching the events"
            )
        return {
            "stream": self.name,
            "batch_id": batch_id,
            "outcomes": [self._parse_batch_outcome(row) for row in outcomes],
        }

    def _parse_batch_outcome(self, raw: object) -> dict:
        if not isinstance(raw, dict):
            raise SnapshotError(
                f"stream {self.name!r}: batch outcome must be an object"
            )
        fields = {"dropped"}
        if self.dedup_retention_ms is not None:
            fields |= {"duplicate"}
        if self.auto_watermark_lag_ms is not None:
            fields |= {"watermark_ms", "finalized"}
        extra = sorted(set(raw) - fields)
        if extra:
            raise SnapshotError(
                f"stream {self.name!r}: batch outcome has unexpected field: "
                f"{extra[0]}"
            )
        missing = sorted(fields - set(raw))
        if missing:
            raise SnapshotError(
                f"stream {self.name!r}: batch outcome missing field: {missing[0]}"
            )
        if not isinstance(raw["dropped"], bool):
            raise SnapshotError(
                f"stream {self.name!r}: batch outcome dropped must be a boolean"
            )
        outcome = {"dropped": raw["dropped"]}
        if self.dedup_retention_ms is not None:
            if not isinstance(raw["duplicate"], bool):
                raise SnapshotError(
                    f"stream {self.name!r}: batch outcome duplicate must be a "
                    f"boolean"
                )
            outcome["duplicate"] = raw["duplicate"]
        if self.auto_watermark_lag_ms is not None:
            watermark = raw["watermark_ms"]
            if watermark is not None and not _is_int(watermark):
                raise SnapshotError(
                    f"stream {self.name!r}: batch outcome watermark_ms must be "
                    f"an integer or null"
                )
            rows = self._parse_window_list(
                raw["finalized"],
                self.name,
                self.window_ms,
                "batch outcome finalized",
                with_stream=True,
                step_ms=self._step_ms,
            )
            for row in rows:
                if row["stream"] != self.name:
                    raise SnapshotError(
                        f"stream {self.name!r}: batch outcome finalized row "
                        f"labels stream {row['stream']!r}"
                    )
            outcome["watermark_ms"] = watermark
            outcome["finalized"] = [dict(row) for row in rows]
        return outcome


class Service:
    """StreamMill service: health reporting plus windowed event aggregation."""

    name = "streammill"
    version = __version__

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._streams: dict[str, _Stream] = {}
        # table name -> {key: label}; in-process dimension tables.
        self._tables: dict[str, dict[str, str]] = {}

    def health(self) -> dict[str, str]:
        return {"status": "ok", "service": self.name, "version": self.version}

    def create_table(self, name: str) -> dict:
        with self._lock:
            if name in self._tables:
                raise TableExistsError(name)
            self._tables[name] = {}
        return {"table": name}

    def put_row(self, table: str, key: str, label: str) -> dict:
        with self._lock:
            rows = self._tables.get(table)
            if rows is None:
                raise TableNotFoundError(table)
            changed = rows.get(key) != label
            rows[key] = label
        return {"table": table, "key": key, "label": label, "changed": changed}

    def create_stream(
        self,
        name: str,
        window_ms: int,
        allowed_lateness_ms: int,
        dedup_retention_ms: int | None = None,
        auto_watermark_lag_ms: int | None = None,
        slide_ms: int | None = None,
        lookup_table: str | None = None,
        change_retention: int | None = None,
        batch_retention: int | None = None,
    ) -> dict:
        with self._lock:
            if name in self._streams:
                raise StreamExistsError(name)
            if lookup_table is not None and lookup_table not in self._tables:
                raise TableNotFoundError(lookup_table)
            self._streams[name] = _Stream(
                name,
                window_ms,
                allowed_lateness_ms,
                dedup_retention_ms,
                auto_watermark_lag_ms,
                slide_ms,
                lookup_table,
                change_retention,
                batch_retention,
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
        if lookup_table is not None:
            payload["lookup_table"] = lookup_table
        if change_retention is not None:
            payload["change_retention"] = change_retention
        if batch_retention is not None:
            payload["batch_retention"] = batch_retention
        return payload

    def stream_features(self, name: str) -> dict | None:
        """Event-surface features of the stream; None if it is unknown."""
        with self._lock:
            stream = self._streams.get(name)
            if stream is None:
                return None
            return {
                "dedup": stream.dedup_retention_ms is not None,
                "join": stream.lookup_table is not None,
            }

    def add_event(
        self,
        name: str,
        timestamp_ms: int,
        value: float,
        event_id: str | None = None,
        lookup_key: str | None = None,
    ) -> dict:
        with self._lock:
            stream = self._get(name)
            outcome = stream.add_event(
                timestamp_ms, value, event_id, lookup_key, self._label_of(stream)
            )
        return {"stream": name, **outcome}

    def add_batch(self, name: str, batch_id: str, events: list[dict]) -> dict:
        """Apply one atomic batch of events to a batch-enabled stream.

        The whole call runs under the service lock, so no other request
        can observe an intermediate state. A retained record with the
        same ``batch_id`` and an identical request (same field values in
        the same event order) replays its stored response without
        writing again; the same id with different content raises
        BatchIdConflictError. A processing failure rolls the whole batch
        back and occupies no identifier. Only successful batches are
        remembered, capped at the stream's ``batch_retention`` newest
        records; replaying never refreshes that order.
        """
        with self._lock:
            stream = self._get(name)
            if stream.batch_retention is None:
                raise BatchIngestNotEnabledError(name)
            for record in stream._batches:
                if record["batch_id"] == batch_id:
                    if record["request"]["events"] == events:
                        return copy.deepcopy(record["response"])
                    raise BatchIdConflictError(batch_id)
            outcomes = stream.apply_batch(events, self._label_of(stream))
            response = {"stream": name, "batch_id": batch_id, "outcomes": outcomes}
            stream._batches.append(
                {
                    "batch_id": batch_id,
                    "request": {
                        "batch_id": batch_id,
                        "events": copy.deepcopy(events),
                    },
                    "response": copy.deepcopy(response),
                }
            )
            overflow = len(stream._batches) - stream.batch_retention
            if overflow > 0:
                del stream._batches[:overflow]
            return response

    def _label_of(self, stream: _Stream):
        """Current-label reader for a joined stream, else None."""
        if stream.lookup_table is None:
            return None
        rows = self._tables[stream.lookup_table]

        def label_of(key: str, _rows=rows) -> str:
            try:
                return _rows[key]
            except KeyError:
                raise LookupKeyNotFoundError(key) from None

        return label_of

    def advance_watermark(self, name: str, watermark_ms: int) -> dict:
        with self._lock:
            finalized = self._get(name).advance_watermark(watermark_ms)
        return {"stream": name, "watermark_ms": watermark_ms, "finalized": finalized}

    def results(self, name: str) -> dict:
        with self._lock:
            rows = self._get(name).results()
        return {"stream": name, "results": rows}

    def joined_results(self, name: str) -> dict:
        with self._lock:
            stream = self._get(name)
            if stream.lookup_table is None:
                raise JoinNotEnabledError(name)
            rows = [dict(row) for row in stream._joined_finalized]
        return {"stream": name, "results": rows}

    def changes(self, name: str, after_seq: int, limit: int) -> dict:
        """Read retained change-feed records with ``seq`` greater than
        ``after_seq``, ascending, at most ``limit`` of them.

        The read happens under the service lock, so it only ever observes
        complete commits. A cursor past the latest sequence raises
        ChangeCursorAheadError; a cursor older than the oldest retained
        record (so records were trimmed past it) raises
        ChangeCursorExpiredError.
        """
        with self._lock:
            stream = self._get(name)
            if stream.change_retention is None:
                raise ChangeFeedNotEnabledError(name)
            if after_seq > stream._latest_seq:
                raise ChangeCursorAheadError(
                    f"cursor {after_seq} is ahead of latest_seq "
                    f"{stream._latest_seq}"
                )
            retained = stream._changes
            if retained and after_seq < retained[0]["seq"] - 1:
                raise ChangeCursorExpiredError(
                    f"cursor {after_seq} is behind the oldest retained "
                    f"record (seq {retained[0]['seq']})"
                )
            if retained:
                offset = max(0, after_seq - retained[0]["seq"] + 1)
                records = [dict(row) for row in retained[offset : offset + limit]]
            else:
                records = []
            latest_seq = stream._latest_seq
        return {"stream": name, "latest_seq": latest_seq, "changes": records}

    def snapshot(self) -> dict:
        """Return a consistent point-in-time, JSON-serializable snapshot.

        The whole document is assembled while holding the service lock, so
        concurrent creates, events and watermark advances are either fully
        included or fully excluded. Instances with at least one
        batch-enabled stream export ``format_version`` 4 (always carrying
        the ``tables`` array plus the batch state on enabled streams);
        otherwise instances with at least one change-feed stream export
        ``format_version`` 3, instances with dimension tables export
        ``format_version`` 2, and without any join, change-feed or batch
        state the document is the unchanged version 1 shape.
        """
        with self._lock:
            streams = [
                self._streams[name].to_snapshot() for name in sorted(self._streams)
            ]
            tables = [
                {
                    "name": table,
                    "rows": [
                        {"key": key, "label": self._tables[table][key]}
                        for key in sorted(self._tables[table])
                    ],
                }
                for table in sorted(self._tables)
            ]
            batches = any(
                stream.batch_retention is not None
                for stream in self._streams.values()
            )
            if batches:
                return {
                    "format_version": SNAPSHOT_FORMAT_VERSION_BATCHES,
                    "tables": tables,
                    "streams": streams,
                }
            change_feed = any(
                stream.change_retention is not None
                for stream in self._streams.values()
            )
            if change_feed:
                return {
                    "format_version": SNAPSHOT_FORMAT_VERSION_CHANGES,
                    "tables": tables,
                    "streams": streams,
                }
            if self._tables:
                return {
                    "format_version": SNAPSHOT_FORMAT_VERSION_JOIN,
                    "tables": tables,
                    "streams": streams,
                }
            return {
                "format_version": SNAPSHOT_FORMAT_VERSION,
                "streams": streams,
            }

    def restore_snapshot(self, document: object) -> int:
        """Replace instance state with a validated snapshot, atomically.

        Only callable on an instance without streams or tables; otherwise
        raises RestoreConflictError, regardless of document content. Any
        invalid document raises SnapshotError and leaves the (empty)
        instance untouched. Version 1 documents restore as before;
        version 2 documents additionally restore dimension tables and
        joined-stream state under strict validation; version 3 documents
        additionally restore change-feed state (retention, cursor and
        retained records) under strict validation; version 4 documents
        additionally restore batch-ingest state (retention and the
        retained batch records, so replay and eviction continue exactly
        as on the uninterrupted instance). Returns the restored stream
        count.
        """
        with self._lock:
            # Conflict takes priority over every content check: an instance
            # with streams or tables always gets restore_conflict.
            if self._streams or self._tables:
                raise RestoreConflictError(
                    "restore is only allowed on an instance without streams "
                    "or tables"
                )
            if not isinstance(document, dict):
                raise SnapshotError("snapshot document must be a JSON object")
            if "format_version" not in document:
                raise SnapshotError(
                    "snapshot document missing required field: format_version"
                )
            version = document["format_version"]
            if not _is_int(version):
                raise SnapshotError("format_version must be an integer")
            if version == SNAPSHOT_FORMAT_VERSION:
                allowed = {"format_version", "streams"}
            elif version in (
                SNAPSHOT_FORMAT_VERSION_JOIN,
                SNAPSHOT_FORMAT_VERSION_CHANGES,
                SNAPSHOT_FORMAT_VERSION_BATCHES,
            ):
                allowed = {"format_version", "streams", "tables"}
            else:
                raise SnapshotError(f"unsupported format_version: {version}")
            extra = sorted(set(document) - allowed)
            if extra:
                raise SnapshotError(
                    f"snapshot document has unexpected field: {extra[0]}"
                )
            missing = sorted(allowed - set(document))
            if missing:
                raise SnapshotError(
                    f"snapshot document missing required field: {missing[0]}"
                )
            entries = document["streams"]
            if not isinstance(entries, list):
                raise SnapshotError("streams must be an array")

            tables: dict[str, dict[str, str]] = {}
            if version in (
                SNAPSHOT_FORMAT_VERSION_JOIN,
                SNAPSHOT_FORMAT_VERSION_CHANGES,
                SNAPSHOT_FORMAT_VERSION_BATCHES,
            ):
                tables = self._parse_tables(document["tables"])

            # Build and validate everything before the single publishing
            # assignment, so a failure leaves no partial state and concurrent
            # requests never observe half-restored streams.
            restored: dict[str, _Stream] = {}
            previous_name: str | None = None
            for entry in entries:
                stream = _Stream.from_snapshot(
                    entry,
                    table_names=set(tables)
                    if version
                    in (
                        SNAPSHOT_FORMAT_VERSION_JOIN,
                        SNAPSHOT_FORMAT_VERSION_CHANGES,
                        SNAPSHOT_FORMAT_VERSION_BATCHES,
                    )
                    else None,
                    allow_change_feed=version
                    in (
                        SNAPSHOT_FORMAT_VERSION_CHANGES,
                        SNAPSHOT_FORMAT_VERSION_BATCHES,
                    ),
                    allow_batches=version == SNAPSHOT_FORMAT_VERSION_BATCHES,
                )
                if stream.name in restored:
                    raise SnapshotError(
                        f"duplicate stream name in snapshot: {stream.name!r}"
                    )
                if previous_name is not None and stream.name <= previous_name:
                    raise SnapshotError("streams must be strictly ordered by name")
                previous_name = stream.name
                restored[stream.name] = stream
            self._tables = tables
            self._streams = restored
            return len(restored)

    @staticmethod
    def _parse_tables(value: object) -> dict[str, dict[str, str]]:
        """Validate the ``tables`` array of a version 2 snapshot."""
        if not isinstance(value, list):
            raise SnapshotError("tables must be an array")
        tables: dict[str, dict[str, str]] = {}
        previous_name: str | None = None
        for entry in value:
            if not isinstance(entry, dict):
                raise SnapshotError("table entries must be objects")
            extra = sorted(set(entry) - {"name", "rows"})
            if extra:
                raise SnapshotError(f"table entry has unexpected field: {extra[0]}")
            missing = sorted({"name", "rows"} - set(entry))
            if missing:
                raise SnapshotError(
                    f"table entry missing required field: {missing[0]}"
                )
            name = entry["name"]
            if not isinstance(name, str) or not name:
                raise SnapshotError("table name must be a non-empty string")
            if previous_name is not None and name <= previous_name:
                raise SnapshotError("tables must be strictly ordered by name")
            previous_name = name
            raw_rows = entry["rows"]
            if not isinstance(raw_rows, list):
                raise SnapshotError(f"table {name!r}: rows must be an array")
            rows: dict[str, str] = {}
            previous_key: str | None = None
            for raw in raw_rows:
                if not isinstance(raw, dict):
                    raise SnapshotError(f"table {name!r}: rows entries must be objects")
                extra = sorted(set(raw) - {"key", "label"})
                if extra:
                    raise SnapshotError(
                        f"table {name!r}: row has unexpected field: {extra[0]}"
                    )
                missing = sorted({"key", "label"} - set(raw))
                if missing:
                    raise SnapshotError(
                        f"table {name!r}: row missing required field: {missing[0]}"
                    )
                key = raw["key"]
                label = raw["label"]
                if not isinstance(key, str) or not key:
                    raise SnapshotError(
                        f"table {name!r}: row key must be a non-empty string"
                    )
                if not isinstance(label, str) or not label:
                    raise SnapshotError(
                        f"table {name!r}: row label must be a non-empty string"
                    )
                if previous_key is not None and key <= previous_key:
                    raise SnapshotError(
                        f"table {name!r}: rows must be strictly ordered by key"
                    )
                previous_key = key
                rows[key] = label
            tables[name] = rows
        return tables

    def _get(self, name: str) -> _Stream:
        try:
            return self._streams[name]
        except KeyError:
            raise StreamNotFoundError(name) from None
