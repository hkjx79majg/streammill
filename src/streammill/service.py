"""Core service surface for StreamMill.

The health surface from the frozen baseline is unchanged. On top of it the
service now hosts named, mutually isolated event-time tumbling window flows:
stream configuration, explicit watermarks, events and finalized results all
live in per-stream state.
"""

from __future__ import annotations

import threading

from . import __version__


class ServiceError(Exception):
    """An error that maps directly onto the public JSON error object."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


class _Stream:
    """All mutable state for one named stream."""

    def __init__(self, name: str, window_ms: int, allowed_lateness_ms: int) -> None:
        self.name = name
        self.window_ms = window_ms
        self.allowed_lateness_ms = allowed_lateness_ms
        self.watermark_ms: int | None = None
        # window_start_ms -> [count, sum] for windows that may still receive events
        self.pending: dict[int, list] = {}
        # finalized windows, kept sorted by window_end_ms
        self.results: list[dict] = []

    def describe(self) -> dict:
        return {
            "name": self.name,
            "window_ms": self.window_ms,
            "allowed_lateness_ms": self.allowed_lateness_ms,
        }

    def result(self, start: int, count: int, total: float) -> dict:
        return {
            "stream": self.name,
            "window_start_ms": start,
            "window_end_ms": start + self.window_ms,
            "count": count,
            "sum": total,
        }


class Service:
    """Stream registry with health reporting and event-time windowing."""

    name = "streammill"
    version = __version__

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._streams: dict[str, _Stream] = {}

    def health(self) -> dict[str, str]:
        return {"status": "ok", "service": self.name, "version": self.version}

    def _require_stream(self, name: str) -> _Stream:
        stream = self._streams.get(name)
        if stream is None:
            raise ServiceError(404, "stream_not_found", f"no stream named {name!r}")
        return stream

    def create_stream(
        self, name: str, window_ms: int, allowed_lateness_ms: int
    ) -> dict:
        with self._lock:
            if name in self._streams:
                raise ServiceError(409, "stream_exists", f"stream {name!r} already exists")
            stream = _Stream(name, window_ms, allowed_lateness_ms)
            self._streams[name] = stream
            return stream.describe()

    def add_event(self, name: str, timestamp_ms: int, value: float) -> dict:
        with self._lock:
            stream = self._require_stream(name)
            boundary = stream.watermark_ms
            if boundary is not None:
                boundary -= stream.allowed_lateness_ms
            # Events strictly behind the drop boundary are acknowledged but
            # ignored; an event exactly on the boundary is still received.
            if boundary is not None and timestamp_ms < boundary:
                return {"dropped": True}
            start = (timestamp_ms // stream.window_ms) * stream.window_ms
            agg = stream.pending.get(start)
            if agg is None:
                stream.pending[start] = [1, value]
            else:
                agg[0] += 1
                agg[1] += value
            return {"dropped": False}

    def advance_watermark(self, name: str, watermark_ms: int) -> dict:
        with self._lock:
            stream = self._require_stream(name)
            if stream.watermark_ms is not None and watermark_ms < stream.watermark_ms:
                raise ServiceError(
                    409,
                    "watermark_regression",
                    f"watermark {watermark_ms} is below current {stream.watermark_ms}",
                )
            stream.watermark_ms = watermark_ms
            return {"results": self._close_due_windows(stream)}

    def _close_due_windows(self, stream: _Stream) -> list[dict]:
        # A window becomes final once the watermark reaches
        # window_end_ms + allowed_lateness_ms, i.e. end <= watermark - lateness.
        boundary = stream.watermark_ms - stream.allowed_lateness_ms
        ready = [
            start
            for start in stream.pending
            if start + stream.window_ms <= boundary
        ]
        ready.sort()
        finalized: list[dict] = []
        for start in ready:
            count, total = stream.pending.pop(start)
            finalized.append(stream.result(start, count, total))
        # Watermarks are monotonic and each batch is sorted by end, so the
        # accumulated list stays ordered by window_end_ms.
        stream.results.extend(finalized)
        return finalized

    def get_results(self, name: str) -> dict:
        with self._lock:
            stream = self._require_stream(name)
            return {"results": [dict(item) for item in stream.results]}
