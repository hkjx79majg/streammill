"""HTTP entry point for StreamMill."""

from __future__ import annotations

import argparse
import json
import math
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

from .service import (
    EventIdConflictError,
    JoinNotEnabledError,
    LookupKeyNotFoundError,
    RestoreConflictError,
    Service,
    SnapshotError,
    StreamExistsError,
    StreamNotFoundError,
    TableExistsError,
    TableNotFoundError,
    WatermarkRegressionError,
)


def env_address() -> tuple[str, int]:
    raw = os.environ.get("STREAMMILL_ADDR", "127.0.0.1:8080")
    host, _, port = raw.rpartition(":")
    if not host or not port.isdigit():
        raise SystemExit(f"invalid STREAMMILL_ADDR: {raw!r}")
    return host, int(port)


class _RequestError(Exception):
    """Validation or domain failure mapped onto a JSON error response."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def _invalid_request(message: str) -> _RequestError:
    return _RequestError(422, "invalid_request", message)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


_CREATE_FIELDS = {
    "name": lambda v: isinstance(v, str) and len(v) > 0,
    "window_ms": lambda v: _is_int(v) and v > 0,
    "allowed_lateness_ms": lambda v: _is_int(v) and v >= 0,
}
_CREATE_OPTIONAL_FIELDS = {
    "dedup_retention_ms": lambda v: _is_int(v) and v > 0,
    "auto_watermark_lag_ms": lambda v: _is_int(v) and v >= 0,
    "slide_ms": lambda v: _is_int(v) and v > 0,
    "lookup_table": lambda v: isinstance(v, str) and len(v) > 0,
}
_EVENT_FIELDS = {
    "timestamp_ms": _is_int,
    "value": _is_finite_number,
}
_EVENT_DEDUP_FIELDS = {
    "event_id": lambda v: isinstance(v, str) and len(v) > 0,
}
_EVENT_JOIN_FIELDS = {
    "lookup_key": lambda v: isinstance(v, str) and len(v) > 0,
}
_TABLE_FIELDS = {
    "name": lambda v: isinstance(v, str) and len(v) > 0,
}
_ROW_FIELDS = {
    "key": lambda v: isinstance(v, str) and len(v) > 0,
    "label": lambda v: isinstance(v, str) and len(v) > 0,
}
_WATERMARK_FIELDS = {
    "watermark_ms": _is_int,
}


def _validate(body: object, fields: dict, optional: dict | None = None) -> dict:
    """Check required fields, types and reject undeclared fields."""
    if not isinstance(body, dict):
        raise _invalid_request("request body must be a JSON object")
    declared = set(fields) | set(optional or ())
    extra = sorted(set(body) - declared)
    if extra:
        raise _invalid_request(f"unexpected fields: {', '.join(extra)}")
    values = {}
    for field, check in fields.items():
        if field not in body:
            raise _invalid_request(f"missing required field: {field}")
        value = body[field]
        if not check(value):
            raise _invalid_request(f"invalid value for field: {field}")
        values[field] = value
    for field, check in (optional or {}).items():
        if field in body:
            value = body[field]
            if not check(value):
                raise _invalid_request(f"invalid value for field: {field}")
            values[field] = value
    return values


class Handler(BaseHTTPRequestHandler):
    service = Service()

    def send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, status: int, code: str, message: str) -> None:
        self.send_json(status, {"error": {"code": code, "message": message}})

    def _segments(self) -> list[str]:
        path = urlsplit(self.path).path
        return [unquote(part) for part in path.split("/") if part]

    def _read_json(self) -> object:
        try:
            length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b""
        try:
            return json.loads(raw)
        except ValueError:
            raise _RequestError(400, "invalid_json", "request body is not valid JSON") from None

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.send_json(200, self.service.health())
            return
        segments = self._segments()
        if segments == ["snapshot"]:
            self.send_json(200, self.service.snapshot())
            return
        if len(segments) == 3 and segments[0] == "streams" and segments[2] == "results":
            try:
                self.send_json(200, self.service.results(segments[1]))
            except StreamNotFoundError:
                self.send_error_json(404, "stream_not_found", f"unknown stream: {segments[1]}")
            return
        if len(segments) == 3 and segments[0] == "streams" and segments[2] == "joined-results":
            try:
                self.send_json(200, self.service.joined_results(segments[1]))
            except StreamNotFoundError:
                self.send_error_json(404, "stream_not_found", f"unknown stream: {segments[1]}")
            except JoinNotEnabledError:
                self.send_error_json(
                    409, "join_not_enabled", f"stream has no lookup_table: {segments[1]}"
                )
            return
        self.send_json(404, {"error": {"code": "not_found", "message": f"no route for {self.path}"}})

    def do_POST(self) -> None:
        segments = self._segments()
        try:
            if segments == ["streams"]:
                self._create_stream()
                return
            if segments == ["tables"]:
                self._create_table()
                return
            if len(segments) == 3 and segments[0] == "tables" and segments[2] == "rows":
                self._put_row(segments[1])
                return
            if len(segments) == 3 and segments[0] == "streams" and segments[2] == "events":
                self._add_event(segments[1])
                return
            if len(segments) == 3 and segments[0] == "streams" and segments[2] == "watermark":
                self._advance_watermark(segments[1])
                return
            if segments == ["snapshot", "restore"]:
                self._restore_snapshot()
                return
        except _RequestError as exc:
            self.send_error_json(exc.status, exc.code, exc.message)
            return
        self.send_json(404, {"error": {"code": "not_found", "message": f"no route for {self.path}"}})

    def _create_stream(self) -> None:
        values = _validate(self._read_json(), _CREATE_FIELDS, _CREATE_OPTIONAL_FIELDS)
        retention = values.get("dedup_retention_ms")
        if retention is not None and retention < values["allowed_lateness_ms"]:
            raise _invalid_request(
                "dedup_retention_ms must be at least allowed_lateness_ms"
            )
        slide = values.get("slide_ms")
        if slide is not None and (
            slide > values["window_ms"] or values["window_ms"] % slide != 0
        ):
            raise _invalid_request(
                "slide_ms must be no larger than window_ms and evenly divide it"
            )
        try:
            payload = self.service.create_stream(
                values["name"],
                values["window_ms"],
                values["allowed_lateness_ms"],
                retention,
                values.get("auto_watermark_lag_ms"),
                slide,
                values.get("lookup_table"),
            )
        except StreamExistsError:
            raise _RequestError(409, "stream_exists", f"stream already exists: {values['name']}") from None
        except TableNotFoundError:
            raise _invalid_request(
                f"unknown lookup_table: {values['lookup_table']}"
            ) from None
        self.send_json(201, payload)

    def _create_table(self) -> None:
        values = _validate(self._read_json(), _TABLE_FIELDS)
        try:
            payload = self.service.create_table(values["name"])
        except TableExistsError:
            raise _RequestError(409, "table_exists", f"table already exists: {values['name']}") from None
        self.send_json(201, payload)

    def _put_row(self, name: str) -> None:
        values = _validate(self._read_json(), _ROW_FIELDS)
        try:
            payload = self.service.put_row(name, values["key"], values["label"])
        except TableNotFoundError:
            raise _RequestError(404, "table_not_found", f"unknown table: {name}") from None
        self.send_json(200, payload)

    def _add_event(self, name: str) -> None:
        body = self._read_json()
        fields = dict(_EVENT_FIELDS)
        if self.service.dedup_enabled(name):
            fields.update(_EVENT_DEDUP_FIELDS)
        if self.service.join_enabled(name):
            fields.update(_EVENT_JOIN_FIELDS)
        # Unknown streams validate against the base shape, matching the
        # historical 422-before-404 ordering; event_id and lookup_key stay
        # undeclared there.
        values = _validate(body, fields)
        try:
            payload = self.service.add_event(
                name,
                values["timestamp_ms"],
                values["value"],
                values.get("event_id"),
                values.get("lookup_key"),
            )
        except StreamNotFoundError:
            raise _RequestError(404, "stream_not_found", f"unknown stream: {name}") from None
        except EventIdConflictError:
            raise _RequestError(
                409, "event_id_conflict", f"conflicting event_id: {values['event_id']}"
            ) from None
        except LookupKeyNotFoundError:
            raise _RequestError(
                409, "lookup_key_not_found", f"unknown lookup_key: {values['lookup_key']}"
            ) from None
        self.send_json(200, payload)

    def _advance_watermark(self, name: str) -> None:
        values = _validate(self._read_json(), _WATERMARK_FIELDS)
        try:
            payload = self.service.advance_watermark(name, values["watermark_ms"])
        except StreamNotFoundError:
            raise _RequestError(404, "stream_not_found", f"unknown stream: {name}") from None
        except WatermarkRegressionError:
            raise _RequestError(409, "watermark_regression", "watermark must not move backwards") from None
        self.send_json(200, payload)

    def _restore_snapshot(self) -> None:
        document = self._read_json()
        try:
            restored = self.service.restore_snapshot(document)
        except RestoreConflictError:
            raise _RequestError(
                409,
                "restore_conflict",
                "snapshot restore is only allowed on an instance without streams",
            ) from None
        except SnapshotError as exc:
            raise _RequestError(422, "invalid_snapshot", str(exc)) from None
        self.send_json(200, {"restored_streams": restored})

    def log_message(self, fmt: str, *args: object) -> None:
        """Silence per-request logging so recorded output stays stable."""


def main() -> int:
    parser = argparse.ArgumentParser(prog="streammill.server", description="流式仓库与实时分析引擎")
    host, port = env_address()
    parser.add_argument("--host", default=host)
    parser.add_argument("--port", type=int, default=port)
    args = parser.parse_args()
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"StreamMill listening on http://{args.host}:{httpd.server_address[1]}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
