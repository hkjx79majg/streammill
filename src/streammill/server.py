"""HTTP entry point for StreamMill."""

from __future__ import annotations

import argparse
import json
import math
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

from .service import Service, ServiceError


def env_address() -> tuple[str, int]:
    raw = os.environ.get("STREAMMILL_ADDR", "127.0.0.1:8080")
    host, _, port = raw.rpartition(":")
    if not host or not port.isdigit():
        raise SystemExit(f"invalid STREAMMILL_ADDR: {raw!r}")
    return host, int(port)


def _is_int(value: object) -> bool:
    # bool is a subclass of int but is not an integer configuration value.
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isfinite(value)


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

    def read_json_object(self) -> dict:
        length = self.headers.get("Content-Length")
        if length is None or not length.isdigit():
            self.send_error_json(400, "invalid_json", "request body is not valid JSON")
            return None  # type: ignore[return-value]
        try:
            payload = json.loads(self.rfile.read(int(length)).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self.send_error_json(400, "invalid_json", "request body is not valid JSON")
            return None  # type: ignore[return-value]
        if not isinstance(payload, dict):
            self.send_error_json(422, "invalid_request", "request body must be a JSON object")
            return None  # type: ignore[return-value]
        return payload

    def require_fields(
        self, payload: dict, fields: dict[str, type | tuple[type, ...]]
    ) -> dict | None:
        unknown = set(payload) - set(fields)
        if unknown:
            self.send_error_json(
                422, "invalid_request", f"unexpected fields: {sorted(unknown)}"
            )
            return None
        values: dict = {}
        for name, expected in fields.items():
            if name not in payload:
                self.send_error_json(
                    422, "invalid_request", f"missing required field: {name}"
                )
                return None
            value = payload[name]
            if expected is int:
                ok = _is_int(value)
            elif expected == "number":
                ok = _is_finite_number(value)
            elif expected is str:
                ok = isinstance(value, str) and len(value) > 0
            else:
                ok = isinstance(value, expected)
            if not ok:
                self.send_error_json(
                    422, "invalid_request", f"field {name} has an invalid value"
                )
                return None
            values[name] = value
        return values

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path == "/healthz":
            self.send_json(200, self.service.health())
            return
        if path.startswith("/streams/") and path.endswith("/results"):
            name = unquote(path[len("/streams/") : -len("/results")])
            if name:
                self.call_service(lambda: self.send_json(200, self.service.get_results(name)))
                return
        self.send_json(404, {"error": {"code": "not_found", "message": f"no route for {self.path}"}})

    def do_POST(self) -> None:
        path = urlsplit(self.path).path
        if path == "/streams":
            self.create_stream()
            return
        if path.startswith("/streams/"):
            rest = unquote(path[len("/streams/") :])
            if rest.endswith("/events"):
                name = rest[: -len("/events")].rstrip("/")
                if name:
                    self.add_event(name)
                    return
            if rest.endswith("/watermark"):
                name = rest[: -len("/watermark")].rstrip("/")
                if name:
                    self.advance_watermark(name)
                    return
        self.send_json(404, {"error": {"code": "not_found", "message": f"no route for {self.path}"}})

    def call_service(self, action: object) -> None:
        try:
            action()  # type: ignore[operator]
        except ServiceError as exc:
            self.send_error_json(exc.status, exc.code, exc.message)

    def create_stream(self) -> None:
        payload = self.read_json_object()
        if payload is None:
            return
        values = self.require_fields(
            payload,
            {"name": str, "window_ms": int, "allowed_lateness_ms": int},
        )
        if values is None:
            return
        if values["window_ms"] <= 0:
            self.send_error_json(422, "invalid_request", "window_ms must be a positive integer")
            return
        if values["allowed_lateness_ms"] < 0:
            self.send_error_json(
                422, "invalid_request", "allowed_lateness_ms must be a non-negative integer"
            )
            return
        self.call_service(
            lambda: self.send_json(
                201,
                self.service.create_stream(
                    values["name"], values["window_ms"], values["allowed_lateness_ms"]
                ),
            )
        )

    def add_event(self, name: str) -> None:
        payload = self.read_json_object()
        if payload is None:
            return
        values = self.require_fields(payload, {"timestamp_ms": int, "value": "number"})
        if values is None:
            return
        self.call_service(
            lambda: self.send_json(
                200,
                self.service.add_event(name, values["timestamp_ms"], values["value"]),
            )
        )

    def advance_watermark(self, name: str) -> None:
        payload = self.read_json_object()
        if payload is None:
            return
        values = self.require_fields(payload, {"watermark_ms": int})
        if values is None:
            return
        self.call_service(
            lambda: self.send_json(
                200, self.service.advance_watermark(name, values["watermark_ms"])
            )
        )

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
