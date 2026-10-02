import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from streammill import __version__
from streammill import server as server_module
from streammill.service import Service, ServiceError


class HealthSmokeTest(unittest.TestCase):
    def test_health_reports_ok(self) -> None:
        payload = Service().health()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "streammill")
        self.assertEqual(payload["version"], __version__)
        json.dumps(payload, sort_keys=True)


def _json_request(method, url, body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body = exc.read()
        exc.close()
        return exc.code, json.loads(body)


class _Server:
    def __init__(self):
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), server_module.Handler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def url(self, suffix=""):
        return f"http://127.0.0.1:{self.port}{suffix}"

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join()


class HttpFlowTest(unittest.TestCase):
    def setUp(self):
        self.srv = _Server()

    def tearDown(self):
        self.srv.stop()

    def create(self, name="s", window_ms=100, lateness=10):
        return _json_request(
            "POST", self.srv.url("/streams"),
            {"name": name, "window_ms": window_ms, "allowed_lateness_ms": lateness},
        )

    def test_window_assignment_and_finalization(self):
        status, payload = self.create()
        self.assertEqual(status, 201)
        self.assertEqual(payload, {
            "name": "s", "window_ms": 100, "allowed_lateness_ms": 10,
        })

        # 50, 90 -> [0,100); 100, 150 -> [100,200)
        for ts, value in [(50, 1.5), (90, 2.5), (100, 4), (150, -2)]:
            status, payload = _json_request(
                "POST", self.srv.url("/streams/s/events"),
                {"timestamp_ms": ts, "value": value},
            )
            self.assertEqual(status, 200)
            self.assertEqual(payload, {"dropped": False})

        # Watermark 105 closes nothing: end 100 + lateness 10 = 110 > 105.
        status, payload = _json_request(
            "POST", self.srv.url("/streams/s/watermark"), {"watermark_ms": 105}
        )
        self.assertEqual((status, payload), (200, {"results": []}))

        status, payload = _json_request("GET", self.srv.url("/streams/s/results"))
        self.assertEqual((status, payload), (200, {"results": []}))

        # Watermark 110 finalizes [0,100) exactly on the boundary.
        status, payload = _json_request(
            "POST", self.srv.url("/streams/s/watermark"), {"watermark_ms": 110}
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["results"], [{
            "stream": "s", "window_start_ms": 0, "window_end_ms": 100,
            "count": 2, "sum": 4.0,
        }])

        status, payload = _json_request("GET", self.srv.url("/streams/s/results"))
        self.assertEqual(payload["results"], [{
            "stream": "s", "window_start_ms": 0, "window_end_ms": 100,
            "count": 2, "sum": 4.0,
        }])

        # Repeating the same watermark succeeds without duplicating results.
        status, payload = _json_request(
            "POST", self.srv.url("/streams/s/watermark"), {"watermark_ms": 110}
        )
        self.assertEqual((status, payload), (200, {"results": []}))

        # Event at 100 is exactly on watermark(110) - lateness(10): accepted.
        status, payload = _json_request(
            "POST", self.srv.url("/streams/s/events"),
            {"timestamp_ms": 100, "value": 10},
        )
        self.assertEqual((status, payload), (200, {"dropped": False}))

        # Event at 99 is strictly behind the boundary: dropped, aggregate intact.
        status, payload = _json_request(
            "POST", self.srv.url("/streams/s/events"),
            {"timestamp_ms": 99, "value": 1000},
        )
        self.assertEqual((status, payload), (200, {"dropped": True}))

        # Close [100,200): 100->4, 150->-2, late-arrival 100->10 => count 3 sum 12
        status, payload = _json_request(
            "POST", self.srv.url("/streams/s/watermark"), {"watermark_ms": 210}
        )
        self.assertEqual(payload["results"], [{
            "stream": "s", "window_start_ms": 100, "window_end_ms": 200,
            "count": 3, "sum": 12.0,
        }])

        status, payload = _json_request("GET", self.srv.url("/streams/s/results"))
        self.assertEqual([r["window_end_ms"] for r in payload["results"]], [100, 200])

    def test_negative_timestamp_floor_division(self):
        self.create(name="neg", window_ms=100, lateness=0)
        # Python floor division: -1 // 100 -> start -100, i.e. window [-100, 0)
        _json_request("POST", self.srv.url("/streams/neg/events"),
                      {"timestamp_ms": -1, "value": 1})
        status, payload = _json_request(
            "POST", self.srv.url("/streams/neg/watermark"), {"watermark_ms": 0}
        )
        self.assertEqual(payload["results"], [{
            "stream": "neg", "window_start_ms": -100, "window_end_ms": 0,
            "count": 1, "sum": 1,
        }])

    def test_empty_windows_never_emitted_and_ordering_stable(self):
        self.create(name="e", window_ms=10, lateness=0)
        # Only an event in window [30,40).
        _json_request("POST", self.srv.url("/streams/e/events"),
                      {"timestamp_ms": 35, "value": 2})
        _json_request("POST", self.srv.url("/streams/e/watermark"),
                      {"watermark_ms": 100})
        status, payload = _json_request("GET", self.srv.url("/streams/e/results"))
        self.assertEqual(payload["results"], [{
            "stream": "e", "window_start_ms": 30, "window_end_ms": 40,
            "count": 1, "sum": 2,
        }])
        status, again = _json_request("GET", self.srv.url("/streams/e/results"))
        self.assertEqual(payload, again)

    def test_results_are_returned_ordered_by_window_end(self):
        self.create(name="o", window_ms=10, lateness=0)
        for ts, value in [(35, 1), (5, 2), (15, 3), (25, 4)]:
            _json_request("POST", self.srv.url("/streams/o/events"),
                          {"timestamp_ms": ts, "value": value})
        status, payload = _json_request(
            "POST", self.srv.url("/streams/o/watermark"), {"watermark_ms": 100}
        )
        self.assertEqual([r["window_start_ms"] for r in payload["results"]],
                         [0, 10, 20, 30])

    def test_streams_are_isolated(self):
        self.create(name="a", window_ms=10, lateness=0)
        self.create(name="b", window_ms=10, lateness=0)
        _json_request("POST", self.srv.url("/streams/a/events"),
                      {"timestamp_ms": 0, "value": 1})
        _json_request("POST", self.srv.url("/streams/b/events"),
                      {"timestamp_ms": 5, "value": 9})
        _json_request("POST", self.srv.url("/streams/a/watermark"),
                      {"watermark_ms": 50})
        status, a = _json_request("GET", self.srv.url("/streams/a/results"))
        status, b = _json_request("GET", self.srv.url("/streams/b/results"))
        self.assertEqual(a["results"][0]["stream"], "a")
        self.assertEqual(a["results"][0]["sum"], 1)
        self.assertEqual(b["results"], [])

    def test_stream_errors(self):
        self.create(name="dup")
        status, payload = self.create(name="dup")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "stream_exists")

        status, payload = _json_request(
            "GET", self.srv.url("/streams/missing/results"))
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "stream_not_found")

        status, payload = _json_request(
            "POST", self.srv.url("/streams/missing/events"),
            {"timestamp_ms": 0, "value": 1})
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "stream_not_found")

        status, payload = _json_request(
            "POST", self.srv.url("/streams/missing/watermark"),
            {"watermark_ms": 1})
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "stream_not_found")

    def test_watermark_regression(self):
        self.create(name="w", window_ms=10, lateness=5)
        _json_request("POST", self.srv.url("/streams/w/watermark"),
                      {"watermark_ms": 20})
        status, payload = _json_request(
            "POST", self.srv.url("/streams/w/watermark"), {"watermark_ms": 19}
        )
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "watermark_regression")
        # State unchanged: equal watermark still succeeds.
        status, payload = _json_request(
            "POST", self.srv.url("/streams/w/watermark"), {"watermark_ms": 20}
        )
        self.assertEqual(status, 200)

    def test_validation_errors(self):
        # Not JSON at all.
        req = urllib.request.Request(
            self.srv.url("/streams"), data=b"{not json", method="POST")
        req.add_header("Content-Type", "application/json")
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req)
        self.assertEqual(cm.exception.code, 400)
        body = cm.exception.read()
        cm.exception.close()
        self.assertEqual(json.loads(body)["error"]["code"], "invalid_json")

        def bad(body, code=422):
            status, payload = _json_request("POST", self.srv.url("/streams"), body)
            self.assertEqual(status, code)
            self.assertEqual(payload["error"]["code"],
                             "invalid_json" if code == 400 else "invalid_request")

        bad({"name": "x", "window_ms": 0, "allowed_lateness_ms": 0})
        bad({"name": "x", "window_ms": -1, "allowed_lateness_ms": 0})
        bad({"name": "x", "window_ms": 1.5, "allowed_lateness_ms": 0})
        bad({"name": "x", "window_ms": True, "allowed_lateness_ms": 0})
        bad({"name": "x", "window_ms": 10, "allowed_lateness_ms": -1})
        bad({"name": "x", "window_ms": 10, "allowed_lateness_ms": 1.0})
        bad({"window_ms": 10, "allowed_lateness_ms": 0})  # missing name
        bad({"name": "x", "allowed_lateness_ms": 0})  # missing window_ms
        bad({"name": "x", "window_ms": 10})  # missing allowed_lateness_ms
        bad({"name": "x", "window_ms": 10, "allowed_lateness_ms": 0, "extra": 1})
        bad({"name": "", "window_ms": 10, "allowed_lateness_ms": 0})

        self.create(name="v")
        for body in [
            {"timestamp_ms": 1.0, "value": 1},
            {"timestamp_ms": True, "value": 1},
            {"timestamp_ms": 1},
            {"timestamp_ms": 1, "value": float("inf")},
            {"timestamp_ms": 1, "value": float("nan")},
            {"timestamp_ms": 1, "value": "1"},
            {"timestamp_ms": 1, "value": 1, "x": 2},
            {"value": 1},
        ]:
            status, payload = _json_request(
                "POST", self.srv.url("/streams/v/events"), body)
            self.assertEqual(status, 422, body)
            self.assertEqual(payload["error"]["code"], "invalid_request", body)

        status, payload = _json_request(
            "POST", self.srv.url("/streams/v/watermark"), {"watermark_ms": 1.5})
        self.assertEqual(status, 422)
        status, payload = _json_request(
            "POST", self.srv.url("/streams/v/watermark"), {"wm": 1})
        self.assertEqual(status, 422)
        status, payload = _json_request(
            "POST", self.srv.url("/streams/v/watermark"), {"watermark_ms": 1, "x": 1})
        self.assertEqual(status, 422)

    def test_failed_requests_leave_no_partial_state(self):
        # Invalid config must not create the stream.
        _json_request("POST", self.srv.url("/streams"),
                      {"name": "ghost", "window_ms": 0, "allowed_lateness_ms": 0})
        status, payload = _json_request(
            "POST", self.srv.url("/streams"),
            {"name": "ghost", "window_ms": 10, "allowed_lateness_ms": 0})
        self.assertEqual(status, 201)

    def test_unknown_routes_and_health(self):
        status, payload = _json_request("GET", self.srv.url("/nope"))
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "not_found")

        status, payload = _json_request("POST", self.srv.url("/streams/s/bogus"), {})
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "not_found")

        status, payload = _json_request("GET", self.srv.url("/healthz"))
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(set(payload), {"status", "service", "version"})


class ServiceUnitTest(unittest.TestCase):
    def test_unknown_stream_raises(self):
        svc = Service()
        with self.assertRaises(ServiceError) as cm:
            svc.get_results("nope")
        self.assertEqual(cm.exception.status, 404)
        self.assertEqual(cm.exception.code, "stream_not_found")


if __name__ == "__main__":
    unittest.main()
