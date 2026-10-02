import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from streammill.server import Handler
from streammill.service import Service


class HttpTestCase(unittest.TestCase):
    def setUp(self) -> None:
        Handler.service = Service()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)

    def request(self, method, path, body=None, raw=None):
        data = raw
        if data is None and body is not None:
            data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(self.base + path, data=data, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def create(self, name="s", window_ms=1000, allowed_lateness_ms=0):
        return self.request(
            "POST",
            "/streams",
            {"name": name, "window_ms": window_ms, "allowed_lateness_ms": allowed_lateness_ms},
        )


class HealthAndRoutingTest(HttpTestCase):
    def test_health_unchanged(self):
        status, payload = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "streammill")
        self.assertIn("version", payload)

    def test_unknown_route_still_not_found(self):
        status, payload = self.request("GET", "/nope")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "not_found")
        status, payload = self.request("POST", "/nope", {"a": 1})
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "not_found")


class CreateStreamTest(HttpTestCase):
    def test_create_returns_201(self):
        status, payload = self.create("orders", 5000, 250)
        self.assertEqual(status, 201)
        self.assertEqual(payload["stream"], "orders")
        self.assertEqual(payload["window_ms"], 5000)
        self.assertEqual(payload["allowed_lateness_ms"], 250)

    def test_duplicate_stream_conflicts(self):
        self.create("orders")
        status, payload = self.create("orders")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "stream_exists")

    def test_invalid_configs_rejected(self):
        for body in (
            {"name": "x", "window_ms": 0, "allowed_lateness_ms": 0},
            {"name": "x", "window_ms": -5, "allowed_lateness_ms": 0},
            {"name": "x", "window_ms": 1000, "allowed_lateness_ms": -1},
            {"name": "x", "window_ms": 1.5, "allowed_lateness_ms": 0},
            {"name": "x", "window_ms": True, "allowed_lateness_ms": 0},
            {"name": "", "window_ms": 1000, "allowed_lateness_ms": 0},
            {"name": 7, "window_ms": 1000, "allowed_lateness_ms": 0},
            {"name": "x", "window_ms": 1000},
            {"name": "x", "window_ms": 1000, "allowed_lateness_ms": 0, "extra": 1},
        ):
            status, payload = self.request("POST", "/streams", body)
            self.assertEqual(status, 422, body)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        # failed creates leave no partial state behind
        status, _ = self.request("GET", "/streams/x/results")
        self.assertEqual(status, 404)


class EventAndWatermarkTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create("s", 1000, 100)

    def event(self, ts, value, stream="s"):
        return self.request("POST", f"/streams/{stream}/events", {"timestamp_ms": ts, "value": value})

    def watermark(self, wm, stream="s"):
        return self.request("POST", f"/streams/{stream}/watermark", {"watermark_ms": wm})

    def results(self, stream="s"):
        return self.request("GET", f"/streams/{stream}/results")

    def test_window_aggregation_and_finalization(self):
        self.event(0, 1.5)
        self.event(999, 2.5)   # same window [0, 1000)
        self.event(1000, 10)   # next window [1000, 2000)

        # watermark below window_end + lateness: nothing final yet
        status, payload = self.watermark(1099)
        self.assertEqual(status, 200)
        self.assertEqual(payload["finalized"], [])
        status, payload = self.results()
        self.assertEqual(payload["results"], [])

        # reaches 1000 + 100: first window final, second still open
        status, payload = self.watermark(1100)
        self.assertEqual(
            payload["finalized"],
            [{"stream": "s", "window_start_ms": 0, "window_end_ms": 1000, "count": 2, "sum": 4.0}],
        )

        # repeating the same watermark succeeds without duplicate results
        status, payload = self.watermark(1100)
        self.assertEqual(status, 200)
        self.assertEqual(payload["finalized"], [])

        status, payload = self.watermark(2100)
        self.assertEqual(len(payload["finalized"]), 1)
        self.assertEqual(payload["finalized"][0]["window_start_ms"], 1000)
        self.assertEqual(payload["finalized"][0]["count"], 1)
        self.assertEqual(payload["finalized"][0]["sum"], 10)

        status, payload = self.results()
        self.assertEqual(status, 200)
        self.assertEqual([r["window_end_ms"] for r in payload["results"]], [1000, 2000])
        # stable under repeated queries
        _, again = self.results()
        self.assertEqual(payload, again)

    def test_results_sorted_by_window_end(self):
        self.event(2500, 1)
        self.event(500, 2)
        self.event(1500, 3)
        self.watermark(10**9)
        _, payload = self.results()
        ends = [r["window_end_ms"] for r in payload["results"]]
        self.assertEqual(ends, sorted(ends))
        self.assertEqual(len(ends), 3)

    def test_left_closed_right_open_boundary(self):
        self.event(1999, 1)
        self.event(2000, 2)
        self.watermark(10**9)
        _, payload = self.results()
        by_start = {r["window_start_ms"]: r for r in payload["results"]}
        self.assertEqual(by_start[1000]["count"], 1)
        self.assertEqual(by_start[2000]["count"], 1)

    def test_late_event_dropped_boundary_accepted(self):
        self.watermark(5000)  # lateness horizon: 5000 - 100 = 4900
        status, payload = self.event(4899, 1)
        self.assertEqual(status, 200)
        self.assertTrue(payload["dropped"])
        status, payload = self.event(4900, 2)
        self.assertTrue(payload["dropped"] is False)
        _, payload = self.results()
        self.assertEqual(payload["results"], [])  # window [4000,5000) closed at wm 5100
        status, payload = self.watermark(5099)
        self.assertEqual(payload["finalized"], [])
        status, payload = self.watermark(5100)
        self.assertEqual(len(payload["finalized"]), 1)
        self.assertEqual(payload["finalized"][0]["sum"], 2)

    def test_watermark_regression_conflicts(self):
        self.watermark(5000)
        status, payload = self.watermark(4999)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "watermark_regression")
        # failed advance leaves state untouched
        status, payload = self.watermark(5000)
        self.assertEqual(status, 200)

    def test_unknown_stream_404(self):
        for method, path, body in (
            ("POST", "/streams/ghost/events", {"timestamp_ms": 0, "value": 1}),
            ("POST", "/streams/ghost/watermark", {"watermark_ms": 0}),
            ("GET", "/streams/ghost/results", None),
        ):
            status, payload = self.request(method, path, body)
            self.assertEqual(status, 404, path)
            self.assertEqual(payload["error"]["code"], "stream_not_found")

    def test_streams_are_isolated(self):
        self.create("other", 1000, 0)
        self.event(100, 5)
        self.event(100, 7, stream="other")
        self.watermark(1100)  # stream "s" has allowed_lateness_ms=100
        self.watermark(1000, stream="other")
        _, mine = self.results()
        _, other = self.results("other")
        self.assertEqual(mine["results"][0]["sum"], 5)
        self.assertEqual(other["results"][0]["sum"], 7)

    def test_invalid_json_and_event_validation(self):
        status, payload = self.request("POST", "/streams/s/events", raw=b"{not json")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_json")

        for body in (
            {"timestamp_ms": 0},                       # missing value
            {"timestamp_ms": "0", "value": 1},         # wrong type
            {"timestamp_ms": 0, "value": "x"},         # wrong type
            {"timestamp_ms": 0, "value": True},        # bool is not a number here
            {"timestamp_ms": 0.5, "value": 1},         # timestamp must be integer
            {"timestamp_ms": 0, "value": 1, "x": 2},   # undeclared field
        ):
            status, payload = self.request("POST", "/streams/s/events", body)
            self.assertEqual(status, 422, body)
            self.assertEqual(payload["error"]["code"], "invalid_request")

        # NaN / Infinity literals parse but are not finite values
        status, payload = self.request(
            "POST", "/streams/s/events", raw=b'{"timestamp_ms": 0, "value": NaN}'
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")

        # failed events leave no partial state
        _, payload = self.results()
        self.assertEqual(payload["results"], [])


if __name__ == "__main__":
    unittest.main()
