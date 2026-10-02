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

    def request(self, method, path, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def create(self, name="s", window_ms=1000, allowed_lateness_ms=0, **extra):
        body = {
            "name": name,
            "window_ms": window_ms,
            "allowed_lateness_ms": allowed_lateness_ms,
            **extra,
        }
        return self.request("POST", "/streams", body)

    def event(self, ts, value, event_id=None, stream="s"):
        body = {"timestamp_ms": ts, "value": value}
        if event_id is not None:
            body["event_id"] = event_id
        return self.request("POST", f"/streams/{stream}/events", body)

    def watermark(self, wm, stream="s"):
        return self.request("POST", f"/streams/{stream}/watermark", {"watermark_ms": wm})

    def results(self, stream="s"):
        return self.request("GET", f"/streams/{stream}/results")


class CreateWithDedupTest(HttpTestCase):
    def test_retention_echoed_when_provided(self):
        status, payload = self.create("d", 1000, 100, dedup_retention_ms=5000)
        self.assertEqual(status, 201)
        self.assertEqual(payload["dedup_retention_ms"], 5000)
        self.assertEqual(payload["stream"], "d")

    def test_retention_absent_by_default(self):
        status, payload = self.create("plain", 1000, 100)
        self.assertEqual(status, 201)
        self.assertNotIn("dedup_retention_ms", payload)

    def test_invalid_retention_rejected(self):
        for retention in (0, -1, 1.5, "1000", True):
            status, payload = self.create("d", 1000, 100, dedup_retention_ms=retention)
            self.assertEqual(status, 422, retention)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        # retention below allowed lateness is rejected too
        status, payload = self.create("d", 1000, 100, dedup_retention_ms=99)
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        # equal to allowed lateness is accepted
        status, _ = self.create("d", 1000, 100, dedup_retention_ms=100)
        self.assertEqual(status, 201)

    def test_failed_create_leaves_no_stream(self):
        self.create("d", 1000, 100, dedup_retention_ms=50)
        status, payload = self.request("GET", "/streams/d/results")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "stream_not_found")
        # and the name can be reused with a valid config
        status, _ = self.create("d", 1000, 100, dedup_retention_ms=100)
        self.assertEqual(status, 201)


class DedupEventValidationTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create("s", 1000, 100, dedup_retention_ms=5000)
        self.create("plain", 1000, 100)

    def test_event_id_required_on_dedup_stream(self):
        for body in (
            {"timestamp_ms": 0, "value": 1},                        # missing
            {"timestamp_ms": 0, "value": 1, "event_id": 7},         # wrong type
            {"timestamp_ms": 0, "value": 1, "event_id": ""},        # empty
            {"timestamp_ms": 0, "value": 1, "event_id": "a", "x": 1},  # extra
        ):
            status, payload = self.request("POST", "/streams/s/events", body)
            self.assertEqual(status, 422, body)
            self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_event_id_undeclared_on_plain_stream(self):
        status, payload = self.event(0, 1, event_id="a", stream="plain")
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        # plain stream events keep the historical response shape
        status, payload = self.event(0, 1, stream="plain")
        self.assertEqual(status, 200)
        self.assertNotIn("duplicate", payload)


class DedupBehaviorTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create("s", 1000, 100, dedup_retention_ms=5000)

    def test_new_then_duplicate_then_conflict(self):
        status, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(status, 200)
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], False)

        # exact retry: acknowledged as duplicate, window unchanged
        status, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(status, 200)
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], True)

        # numerically equal value of another JSON type still duplicates
        status, payload = self.event(100, 5.0, event_id="e1")
        self.assertEqual(payload["duplicate"], True)

        # same id, different value or timestamp: conflict, nothing changes
        status, payload = self.event(100, 6, event_id="e1")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")
        status, payload = self.event(101, 5, event_id="e1")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")

        self.watermark(1100)
        _, payload = self.results()
        self.assertEqual(payload["results"][0]["count"], 1)
        self.assertEqual(payload["results"][0]["sum"], 5)

    def test_dedup_check_precedes_lateness(self):
        self.event(100, 5, event_id="e1")
        self.watermark(1000)  # lateness horizon: 900, past ts=100; dedup keeps e1
        # retained exact retry is still a duplicate, not a drop
        status, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], True)
        # unseen id beyond the horizon is dropped and not remembered
        status, payload = self.event(200, 7, event_id="e2")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)
        # resubmitting e2 is dropped again (not a duplicate: never recorded)
        status, payload = self.event(200, 7, event_id="e2")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)

    def test_retention_eviction_and_id_reuse(self):
        self.event(100, 5, event_id="e1")
        # horizon = wm - retention; ts=100 evicted only when wm - 5000 > 100
        self.watermark(5100)  # horizon 100: boundary kept
        status, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(payload["duplicate"], True)
        self.watermark(5101)  # horizon 101: e1 evicted
        # reused id is a brand-new event (and too late now)
        status, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)

        # reuse after eviction for a non-late event aggregates again
        self.event(10000, 1, event_id="e3")
        self.watermark(15200)  # horizon 10200: e3 evicted; lateness horizon 15100
        status, payload = self.event(15500, 9, event_id="e3")
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], False)
        self.watermark(10**9)
        _, payload = self.results()
        by_start = {r["window_start_ms"]: r for r in payload["results"]}
        self.assertEqual(by_start[10000]["count"], 1)
        self.assertEqual(by_start[10000]["sum"], 1)
        self.assertEqual(by_start[15000]["count"], 1)
        self.assertEqual(by_start[15000]["sum"], 9)

    def test_no_watermark_no_eviction(self):
        self.event(100, 5, event_id="e1")
        # without any watermark advance the id is retained indefinitely
        status, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(payload["duplicate"], True)

    def test_id_spaces_isolated_between_streams(self):
        self.create("other", 1000, 100, dedup_retention_ms=5000)
        self.event(100, 5, event_id="e1")
        status, payload = self.event(100, 5, event_id="e1", stream="other")
        self.assertEqual(payload["duplicate"], False)
        # same id with different data on the other stream conflicts there only
        status, payload = self.event(100, 6, event_id="e1", stream="other")
        self.assertEqual(status, 409)
        status, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(payload["duplicate"], True)

    def test_concurrent_same_id_aggregates_once(self):
        outcomes = []

        def submit():
            _, payload = self.event(100, 5, event_id="e1")
            outcomes.append(payload["duplicate"])

        threads = [threading.Thread(target=submit) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(outcomes), [False] + [True] * 19)
        self.watermark(1100)
        _, payload = self.results()
        self.assertEqual(payload["results"][0]["count"], 1)


if __name__ == "__main__":
    unittest.main()
