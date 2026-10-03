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

    def create_auto(self, name="a", window_ms=1000, allowed_lateness_ms=0, lag=100,
                    dedup_retention_ms=None):
        body = {
            "name": name,
            "window_ms": window_ms,
            "allowed_lateness_ms": allowed_lateness_ms,
            "auto_watermark_lag_ms": lag,
        }
        if dedup_retention_ms is not None:
            body["dedup_retention_ms"] = dedup_retention_ms
        return self.request("POST", "/streams", body)

    def create_manual(self, name="m", window_ms=1000, allowed_lateness_ms=0):
        return self.request(
            "POST",
            "/streams",
            {"name": name, "window_ms": window_ms,
             "allowed_lateness_ms": allowed_lateness_ms},
        )

    def event(self, ts, value, stream="a", event_id=None):
        body = {"timestamp_ms": ts, "value": value}
        if event_id is not None:
            body["event_id"] = event_id
        return self.request("POST", f"/streams/{stream}/events", body)

    def watermark(self, wm, stream="a"):
        return self.request("POST", f"/streams/{stream}/watermark",
                            {"watermark_ms": wm})

    def results(self, stream="a"):
        return self.request("GET", f"/streams/{stream}/results")


class CreateAutoStreamTest(HttpTestCase):
    def test_create_echoes_lag(self):
        status, payload = self.create_auto("orders", 5000, 250, lag=300)
        self.assertEqual(status, 201)
        self.assertEqual(payload["auto_watermark_lag_ms"], 300)

    def test_lag_zero_accepted(self):
        status, payload = self.create_auto("z", lag=0)
        self.assertEqual(status, 201)
        self.assertEqual(payload["auto_watermark_lag_ms"], 0)

    def test_invalid_lag_rejected_without_partial_state(self):
        for bad in (-1, 1.5, True, "100", None):
            body = {"name": "x", "window_ms": 1000, "allowed_lateness_ms": 0,
                    "auto_watermark_lag_ms": bad}
            status, payload = self.request("POST", "/streams", body)
            self.assertEqual(status, 422, body)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        status, payload = self.request(
            "POST", "/streams",
            {"name": "x", "window_ms": 1000, "allowed_lateness_ms": 0,
             "auto_watermark_lag_ms": 100, "bogus": 1},
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        # no partial state: the name is free afterwards
        status, payload = self.create_auto("x")
        self.assertEqual(status, 201)

    def test_manual_stream_creation_response_unchanged(self):
        status, payload = self.create_manual("plain")
        self.assertEqual(status, 201)
        self.assertNotIn("auto_watermark_lag_ms", payload)


class AutoWatermarkEventTest(HttpTestCase):
    def test_first_event_watermark_null_until_effective(self):
        # lag 500, window 1000: event at 100 -> wm 100 - 500 = -400
        self.create_auto(lag=500)
        status, payload = self.event(100, 1)
        self.assertEqual(status, 200)
        self.assertIsNone(payload["watermark_ms"])
        self.assertEqual(payload["finalized"], [])
        self.assertFalse(payload["dropped"])

    def test_watermark_tracks_max_event_minus_lag(self):
        self.create_auto(lag=100)
        _, payload = self.event(999, 1)
        self.assertEqual(payload["watermark_ms"], 899)
        self.assertEqual(payload["finalized"], [])
        # window [0,1000) finalizes once wm >= 1000 + 0
        _, payload = self.event(1100, 2)
        self.assertEqual(payload["watermark_ms"], 1000)
        self.assertEqual(
            payload["finalized"],
            [{"stream": "a", "window_start_ms": 0, "window_end_ms": 1000,
              "count": 1, "sum": 1}],
        )
        # window only finalizes once
        _, payload = self.event(1200, 3)
        self.assertEqual(payload["watermark_ms"], 1100)
        self.assertEqual(payload["finalized"], [])
        _, payload = self.results()
        self.assertEqual([r["window_start_ms"] for r in payload["results"]], [0])

    def test_older_acceptable_event_does_not_regress(self):
        self.create_auto(allowed_lateness_ms=500, lag=100)
        self.event(2000, 1)       # wm 1900
        status, payload = self.event(1500, 2)  # >= 1900-500=1400: accepted
        self.assertEqual(status, 200)
        self.assertFalse(payload["dropped"])
        self.assertEqual(payload["watermark_ms"], 1900)
        self.assertEqual(payload["finalized"], [])

    def test_late_event_dropped_keeps_watermark_and_empty_finalized(self):
        self.create_auto(allowed_lateness_ms=100, lag=0)
        self.event(5000, 1)       # wm 5000
        status, payload = self.event(4899, 2)  # < 5000-100 = 4900
        self.assertEqual(status, 200)
        self.assertTrue(payload["dropped"])
        self.assertEqual(payload["watermark_ms"], 5000)
        self.assertEqual(payload["finalized"], [])

    def test_equal_timestamp_does_not_move_watermark(self):
        self.create_auto(lag=100)
        self.event(1000, 1)
        _, payload = self.event(1000, 2)
        self.assertEqual(payload["watermark_ms"], 900)
        self.assertEqual(payload["finalized"], [])

    def test_allowed_lateness_honored_when_finalizing(self):
        self.create_auto(allowed_lateness_ms=200, lag=0)
        self.event(999, 1)    # wm 999 < 1000+200: [0,1000) stays open
        _, payload = self.event(999, 1)
        self.assertEqual(payload["finalized"], [])
        _, payload = self.event(1200, 2)  # wm 1200 reaches 1000+200: final
        self.assertEqual([r["window_start_ms"] for r in payload["finalized"]], [0])
        _, payload = self.results()
        self.assertEqual([r["window_start_ms"] for r in payload["results"]], [0])

    def test_manual_watermark_then_auto_never_regresses(self):
        self.create_auto(allowed_lateness_ms=10000, lag=100)
        status, payload = self.watermark(5000)
        self.assertEqual(status, 200)
        self.assertEqual(payload["watermark_ms"], 5000)
        _, payload = self.event(2000, 1)  # accepted (within lateness); target 1900
        self.assertFalse(payload["dropped"])
        self.assertEqual(payload["watermark_ms"], 5000)
        self.assertEqual(payload["finalized"], [])
        _, payload = self.event(6000, 1)  # target 5900 > 5000
        self.assertEqual(payload["watermark_ms"], 5900)

    def test_watermark_regression_still_conflicts_on_auto_stream(self):
        self.create_auto(lag=0)
        self.event(5000, 1)
        status, payload = self.watermark(4999)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "watermark_regression")
        status, _ = self.watermark(5000)
        self.assertEqual(status, 200)


class AutoWatermarkDedupTest(HttpTestCase):
    def create_dedup_auto(self, lag=100, allowed_lateness_ms=0, retention=10000):
        return self.create_auto(
            "a", window_ms=1000, allowed_lateness_ms=allowed_lateness_ms,
            lag=lag, dedup_retention_ms=retention)

    def test_duplicate_does_not_advance(self):
        self.create_dedup_auto(lag=0)
        _, payload = self.event(1000, 1, event_id="e1")
        self.assertEqual(payload["watermark_ms"], 1000)
        status, payload = self.event(1000, 1, event_id="e1")
        self.assertEqual(status, 200)
        self.assertTrue(payload["duplicate"])
        self.assertEqual(payload["watermark_ms"], 1000)
        self.assertEqual(payload["finalized"], [])

    def test_conflict_does_not_advance(self):
        self.create_dedup_auto(lag=0)
        self.event(1000, 1, event_id="e1")
        status, payload = self.event(2000, 1, event_id="e1")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")
        # an unseen event proves wm stayed at 1000 (2000-0 would be 2000)
        _, payload = self.event(1500, 1, event_id="e2")
        self.assertEqual(payload["watermark_ms"], 1500)

    def test_late_unseen_dropped_does_not_advance(self):
        self.create_dedup_auto(lag=0, allowed_lateness_ms=100)
        self.event(5000, 1, event_id="e1")  # wm 5000
        status, payload = self.event(4899, 1, event_id="e2")
        self.assertTrue(payload["dropped"])
        self.assertFalse(payload["duplicate"])
        self.assertEqual(payload["watermark_ms"], 5000)
        self.assertEqual(payload["finalized"], [])

    def test_finalize_once_under_mix(self):
        self.create_dedup_auto(lag=0, allowed_lateness_ms=0)
        self.event(999, 1, event_id="a")
        _, payload = self.event(1000, 1, event_id="b")  # wm 1000 closes [0,1000)
        self.assertEqual([r["window_start_ms"] for r in payload["finalized"]], [0])
        # retry of "a" past eviction horizon is still a retained duplicate
        _, payload = self.event(999, 1, event_id="a")
        self.assertTrue(payload["duplicate"])
        self.assertEqual(payload["finalized"], [])


class SnapshotIntegrationTest(HttpTestCase):
    def test_snapshot_contains_auto_fields(self):
        self.create_auto(lag=100)
        self.event(1500, 1)
        status, payload = self.request("GET", "/snapshot")
        self.assertEqual(status, 200)
        entry = payload["streams"][0]
        self.assertEqual(entry["auto_watermark_lag_ms"], 100)
        self.assertEqual(entry["max_event_timestamp_ms"], 1500)
        self.assertEqual(entry["watermark_ms"], 1400)

    def test_fresh_auto_stream_snapshots_null_max(self):
        self.create_auto(lag=100)
        _, payload = self.request("GET", "/snapshot")
        entry = payload["streams"][0]
        self.assertEqual(entry["auto_watermark_lag_ms"], 100)
        self.assertIsNone(entry["max_event_timestamp_ms"])
        self.assertIsNone(entry["watermark_ms"])

    def test_manual_snapshot_unchanged(self):
        self.create_manual()
        _, payload = self.request("GET", "/snapshot")
        entry = payload["streams"][0]
        self.assertNotIn("auto_watermark_lag_ms", entry)
        self.assertNotIn("max_event_timestamp_ms", entry)

    def _restore(self, document):
        return self.request("POST", "/snapshot/restore", document)

    def test_round_trip_restores_behavior(self):
        self.create_auto(allowed_lateness_ms=100, lag=100)
        self.event(500, 1)
        self.event(1400, 2)
        _, before = self.request("GET", "/snapshot")
        _, results_before = self.results()

        Handler.service = Service()
        status, payload = self._restore(before)
        self.assertEqual(status, 200)
        self.assertEqual(payload["restored_streams"], 1)

        _, exported = self.request("GET", "/snapshot")
        self.assertEqual(exported, before)
        _, results_after = self.results()
        self.assertEqual(results_after, results_before)

        # automatic advancement continues with the same semantics
        _, payload = self.event(1400, 2)  # older accepted event, no move
        self.assertEqual(payload["watermark_ms"], 1300)
        _, payload = self.event(2200, 3)  # wm 2100 closes [1000,2000)
        self.assertEqual(payload["watermark_ms"], 2100)
        starts = [r["window_start_ms"] for r in payload["finalized"]]
        self.assertEqual(starts, [1000])

    def test_restore_manual_document_stays_manual(self):
        self.create_manual()
        self.event(100, 1)
        self.watermark(10**9, stream="m")
        _, doc = self.request("GET", "/snapshot")
        Handler.service = Service()
        status, payload = self._restore(doc)
        self.assertEqual(status, 200)
        _, resp = self.event(50, 1, stream="m")
        self.assertNotIn("watermark_ms", resp)
        self.assertNotIn("finalized", resp)

    def test_invalid_auto_snapshots_rejected_and_leave_empty(self):
        def restore_and_expect_invalid(document):
            Handler.service = Service()
            status, payload = self._restore(document)
            self.assertEqual(status, 422, document)
            self.assertEqual(payload["error"]["code"], "invalid_snapshot")
            _, health = self.request("GET", "/snapshot")
            self.assertEqual(health["streams"], [])

        good_entry = {
            "name": "a", "window_ms": 1000, "allowed_lateness_ms": 0,
            "dedup_retention_ms": None, "watermark_ms": None,
            "windows": [], "finalized": [],
            "auto_watermark_lag_ms": 100, "max_event_timestamp_ms": None,
        }
        base = {"format_version": 1, "streams": [good_entry]}

        def clone(entry_mut):
            doc = json.loads(json.dumps(base))
            entry_mut(doc["streams"][0])
            return doc

        # only one of the paired fields
        restore_and_expect_invalid(clone(lambda e: e.pop("max_event_timestamp_ms")))
        restore_and_expect_invalid(clone(lambda e: e.pop("auto_watermark_lag_ms")))
        # bad types
        restore_and_expect_invalid(clone(lambda e: e.update(auto_watermark_lag_ms=-1)))
        restore_and_expect_invalid(clone(lambda e: e.update(auto_watermark_lag_ms=1.5)))
        restore_and_expect_invalid(clone(lambda e: e.update(auto_watermark_lag_ms=True)))
        restore_and_expect_invalid(
            clone(lambda e: e.update(max_event_timestamp_ms="100")))
        # non-null max without any window
        restore_and_expect_invalid(
            clone(lambda e: e.update(max_event_timestamp_ms=500, watermark_ms=400)))
        # max falls into a window not present
        with_window = clone(lambda e: e.update(
            max_event_timestamp_ms=2500, watermark_ms=2400,
            windows=[{"window_start_ms": 0, "window_end_ms": 1000,
                      "count": 1, "sum": 1}]))
        restore_and_expect_invalid(with_window)
        # watermark below max - lag
        low_wm = clone(lambda e: e.update(
            max_event_timestamp_ms=1500, watermark_ms=1399,
            windows=[{"window_start_ms": 1000, "window_end_ms": 2000,
                      "count": 1, "sum": 1}]))
        restore_and_expect_invalid(low_wm)

    def test_restore_conflict_priority_unchanged(self):
        self.create_auto(lag=100)
        # instance already has a stream; even an invalid doc gets 409
        status, payload = self._restore({"format_version": 1, "streams": []})
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "restore_conflict")
        # invalid JSON is still a request error
        status, payload = self.request(
            "POST", "/snapshot/restore", raw=b"{bad")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_json")


class AutoWatermarkConcurrencyTest(HttpTestCase):
    def test_concurrent_events_serialize_and_finalize_once(self):
        self.create_auto(window_ms=100, allowed_lateness_ms=0, lag=0)
        errors = []

        def submit(i):
            try:
                # identical timestamps: every event aggregates into
                # [100,200) and the watermark stays at 100 until the end.
                self.event(100, i)
            except Exception as exc:  # pragma: no cover - failure reporting
                errors.append(exc)

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(40)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        self.assertEqual(errors, [])

        # push the watermark past the window and check exactly one final row
        status, payload = self.event(200, 1)
        self.assertEqual(status, 200)
        finals = [r for r in payload["finalized"] if r["window_start_ms"] == 100]
        self.assertEqual(len(finals), 1)
        self.watermark(10**9)
        _, payload = self.results()
        target = [r for r in payload["results"] if r["window_start_ms"] == 100]
        self.assertEqual(len(target), 1)
        self.assertEqual(target[0]["count"], 40)


if __name__ == "__main__":
    unittest.main()
