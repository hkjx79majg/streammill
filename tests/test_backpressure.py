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

    def create(self, name="s", window_ms=1000, allowed_lateness_ms=0, **extra):
        return self.request(
            "POST",
            "/streams",
            {
                "name": name,
                "window_ms": window_ms,
                "allowed_lateness_ms": allowed_lateness_ms,
                **extra,
            },
        )

    def event(self, ts, value, stream="s", **extra):
        return self.request(
            "POST",
            f"/streams/{stream}/events",
            {"timestamp_ms": ts, "value": value, **extra},
        )

    def batch(self, batch_id, events, stream="s"):
        return self.request(
            "POST",
            f"/streams/{stream}/batches",
            {"batch_id": batch_id, "events": events},
        )

    def watermark(self, wm, stream="s"):
        return self.request(
            "POST", f"/streams/{stream}/watermark", {"watermark_ms": wm}
        )

    def pressure(self, stream="s"):
        return self.request("GET", f"/streams/{stream}/pressure")

    def results(self, stream="s"):
        return self.request("GET", f"/streams/{stream}/results")

    def changes(self, after_seq=0, limit=1000, stream="s"):
        return self.request(
            "GET", f"/streams/{stream}/changes?after_seq={after_seq}&limit={limit}"
        )

    def snapshot(self):
        return self.request("GET", "/snapshot")

    def restore(self, document):
        return self.request("POST", "/snapshot/restore", document)


class CreateAndQueryTest(HttpTestCase):
    def test_create_echoes_max_open_windows(self):
        status, body = self.create(max_open_windows=3)
        self.assertEqual(status, 201)
        self.assertEqual(body["max_open_windows"], 3)

    def test_create_without_limit_keeps_baseline_shape(self):
        status, body = self.create()
        self.assertEqual(status, 201)
        self.assertNotIn("max_open_windows", body)

    def test_create_validates_max_open_windows(self):
        for bad in (0, -1, 1.5, "2", True):
            status, body = self.create(max_open_windows=bad)
            self.assertEqual(status, 422, bad)
            self.assertEqual(body["error"]["code"], "invalid_request")
        status, _ = self.pressure()
        self.assertEqual(status, 404)

    def test_pressure_unknown_stream(self):
        status, body = self.pressure("nope")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "stream_not_found")

    def test_pressure_not_enabled(self):
        self.create()
        status, body = self.pressure()
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "backpressure_not_enabled")

    def test_pressure_counts_only_open_base_windows(self):
        self.create(max_open_windows=2)
        status, body = self.pressure()
        self.assertEqual(
            body,
            {
                "stream": "s",
                "max_open_windows": 2,
                "open_windows": 0,
                "available_windows": 2,
            },
        )
        self.event(0, 1.0)
        self.event(500, 2.0)
        _, body = self.pressure()
        self.assertEqual(body["open_windows"], 1)
        self.assertEqual(body["available_windows"], 1)
        self.event(1500, 3.0)
        _, body = self.pressure()
        self.assertEqual(body["open_windows"], 2)
        self.assertEqual(body["available_windows"], 0)


class SingleEventBackpressureTest(HttpTestCase):
    def test_event_over_limit_is_rejected_and_rolled_back(self):
        self.create(max_open_windows=1, dedup_retention_ms=100000)
        self.assertEqual(self.event(0, 1.0, event_id="a")[0], 200)
        status, body = self.event(5000, 2.0, event_id="b")
        self.assertEqual(status, 429)
        self.assertEqual(body["error"]["code"], "stream_backpressured")
        # State is exactly as before the rejected event.
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)
        # The rejected id was not registered: it can be reused.
        self.assertEqual(self.event(500, 2.0, event_id="b")[0], 200)
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)

    def test_event_within_existing_windows_stays_accepted_at_limit(self):
        self.create(max_open_windows=1)
        self.assertEqual(self.event(0, 1.0)[0], 200)
        self.assertEqual(self.event(100, 2.0)[0], 200)
        self.assertEqual(self.event(999, 3.0)[0], 200)
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)

    def test_duplicate_and_too_late_never_backpressure(self):
        self.create(max_open_windows=1, dedup_retention_ms=100000)
        self.event(0, 1.0, event_id="a")
        self.watermark(2000)
        # Window finalized; a new window opens.
        self.event(3000, 1.0, event_id="c")
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)
        # Exact duplicate of a retained id: still a success.
        status, body = self.event(0, 1.0, event_id="a")
        self.assertEqual(status, 200)
        self.assertTrue(body["duplicate"])
        # Too-late unseen event: dropped successfully, no new window.
        status, body = self.event(1500, 9.0, event_id="d")
        self.assertEqual(status, 200)
        self.assertTrue(body["dropped"])
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)

    def test_manual_watermark_frees_slots(self):
        self.create(max_open_windows=1)
        self.event(0, 1.0)
        status, _ = self.event(5000, 2.0)
        self.assertEqual(status, 429)
        status, body = self.watermark(1000)
        self.assertEqual(status, 200)
        self.assertEqual(len(body["finalized"]), 1)
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 0)
        self.assertEqual(self.event(5000, 2.0)[0], 200)
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)

    def test_sliding_windows_count_separately(self):
        # window 1000, slide 250: one event lands in four windows.
        self.create(window_ms=1000, slide_ms=250, max_open_windows=4)
        self.assertEqual(self.event(1000, 1.0)[0], 200)
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 4)
        status, _ = self.event(5000, 1.0)
        self.assertEqual(status, 429)

    def test_automatic_advance_frees_slots_before_check(self):
        # lag 0: accepting an event at t=1000 advances the watermark to
        # 1000, finalizing the [0,1000) window before the limit is checked.
        self.create(max_open_windows=1, auto_watermark_lag_ms=0)
        self.assertEqual(self.event(0, 1.0)[0], 200)
        status, body = self.event(1000, 2.0)
        self.assertEqual(status, 200)
        self.assertEqual(len(body["finalized"]), 1)
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)

    def test_automatic_rollback_restores_watermark_and_max_timestamp(self):
        self.create(max_open_windows=1, auto_watermark_lag_ms=500)
        _, body = self.event(0, 1.0)
        self.assertEqual(body["watermark_ms"], -500)
        # Event at 1200 opens [1000,2000) and advances the watermark to
        # 700, which does not finalize the first window: over the limit,
        # so the event and the advance are rolled back.
        status, _ = self.event(1200, 2.0)
        self.assertEqual(status, 429)
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)
        # The maximum event timestamp was rolled back: an event at 600
        # advances the watermark from -500 to 100, not to 700.
        _, body = self.event(600, 3.0)
        self.assertEqual(body["watermark_ms"], 100)
        # A manual advance finalizes the first window and frees its slot.
        status, body = self.watermark(1000)
        self.assertEqual(status, 200)
        self.assertEqual(len(body["finalized"]), 1)
        self.assertEqual(self.event(1200, 2.0)[0], 200)

    def test_change_sequence_rolled_back_on_rejection(self):
        self.create(max_open_windows=1, change_retention=100)
        self.event(0, 1.0)
        _, feed = self.changes()
        self.assertEqual(feed["latest_seq"], 1)
        self.assertEqual(self.event(5000, 2.0)[0], 429)
        _, feed = self.changes()
        self.assertEqual(feed["latest_seq"], 1)
        self.assertEqual(len(feed["changes"]), 1)

    def test_conflict_and_lookup_errors_keep_priority(self):
        self.create(max_open_windows=1, dedup_retention_ms=100000)
        self.event(0, 1.0, event_id="a")
        # Same id, different content: 409 even though the stream is at
        # its limit and the event would also exceed it.
        status, body = self.event(5000, 2.0, event_id="a")
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "event_id_conflict")


class BatchBackpressureTest(HttpTestCase):
    def create_batch_stream(self, **extra):
        return self.create(batch_retention=10, **extra)

    def test_batch_over_limit_rolls_back_atomically(self):
        self.create_batch_stream(max_open_windows=2)
        status, body = self.batch(
            "b1",
            [
                {"timestamp_ms": 0, "value": 1.0},
                {"timestamp_ms": 1000, "value": 2.0},
                {"timestamp_ms": 2000, "value": 3.0},
            ],
        )
        self.assertEqual(status, 429)
        self.assertEqual(body["error"]["code"], "stream_backpressured")
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 0)
        # The failed batch did not occupy its id: the same id with
        # fitting content succeeds.
        status, body = self.batch(
            "b1", [{"timestamp_ms": 0, "value": 1.0}]
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["batch_id"], "b1")
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)

    def test_batch_exactly_at_limit_succeeds(self):
        self.create_batch_stream(max_open_windows=2)
        status, body = self.batch(
            "b1",
            [
                {"timestamp_ms": 0, "value": 1.0},
                {"timestamp_ms": 1000, "value": 2.0},
            ],
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(body["outcomes"]), 2)
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 2)

    def test_batch_rollback_restores_dedup_and_changes(self):
        self.create_batch_stream(
            max_open_windows=1, dedup_retention_ms=100000, change_retention=100
        )
        status, _ = self.batch(
            "b1",
            [
                {"timestamp_ms": 0, "value": 1.0, "event_id": "x"},
                {"timestamp_ms": 5000, "value": 2.0, "event_id": "y"},
            ],
        )
        self.assertEqual(status, 429)
        _, feed = self.changes()
        self.assertEqual(feed["latest_seq"], 0)
        # The first element's id was rolled back with everything else.
        status, body = self.event(0, 1.0, event_id="x")
        self.assertEqual(status, 200)
        self.assertFalse(body["duplicate"])


class SnapshotBackpressureTest(HttpTestCase):
    def test_snapshot_version_6_round_trip(self):
        self.create(max_open_windows=2)
        self.event(0, 1.0)
        self.event(1000, 2.0)
        status, snap = self.snapshot()
        self.assertEqual(status, 200)
        self.assertEqual(snap["format_version"], 6)
        self.assertEqual(snap["tables"], [])
        self.assertEqual(snap["streams"][0]["max_open_windows"], 2)

        Handler.service = Service()
        status, body = self.restore(snap)
        self.assertEqual(status, 200)
        self.assertEqual(body["restored_streams"], 1)
        _, pressure = self.pressure()
        self.assertEqual(
            pressure,
            {
                "stream": "s",
                "max_open_windows": 2,
                "open_windows": 2,
                "available_windows": 0,
            },
        )
        # The limit is still enforced after the restore.
        self.assertEqual(self.event(5000, 3.0)[0], 429)
        self.watermark(2000)
        self.assertEqual(self.event(5000, 3.0)[0], 200)

    def test_snapshot_without_backpressure_keeps_prior_versions(self):
        self.create()
        self.event(0, 1.0)
        _, snap = self.snapshot()
        self.assertEqual(snap["format_version"], 1)
        self.assertNotIn("max_open_windows", snap["streams"][0])
        self.assertNotIn("tables", snap)

    def test_restore_version_6_requires_an_enabled_stream(self):
        status, body = self.restore(
            {
                "format_version": 6,
                "tables": [],
                "streams": [
                    {
                        "name": "s",
                        "window_ms": 1000,
                        "allowed_lateness_ms": 0,
                        "dedup_retention_ms": None,
                        "watermark_ms": None,
                        "windows": [],
                        "finalized": [],
                    }
                ],
            }
        )
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_snapshot")

    def test_restore_older_versions_reject_max_open_windows(self):
        for version in (1, 2, 3, 4, 5):
            Handler.service = Service()
            document = {
                "format_version": version,
                "streams": [
                    {
                        "name": "s",
                        "window_ms": 1000,
                        "allowed_lateness_ms": 0,
                        "dedup_retention_ms": None,
                        "watermark_ms": None,
                        "windows": [],
                        "finalized": [],
                        "max_open_windows": 2,
                    }
                ],
            }
            if version >= 2:
                document["tables"] = []
            status, body = self.restore(document)
            self.assertEqual(status, 422, version)
            self.assertEqual(body["error"]["code"], "invalid_snapshot")

    def test_restore_rejects_open_windows_over_limit(self):
        document = {
            "format_version": 6,
            "tables": [],
            "streams": [
                {
                    "name": "s",
                    "window_ms": 1000,
                    "allowed_lateness_ms": 0,
                    "dedup_retention_ms": None,
                    "watermark_ms": None,
                    "windows": [
                        {
                            "window_start_ms": 0,
                            "window_end_ms": 1000,
                            "count": 1,
                            "sum": 1.0,
                        },
                        {
                            "window_start_ms": 1000,
                            "window_end_ms": 2000,
                            "count": 1,
                            "sum": 2.0,
                        },
                    ],
                    "finalized": [],
                    "max_open_windows": 1,
                }
            ],
        }
        status, body = self.restore(document)
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_snapshot")
        # The instance stays empty.
        status, _ = self.pressure()
        self.assertEqual(status, 404)

    def test_restore_rejects_invalid_max_open_windows(self):
        for bad in (0, -3, 1.5, "2", True):
            Handler.service = Service()
            document = {
                "format_version": 6,
                "tables": [],
                "streams": [
                    {
                        "name": "s",
                        "window_ms": 1000,
                        "allowed_lateness_ms": 0,
                        "dedup_retention_ms": None,
                        "watermark_ms": None,
                        "windows": [],
                        "finalized": [],
                        "max_open_windows": bad,
                    }
                ],
            }
            status, body = self.restore(document)
            self.assertEqual(status, 422, bad)
            self.assertEqual(body["error"]["code"], "invalid_snapshot")

    def test_restore_version_6_with_other_features(self):
        self.create(
            max_open_windows=2,
            dedup_retention_ms=100000,
            change_retention=100,
            batch_retention=10,
        )
        self.batch("b1", [{"timestamp_ms": 0, "value": 1.0, "event_id": "x"}])
        _, snap = self.snapshot()
        self.assertEqual(snap["format_version"], 6)

        Handler.service = Service()
        status, _ = self.restore(snap)
        self.assertEqual(status, 200)
        # Batch replay still works and does not write again.
        status, body = self.batch(
            "b1", [{"timestamp_ms": 0, "value": 1.0, "event_id": "x"}]
        )
        self.assertEqual(status, 200)
        _, feed = self.changes()
        self.assertEqual(feed["latest_seq"], 1)
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)


class ConcurrencyTest(HttpTestCase):
    def test_concurrent_writes_never_exceed_limit(self):
        self.create(max_open_windows=4)
        accepted = 0
        lock = threading.Lock()

        def write(ts):
            nonlocal accepted
            status, _ = self.event(ts * 1000, 1.0)
            if status == 200:
                with lock:
                    accepted += 1

        threads = [
            threading.Thread(target=write, args=(i,)) for i in range(20)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(accepted, 4)
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 4)


if __name__ == "__main__":
    unittest.main()
