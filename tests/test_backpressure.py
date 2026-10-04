import json
import os
import shutil
import tempfile
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

    def snapshot(self):
        return self.request("GET", "/snapshot")

    def restore(self, document):
        return self.request("POST", "/snapshot/restore", document)


class CreateValidationTest(HttpTestCase):
    def test_create_echoes_max_open_windows(self):
        status, body = self.create(max_open_windows=3)
        self.assertEqual(status, 201)
        self.assertEqual(body["max_open_windows"], 3)

    def test_create_without_option_keeps_base_shape(self):
        status, body = self.create()
        self.assertEqual(status, 201)
        self.assertNotIn("max_open_windows", body)

    def test_invalid_values_rejected(self):
        for bad in (0, -1, 1.5, "2", True, None):
            status, body = self.create(name=f"s-{bad}", max_open_windows=bad)
            self.assertEqual(status, 422, bad)
            self.assertEqual(body["error"]["code"], "invalid_request")
        status, _ = self.pressure("s-0")
        self.assertEqual(status, 404)


class PressureQueryTest(HttpTestCase):
    def test_unknown_stream_is_404(self):
        status, body = self.pressure("nope")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "stream_not_found")

    def test_not_enabled_is_409(self):
        self.create()
        status, body = self.pressure()
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "backpressure_not_enabled")

    def test_enabled_reports_counts(self):
        self.create(max_open_windows=2)
        status, body = self.pressure()
        self.assertEqual(status, 200)
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
        self.event(1000, 1.0)
        status, body = self.pressure()
        self.assertEqual((body["open_windows"], body["available_windows"]), (2, 0))


class SingleEventBackpressureTest(HttpTestCase):
    def test_over_limit_event_is_429_and_rolls_back(self):
        self.create(max_open_windows=1, change_retention=10,
                    dedup_retention_ms=100000)
        self.event(0, 1.0, event_id="e1")
        status, body = self.event(1000, 2.0, event_id="e2")
        self.assertEqual(status, 429)
        self.assertEqual(body["error"]["code"], "stream_backpressured")
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)
        # The dedup record and change sequence were rolled back too: the
        # same id with different content is a fresh event, not a conflict.
        status, _ = self.event(1000, 9.0, event_id="e2")
        self.assertEqual(status, 429)
        status, body = self.request(
            "GET", "/streams/s/changes?after_seq=0&limit=100"
        )
        self.assertEqual(body["latest_seq"], 1)

    def test_same_window_event_fits_at_limit(self):
        self.create(max_open_windows=1)
        self.event(0, 1.0)
        status, body = self.event(500, 2.0)
        self.assertEqual(status, 200)
        self.assertFalse(body["dropped"])

    def test_manual_watermark_releases_slots(self):
        self.create(max_open_windows=1)
        self.event(0, 1.0)
        self.event(1000, 1.0)
        status, _ = self.event(2000, 1.0)
        self.assertEqual(status, 429)
        status, body = self.watermark(1000)
        self.assertEqual([w["window_start_ms"] for w in body["finalized"]], [0])
        _, pressure = self.pressure()
        self.assertEqual(pressure["available_windows"], 1)
        status, _ = self.event(2000, 1.0)
        self.assertEqual(status, 200)

    def test_duplicate_and_too_late_keep_success_on_full_stream(self):
        self.create(max_open_windows=1, dedup_retention_ms=100000)
        self.event(0, 1.0, event_id="e1")
        status, body = self.event(0, 1.0, event_id="e1")
        self.assertEqual(status, 200)
        self.assertTrue(body["duplicate"])
        self.watermark(5000)
        status, body = self.event(100, 1.0, event_id="late")
        self.assertEqual(status, 200)
        self.assertTrue(body["dropped"])

    def test_conflict_and_lookup_errors_keep_priority(self):
        self.create(max_open_windows=1, dedup_retention_ms=100000)
        self.event(0, 1.0, event_id="e1")
        status, body = self.event(0, 2.0, event_id="e1")
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "event_id_conflict")

    def test_sliding_windows_count_separately(self):
        self.create(window_ms=1000, slide_ms=500, max_open_windows=2)
        self.event(600, 1.0)  # windows 0 and 500
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 2)
        status, _ = self.event(1100, 1.0)  # would add window 1000
        self.assertEqual(status, 429)

    def test_automatic_advance_releases_slots_within_event(self):
        self.create(max_open_windows=1, auto_watermark_lag_ms=0)
        self.event(0, 1.0)
        status, body = self.event(1000, 1.0)
        self.assertEqual(status, 200)
        self.assertEqual([w["window_start_ms"] for w in body["finalized"]], [0])
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)

    def test_automatic_rollback_restores_watermark(self):
        self.create(max_open_windows=1, auto_watermark_lag_ms=2500)
        self.event(0, 1.0)
        status, _ = self.event(1000, 1.0)
        self.assertEqual(status, 429)
        _, doc = self.snapshot()
        entry = doc["streams"][0]
        self.assertEqual(entry["watermark_ms"], -2500)
        self.assertEqual(entry["max_event_timestamp_ms"], 0)


class BatchBackpressureTest(HttpTestCase):
    def test_over_limit_batch_is_429_and_atomic(self):
        self.create(max_open_windows=1, batch_retention=10, change_retention=10)
        self.event(0, 1.0)
        status, body = self.batch(
            "b1", [{"timestamp_ms": 100, "value": 1.0},
                   {"timestamp_ms": 1500, "value": 1.0}]
        )
        self.assertEqual(status, 429)
        self.assertEqual(body["error"]["code"], "stream_backpressured")
        # The earlier element was rolled back with its change records.
        _, pressure = self.pressure()
        self.assertEqual(pressure["open_windows"], 1)
        status, body = self.request(
            "GET", "/streams/s/changes?after_seq=0&limit=100"
        )
        self.assertEqual(body["latest_seq"], 1)
        # The failed batch occupies no identifier.
        status, body = self.batch("b1", [{"timestamp_ms": 100, "value": 2.0}])
        self.assertEqual(status, 200)


class SnapshotBackpressureTest(HttpTestCase):
    def test_enabled_stream_exports_version_6(self):
        self.create(name="plain")
        self.create(name="bp", max_open_windows=2)
        status, doc = self.snapshot()
        self.assertEqual(status, 200)
        self.assertEqual(doc["format_version"], 6)
        self.assertIn("tables", doc)
        by_name = {entry["name"]: entry for entry in doc["streams"]}
        self.assertEqual(by_name["bp"]["max_open_windows"], 2)
        self.assertNotIn("max_open_windows", by_name["plain"])

    def test_disabled_instance_keeps_version_1(self):
        self.create()
        self.event(0, 1.0)
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 1)
        self.assertNotIn("tables", doc)

    def test_restore_round_trip(self):
        self.create(max_open_windows=2, change_retention=10, batch_retention=5)
        self.event(0, 1.0)
        self.event(1000, 1.0)
        _, doc = self.snapshot()

        Handler.service = Service()
        status, body = self.restore(doc)
        self.assertEqual(status, 200)
        self.assertEqual(body["restored_streams"], 1)
        _, pressure = self.pressure()
        self.assertEqual((pressure["open_windows"], pressure["available_windows"]), (2, 0))
        status, _ = self.event(2000, 1.0)
        self.assertEqual(status, 429)
        _, doc2 = self.snapshot()
        self.assertEqual(doc2, doc)

    def test_version_6_requires_the_config(self):
        self.create(max_open_windows=2)
        _, doc = self.snapshot()
        for entry in doc["streams"]:
            entry.pop("max_open_windows")
        Handler.service = Service()
        status, body = self.restore(doc)
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_snapshot")
        _, doc2 = self.snapshot()
        self.assertEqual(doc2["streams"], [])

    def test_older_versions_must_not_carry_the_field(self):
        self.create(max_open_windows=2)
        _, doc = self.snapshot()
        for version in (1, 2, 3, 4, 5):
            doc["format_version"] = version
            if version == 1:
                doc.pop("tables", None)
            else:
                doc.setdefault("tables", [])
            Handler.service = Service()
            status, body = self.restore(doc)
            self.assertEqual(status, 422, version)
            self.assertEqual(body["error"]["code"], "invalid_snapshot")

    def test_open_windows_must_not_exceed_the_limit(self):
        self.create(max_open_windows=2)
        self.event(0, 1.0)
        self.event(1000, 1.0)
        _, doc = self.snapshot()
        doc["streams"][0]["max_open_windows"] = 1
        Handler.service = Service()
        status, body = self.restore(doc)
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_snapshot")
        _, doc2 = self.snapshot()
        self.assertEqual(doc2["streams"], [])

    def test_invalid_config_values_rejected_on_restore(self):
        self.create(max_open_windows=2)
        _, doc = self.snapshot()
        for bad in (0, -1, 1.5, "2", True):
            doc["streams"][0]["max_open_windows"] = bad
            Handler.service = Service()
            status, body = self.restore(doc)
            self.assertEqual(status, 422, bad)
            self.assertEqual(body["error"]["code"], "invalid_snapshot")


class PersistenceBackpressureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="streammill-bp-")
        self.state_path = os.path.join(self.dir, "state.json")

    def tearDown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_config_and_state_persist_atomically(self):
        service = Service.from_state_file(self.state_path)
        service.create_stream("s", 1000, 0, max_open_windows=2)
        service.add_event("s", 0, 1.0)
        service.add_event("s", 1000, 1.0)
        with open(self.state_path, encoding="utf-8") as handle:
            doc = json.load(handle)
        self.assertEqual(doc["format_version"], 6)
        self.assertEqual(doc["streams"][0]["max_open_windows"], 2)

        # A backpressured event is not a commit and never touches the file.
        with open(self.state_path, "rb") as handle:
            before = handle.read()
        with self.assertRaises(Exception):
            service.add_event("s", 2000, 1.0)
        with open(self.state_path, "rb") as handle:
            self.assertEqual(handle.read(), before)

        # A restart keeps the configuration, the pressure and the limit.
        restarted = Service.from_state_file(self.state_path)
        pressure = restarted.pressure("s")
        self.assertEqual(
            (pressure["open_windows"], pressure["available_windows"]), (2, 0)
        )
        with self.assertRaises(Exception):
            restarted.add_event("s", 2000, 1.0)
        restarted.advance_watermark("s", 1000)
        self.assertEqual(restarted.pressure("s")["available_windows"], 1)
        restarted.add_event("s", 2000, 1.0)
        self.assertEqual(restarted.pressure("s")["open_windows"], 2)


if __name__ == "__main__":
    unittest.main()
