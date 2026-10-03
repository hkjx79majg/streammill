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

    def watermark(self, wm, stream="s"):
        return self.request(
            "POST", f"/streams/{stream}/watermark", {"watermark_ms": wm}
        )

    def changes(self, after_seq=0, limit=1000, stream="s"):
        return self.request(
            "GET", f"/streams/{stream}/changes?after_seq={after_seq}&limit={limit}"
        )


class ChangeFeedCreateTest(HttpTestCase):
    def test_create_echoes_change_retention(self):
        status, payload = self.create(change_retention=50)
        self.assertEqual(status, 201)
        self.assertEqual(payload["change_retention"], 50)

    def test_create_without_change_retention_keeps_shape(self):
        status, payload = self.create()
        self.assertEqual(status, 201)
        self.assertNotIn("change_retention", payload)

    def test_invalid_change_retention_rejected(self):
        for bad in (0, -3, 1.5, "10", True, None):
            status, payload = self.create(change_retention=bad)
            self.assertEqual(status, 422, bad)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        status, _ = self.changes(stream="s")
        self.assertEqual(status, 404)

    def test_feed_not_enabled(self):
        self.create()
        status, payload = self.changes()
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "change_feed_not_enabled")

    def test_unknown_stream(self):
        status, payload = self.changes()
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "stream_not_found")

    def test_initial_cursor_is_zero(self):
        self.create(change_retention=10)
        status, payload = self.changes()
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"stream": "s", "latest_seq": 0, "changes": []})


class ChangeFeedQueryParamTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create(change_retention=10)

    def assert_invalid(self, query):
        status, payload = self.request("GET", f"/streams/s/changes{query}")
        self.assertEqual(status, 422, query)
        self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_missing_params(self):
        self.assert_invalid("")
        self.assert_invalid("?after_seq=0")
        self.assert_invalid("?limit=10")

    def test_duplicate_params(self):
        self.assert_invalid("?after_seq=0&after_seq=1&limit=10")
        self.assert_invalid("?after_seq=0&limit=10&limit=20")

    def test_unknown_params(self):
        self.assert_invalid("?after_seq=0&limit=10&foo=1")

    def test_out_of_range_params(self):
        self.assert_invalid("?after_seq=-1&limit=10")
        self.assert_invalid("?after_seq=0&limit=0")
        self.assert_invalid("?after_seq=0&limit=1001")
        self.assert_invalid("?after_seq=0&limit=-5")

    def test_non_integer_params(self):
        self.assert_invalid("?after_seq=x&limit=10")
        self.assert_invalid("?after_seq=1.5&limit=10")
        self.assert_invalid("?after_seq=0&limit=abc")
        self.assert_invalid("?after_seq=&limit=10")

    def test_param_validation_precedes_unknown_stream(self):
        status, payload = self.request("GET", "/streams/nope/changes?after_seq=x&limit=10")
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")


class ChangeFeedRecordsTest(HttpTestCase):
    def test_upserts_carry_updated_aggregates(self):
        self.create(change_retention=10)
        self.event(100, 2)
        self.event(200, 3)
        status, payload = self.changes()
        self.assertEqual(status, 200)
        self.assertEqual(payload["latest_seq"], 2)
        self.assertEqual(
            payload["changes"],
            [
                {
                    "seq": 1,
                    "kind": "upsert",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 1,
                    "sum": 2,
                },
                {
                    "seq": 2,
                    "kind": "upsert",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 2,
                    "sum": 5,
                },
            ],
        )

    def test_manual_watermark_produces_final_only(self):
        self.create(change_retention=10)
        self.event(100, 2)
        self.watermark(1000)
        status, payload = self.changes(after_seq=1)
        self.assertEqual(
            payload["changes"],
            [
                {
                    "seq": 2,
                    "kind": "final",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 1,
                    "sum": 2,
                }
            ],
        )

    def test_watermark_without_new_results_appends_nothing(self):
        self.create(change_retention=10)
        self.event(100, 2)
        self.watermark(500)
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 1)
        self.watermark(500)  # idempotent repost
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 1)

    def test_sliding_event_produces_one_upsert_per_window(self):
        self.create(window_ms=1000, slide_ms=500, change_retention=10)
        self.event(700, 1)
        _, payload = self.changes()
        self.assertEqual(
            [(c["seq"], c["kind"], c["window_start_ms"]) for c in payload["changes"]],
            [(1, "upsert", 0), (2, "upsert", 500)],
        )

    def test_auto_watermark_orders_upserts_before_finals(self):
        self.create(auto_watermark_lag_ms=0, change_retention=10)
        self.event(100, 1)
        self.event(1500, 2)
        _, payload = self.changes()
        self.assertEqual(
            [(c["seq"], c["kind"], c["window_start_ms"]) for c in payload["changes"]],
            [(1, "upsert", 0), (2, "upsert", 1000), (3, "final", 0)],
        )

    def test_dropped_duplicate_and_conflict_do_not_consume_seq(self):
        self.create(dedup_retention_ms=5000, change_retention=10)
        self.event(100, 1, event_id="x")
        self.event(100, 1, event_id="x")  # exact retry
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 1)
        status, _ = self.event(200, 9, event_id="x")  # conflict
        self.assertEqual(status, 409)
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 1)
        self.watermark(10000)
        self.event(300, 5, event_id="y")  # too late, dropped
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 2)
        self.assertEqual(payload["changes"][-1]["kind"], "final")

    def test_joined_stream_publishes_base_windows_only(self):
        self.request("POST", "/tables", {"name": "t"})
        self.request("POST", "/tables/t/rows", {"key": "k", "label": "L"})
        self.create(lookup_table="t", change_retention=10)
        self.event(100, 3, lookup_key="k")
        status, _ = self.event(101, 3, lookup_key="unknown")
        self.assertEqual(status, 409)
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 1)
        self.assertEqual(
            payload["changes"],
            [
                {
                    "seq": 1,
                    "kind": "upsert",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 1,
                    "sum": 3,
                }
            ],
        )

    def test_cursor_and_limit(self):
        self.create(change_retention=10)
        for ts in (100, 200, 300, 400):
            self.event(ts, 1)
        _, payload = self.changes(after_seq=1, limit=2)
        self.assertEqual([c["seq"] for c in payload["changes"]], [2, 3])
        self.assertEqual(payload["latest_seq"], 4)
        _, payload = self.changes(after_seq=4)
        self.assertEqual(payload["changes"], [])
        self.assertEqual(payload["latest_seq"], 4)

    def test_cursor_ahead(self):
        self.create(change_retention=10)
        self.event(100, 1)
        status, payload = self.changes(after_seq=2)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "change_cursor_ahead")

    def test_retention_trims_oldest_and_expires_cursor(self):
        self.create(change_retention=3)
        for ts in range(5):
            self.event(ts * 1000, 1)
        _, payload = self.changes(after_seq=2)
        self.assertEqual(payload["latest_seq"], 5)
        self.assertEqual([c["seq"] for c in payload["changes"]], [3, 4, 5])
        status, payload = self.changes(after_seq=1)
        self.assertEqual(status, 410)
        self.assertEqual(payload["error"]["code"], "change_cursor_expired")
        # latest_seq never regresses as trimming continues
        self.event(5000, 1)
        _, payload = self.changes(after_seq=3)
        self.assertEqual(payload["latest_seq"], 6)
        self.assertEqual([c["seq"] for c in payload["changes"]], [4, 5, 6])


class ChangeFeedSnapshotTest(HttpTestCase):
    def test_enabled_instance_exports_version_3(self):
        self.create(change_retention=10)
        self.event(100, 2)
        status, doc = self.request("GET", "/snapshot")
        self.assertEqual(status, 200)
        self.assertEqual(doc["format_version"], 3)
        self.assertEqual(doc["tables"], [])
        (entry,) = doc["streams"]
        self.assertEqual(entry["change_retention"], 10)
        self.assertEqual(entry["latest_seq"], 1)
        self.assertEqual(len(entry["changes"]), 1)

    def test_disabled_instances_keep_original_versions(self):
        self.create()
        _, doc = self.request("GET", "/snapshot")
        self.assertEqual(doc["format_version"], 1)
        self.assertNotIn("tables", doc)
        self.request("POST", "/tables", {"name": "t"})
        _, doc = self.request("GET", "/snapshot")
        self.assertEqual(doc["format_version"], 2)

    def test_round_trip_continues_cursor_and_sequence(self):
        self.create(change_retention=3)
        for ts in range(5):
            self.event(ts * 1000, 1)
        _, doc = self.request("GET", "/snapshot")
        Handler.service = Service()
        status, payload = self.request("POST", "/snapshot/restore", doc)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 1})
        # The retained window and the cursor survive the restart.
        _, after = self.changes(after_seq=2)
        self.assertEqual([c["seq"] for c in after["changes"]], [3, 4, 5])
        status, _ = self.changes(after_seq=1)
        self.assertEqual(status, 410)
        # New records continue the sequence without a gap.
        self.event(5000, 1)
        _, after = self.changes(after_seq=5)
        self.assertEqual([c["seq"] for c in after["changes"]], [6])
        # Exporting again reproduces the uninterrupted document.
        _, doc2 = self.request("GET", "/snapshot")
        self.assertEqual(doc2["format_version"], 3)
        (entry,) = doc2["streams"]
        self.assertEqual(entry["latest_seq"], 6)
        self.assertEqual([c["seq"] for c in entry["changes"]], [4, 5, 6])

    def test_mixed_instance_restores_plain_and_enabled_streams(self):
        self.create(name="plain")
        self.create(name="fed", change_retention=5)
        self.event(100, 1, stream="fed")
        _, doc = self.request("GET", "/snapshot")
        Handler.service = Service()
        status, _ = self.request("POST", "/snapshot/restore", doc)
        self.assertEqual(status, 200)
        status, _ = self.changes(stream="plain")
        self.assertEqual(status, 409)
        _, payload = self.changes(stream="fed")
        self.assertEqual(payload["latest_seq"], 1)

    def restore_doc(self, **entry_over):
        entry = {
            "name": "c",
            "window_ms": 1000,
            "allowed_lateness_ms": 0,
            "dedup_retention_ms": None,
            "watermark_ms": 1000,
            "windows": [
                {"window_start_ms": 1000, "window_end_ms": 2000, "count": 1, "sum": 7}
            ],
            "finalized": [
                {
                    "stream": "c",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 2,
                    "sum": 5,
                }
            ],
            "change_retention": 10,
            "latest_seq": 4,
            "changes": [
                {
                    "seq": 1,
                    "kind": "upsert",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 1,
                    "sum": 2,
                },
                {
                    "seq": 2,
                    "kind": "upsert",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 2,
                    "sum": 5,
                },
                {
                    "seq": 3,
                    "kind": "final",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 2,
                    "sum": 5,
                },
                {
                    "seq": 4,
                    "kind": "upsert",
                    "window_start_ms": 1000,
                    "window_end_ms": 2000,
                    "count": 1,
                    "sum": 7,
                },
            ],
        }
        entry.update(entry_over)
        return {"format_version": 3, "tables": [], "streams": [entry]}

    def assert_invalid_snapshot(self, doc):
        Handler.service = Service()
        status, payload = self.request("POST", "/snapshot/restore", doc)
        self.assertEqual(status, 422, doc)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")
        # A failed restore publishes nothing.
        _, snap = self.request("GET", "/snapshot")
        self.assertEqual(snap, {"format_version": 1, "streams": []})

    def test_restore_accepts_valid_document(self):
        Handler.service = Service()
        status, payload = self.request("POST", "/snapshot/restore", self.restore_doc())
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 1})
        _, feed = self.changes(after_seq=0, stream="c")
        self.assertEqual(feed["latest_seq"], 4)
        self.assertEqual(len(feed["changes"]), 4)

    def test_restore_rejects_broken_sequence(self):
        doc = self.restore_doc()
        doc["streams"][0]["changes"][1]["seq"] = 9
        self.assert_invalid_snapshot(doc)

    def test_restore_rejects_latest_seq_mismatch(self):
        self.assert_invalid_snapshot(self.restore_doc(latest_seq=5))
        doc = self.restore_doc()
        doc["streams"][0]["changes"] = doc["streams"][0]["changes"][:3]
        self.assert_invalid_snapshot(doc)

    def test_restore_rejects_retention_overflow(self):
        doc = self.restore_doc(change_retention=3)
        self.assert_invalid_snapshot(doc)

    def test_restore_rejects_misaligned_window(self):
        doc = self.restore_doc()
        doc["streams"][0]["changes"][0]["window_start_ms"] = 5
        self.assert_invalid_snapshot(doc)
        doc = self.restore_doc()
        doc["streams"][0]["changes"][0]["window_end_ms"] = 999
        self.assert_invalid_snapshot(doc)

    def test_restore_rejects_unknown_window(self):
        doc = self.restore_doc()
        doc["streams"][0]["changes"][0].update(
            window_start_ms=9000, window_end_ms=10000
        )
        self.assert_invalid_snapshot(doc)

    def test_restore_rejects_final_on_open_window(self):
        doc = self.restore_doc()
        doc["streams"][0]["changes"][3]["kind"] = "final"
        self.assert_invalid_snapshot(doc)

    def test_restore_rejects_records_after_final(self):
        doc = self.restore_doc(latest_seq=5)
        doc["streams"][0]["changes"].append(
            {
                "seq": 5,
                "kind": "upsert",
                "window_start_ms": 0,
                "window_end_ms": 1000,
                "count": 3,
                "sum": 8,
            }
        )
        self.assert_invalid_snapshot(doc)

    def test_restore_rejects_aggregate_mismatch(self):
        doc = self.restore_doc()
        doc["streams"][0]["changes"][3]["count"] = 2
        self.assert_invalid_snapshot(doc)
        doc = self.restore_doc()
        doc["streams"][0]["changes"][2]["sum"] = 6
        self.assert_invalid_snapshot(doc)

    def test_restore_rejects_incomplete_triple(self):
        doc = self.restore_doc()
        del doc["streams"][0]["change_retention"]
        self.assert_invalid_snapshot(doc)
        doc = self.restore_doc()
        del doc["streams"][0]["latest_seq"]
        self.assert_invalid_snapshot(doc)
        doc = self.restore_doc()
        del doc["streams"][0]["changes"]
        self.assert_invalid_snapshot(doc)

    def test_restore_rejects_change_fields_in_older_versions(self):
        doc = self.restore_doc()
        doc["format_version"] = 2
        self.assert_invalid_snapshot(doc)
        doc = self.restore_doc()
        doc["format_version"] = 1
        del doc["tables"]
        self.assert_invalid_snapshot(doc)


if __name__ == "__main__":
    unittest.main()
