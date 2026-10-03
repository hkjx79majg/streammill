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

    def create_table(self, name="t"):
        return self.request("POST", "/tables", {"name": name})

    def put_row(self, key, label, table="t"):
        return self.request(
            "POST", f"/tables/{table}/rows", {"key": key, "label": label}
        )

    def create_stream(self, name="s", window_ms=1000, allowed_lateness_ms=0, **extra):
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
            "POST", f"/streams/{stream}/events",
            {"timestamp_ms": ts, "value": value, **extra},
        )

    def watermark(self, wm, stream="s"):
        return self.request(
            "POST", f"/streams/{stream}/watermark", {"watermark_ms": wm}
        )

    def joined_results(self, stream="s"):
        return self.request("GET", f"/streams/{stream}/joined-results")


class TableTest(HttpTestCase):
    def test_create_table(self):
        status, body = self.create_table()
        self.assertEqual(status, 201)
        self.assertEqual(body, {"table": "t"})

    def test_create_table_conflict(self):
        self.create_table()
        status, body = self.create_table()
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "table_exists")

    def test_create_table_validation(self):
        status, body = self.request("POST", "/tables", {"name": ""})
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_request")
        status, body = self.request("POST", "/tables", {"name": "t", "x": 1})
        self.assertEqual(status, 422)
        status, body = self.request("POST", "/tables", raw=b"{")
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "invalid_json")

    def test_put_row_changed_flag(self):
        self.create_table()
        status, body = self.put_row("k1", "a")
        self.assertEqual(status, 200)
        self.assertTrue(body["changed"])
        status, body = self.put_row("k1", "a")
        self.assertTrue(status == 200 and body["changed"] is False)
        status, body = self.put_row("k1", "b")
        self.assertTrue(status == 200 and body["changed"] is True)

    def test_put_row_unknown_table(self):
        status, body = self.put_row("k1", "a", table="nope")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "table_not_found")

    def test_put_row_validation_before_not_found(self):
        status, body = self.request(
            "POST", "/tables/nope/rows", {"key": "", "label": "a"}
        )
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_request")


class JoinStreamCreateTest(HttpTestCase):
    def test_create_with_lookup_table_echoes(self):
        self.create_table()
        status, body = self.create_stream(lookup_table="t")
        self.assertEqual(status, 201)
        self.assertEqual(body["lookup_table"], "t")

    def test_create_with_unknown_lookup_table(self):
        status, body = self.create_stream(lookup_table="ghost")
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_request")
        # The stream must not have been created.
        status, body = self.request("GET", "/streams/s/results")
        self.assertEqual(status, 404)

    def test_create_lookup_table_field_validation(self):
        self.create_table()
        status, body = self.create_stream(lookup_table="")
        self.assertEqual(status, 422)
        status, body = self.create_stream(lookup_table=7)
        self.assertEqual(status, 422)

    def test_plain_stream_shape_unchanged(self):
        status, body = self.create_stream()
        self.assertEqual(status, 201)
        self.assertNotIn("lookup_table", body)


class JoinEventTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table()
        self.put_row("k1", "red")
        self.put_row("k2", "blue")
        self.create_stream(lookup_table="t")

    def test_event_requires_lookup_key(self):
        status, body = self.event(100, 1.0)
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_request")

    def test_plain_stream_rejects_lookup_key(self):
        self.create_stream(name="plain")
        status, body = self.event(100, 1.0, stream="plain", lookup_key="k1")
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_request")

    def test_unknown_lookup_key(self):
        status, body = self.event(100, 1.0, lookup_key="ghost")
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "lookup_key_not_found")
        # State untouched: no aggregates anywhere.
        status, body = self.watermark(10_000)
        self.assertEqual(body["finalized"], [])

    def test_joined_aggregation_and_finalization(self):
        self.event(100, 1.0, lookup_key="k1")
        self.event(200, 2.0, lookup_key="k1")
        self.event(300, 4.0, lookup_key="k2")
        # Not yet finalized: joined-results stays empty.
        status, body = self.joined_results()
        self.assertEqual(status, 200)
        self.assertEqual(body["results"], [])
        status, body = self.watermark(1000)
        self.assertEqual(len(body["finalized"]), 1)
        status, body = self.joined_results()
        self.assertEqual(status, 200)
        self.assertEqual(
            body["results"],
            [
                {
                    "stream": "s",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "lookup_key": "k1",
                    "label": "red",
                    "count": 2,
                    "sum": 3.0,
                },
                {
                    "stream": "s",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "lookup_key": "k2",
                    "label": "blue",
                    "count": 1,
                    "sum": 4.0,
                },
            ],
        )
        # Base totals still tracked.
        status, body = self.request("GET", "/streams/s/results")
        self.assertEqual(body["results"][0]["count"], 3)
        self.assertEqual(body["results"][0]["sum"], 7.0)

    def test_label_change_only_affects_later_events(self):
        self.event(100, 1.0, lookup_key="k1")
        self.put_row("k1", "green")
        self.event(200, 2.0, lookup_key="k1")
        self.watermark(1000)
        _, body = self.joined_results()
        by_label = {row["label"]: row["count"] for row in body["results"]}
        self.assertEqual(by_label, {"red": 1, "green": 1})

    def test_late_event_dropped_before_lookup(self):
        self.watermark(5000)
        status, body = self.event(100, 1.0, lookup_key="ghost")
        self.assertEqual(status, 200)
        self.assertTrue(body["dropped"])

    def test_joined_results_unknown_stream(self):
        status, body = self.joined_results(stream="nope")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "stream_not_found")

    def test_joined_results_plain_stream(self):
        self.create_stream(name="plain")
        status, body = self.joined_results(stream="plain")
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "join_not_enabled")


class JoinDedupTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table()
        self.put_row("k1", "red")
        self.put_row("k2", "blue")
        self.create_stream(lookup_table="t", dedup_retention_ms=10_000)

    def test_exact_retry_is_duplicate(self):
        self.event(100, 1.0, event_id="e1", lookup_key="k1")
        status, body = self.event(100, 1.0, event_id="e1", lookup_key="k1")
        self.assertEqual(status, 200)
        self.assertTrue(body["duplicate"])
        self.watermark(1000)
        _, body = self.joined_results()
        self.assertEqual(body["results"][0]["count"], 1)

    def test_same_id_different_key_conflicts(self):
        self.event(100, 1.0, event_id="e1", lookup_key="k1")
        status, body = self.event(100, 1.0, event_id="e1", lookup_key="k2")
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "event_id_conflict")

    def test_duplicate_beats_unknown_key(self):
        self.event(100, 1.0, event_id="e1", lookup_key="k1")
        # Retained retry short-circuits before the table lookup.
        status, body = self.event(100, 1.0, event_id="e1", lookup_key="k1")
        self.assertEqual(status, 200)
        self.assertTrue(body["duplicate"])

    def test_failed_lookup_not_remembered(self):
        self.event(100, 1.0, event_id="e1", lookup_key="ghost")
        # The id was never registered, so a later valid event with the same
        # id is a fresh event, not a conflict.
        status, body = self.event(100, 1.0, event_id="e1", lookup_key="k1")
        self.assertEqual(status, 200)
        self.assertFalse(body["duplicate"])


class JoinSlidingTest(HttpTestCase):
    def test_sliding_join_counts_all_overlapping_windows(self):
        self.create_table()
        self.put_row("k1", "red")
        self.create_stream(lookup_table="t", slide_ms=500)
        self.event(600, 2.0, lookup_key="k1")
        status, body = self.watermark(1500)
        self.assertEqual(status, 200)
        _, body = self.joined_results()
        # Windows [0,1000) and [500,1500) both contain the event; the
        # earlier window [-500,500) does not.
        starts = [row["window_start_ms"] for row in body["results"]]
        self.assertEqual(starts, [0, 500])
        for row in body["results"]:
            self.assertEqual(row["count"], 1)
            self.assertEqual(row["sum"], 2.0)


class JoinSnapshotTest(HttpTestCase):
    def _build_joined_state(self):
        self.create_table()
        self.put_row("k1", "red")
        self.put_row("k2", "blue")
        self.create_stream(lookup_table="t", dedup_retention_ms=10_000)
        self.event(100, 1.0, event_id="e1", lookup_key="k1")
        self.event(200, 2.0, event_id="e2", lookup_key="k2")
        self.event(1200, 4.0, event_id="e3", lookup_key="k1")
        self.watermark(1000)  # finalizes window [0,1000)

    def test_snapshot_upgrades_to_version_2(self):
        self._build_joined_state()
        status, snap = self.request("GET", "/snapshot")
        self.assertEqual(status, 200)
        self.assertEqual(snap["format_version"], 2)
        self.assertEqual(
            snap["tables"],
            [
                {
                    "name": "t",
                    "rows": [
                        {"key": "k1", "label": "red"},
                        {"key": "k2", "label": "blue"},
                    ],
                }
            ],
        )
        stream = snap["streams"][0]
        self.assertEqual(stream["lookup_table"], "t")
        self.assertEqual(len(stream["joined_finalized"]), 2)
        self.assertEqual(len(stream["joined_windows"]), 1)
        self.assertEqual(stream["dedup_records"][0]["lookup_key"], "k1")

    def test_snapshot_round_trip(self):
        self._build_joined_state()
        _, snap = self.request("GET", "/snapshot")
        status, body = self.request("POST", "/snapshot/restore", snap)
        # Same instance already has state: conflict.
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "restore_conflict")
        # Fresh instance restores everything.
        Handler.service = Service()
        status, body = self.request("POST", "/snapshot/restore", snap)
        self.assertEqual(status, 200)
        self.assertEqual(body["restored_streams"], 1)
        _, again = self.request("GET", "/snapshot")
        self.assertEqual(again, snap)
        _, joined = self.joined_results()
        self.assertEqual(len(joined["results"]), 2)
        # Open window keeps accepting events and finalizes correctly.
        self.event(1300, 8.0, event_id="e4", lookup_key="k2")
        self.watermark(2000)
        _, joined = self.joined_results()
        self.assertEqual(len(joined["results"]), 4)
        # Retained dedup ids still conflict on a different key.
        status, body = self.event(100, 1.0, event_id="e1", lookup_key="k2")
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "event_id_conflict")

    def test_version_1_snapshot_still_restores(self):
        self.create_stream(name="plain")
        self.event(100, 1.0, stream="plain")
        _, snap = self.request("GET", "/snapshot")
        self.assertEqual(snap["format_version"], 1)
        self.assertNotIn("tables", snap)
        Handler.service = Service()
        status, body = self.request("POST", "/snapshot/restore", snap)
        self.assertEqual(status, 200)
        self.assertEqual(body["restored_streams"], 1)

    def test_restore_conflict_with_tables_only(self):
        self.create_table()
        status, body = self.request(
            "POST", "/snapshot/restore", {"format_version": 1, "streams": []}
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "restore_conflict")

    def test_restore_rejects_bad_documents(self):
        base = {
            "format_version": 2,
            "streams": [],
            "tables": [{"name": "t", "rows": [{"key": "k1", "label": "red"}]}],
        }

        def clone(**overrides):
            doc = json.loads(json.dumps(base))
            doc.update(overrides)
            return doc

        # Unknown table reference from a join stream.
        doc = clone(
            streams=[
                {
                    "name": "s",
                    "window_ms": 1000,
                    "allowed_lateness_ms": 0,
                    "dedup_retention_ms": None,
                    "watermark_ms": None,
                    "windows": [],
                    "finalized": [],
                    "lookup_table": "ghost",
                    "joined_windows": [],
                    "joined_finalized": [],
                }
            ]
        )
        status, body = self.request("POST", "/snapshot/restore", doc)
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_snapshot")

        # Misordered row keys.
        doc = clone(
            tables=[
                {
                    "name": "t",
                    "rows": [
                        {"key": "k2", "label": "a"},
                        {"key": "k1", "label": "b"},
                    ],
                }
            ]
        )
        status, _ = self.request("POST", "/snapshot/restore", doc)
        self.assertEqual(status, 422)

        # Duplicate row keys.
        doc = clone(
            tables=[
                {
                    "name": "t",
                    "rows": [
                        {"key": "k1", "label": "a"},
                        {"key": "k1", "label": "b"},
                    ],
                }
            ]
        )
        status, _ = self.request("POST", "/snapshot/restore", doc)
        self.assertEqual(status, 422)

        # Misordered tables.
        doc = clone(
            tables=[
                {"name": "z", "rows": []},
                {"name": "a", "rows": []},
            ]
        )
        status, _ = self.request("POST", "/snapshot/restore", doc)
        self.assertEqual(status, 422)

        # Group counts inconsistent with the base window.
        doc = clone(
            streams=[
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
                            "count": 2,
                            "sum": 3.0,
                        }
                    ],
                    "finalized": [],
                    "lookup_table": "t",
                    "joined_windows": [
                        {
                            "window_start_ms": 0,
                            "window_end_ms": 1000,
                            "lookup_key": "k1",
                            "label": "red",
                            "count": 1,
                            "sum": 3.0,
                        }
                    ],
                    "joined_finalized": [],
                }
            ]
        )
        status, _ = self.request("POST", "/snapshot/restore", doc)
        self.assertEqual(status, 422)

        # Non-finite group sum.
        doc = clone(
            streams=[
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
                        }
                    ],
                    "finalized": [],
                    "lookup_table": "t",
                    "joined_windows": [
                        {
                            "window_start_ms": 0,
                            "window_end_ms": 1000,
                            "lookup_key": "k1",
                            "label": "red",
                            "count": 1,
                            "sum": "NaN",
                        }
                    ],
                    "joined_finalized": [],
                }
            ]
        )
        status, _ = self.request("POST", "/snapshot/restore", doc)
        self.assertEqual(status, 422)

        # Misordered joined groups.
        doc = clone(
            streams=[
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
                            "count": 2,
                            "sum": 3.0,
                        }
                    ],
                    "finalized": [],
                    "lookup_table": "t",
                    "joined_windows": [
                        {
                            "window_start_ms": 0,
                            "window_end_ms": 1000,
                            "lookup_key": "k2",
                            "label": "red",
                            "count": 1,
                            "sum": 1.0,
                        },
                        {
                            "window_start_ms": 0,
                            "window_end_ms": 1000,
                            "lookup_key": "k1",
                            "label": "red",
                            "count": 1,
                            "sum": 2.0,
                        },
                    ],
                    "joined_finalized": [],
                }
            ]
        )
        status, _ = self.request("POST", "/snapshot/restore", doc)
        self.assertEqual(status, 422)

        # Unsupported version and version-1 doc carrying tables.
        status, _ = self.request(
            "POST", "/snapshot/restore", clone(format_version=3)
        )
        self.assertEqual(status, 422)
        status, _ = self.request(
            "POST",
            "/snapshot/restore",
            {"format_version": 1, "streams": [], "tables": []},
        )
        self.assertEqual(status, 422)

        # All failures left the instance completely empty.
        status, snap = self.request("GET", "/snapshot")
        self.assertEqual(snap, {"format_version": 1, "streams": []})
        status, body = self.request("POST", "/snapshot/restore", base)
        self.assertEqual(status, 200)
        self.assertEqual(body["restored_streams"], 0)


if __name__ == "__main__":
    unittest.main()
