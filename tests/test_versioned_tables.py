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

    def create_table(self, name="t", **extra):
        return self.request("POST", "/tables", {"name": name, **extra})

    def put_row(self, key, label, table="t", **extra):
        body = {"key": key, "label": label, **extra}
        return self.request("POST", f"/tables/{table}/rows", body)

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
        body = {"timestamp_ms": ts, "value": value, **extra}
        return self.request("POST", f"/streams/{stream}/events", body)

    def watermark(self, wm, stream="s"):
        return self.request(
            "POST", f"/streams/{stream}/watermark", {"watermark_ms": wm}
        )

    def results(self, stream="s"):
        return self.request("GET", f"/streams/{stream}/results")

    def joined_results(self, stream="s"):
        return self.request("GET", f"/streams/{stream}/joined-results")

    def snapshot(self):
        return self.request("GET", "/snapshot")

    def restore(self, body=None, raw=None):
        return self.request("POST", "/snapshot/restore", body=body, raw=raw)


class VersionedTableTest(HttpTestCase):
    def test_create_versioned_table_echoes_flag(self):
        status, payload = self.create_table(event_time_versioned=True)
        self.assertEqual(status, 201)
        self.assertEqual(payload, {"table": "t", "event_time_versioned": True})

    def test_create_without_flag_keeps_base_shape(self):
        status, payload = self.create_table()
        self.assertEqual(status, 201)
        self.assertEqual(payload, {"table": "t"})
        status, payload = self.create_table(name="t2", event_time_versioned=False)
        self.assertEqual(status, 201)
        self.assertEqual(payload, {"table": "t2"})

    def test_create_table_flag_validation(self):
        status, payload = self.request(
            "POST", "/tables", {"name": "t", "event_time_versioned": 1}
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        status, payload = self.request(
            "POST", "/tables", {"name": "t", "event_time_versioned": "yes"}
        )
        self.assertEqual(status, 422)
        status, payload = self.request(
            "POST", "/tables", {"name": "t", "event_time_versioned": True, "x": 1}
        )
        self.assertEqual(status, 422)
        # failed creates leave no table behind
        status, payload = self.put_row("k", "a")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "table_not_found")

    def test_version_writes_changed_flag(self):
        self.create_table(event_time_versioned=True)
        status, payload = self.put_row("k1", "a", effective_from_ms=100)
        self.assertEqual(status, 200)
        self.assertEqual(
            payload,
            {
                "table": "t",
                "key": "k1",
                "label": "a",
                "effective_from_ms": 100,
                "changed": True,
            },
        )
        # identical retry
        status, payload = self.put_row("k1", "a", effective_from_ms=100)
        self.assertEqual(payload["changed"], False)
        # a new effective time, out of order
        status, payload = self.put_row("k1", "b", effective_from_ms=50)
        self.assertEqual(payload["changed"], True)
        status, payload = self.put_row("k1", "c", effective_from_ms=200)
        self.assertEqual(payload["changed"], True)

    def test_version_conflict_keeps_history(self):
        self.create_table(event_time_versioned=True)
        self.put_row("k1", "a", effective_from_ms=100)
        status, payload = self.put_row("k1", "b", effective_from_ms=100)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "dimension_version_conflict")
        # history is unchanged: the original label still serves lookups
        _, doc = self.snapshot()
        self.assertEqual(
            doc["tables"][0]["versions"],
            [{"key": "k1", "label": "a", "effective_from_ms": 100}],
        )

    def test_version_row_validation(self):
        self.create_table(event_time_versioned=True)
        for body in (
            {"key": "k", "label": "a"},  # missing effective_from_ms
            {"key": "k", "label": "a", "effective_from_ms": "100"},
            {"key": "k", "label": "a", "effective_from_ms": 1.5},
            {"key": "k", "label": "a", "effective_from_ms": True},
            {"key": "k", "label": "a", "effective_from_ms": 1, "x": 1},
        ):
            status, payload = self.request("POST", "/tables/t/rows", body)
            self.assertEqual(status, 422, body)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        status, payload = self.request("POST", "/tables/t/rows", raw=b"{nope")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_json")

    def test_current_value_table_rejects_effective_from_ms(self):
        self.create_table()
        status, payload = self.put_row("k", "a", effective_from_ms=100)
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_unknown_table_validates_base_shape_first(self):
        # 422-before-404 ordering matches the event route
        status, payload = self.put_row("k", "a", table="missing",
                                       effective_from_ms=100)
        self.assertEqual(status, 422)
        status, payload = self.put_row("k", "a", table="missing")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "table_not_found")


class VersionedJoinTest(HttpTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.create_table(event_time_versioned=True)
        self.create(lookup_table="t")

    def test_event_uses_version_effective_at_event_time(self):
        self.put_row("k1", "new", effective_from_ms=500)
        self.put_row("k1", "old", effective_from_ms=100)  # out of order
        status, payload = self.event(700, 1, lookup_key="k1")
        self.assertEqual(status, 200)
        status, payload = self.event(200, 2, lookup_key="k1")
        self.assertEqual(status, 200)
        self.watermark(1000)
        _, payload = self.joined_results()
        self.assertEqual(
            [(row["lookup_key"], row["label"], row["count"], row["sum"])
             for row in payload["results"]],
            [("k1", "new", 1, 1), ("k1", "old", 1, 2)],
        )

    def test_event_before_first_version_is_rejected(self):
        self.put_row("k1", "a", effective_from_ms=500)
        status, payload = self.event(499, 1, lookup_key="k1")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "lookup_version_not_found")
        # state untouched: the window stays empty
        _, payload = self.watermark(1000)
        self.assertEqual(payload["finalized"], [])

    def test_unknown_key_is_rejected(self):
        status, payload = self.event(100, 1, lookup_key="ghost")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "lookup_version_not_found")

    def test_backfill_does_not_recompute_received_events(self):
        self.put_row("k1", "a", effective_from_ms=0)
        self.event(500, 1, lookup_key="k1")
        # backfilled history only affects later events
        self.put_row("k1", "b", effective_from_ms=400)
        self.event(500, 2, lookup_key="k1")
        self.event(300, 4, lookup_key="k1")
        self.watermark(1000)
        _, payload = self.joined_results()
        self.assertEqual(
            [(row["label"], row["count"], row["sum"])
             for row in payload["results"]],
            [("a", 2, 5), ("b", 1, 2)],
        )

    def test_base_windows_and_results_unchanged(self):
        self.put_row("k1", "a", effective_from_ms=0)
        self.event(100, 1, lookup_key="k1")
        self.event(200, 2, lookup_key="k1")
        _, payload = self.watermark(1000)
        self.assertEqual(
            payload["finalized"],
            [{
                "stream": "s",
                "window_start_ms": 0,
                "window_end_ms": 1000,
                "count": 2,
                "sum": 3,
            }],
        )

    def test_dedup_still_includes_lookup_key(self):
        self.create(
            name="sd", lookup_table="t", dedup_retention_ms=10000
        )
        self.put_row("k1", "a", effective_from_ms=0)
        self.put_row("k2", "b", effective_from_ms=0)
        status, payload = self.event(100, 1, stream="sd",
                                     event_id="e1", lookup_key="k1")
        self.assertEqual(payload["duplicate"], False)
        status, payload = self.event(100, 1, stream="sd",
                                     event_id="e1", lookup_key="k1")
        self.assertEqual(payload["duplicate"], True)
        status, payload = self.event(100, 1, stream="sd",
                                     event_id="e1", lookup_key="k2")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")

    def test_batch_rolls_back_on_missing_version(self):
        self.create(name="sb", lookup_table="t", batch_retention=10)
        self.put_row("k1", "a", effective_from_ms=0)
        status, payload = self.request(
            "POST",
            "/streams/sb/batches",
            {
                "batch_id": "b1",
                "events": [
                    {"timestamp_ms": 100, "value": 1, "lookup_key": "k1"},
                    {"timestamp_ms": 200, "value": 2, "lookup_key": "ghost"},
                ],
            },
        )
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "lookup_version_not_found")
        # the whole batch rolled back and the id was not occupied
        _, payload = self.watermark(10000, stream="sb")
        self.assertEqual(payload["finalized"], [])
        status, payload = self.request(
            "POST",
            "/streams/sb/batches",
            {
                "batch_id": "b1",
                "events": [{"timestamp_ms": 100, "value": 1, "lookup_key": "k1"}],
            },
        )
        self.assertEqual(status, 200)

    def test_plain_stream_and_plain_table_behaviour_unchanged(self):
        self.create(name="plain")
        status, payload = self.event(100, 1, stream="plain")
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"stream": "plain", "dropped": False})


class VersionedSnapshotTest(HttpTestCase):
    def test_versioned_table_forces_format_5(self):
        self.create_table(event_time_versioned=True)
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 5)
        self.assertEqual(doc["tables"], [
            {"name": "t", "event_time_versioned": True, "versions": []}
        ])

    def test_versions_export_sorted_by_key_and_time(self):
        self.create_table(event_time_versioned=True)
        self.put_row("k2", "x", effective_from_ms=10)
        self.put_row("k1", "b", effective_from_ms=200)
        self.put_row("k1", "a", effective_from_ms=100)
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 5)
        self.assertEqual(
            doc["tables"][0]["versions"],
            [
                {"key": "k1", "label": "a", "effective_from_ms": 100},
                {"key": "k1", "label": "b", "effective_from_ms": 200},
                {"key": "k2", "label": "x", "effective_from_ms": 10},
            ],
        )

    def test_plain_table_still_exports_version_2(self):
        self.create_table()
        self.put_row("k1", "a")
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 2)
        self.assertEqual(
            doc["tables"], [{"name": "t", "rows": [{"key": "k1", "label": "a"}]}]
        )

    def test_versioned_table_wins_over_batch_streams(self):
        self.create_table(event_time_versioned=True)
        self.create(batch_retention=5)
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 5)
        self.assertEqual(doc["streams"][0]["batch_retention"], 5)

    def test_round_trip_preserves_versioned_state(self):
        self.create_table(event_time_versioned=True)
        self.put_row("k1", "a", effective_from_ms=100)
        self.put_row("k1", "b", effective_from_ms=500)
        self.create(lookup_table="t")
        self.event(150, 1, lookup_key="k1")   # resolves to "a"
        self.event(600, 2, lookup_key="k1")   # resolves to "b"
        self.watermark(1000)
        _, doc = self.snapshot()

        Handler.service = Service()
        status, payload = self.restore(doc)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 1})
        _, again = self.snapshot()
        self.assertEqual(again, doc)
        # lookups keep resolving by event time after the restore
        status, payload = self.event(1500, 4, lookup_key="k1")
        self.assertEqual(status, 200)
        self.watermark(2000)
        _, payload = self.joined_results()
        self.assertEqual(
            [(row["window_start_ms"], row["label"], row["count"], row["sum"])
             for row in payload["results"]],
            [(0, "a", 1, 1), (0, "b", 1, 2), (1000, "b", 1, 4)],
        )

    def test_restore_conflict_still_wins(self):
        self.create_table(event_time_versioned=True)
        status, payload = self.restore({"format_version": 5, "tables": [],
                                        "streams": []})
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "restore_conflict")


class VersionedSnapshotValidationTest(HttpTestCase):
    def assert_invalid(self, doc):
        status, payload = self.restore(doc)
        self.assertEqual(status, 422, doc)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")
        _, current = self.snapshot()
        self.assertEqual(current, {"format_version": 1, "streams": []})

    def _doc(self, tables, streams=None):
        return {
            "format_version": 5,
            "tables": tables,
            "streams": streams if streams is not None else [],
        }

    def test_empty_v5_document_is_valid(self):
        status, payload = self.restore(self._doc([]))
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 0})

    def test_mode_field_combinations(self):
        # rows on a versioned table
        self.assert_invalid(self._doc([
            {"name": "t", "event_time_versioned": True, "rows": [],
             "versions": []}
        ]))
        # versions without the flag
        self.assert_invalid(self._doc([{"name": "t", "versions": []}]))
        # flag that is not exactly true
        self.assert_invalid(self._doc([
            {"name": "t", "event_time_versioned": False, "versions": []}
        ]))
        self.assert_invalid(self._doc([
            {"name": "t", "event_time_versioned": 1, "versions": []}
        ]))
        # flag without versions
        self.assert_invalid(self._doc([
            {"name": "t", "event_time_versioned": True}
        ]))
        # versioned fields in an older document
        self.assert_invalid({
            "format_version": 2,
            "tables": [{"name": "t", "event_time_versioned": True,
                        "versions": []}],
            "streams": [],
        })

    def test_version_point_validation(self):
        def doc(versions):
            return self._doc([
                {"name": "t", "event_time_versioned": True,
                 "versions": versions}
            ])

        point = {"key": "k1", "label": "a", "effective_from_ms": 100}
        # duplicate point
        self.assert_invalid(doc([point, dict(point)]))
        # same point with a different label
        self.assert_invalid(doc([point, {**point, "label": "b"}]))
        # out-of-order effective times within one key
        self.assert_invalid(doc([
            {"key": "k1", "label": "b", "effective_from_ms": 200},
            {"key": "k1", "label": "a", "effective_from_ms": 100},
        ]))
        # out-of-order keys
        self.assert_invalid(doc([
            {"key": "k2", "label": "a", "effective_from_ms": 100},
            {"key": "k1", "label": "a", "effective_from_ms": 100},
        ]))
        # invalid effective times
        self.assert_invalid(doc([{**point, "effective_from_ms": "100"}]))
        self.assert_invalid(doc([{**point, "effective_from_ms": 1.5}]))
        self.assert_invalid(doc([{**point, "effective_from_ms": True}]))
        # bad key/label, extra and missing fields
        self.assert_invalid(doc([{**point, "key": ""}]))
        self.assert_invalid(doc([{**point, "label": ""}]))
        self.assert_invalid(doc([{**point, "x": 1}]))
        self.assert_invalid(doc([{"key": "k1", "label": "a"}]))

    def test_v5_streams_keep_change_feed_and_batch_state(self):
        self.create_table(event_time_versioned=True)
        self.put_row("k1", "a", effective_from_ms=0)
        self.create(
            lookup_table="t",
            change_retention=10,
            batch_retention=10,
            dedup_retention_ms=1000,
        )
        self.request(
            "POST",
            "/streams/s/batches",
            {
                "batch_id": "b1",
                "events": [{
                    "timestamp_ms": 100, "value": 1,
                    "event_id": "e1", "lookup_key": "k1",
                }],
            },
        )
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 5)
        Handler.service = Service()
        status, payload = self.restore(doc)
        self.assertEqual(status, 200)
        # the retained batch replays and the change cursor continues
        status, payload = self.request(
            "POST",
            "/streams/s/batches",
            {
                "batch_id": "b1",
                "events": [{
                    "timestamp_ms": 100, "value": 1,
                    "event_id": "e1", "lookup_key": "k1",
                }],
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["outcomes"][0]["duplicate"], False)
        _, payload = self.request(
            "GET", "/streams/s/changes?after_seq=0&limit=10"
        )
        self.assertEqual(payload["latest_seq"], 1)


if __name__ == "__main__":
    unittest.main()
