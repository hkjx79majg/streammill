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
        return self.request("POST", f"/tables/{table}/rows", {"key": key, "label": label})

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

    def joined_results(self, stream="s"):
        return self.request("GET", f"/streams/{stream}/joined-results")

    def snapshot(self):
        return self.request("GET", "/snapshot")

    def restore(self, body=None, raw=None):
        return self.request("POST", "/snapshot/restore", body=body, raw=raw)


class TableTest(HttpTestCase):
    def test_create_table(self):
        status, payload = self.create_table()
        self.assertEqual(status, 201)
        self.assertEqual(payload, {"table": "t"})

    def test_create_table_conflict(self):
        self.create_table()
        status, payload = self.create_table()
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "table_exists")

    def test_create_table_validation(self):
        status, payload = self.request("POST", "/tables", {"name": ""})
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        status, payload = self.request("POST", "/tables", {"name": 3})
        self.assertEqual(status, 422)
        status, payload = self.request("POST", "/tables", {"name": "t", "extra": 1})
        self.assertEqual(status, 422)
        status, payload = self.request("POST", "/tables", raw=b"{nope")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_json")

    def test_put_row_changed_flag(self):
        self.create_table()
        status, payload = self.put_row("k1", "a")
        self.assertEqual(status, 200)
        self.assertEqual(payload["changed"], True)
        self.assertEqual(payload["key"], "k1")
        self.assertEqual(payload["label"], "a")
        # identical retry
        status, payload = self.put_row("k1", "a")
        self.assertEqual(payload["changed"], False)
        # changed value
        status, payload = self.put_row("k1", "b")
        self.assertEqual(payload["changed"], True)

    def test_put_row_unknown_table(self):
        status, payload = self.put_row("k1", "a", table="missing")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "table_not_found")

    def test_put_row_validation(self):
        self.create_table()
        for body in ({"key": "", "label": "a"}, {"key": "k", "label": ""},
                     {"key": 1, "label": "a"}, {"key": "k"},
                     {"key": "k", "label": "a", "x": 1}):
            status, payload = self.request("POST", "/tables/t/rows", body)
            self.assertEqual(status, 422, body)
            self.assertEqual(payload["error"]["code"], "invalid_request")


class JoinedStreamTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table()
        self.put_row("k1", "red")
        self.put_row("k2", "blue")

    def test_create_echoes_lookup_table(self):
        status, payload = self.create("s", 1000, 0, lookup_table="t")
        self.assertEqual(status, 201)
        self.assertEqual(payload["lookup_table"], "t")

    def test_create_unknown_lookup_table(self):
        status, payload = self.create("s", 1000, 0, lookup_table="missing")
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        # stream must not have been created
        status, payload = self.create("s", 1000, 0)
        self.assertEqual(status, 201)

    def test_create_lookup_table_field_validation(self):
        for bad in ("", 7, None):
            status, payload = self.create("s", 1000, 0, lookup_table=bad)
            self.assertEqual(status, 422, bad)
            self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_joined_event_requires_lookup_key(self):
        self.create("s", 1000, 0, lookup_table="t")
        status, payload = self.event(100, 5)
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        status, payload = self.event(100, 5, lookup_key="")
        self.assertEqual(status, 422)

    def test_plain_stream_rejects_lookup_key(self):
        self.create("plain", 1000, 0)
        status, payload = self.event(100, 5, stream="plain", lookup_key="k1")
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_unknown_lookup_key(self):
        self.create("s", 1000, 0, lookup_table="t")
        status, payload = self.event(100, 5, lookup_key="nope")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "lookup_key_not_found")
        # state unchanged: no window aggregate
        _, snap = self.snapshot()
        stream = snap["streams"][0]
        self.assertEqual(stream["windows"], [])

    def test_joined_results_grouping_and_label_snapshot(self):
        self.create("s", 1000, 0, lookup_table="t")
        self.event(100, 5, lookup_key="k1")
        self.event(200, 3, lookup_key="k1")
        self.event(300, 2, lookup_key="k2")
        # relabel k1: only later events see the new label
        self.put_row("k1", "green")
        self.event(400, 7, lookup_key="k1")
        self.watermark(1000)

        status, payload = self.joined_results()
        self.assertEqual(status, 200)
        self.assertEqual(payload["stream"], "s")
        self.assertEqual(
            payload["results"],
            [
                {"stream": "s", "window_start_ms": 0, "window_end_ms": 1000,
                 "lookup_key": "k1", "label": "green", "count": 1, "sum": 7},
                {"stream": "s", "window_start_ms": 0, "window_end_ms": 1000,
                 "lookup_key": "k1", "label": "red", "count": 2, "sum": 8},
                {"stream": "s", "window_start_ms": 0, "window_end_ms": 1000,
                 "lookup_key": "k2", "label": "blue", "count": 1, "sum": 2},
            ],
        )
        # base window still aggregates everything
        _, results = self.request("GET", "/streams/s/results")
        self.assertEqual(results["results"][0]["count"], 4)
        self.assertEqual(results["results"][0]["sum"], 17)

    def test_joined_results_only_finalized(self):
        self.create("s", 1000, 0, lookup_table="t")
        self.event(100, 5, lookup_key="k1")
        status, payload = self.joined_results()
        self.assertEqual(status, 200)
        self.assertEqual(payload["results"], [])

    def test_joined_results_errors(self):
        self.create("plain", 1000, 0)
        status, payload = self.joined_results("plain")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "join_not_enabled")
        status, payload = self.joined_results("missing")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "stream_not_found")

    def test_joined_results_sorted_across_windows(self):
        self.create("s", 1000, 0, lookup_table="t")
        self.event(1100, 1, lookup_key="k2")
        self.event(100, 1, lookup_key="k2")
        self.event(1200, 1, lookup_key="k1")
        self.event(200, 1, lookup_key="k1")
        self.watermark(3000)
        _, payload = self.joined_results()
        order = [
            (r["window_start_ms"], r["lookup_key"], r["label"])
            for r in payload["results"]
        ]
        self.assertEqual(order, sorted(order))
        self.assertEqual(len(order), 4)

    def test_dedup_includes_lookup_key(self):
        self.create("s", 1000, 0, lookup_table="t", dedup_retention_ms=5000)
        status, payload = self.event(100, 5, event_id="e1", lookup_key="k1")
        self.assertEqual(payload["duplicate"], False)
        # exact retry including the key
        status, payload = self.event(100, 5, event_id="e1", lookup_key="k1")
        self.assertEqual(payload["duplicate"], True)
        # same id, different key -> conflict
        status, payload = self.event(100, 5, event_id="e1", lookup_key="k2")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")

    def test_unknown_key_leaves_dedup_state_untouched(self):
        self.create("s", 1000, 0, lookup_table="t", dedup_retention_ms=5000)
        status, _ = self.event(100, 5, event_id="e1", lookup_key="missing")
        self.assertEqual(status, 409)
        # the id was not registered: a later valid use aggregates normally
        status, payload = self.event(100, 5, event_id="e1", lookup_key="k1")
        self.assertEqual(status, 200)
        self.assertEqual(payload["duplicate"], False)

    def test_sliding_join_counts_all_overlapping_windows(self):
        self.create("s", 1000, 0, slide_ms=500, lookup_table="t")
        self.event(600, 2, lookup_key="k1")  # windows [0,1000) and [500,1500)
        self.watermark(1000)  # finalizes [0,1000)
        _, payload = self.joined_results()
        self.assertEqual(
            payload["results"],
            [{"stream": "s", "window_start_ms": 0, "window_end_ms": 1000,
              "lookup_key": "k1", "label": "red", "count": 1, "sum": 2}],
        )
        self.watermark(1500)  # finalizes [500,1500)
        _, payload = self.joined_results()
        starts = [r["window_start_ms"] for r in payload["results"]]
        self.assertEqual(starts, [0, 500])

    def test_dropped_and_late_events_do_not_join(self):
        self.create("s", 1000, 0, lookup_table="t")
        self.watermark(1000)
        status, payload = self.event(100, 5, lookup_key="k1")
        self.assertEqual(payload["dropped"], True)
        _, payload = self.joined_results()
        self.assertEqual(payload["results"], [])


class JoinedSnapshotTest(HttpTestCase):
    def _build_state(self):
        self.create_table()
        self.put_row("k1", "red")
        self.put_row("k2", "blue")
        self.create("j", 1000, 0, lookup_table="t")
        self.create("plain", 500, 0)
        self.event(100, 5, stream="j", lookup_key="k1")
        self.event(200, 3, stream="j", lookup_key="k2")
        self.event(1200, 1, stream="j", lookup_key="k1")
        self.event(100, 2, stream="plain")
        self.watermark(1000, stream="j")

    def test_snapshot_v2_shape(self):
        self._build_state()
        status, payload = self.snapshot()
        self.assertEqual(status, 200)
        self.assertEqual(payload["format_version"], 2)
        self.assertEqual(
            payload["tables"],
            [{"name": "t", "rows": [
                {"key": "k1", "label": "red"},
                {"key": "k2", "label": "blue"},
            ]}],
        )
        names = [s["name"] for s in payload["streams"]]
        self.assertEqual(names, ["j", "plain"])
        joined = payload["streams"][0]
        self.assertEqual(joined["lookup_table"], "t")
        self.assertEqual(
            joined["joined_finalized"],
            [
                {"stream": "j", "window_start_ms": 0, "window_end_ms": 1000,
                 "lookup_key": "k1", "label": "red", "count": 1, "sum": 5},
                {"stream": "j", "window_start_ms": 0, "window_end_ms": 1000,
                 "lookup_key": "k2", "label": "blue", "count": 1, "sum": 3},
            ],
        )
        self.assertEqual(
            joined["joined_windows"],
            [{"window_start_ms": 1000, "window_end_ms": 2000,
              "lookup_key": "k1", "label": "red", "count": 1, "sum": 1}],
        )
        # plain stream keeps the version 1 shape
        plain = payload["streams"][1]
        self.assertNotIn("lookup_table", plain)
        self.assertNotIn("joined_windows", plain)

    def test_no_join_state_keeps_version_1(self):
        self.create("plain", 500, 0)
        self.event(100, 2, stream="plain")
        _, payload = self.snapshot()
        self.assertEqual(payload["format_version"], 1)
        self.assertNotIn("tables", payload)

    def test_round_trip_preserves_join_state(self):
        self._build_state()
        _, snap = self.snapshot()
        _, before = self.joined_results("j")
        Handler.service = Service()
        status, payload = self.restore(snap)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 2})
        _, after = self.joined_results("j")
        self.assertEqual(after, before)
        # re-export without writes is identical
        _, again = self.snapshot()
        self.assertEqual(again, snap)
        # table rows restored: writes report no change, events resolve labels
        status, payload = self.put_row("k1", "red")
        self.assertEqual(payload["changed"], False)
        status, payload = self.event(1300, 4, stream="j", lookup_key="k1")
        self.assertEqual(status, 200)
        self.watermark(2000, stream="j")
        _, payload = self.joined_results("j")
        last = payload["results"][-1]
        self.assertEqual(last["label"], "red")
        self.assertEqual(last["count"], 2)
        self.assertEqual(last["sum"], 5)

    def test_restore_conflict_with_tables_only(self):
        self.create_table()
        status, payload = self.restore({"format_version": 1, "streams": []})
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "restore_conflict")

    def test_restore_v1_still_accepted(self):
        status, payload = self.restore({"format_version": 1, "streams": []})
        self.assertEqual(status, 200)
        _, snap = self.snapshot()
        self.assertEqual(snap, {"format_version": 1, "streams": []})

    def _joined_stream(self, **overrides):
        entry = {
            "name": "j",
            "window_ms": 1000,
            "allowed_lateness_ms": 0,
            "dedup_retention_ms": None,
            "watermark_ms": None,
            "windows": [{"window_start_ms": 0, "window_end_ms": 1000,
                         "count": 2, "sum": 8}],
            "finalized": [],
            "lookup_table": "t",
            "joined_windows": [
                {"window_start_ms": 0, "window_end_ms": 1000,
                 "lookup_key": "k1", "label": "red", "count": 1, "sum": 5},
                {"window_start_ms": 0, "window_end_ms": 1000,
                 "lookup_key": "k2", "label": "blue", "count": 1, "sum": 3},
            ],
            "joined_finalized": [],
        }
        entry.update(overrides)
        return entry

    def _doc(self, *streams, tables=None):
        if tables is None:
            tables = [{"name": "t", "rows": [{"key": "k1", "label": "red"},
                                             {"key": "k2", "label": "blue"}]}]
        return {"format_version": 2, "tables": tables, "streams": list(streams)}

    def assert_invalid(self, doc):
        status, payload = self.restore(doc)
        self.assertEqual(status, 422, doc)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")
        _, current = self.snapshot()
        self.assertEqual(current, {"format_version": 1, "streams": []})

    def test_valid_v2_document_restores(self):
        status, payload = self.restore(self._doc(self._joined_stream()))
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 1})

    def test_top_level_v2_validation(self):
        self.assert_invalid({"format_version": 2, "streams": []})
        self.assert_invalid(self._doc(self._joined_stream(), tables={}))
        doc = self._doc(self._joined_stream())
        doc["extra"] = 1
        self.assert_invalid(doc)
        # v1 documents must not carry tables
        self.assert_invalid({"format_version": 1, "streams": [], "tables": []})
        self.assert_invalid({"format_version": 4, "streams": [], "tables": []})
        # v3 (change feed) documents are accepted; an empty one restores zero
        status, payload = self.restore(
            {"format_version": 3, "streams": [], "tables": []}
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 0})

    def test_table_validation(self):
        good = self._doc(self._joined_stream())
        # unsorted / duplicate table names
        bad = self._doc(self._joined_stream(), tables=[
            {"name": "z", "rows": []}, {"name": "a", "rows": []}])
        self.assert_invalid(bad)
        bad = self._doc(self._joined_stream(), tables=[
            {"name": "t", "rows": []}, {"name": "t", "rows": []}])
        self.assert_invalid(bad)
        # unsorted / duplicate row keys
        bad = self._doc(self._joined_stream(), tables=[{"name": "t", "rows": [
            {"key": "b", "label": "x"}, {"key": "a", "label": "y"}]}])
        self.assert_invalid(bad)
        bad = self._doc(self._joined_stream(), tables=[{"name": "t", "rows": [
            {"key": "a", "label": "x"}, {"key": "a", "label": "y"}]}])
        self.assert_invalid(bad)
        # empty key / label
        bad = self._doc(self._joined_stream(), tables=[{"name": "t", "rows": [
            {"key": "", "label": "x"}]}])
        self.assert_invalid(bad)
        bad = self._doc(self._joined_stream(), tables=[{"name": "t", "rows": [
            {"key": "a", "label": ""}]}])
        self.assert_invalid(bad)
        # empty table name
        bad = self._doc(self._joined_stream(), tables=[{"name": "", "rows": []}])
        self.assert_invalid(bad)
        self.assertEqual(self.restore(good)[0], 200)

    def test_unknown_table_reference(self):
        self.assert_invalid(self._doc(self._joined_stream(lookup_table="nope")))
        self.assert_invalid(self._doc(self._joined_stream(), tables=[]))

    def test_joined_field_presence_rules(self):
        # joined stream missing joined arrays
        entry = self._joined_stream()
        del entry["joined_windows"]
        self.assert_invalid(self._doc(entry))
        # plain stream carrying joined fields
        plain = {
            "name": "p", "window_ms": 1000, "allowed_lateness_ms": 0,
            "dedup_retention_ms": None, "watermark_ms": None,
            "windows": [], "finalized": [], "joined_windows": [],
            "joined_finalized": [],
        }
        self.assert_invalid(self._doc(plain))
        # v1 stream entry must not carry join fields
        v1 = dict(plain)
        self.assert_invalid({"format_version": 1, "streams": [v1]})

    def test_joined_rows_validation(self):
        # out of order groups
        self.assert_invalid(self._doc(self._joined_stream(joined_windows=[
            {"window_start_ms": 0, "window_end_ms": 1000,
             "lookup_key": "k2", "label": "blue", "count": 1, "sum": 3},
            {"window_start_ms": 0, "window_end_ms": 1000,
             "lookup_key": "k1", "label": "red", "count": 1, "sum": 5},
        ])))
        # duplicate group
        dup = {"window_start_ms": 0, "window_end_ms": 1000,
               "lookup_key": "k1", "label": "red", "count": 1, "sum": 5}
        self.assert_invalid(self._doc(self._joined_stream(
            joined_windows=[dup, dict(dup)])))
        # non-finite sum
        self.assert_invalid(self._doc(self._joined_stream(joined_windows=[
            {"window_start_ms": 0, "window_end_ms": 1000,
             "lookup_key": "k1", "label": "red", "count": 2, "sum": "x"},
        ])))
        # misaligned window start
        self.assert_invalid(self._doc(self._joined_stream(joined_windows=[
            {"window_start_ms": 500, "window_end_ms": 1500,
             "lookup_key": "k1", "label": "red", "count": 2, "sum": 8},
        ])))

    def test_joined_base_consistency(self):
        # groups do not add up to the base window
        self.assert_invalid(self._doc(self._joined_stream(joined_windows=[
            {"window_start_ms": 0, "window_end_ms": 1000,
             "lookup_key": "k1", "label": "red", "count": 1, "sum": 5},
        ])))
        # joined window without a base window
        self.assert_invalid(self._doc(self._joined_stream(
            windows=[],
        )))
        # finalized groups must match finalized base windows
        self.assert_invalid(self._doc(self._joined_stream(
            watermark_ms=1000,
            windows=[],
            joined_windows=[],
            finalized=[{"stream": "j", "window_start_ms": 0,
                        "window_end_ms": 1000, "count": 2, "sum": 8}],
            joined_finalized=[],
        )))

    def test_joined_dedup_records_carry_lookup_key(self):
        entry = self._joined_stream(
            dedup_retention_ms=5000,
            dedup_records=[
                {"event_id": "e1", "timestamp_ms": 0, "value": 5,
                 "lookup_key": "k1"},
                {"event_id": "e2", "timestamp_ms": 100, "value": 3,
                 "lookup_key": "k2"},
            ],
        )
        status, _ = self.restore(self._doc(entry))
        self.assertEqual(status, 200)

        Handler.service = Service()
        # missing lookup_key in a joined dedup record
        bad = self._joined_stream(
            dedup_retention_ms=5000,
            dedup_records=[{"event_id": "e1", "timestamp_ms": 0, "value": 5}],
        )
        self.assert_invalid(self._doc(bad))
        # plain stream dedup record must not carry lookup_key
        plain = {
            "name": "p", "window_ms": 1000, "allowed_lateness_ms": 0,
            "dedup_retention_ms": 5000, "watermark_ms": None,
            "windows": [{"window_start_ms": 0, "window_end_ms": 1000,
                         "count": 1, "sum": 5}],
            "finalized": [],
            "dedup_records": [
                {"event_id": "e1", "timestamp_ms": 0, "value": 5,
                 "lookup_key": "k1"},
            ],
        }
        self.assert_invalid(self._doc(plain))


if __name__ == "__main__":
    unittest.main()
