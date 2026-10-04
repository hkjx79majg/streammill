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

    def changes(self, after_seq=0, limit=1000, table="t"):
        return self.request(
            "GET", f"/tables/{table}/changes?after_seq={after_seq}&limit={limit}"
        )

    def snapshot(self):
        return self.request("GET", "/snapshot")

    def restore(self, body=None, raw=None):
        return self.request("POST", "/snapshot/restore", body=body, raw=raw)


class CdcCreateTest(HttpTestCase):
    def test_create_echoes_cdc_retention(self):
        status, payload = self.create_table(cdc_retention=50)
        self.assertEqual(status, 201)
        self.assertEqual(payload, {"table": "t", "cdc_retention": 50})

    def test_create_echoes_with_versioned_flag(self):
        status, payload = self.create_table(
            event_time_versioned=True, cdc_retention=5
        )
        self.assertEqual(status, 201)
        self.assertEqual(
            payload,
            {"table": "t", "event_time_versioned": True, "cdc_retention": 5},
        )

    def test_create_without_cdc_keeps_shape(self):
        status, payload = self.create_table()
        self.assertEqual(status, 201)
        self.assertEqual(payload, {"table": "t"})

    def test_invalid_cdc_retention_rejected(self):
        for bad in (0, -3, 1.5, "10", True, None):
            status, payload = self.create_table(cdc_retention=bad)
            self.assertEqual(status, 422, bad)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        # failed creates leave no table behind
        status, payload = self.changes()
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "table_not_found")

    def test_feed_not_enabled(self):
        self.create_table()
        status, payload = self.changes()
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "cdc_not_enabled")

    def test_unknown_table(self):
        status, payload = self.changes()
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "table_not_found")

    def test_initial_cursor_is_zero(self):
        self.create_table(cdc_retention=10)
        status, payload = self.changes()
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"table": "t", "latest_seq": 0, "changes": []})


class CdcQueryParamTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table(cdc_retention=10)

    def assert_invalid(self, query, table="t"):
        status, payload = self.request("GET", f"/tables/{table}/changes{query}")
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
        self.assert_invalid("?after_seq=0&limit=10&x=1")
        self.assert_invalid("?after_seq=0&limit=10&cursor=3")

    def test_out_of_range_params(self):
        self.assert_invalid("?after_seq=-1&limit=10")
        self.assert_invalid("?after_seq=0&limit=0")
        self.assert_invalid("?after_seq=0&limit=1001")
        self.assert_invalid("?after_seq=1.5&limit=10")
        self.assert_invalid("?after_seq=0&limit=abc")
        self.assert_invalid("?after_seq=&limit=10")

    def test_validation_precedes_table_existence(self):
        self.assert_invalid("?after_seq=-1&limit=10", table="missing")
        self.assert_invalid("?after_seq=0", table="missing")
        self.assert_invalid("?after_seq=0&limit=10&x=1", table="missing")


class CdcFeedTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table(cdc_retention=10)

    def test_writes_produce_consecutive_records(self):
        self.put_row("k1", "a")
        self.put_row("k2", "b")
        self.put_row("k1", "c")
        status, payload = self.changes()
        self.assertEqual(status, 200)
        self.assertEqual(
            payload,
            {
                "table": "t",
                "latest_seq": 3,
                "changes": [
                    {"seq": 1, "kind": "upsert", "key": "k1", "label": "a"},
                    {"seq": 2, "kind": "upsert", "key": "k2", "label": "b"},
                    {"seq": 3, "kind": "upsert", "key": "k1", "label": "c"},
                ],
            },
        )

    def test_identical_retry_consumes_no_seq(self):
        self.put_row("k1", "a")
        status, payload = self.put_row("k1", "a")
        self.assertEqual(status, 200)
        self.assertEqual(payload["changed"], False)
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 1)
        self.assertEqual(len(payload["changes"]), 1)

    def test_cursor_slices_records(self):
        for index in range(5):
            self.put_row(f"k{index}", "v")
        _, payload = self.changes(after_seq=2, limit=2)
        self.assertEqual(payload["latest_seq"], 5)
        self.assertEqual([row["seq"] for row in payload["changes"]], [3, 4])
        _, payload = self.changes(after_seq=5, limit=10)
        self.assertEqual(payload["changes"], [])

    def test_cursor_ahead(self):
        self.put_row("k1", "a")
        status, payload = self.changes(after_seq=2)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "cdc_cursor_ahead")

    def test_retention_trims_oldest(self):
        self.create_table(name="t2", cdc_retention=2)
        for index in range(4):
            self.put_row(f"k{index}", "v", table="t2")
        _, payload = self.changes(after_seq=2, table="t2")
        self.assertEqual(payload["latest_seq"], 4)
        self.assertEqual([row["seq"] for row in payload["changes"]], [3, 4])

    def test_cursor_expired(self):
        self.create_table(name="t2", cdc_retention=2)
        for index in range(4):
            self.put_row(f"k{index}", "v", table="t2")
        # oldest retained seq is 3: cursors below 2 are expired
        status, payload = self.changes(after_seq=1, table="t2")
        self.assertEqual(status, 410)
        self.assertEqual(payload["error"]["code"], "cdc_cursor_expired")
        # the boundary cursor still works
        status, payload = self.changes(after_seq=2, table="t2")
        self.assertEqual(status, 200)
        self.assertEqual([row["seq"] for row in payload["changes"]], [3, 4])

    def test_row_responses_unchanged(self):
        status, payload = self.put_row("k1", "a")
        self.assertEqual(status, 200)
        self.assertEqual(
            payload, {"table": "t", "key": "k1", "label": "a", "changed": True}
        )


class CdcVersionedFeedTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table(event_time_versioned=True, cdc_retention=10)

    def test_version_points_produce_records(self):
        self.put_row("k1", "a", effective_from_ms=100)
        self.put_row("k1", "b", effective_from_ms=50)  # out of order
        self.put_row("k2", "x", effective_from_ms=0)
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 3)
        self.assertEqual(
            payload["changes"],
            [
                {"seq": 1, "kind": "upsert", "key": "k1", "label": "a",
                 "effective_from_ms": 100},
                {"seq": 2, "kind": "upsert", "key": "k1", "label": "b",
                 "effective_from_ms": 50},
                {"seq": 3, "kind": "upsert", "key": "k2", "label": "x",
                 "effective_from_ms": 0},
            ],
        )

    def test_identical_retry_consumes_no_seq(self):
        self.put_row("k1", "a", effective_from_ms=100)
        status, payload = self.put_row("k1", "a", effective_from_ms=100)
        self.assertEqual(payload["changed"], False)
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 1)

    def test_conflict_produces_no_record(self):
        self.put_row("k1", "a", effective_from_ms=100)
        status, payload = self.put_row("k1", "b", effective_from_ms=100)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "dimension_version_conflict")
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 1)
        self.assertEqual(len(payload["changes"]), 1)


class CdcSnapshotTest(HttpTestCase):
    def test_cdc_table_forces_format_7(self):
        self.create_table(cdc_retention=5)
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 7)
        self.assertEqual(
            doc["tables"],
            [{
                "name": "t",
                "rows": [],
                "cdc_retention": 5,
                "latest_cdc_seq": 0,
                "cdc_changes": [],
            }],
        )
        self.assertEqual(doc["streams"], [])

    def test_export_contains_retained_records(self):
        self.create_table(cdc_retention=2)
        self.put_row("k1", "a")
        self.put_row("k2", "b")
        self.put_row("k1", "c")
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 7)
        table = doc["tables"][0]
        self.assertEqual(table["cdc_retention"], 2)
        self.assertEqual(table["latest_cdc_seq"], 3)
        self.assertEqual(
            table["cdc_changes"],
            [
                {"seq": 2, "kind": "upsert", "key": "k2", "label": "b"},
                {"seq": 3, "kind": "upsert", "key": "k1", "label": "c"},
            ],
        )
        self.assertEqual(
            table["rows"],
            [{"key": "k1", "label": "c"}, {"key": "k2", "label": "b"}],
        )

    def test_plain_table_fields_unchanged_in_v7(self):
        self.create_table(cdc_retention=5)
        self.create_table(name="plain")
        self.put_row("k1", "a", table="plain")
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 7)
        plain = [t for t in doc["tables"] if t["name"] == "plain"][0]
        self.assertEqual(
            plain, {"name": "plain", "rows": [{"key": "k1", "label": "a"}]}
        )

    def test_no_cdc_keeps_earlier_versions(self):
        self.create_table()
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 2)
        self.create_table(name="v", event_time_versioned=True)
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 5)

    def test_versioned_cdc_table_exports_both_shapes(self):
        self.create_table(event_time_versioned=True, cdc_retention=5)
        self.put_row("k1", "a", effective_from_ms=100)
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 7)
        table = doc["tables"][0]
        self.assertEqual(table["event_time_versioned"], True)
        self.assertEqual(
            table["versions"],
            [{"key": "k1", "label": "a", "effective_from_ms": 100}],
        )
        self.assertEqual(
            table["cdc_changes"],
            [{"seq": 1, "kind": "upsert", "key": "k1", "label": "a",
              "effective_from_ms": 100}],
        )

    def test_round_trip_continues_feed(self):
        self.create_table(cdc_retention=3)
        self.put_row("k1", "a")
        self.put_row("k2", "b")
        self.put_row("k1", "c")
        _, doc = self.snapshot()

        Handler.service = Service()
        status, payload = self.restore(doc)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 0})
        _, again = self.snapshot()
        self.assertEqual(again, doc)
        # the cursor and the next sequence number continue uninterrupted
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 3)
        self.put_row("k3", "d")
        _, payload = self.changes(after_seq=1)
        self.assertEqual(payload["latest_seq"], 4)
        self.assertEqual(
            [row["seq"] for row in payload["changes"]], [2, 3, 4]
        )
        # trimming still applies against the restored cursor
        status, payload = self.changes(after_seq=0)
        self.assertEqual(status, 410)
        self.assertEqual(payload["error"]["code"], "cdc_cursor_expired")

    def test_round_trip_versioned_cdc_table(self):
        self.create_table(event_time_versioned=True, cdc_retention=10)
        self.put_row("k1", "a", effective_from_ms=100)
        self.put_row("k1", "b", effective_from_ms=500)
        _, doc = self.snapshot()
        Handler.service = Service()
        status, _ = self.restore(doc)
        self.assertEqual(status, 200)
        _, again = self.snapshot()
        self.assertEqual(again, doc)
        self.put_row("k1", "c", effective_from_ms=900)
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 3)
        self.assertEqual(
            payload["changes"][-1],
            {"seq": 3, "kind": "upsert", "key": "k1", "label": "c",
             "effective_from_ms": 900},
        )


class CdcSnapshotValidationTest(HttpTestCase):
    def assert_invalid(self, doc):
        status, payload = self.restore(doc)
        self.assertEqual(status, 422, doc)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")
        _, current = self.snapshot()
        self.assertEqual(current, {"format_version": 1, "streams": []})

    def _doc(self, tables, streams=None, version=7):
        return {
            "format_version": version,
            "tables": tables,
            "streams": streams if streams is not None else [],
        }

    def _table(self, **extra):
        return {"name": "t", "rows": [], **extra}

    def test_empty_v7_document_is_valid_with_enabled_table(self):
        status, payload = self.restore(
            self._doc([self._table(cdc_retention=5, latest_cdc_seq=0,
                                   cdc_changes=[])])
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 0})

    def test_v7_requires_an_enabled_table(self):
        self.assert_invalid(self._doc([]))
        self.assert_invalid(self._doc([self._table()]))
        self.assert_invalid(self._doc([
            {"name": "t", "event_time_versioned": True, "versions": []}
        ]))

    def test_cdc_fields_rejected_in_older_versions(self):
        table = self._table(cdc_retention=5, latest_cdc_seq=0, cdc_changes=[])
        for version in (2, 3, 4, 5, 6):
            self.assert_invalid(self._doc([table], version=version))

    def test_triple_must_appear_together(self):
        self.assert_invalid(self._doc([self._table(cdc_retention=5)]))
        self.assert_invalid(self._doc([self._table(latest_cdc_seq=0)]))
        self.assert_invalid(self._doc([self._table(cdc_changes=[])]))
        self.assert_invalid(self._doc([
            self._table(cdc_retention=5, latest_cdc_seq=0)
        ]))

    def test_retention_and_cursor_validation(self):
        def doc(retention, latest, changes=None):
            return self._doc([self._table(
                cdc_retention=retention, latest_cdc_seq=latest,
                cdc_changes=changes if changes is not None else [],
            )])

        self.assert_invalid(doc(0, 0))
        self.assert_invalid(doc(-1, 0))
        self.assert_invalid(doc(1.5, 0))
        self.assert_invalid(doc("5", 0))
        self.assert_invalid(doc(True, 0))
        self.assert_invalid(doc(5, -1))
        self.assert_invalid(doc(5, 1.5))
        self.assert_invalid(doc(5, "0"))
        # records required once the cursor moved
        self.assert_invalid(doc(5, 1))
        # records exceed the retention cap
        record = {"seq": 1, "kind": "upsert", "key": "k", "label": "a"}
        self.assert_invalid(doc(1, 2, [record, {**record, "seq": 2}]))
        # fewer records than the live tail would hold
        self.assert_invalid(doc(5, 2, [record]))

    def test_record_shape_validation(self):
        def doc(changes, rows=None):
            table = self._table(
                cdc_retention=10, latest_cdc_seq=len(changes),
                cdc_changes=changes,
            )
            if rows is not None:
                table["rows"] = rows
            return self._doc([table])

        base = {"seq": 1, "kind": "upsert", "key": "k", "label": "a"}
        rows = [{"key": "k", "label": "a"}]
        self.assert_invalid(doc(["nope"]))
        self.assert_invalid(doc([{**base, "x": 1}], rows))
        self.assert_invalid(doc([{k: v for k, v in base.items() if k != "seq"}],
                                rows))
        self.assert_invalid(doc([{**base, "seq": 0}], rows))
        self.assert_invalid(doc([{**base, "seq": -1}], rows))
        self.assert_invalid(doc([{**base, "seq": 1.5}], rows))
        self.assert_invalid(doc([{**base, "kind": "final"}], rows))
        self.assert_invalid(doc([{**base, "key": ""}], rows))
        self.assert_invalid(doc([{**base, "label": ""}], rows))
        # effective_from_ms is a shape error on current-value records
        self.assert_invalid(doc([{**base, "effective_from_ms": 1}], rows))

    def test_record_sequence_validation(self):
        def doc(changes, latest=None):
            return self._doc([{
                "name": "t",
                "rows": [{"key": "k", "label": "b"}],
                "cdc_retention": 10,
                "latest_cdc_seq": latest if latest is not None else len(changes),
                "cdc_changes": changes,
            }])

        first = {"seq": 1, "kind": "upsert", "key": "k", "label": "a"}
        second = {"seq": 2, "kind": "upsert", "key": "k", "label": "b"}
        # duplicate seq
        self.assert_invalid(doc([first, dict(first)]))
        # non-consecutive seq
        self.assert_invalid(doc([first, {**second, "seq": 3}]))
        # out of order
        self.assert_invalid(doc([second, first]))
        # last seq does not match latest_cdc_seq
        self.assert_invalid(doc([first, second], latest=3))

    def test_records_must_not_contradict_current_rows(self):
        def doc(changes, rows):
            return self._doc([{
                "name": "t",
                "rows": rows,
                "cdc_retention": 10,
                "latest_cdc_seq": len(changes),
                "cdc_changes": changes,
            }])

        record = {"seq": 1, "kind": "upsert", "key": "k", "label": "a"}
        # the last record of a key must match its current label
        self.assert_invalid(doc([record], [{"key": "k", "label": "other"}]))
        # records may not reference keys the table does not hold
        self.assert_invalid(doc([record], []))
        # an overwritten earlier record is fine
        older = {"seq": 1, "kind": "upsert", "key": "k", "label": "old"}
        newer = {"seq": 2, "kind": "upsert", "key": "k", "label": "a"}
        status, _ = self.restore(doc([older, newer], [{"key": "k", "label": "a"}]))
        self.assertEqual(status, 200)

    def test_versioned_record_validation(self):
        def doc(changes, versions):
            return self._doc([{
                "name": "t",
                "event_time_versioned": True,
                "versions": versions,
                "cdc_retention": 10,
                "latest_cdc_seq": len(changes),
                "cdc_changes": changes,
            }])

        point = {"key": "k", "label": "a", "effective_from_ms": 100}
        record = {"seq": 1, "kind": "upsert", "key": "k", "label": "a",
                  "effective_from_ms": 100}
        status, _ = self.restore(doc([record], [point]))
        self.assertEqual(status, 200)
        Handler.service = Service()
        # missing effective_from_ms
        self.assert_invalid(doc([
            {k: v for k, v in record.items() if k != "effective_from_ms"}
        ], [point]))
        # non-integer effective time
        self.assert_invalid(doc([{**record, "effective_from_ms": "100"}],
                                [point]))
        # record without a matching version point
        self.assert_invalid(doc([record], []))
        self.assert_invalid(doc([{**record, "effective_from_ms": 200}],
                                [point]))
        # record label contradicts the version point
        self.assert_invalid(doc([{**record, "label": "b"}], [point]))

    def test_restore_conflict_still_wins(self):
        self.create_table(cdc_retention=5)
        status, payload = self.restore(self._doc([
            self._table(cdc_retention=5, latest_cdc_seq=0, cdc_changes=[])
        ]))
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "restore_conflict")

    def test_v7_streams_keep_all_features(self):
        self.create_table(cdc_retention=5)
        self.put_row("k1", "a")
        self.request(
            "POST",
            "/streams",
            {
                "name": "s",
                "window_ms": 1000,
                "allowed_lateness_ms": 0,
                "lookup_table": "t",
                "change_retention": 10,
                "batch_retention": 10,
                "max_open_windows": 10,
            },
        )
        _, doc = self.snapshot()
        self.assertEqual(doc["format_version"], 7)
        stream = doc["streams"][0]
        self.assertEqual(stream["change_retention"], 10)
        self.assertEqual(stream["batch_retention"], 10)
        self.assertEqual(stream["max_open_windows"], 10)
        Handler.service = Service()
        status, payload = self.restore(doc)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 1})
        _, again = self.snapshot()
        self.assertEqual(again, doc)


if __name__ == "__main__":
    unittest.main()
