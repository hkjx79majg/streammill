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

    def put_version(self, key, label, effective_from_ms, table="t"):
        return self.request(
            "POST",
            f"/tables/{table}/rows",
            {
                "key": key,
                "label": label,
                "effective_from_ms": effective_from_ms,
            },
        )

    def put_row(self, key, label, table="t"):
        return self.request(
            "POST", f"/tables/{table}/rows", {"key": key, "label": label}
        )

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

    def joined_results(self, stream="s"):
        return self.request("GET", f"/streams/{stream}/joined-results")

    def snapshot(self):
        return self.request("GET", "/snapshot")

    def restore(self, body=None, raw=None):
        return self.request("POST", "/snapshot/restore", body=body, raw=raw)


class VersionedTableCreationTest(HttpTestCase):
    def test_create_versioned_table_echoes_mode(self):
        status, payload = self.create_table(event_time_versioned=True)
        self.assertEqual(status, 201)
        self.assertEqual(payload, {"table": "t", "event_time_versioned": True})

    def test_plain_table_shape_unchanged(self):
        status, payload = self.create_table()
        self.assertEqual(status, 201)
        self.assertEqual(payload, {"table": "t"})
        status, payload = self.create_table("p", event_time_versioned=False)
        self.assertEqual(payload, {"table": "p"})

    def test_create_conflict_across_modes(self):
        self.create_table(event_time_versioned=True)
        status, payload = self.create_table(event_time_versioned=False)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "table_exists")

    def test_create_validation(self):
        for bad in (1, "true", 0, None):
            status, payload = self.create_table("x", event_time_versioned=bad)
            self.assertEqual(status, 422, bad)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        status, payload = self.request(
            "POST", "/tables", {"name": "t", "event_time_versioned": True, "x": 1}
        )
        self.assertEqual(status, 422)
        status, payload = self.request("POST", "/tables", raw=b"{bad")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_json")


class VersionedRowWriteTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table(event_time_versioned=True)

    def test_changed_flag_for_versions_and_retries(self):
        status, payload = self.put_version("k1", "red", 100)
        self.assertEqual(status, 200)
        self.assertEqual(payload["changed"], True)
        self.assertEqual(payload["effective_from_ms"], 100)
        # exact retry
        status, payload = self.put_version("k1", "red", 100)
        self.assertEqual(payload["changed"], False)
        # a second, later version changes
        status, payload = self.put_version("k1", "green", 300)
        self.assertEqual(payload["changed"], True)
        # an out-of-order earlier version also changes
        status, payload = self.put_version("k1", "amber", 200)
        self.assertEqual(payload["changed"], True)
        # retrying the out-of-order point is not a change
        status, payload = self.put_version("k1", "amber", 200)
        self.assertEqual(payload["changed"], False)

    def test_same_point_different_label_conflicts_and_keeps_history(self):
        self.put_version("k1", "red", 100)
        status, payload = self.put_version("k1", "blue", 100)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "dimension_version_conflict")
        # history untouched: the point still keeps the original label, so
        # the exact triple remains a non-changing retry.
        status, payload = self.put_version("k1", "red", 100)
        self.assertEqual(status, 200)
        self.assertEqual(payload["changed"], False)
        # a different key at the same effective time is fine
        status, payload = self.put_version("k2", "blue", 100)
        self.assertEqual(status, 200)
        self.assertEqual(payload["changed"], True)

    def test_negative_and_zero_effective_times_are_valid(self):
        self.assertEqual(self.put_version("k1", "a", -50)[1]["changed"], True)
        self.assertEqual(self.put_version("k1", "a", -50)[1]["changed"], False)
        self.assertEqual(self.put_version("k1", "b", 0)[1]["changed"], True)

    def test_version_row_validation(self):
        for body in (
            {"key": "k", "label": "a"},
            {"key": "k", "label": "a", "effective_from_ms": None},
            {"key": "k", "label": "a", "effective_from_ms": "100"},
            {"key": "k", "label": "a", "effective_from_ms": 1.5},
            {"key": "k", "label": "a", "effective_from_ms": True},
            {"key": "", "label": "a", "effective_from_ms": 100},
            {"key": "k", "label": "", "effective_from_ms": 100},
            {"key": "k", "label": "a", "effective_from_ms": 100, "x": 1},
        ):
            status, payload = self.request("POST", "/tables/t/rows", body)
            self.assertEqual(status, 422, body)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        status, payload = self.request("POST", "/tables/t/rows", raw=b"no")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_json")

    def test_plain_table_rejects_effective_from(self):
        self.create_table("plain")
        status, payload = self.request(
            "POST",
            "/tables/plain/rows",
            {"key": "k", "label": "a", "effective_from_ms": 100},
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        # plain overwrite semantics stay in place
        self.put_row("k", "red", table="plain")
        _, payload = self.put_row("k", "blue", table="plain")
        self.assertEqual(payload["changed"], True)
        self.assertNotIn("effective_from_ms", payload)

    def test_unknown_table_validates_before_404(self):
        status, payload = self.request(
            "POST",
            "/tables/missing/rows",
            {"key": "k", "label": "a", "effective_from_ms": 100},
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        status, payload = self.request(
            "POST", "/tables/missing/rows", {"key": "k", "label": "a"}
        )
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "table_not_found")


class EventTimeJoinTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table(event_time_versioned=True)
        # versions arrive deliberately out of order
        self.put_version("k1", "red", 100)
        self.put_version("k1", "green", 300)
        self.put_version("k1", "amber", 200)
        self.put_version("k2", "blue", 100)
        self.create("s", 1000, 0, lookup_table="t")

    def test_create_stream_references_versioned_table(self):
        status, payload = self.create("j", 1000, 0, lookup_table="t")
        self.assertEqual(status, 201)
        self.assertEqual(payload["lookup_table"], "t")

    def test_event_picks_version_valid_at_event_time(self):
        for ts, label in (
            (100, "red"),    # boundary: version effective exactly at ts
            (150, "red"),
            (199, "red"),
            (200, "amber"),  # boundary of the out-of-order insert
            (299, "amber"),
            (300, "green"),
            (5000, "green"),  # newest version stays valid forever
        ):
            self.event(ts, 1, lookup_key="k1")
        self.watermark(6000)
        _, payload = self.joined_results()
        groups = {
            (r["window_start_ms"], r["label"]): (r["count"], r["sum"])
            for r in payload["results"]
        }
        self.assertEqual(groups, {
            (0, "red"): (3, 3),
            (0, "amber"): (2, 2),
            (0, "green"): (1, 1),
            (5000, "green"): (1, 1),
        })

    def test_unknown_key_and_before_first_version(self):
        for ts, key in ((100, "nope"), (99, "k1"), (0, "k1"), (-1, "k1")):
            status, payload = self.event(ts, 5, lookup_key=key)
            self.assertEqual(status, 409, (ts, key))
            self.assertEqual(payload["error"]["code"], "lookup_version_not_found")
        # nothing was aggregated
        _, snap = self.snapshot()
        self.assertEqual(snap["streams"][0]["windows"], [])

    def test_late_dimension_backfills_resolve_historical_labels(self):
        # accept an early event once history down to its time exists
        self.put_version("k1", "old", 0)
        status, payload = self.event(50, 2, lookup_key="k1")
        self.assertEqual(status, 200)
        self.watermark(1000)
        _, payload = self.joined_results()
        self.assertEqual(
            [(r["label"], r["count"], r["sum"]) for r in payload["results"]],
            [("old", 1, 2)],
        )

    def test_backfilling_history_does_not_recompute_accepted_events(self):
        self.event(150, 4, lookup_key="k1")  # red at that time
        self.watermark(1000)
        # insert a retroactively valid point at 0 and a *new* version at
        # 150 (a fresh effective time, so the write succeeds) — neither
        # may recompute the already accepted/finalized event
        self.put_version("k1", "retro", 0)
        _, payload = self.put_version("k1", "crimson", 150)
        self.assertEqual(payload["changed"], True)
        _, payload = self.joined_results()
        self.assertEqual(
            [(r["label"], r["count"], r["sum"]) for r in payload["results"]],
            [("red", 1, 4)],
        )

    def test_dedup_still_compares_lookup_key(self):
        self.create("d", 1000, 0, lookup_table="t", dedup_retention_ms=5000)
        status, payload = self.event(
            150, 5, stream="d", event_id="e1", lookup_key="k1"
        )
        self.assertEqual(payload["duplicate"], False)
        # exact retry is a duplicate even if a new version appeared meanwhile
        self.put_version("k1", "later", 400)
        status, payload = self.event(
            150, 5, stream="d", event_id="e1", lookup_key="k1"
        )
        self.assertEqual(payload["duplicate"], True)
        # same id with another key conflicts before the lookup happens
        status, payload = self.event(
            150, 5, stream="d", event_id="e1", lookup_key="k2"
        )
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")

    def test_missing_version_does_not_register_dedup_id(self):
        self.create("d", 1000, 0, lookup_table="t", dedup_retention_ms=5000)
        status, _ = self.event(
            50, 5, stream="d", event_id="e1", lookup_key="k1"
        )
        self.assertEqual(status, 409)
        status, payload = self.event(
            150, 5, stream="d", event_id="e1", lookup_key="k1"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["duplicate"], False)

    def test_lookup_failure_leaves_automatic_watermark_untouched(self):
        self.create("a", 1000, 0, lookup_table="t", auto_watermark_lag_ms=0)
        status, payload = self.event(100, 1, stream="a", lookup_key="nope")
        self.assertEqual(status, 409)
        _, snap = self.snapshot()
        stream = next(s for s in snap["streams"] if s["name"] == "a")
        self.assertIsNone(stream["watermark_ms"])
        self.assertIsNone(stream["max_event_timestamp_ms"])

    def test_lookup_failure_consumes_no_change_sequence(self):
        self.create("c", 1000, 0, lookup_table="t", change_retention=10)
        self.event(150, 1, stream="c", lookup_key="k1")
        _, before = self.request("GET", "/streams/c/changes?after_seq=0&limit=10")
        self.event(50, 1, stream="c", lookup_key="k1")
        _, after = self.request("GET", "/streams/c/changes?after_seq=0&limit=10")
        self.assertEqual(after["latest_seq"], before["latest_seq"])
        self.assertEqual(after["changes"], before["changes"])

    def test_joined_results_ordering_unchanged(self):
        for ts, key in ((350, "k2"), (150, "k2"), (250, "k1"), (1100, "k1")):
            self.event(ts, 1, lookup_key=key)
        self.watermark(3000)
        _, payload = self.joined_results()
        order = [
            (r["window_start_ms"], r["lookup_key"], r["label"])
            for r in payload["results"]
        ]
        self.assertEqual(order, sorted(order))


class VersionedBatchTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table(event_time_versioned=True)
        self.put_version("k1", "red", 100)
        self.put_version("k1", "green", 300)
        self.create(
            "s", 1000, 0, lookup_table="t",
            batch_retention=10, auto_watermark_lag_ms=0, change_retention=50,
        )

    def test_batch_rolls_back_on_missing_version(self):
        events = [
            {"timestamp_ms": 150, "value": 1, "lookup_key": "k1"},
            # not late relative to the automatic watermark (>= 150), but
            # the key has no versions at all
            {"timestamp_ms": 250, "value": 1, "lookup_key": "missing"},
        ]
        status, payload = self.batch("b1", events)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "lookup_version_not_found")
        _, snap = self.snapshot()
        stream = snap["streams"][0]
        self.assertEqual(stream["windows"], [])
        self.assertIsNone(stream["watermark_ms"])
        self.assertEqual(stream["changes"], [])
        self.assertEqual(stream["batches"], [])
        # the identifier was not occupied: a corrected batch reuses it
        events[1]["lookup_key"] = "k1"
        events[1]["timestamp_ms"] = 350
        status, payload = self.batch("b1", events)
        self.assertEqual(status, 200)
        self.assertEqual(len(payload["outcomes"]), 2)

    def test_batch_rolls_back_on_event_id_conflict(self):
        self.create(
            "d", 1000, 0, lookup_table="t", batch_retention=10,
            dedup_retention_ms=5000,
        )
        good = [
            {"timestamp_ms": 150, "value": 1, "event_id": "e1", "lookup_key": "k1"},
        ]
        status, _ = self.batch("g", good, stream="d")
        self.assertEqual(status, 200)
        bad = [
            {"timestamp_ms": 350, "value": 1, "event_id": "e2", "lookup_key": "k1"},
            {"timestamp_ms": 150, "value": 2, "event_id": "e1", "lookup_key": "k1"},
        ]
        status, payload = self.batch("b2", bad, stream="d")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")
        _, snap = self.snapshot()
        stream = next(s for s in snap["streams"] if s["name"] == "d")
        self.assertEqual([w["count"] for w in stream["windows"]], [1])
        self.assertEqual(stream["batches"], [
            b for b in stream["batches"] if b["batch_id"] == "g"
        ])

    def test_batch_replay_stored_response(self):
        events = [
            {"timestamp_ms": 150, "value": 1, "lookup_key": "k1"},
            {"timestamp_ms": 350, "value": 2, "lookup_key": "k1"},
        ]
        _, first = self.batch("b3", events)
        _, second = self.batch("b3", events)
        self.assertEqual(second, first)


class VersionedSnapshotTest(HttpTestCase):
    def _build_state(self):
        self.create_table(event_time_versioned=True)
        # intentionally out-of-order and across keys
        self.put_version("k1", "green", 300)
        self.put_version("k2", "blue", 100)
        self.put_version("k1", "red", 100)
        self.put_version("k1", "amber", 200)
        self.create_table("plain")
        self.put_row("p1", "v", table="plain")
        self.create("j", 1000, 0, lookup_table="t")
        self.event(150, 5, stream="j", lookup_key="k1")
        self.event(350, 2, stream="j", lookup_key="k1")

    def test_snapshot_v5_shape_and_ordering(self):
        self._build_state()
        status, payload = self.snapshot()
        self.assertEqual(status, 200)
        self.assertEqual(payload["format_version"], 5)
        self.assertEqual(payload["tables"][0]["name"], "plain")
        self.assertEqual(
            payload["tables"][0],
            {"name": "plain", "rows": [{"key": "p1", "label": "v"}]},
        )
        versioned = payload["tables"][1]
        self.assertEqual(versioned["name"], "t")
        self.assertEqual(versioned["event_time_versioned"], True)
        self.assertNotIn("rows", versioned)
        self.assertEqual(
            versioned["versions"],
            [
                {"key": "k1", "label": "red", "effective_from_ms": 100},
                {"key": "k1", "label": "amber", "effective_from_ms": 200},
                {"key": "k1", "label": "green", "effective_from_ms": 300},
                {"key": "k2", "label": "blue", "effective_from_ms": 100},
            ],
        )

    def test_versioned_table_alone_promotes_to_v5(self):
        self.create_table(event_time_versioned=True)
        self.put_version("k1", "red", 0)
        _, payload = self.snapshot()
        self.assertEqual(payload["format_version"], 5)

    def test_round_trip(self):
        self._build_state()
        _, snap = self.snapshot()
        Handler.service = Service()
        status, payload = self.restore(snap)
        self.assertEqual(status, 200)
        self.assertEqual(payload["restored_streams"], 1)
        _, again = self.snapshot()
        self.assertEqual(again, snap)
        # restored version lookups keep working
        status, payload = self.event(250, 3, stream="j", lookup_key="k1")
        self.assertEqual(status, 200)
        self.watermark(1000, stream="j")
        _, payload = self.joined_results("j")
        groups = {(r["label"]): (r["count"], r["sum"]) for r in payload["results"]}
        self.assertEqual(groups, {"red": (1, 5), "amber": (1, 3), "green": (1, 2)})
        # restored retry semantics
        _, payload = self.put_version("k1", "red", 100)
        self.assertEqual(payload["changed"], False)
        status, _ = self.put_version("k1", "blue", 100)
        self.assertEqual(status, 409)

    def test_restore_v4_document_still_accepted(self):
        doc = {
            "format_version": 4,
            "tables": [{"name": "t", "rows": [{"key": "k", "label": "v"}]}],
            "streams": [],
        }
        status, payload = self.restore(doc)
        self.assertEqual(status, 200)
        _, snap = self.snapshot()
        self.assertEqual(snap["format_version"], 2)  # no batch/version state
        self.assertEqual(snap["tables"], doc["tables"])

    def _v5(self, tables, streams=None):
        return {
            "format_version": 5,
            "tables": tables,
            "streams": list(streams or []),
        }

    VERSION_TABLE = {
        "name": "t",
        "event_time_versioned": True,
        "versions": [
            {"key": "k1", "label": "red", "effective_from_ms": 100},
            {"key": "k1", "label": "green", "effective_from_ms": 200},
        ],
    }

    def assert_invalid(self, doc):
        status, payload = self.restore(doc)
        self.assertEqual(status, 422, doc)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")
        _, current = self.snapshot()
        self.assertEqual(current, {"format_version": 1, "streams": []})

    def test_v5_empty_tables_restores(self):
        # v5 documents carry tables; an empty array is a valid (if
        # downgraded-on-reexport) document.
        status, payload = self.restore(self._v5([]))
        self.assertEqual(status, 200)
        _, snap = self.snapshot()
        self.assertEqual(snap["format_version"], 1)

    def test_v5_mode_field_combinations(self):
        # event_time_versioned not exactly true
        for mode in (False, 1, "true", None):
            table = dict(self.VERSION_TABLE, event_time_versioned=mode)
            self.assert_invalid(self._v5([table]))
        # versions without the mode marker
        table = {"name": "t", "versions": self.VERSION_TABLE["versions"]}
        self.assert_invalid(self._v5([table]))
        # mode marker but no versions
        table = {"name": "t", "event_time_versioned": True}
        self.assert_invalid(self._v5([table]))
        # marker plus plain rows instead of versions
        table = {
            "name": "t",
            "event_time_versioned": True,
            "rows": [{"key": "k1", "label": "red"}],
        }
        self.assert_invalid(self._v5([table]))
        # both rows and versions
        table = {
            "name": "t",
            "event_time_versioned": True,
            "rows": [],
            "versions": [],
        }
        self.assert_invalid(self._v5([table]))
        # marker explicitly false keeps the plain shape and is invalid
        table = {
            "name": "t",
            "event_time_versioned": False,
            "rows": [{"key": "k1", "label": "red"}],
        }
        self.assert_invalid(self._v5([table]))
        # unknown field
        table = dict(self.VERSION_TABLE, extra=1)
        self.assert_invalid(self._v5([table]))
        # versions must be an array
        table = dict(self.VERSION_TABLE, versions={})
        self.assert_invalid(self._v5([table]))

    def test_v5_version_row_validation(self):
        def doc_with(rows):
            return self._v5([{
                "name": "t",
                "event_time_versioned": True,
                "versions": rows,
            }])

        good = {"key": "k1", "label": "red", "effective_from_ms": 100}
        # duplicate effective point for one key (also covers same-point conflict)
        self.assert_invalid(doc_with([good, dict(good, label="blue")]))
        # out-of-order effective times
        self.assert_invalid(doc_with([
            {"key": "k1", "label": "green", "effective_from_ms": 200},
            good,
        ]))
        # out-of-order / duplicate keys
        self.assert_invalid(doc_with([
            {"key": "k2", "label": "blue", "effective_from_ms": 100},
            good,
        ]))
        # invalid effective times
        for bad_ts in ("100", 1.5, True, None):
            self.assert_invalid(doc_with([dict(good, effective_from_ms=bad_ts)]))
        # missing field / extra field / empty strings
        self.assert_invalid(doc_with([{"key": "k1", "label": "red"}]))
        self.assert_invalid(doc_with([dict(good, extra=1)]))
        self.assert_invalid(doc_with([dict(good, key="")]))
        self.assert_invalid(doc_with([dict(good, label="")]))
        # negative effective times are legal and ordered
        status, _ = self.restore(doc_with([
            {"key": "k1", "label": "old", "effective_from_ms": -10},
            good,
        ]))
        self.assertEqual(status, 200)

    def test_versioned_tables_rejected_in_older_documents(self):
        table = dict(self.VERSION_TABLE)
        for version in (2, 3, 4):
            self.assert_invalid(
                {"format_version": version, "tables": [table], "streams": []}
            )
        # a marker alone is also rejected
        marker = {"name": "t", "event_time_versioned": True, "versions": []}
        self.assert_invalid(
            {"format_version": 2, "tables": [marker], "streams": []}
        )


if __name__ == "__main__":
    unittest.main()
