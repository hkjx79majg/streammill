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

    def event(self, ts, value, event_id=None, lookup_key=None, stream="s"):
        body = {"timestamp_ms": ts, "value": value}
        if event_id is not None:
            body["event_id"] = event_id
        if lookup_key is not None:
            body["lookup_key"] = lookup_key
        return self.request("POST", f"/streams/{stream}/events", body)

    def watermark(self, wm, stream="s"):
        return self.request(
            "POST", f"/streams/{stream}/watermark", {"watermark_ms": wm}
        )

    def changes(self, stream="s", after_seq=0, limit=1000):
        return self.request(
            "GET", f"/streams/{stream}/changes?after_seq={after_seq}&limit={limit}"
        )

    def snapshot(self):
        return self.request("GET", "/snapshot")

    def restore(self, body=None, raw=None):
        return self.request("POST", "/snapshot/restore", body=body, raw=raw)


class ChangeFeedCreateTest(HttpTestCase):
    def test_creation_echoes_change_retention(self):
        status, payload = self.create("s", 1000, 0, change_retention=50)
        self.assertEqual(status, 201)
        self.assertEqual(
            payload,
            {
                "stream": "s",
                "window_ms": 1000,
                "allowed_lateness_ms": 0,
                "change_retention": 50,
            },
        )

    def test_creation_validates_change_retention(self):
        for bad in (0, -1, 1.5, "10", True):
            status, payload = self.create("s", 1000, 0, change_retention=bad)
            self.assertEqual(status, 422, bad)
            self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_disabled_by_default_and_response_unchanged(self):
        status, payload = self.create("s", 1000, 0)
        self.assertEqual(status, 201)
        self.assertNotIn("change_retention", payload)
        status, payload = self.changes()
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "change_feed_not_enabled")


class ChangeFeedPublishTest(HttpTestCase):
    def test_upserts_and_manual_finals(self):
        self.create("s", 1000, 0, change_retention=100)
        self.event(100, 5)
        self.event(200, 7)
        self.event(1500, 1)
        status, payload = self.changes()
        self.assertEqual(status, 200)
        self.assertEqual(payload["stream"], "s")
        self.assertEqual(payload["latest_seq"], 3)
        self.assertEqual(
            payload["changes"],
            [
                {"seq": 1, "kind": "upsert", "window_start_ms": 0,
                 "window_end_ms": 1000, "count": 1, "sum": 5},
                {"seq": 2, "kind": "upsert", "window_start_ms": 0,
                 "window_end_ms": 1000, "count": 2, "sum": 12},
                {"seq": 3, "kind": "upsert", "window_start_ms": 1000,
                 "window_end_ms": 2000, "count": 1, "sum": 1},
            ],
        )
        # manual watermark publishes only finals, in window order
        status, payload = self.watermark(2000)
        self.assertEqual(status, 200)
        status, payload = self.changes(after_seq=3)
        self.assertEqual(payload["latest_seq"], 5)
        self.assertEqual(
            payload["changes"],
            [
                {"seq": 4, "kind": "final", "window_start_ms": 0,
                 "window_end_ms": 1000, "count": 2, "sum": 12},
                {"seq": 5, "kind": "final", "window_start_ms": 1000,
                 "window_end_ms": 2000, "count": 1, "sum": 1},
            ],
        )
        # an advance that finalizes nothing publishes nothing
        status, payload = self.watermark(2500)
        self.assertEqual(payload["finalized"], [])
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 5)

    def test_initial_latest_seq_is_zero(self):
        self.create("s", 1000, 0, change_retention=10)
        status, payload = self.changes()
        self.assertEqual(status, 200)
        self.assertEqual(
            payload, {"stream": "s", "latest_seq": 0, "changes": []}
        )

    def test_sliding_event_produces_one_upsert_per_window(self):
        self.create("s", 1000, 0, slide_ms=500, change_retention=100)
        self.event(700, 4)
        status, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 2)
        self.assertEqual(
            [(c["seq"], c["window_start_ms"]) for c in payload["changes"]],
            [(1, 0), (2, 500)],
        )
        for record in payload["changes"]:
            self.assertEqual(record["kind"], "upsert")
            self.assertEqual(record["count"], 1)
            self.assertEqual(record["sum"], 4)
            self.assertEqual(record["window_end_ms"], record["window_start_ms"] + 1000)

    def test_auto_watermark_orders_upserts_before_finals(self):
        self.create(
            "s", 1000, 0, auto_watermark_lag_ms=0, change_retention=100
        )
        self.event(100, 5)     # wm -> 100, nothing finalizes
        self.event(2500, 1)    # wm -> 2500, finalizes [0,1000) and [1000,2000)? no: [1000,2000) empty
        status, payload = self.changes()
        self.assertEqual(
            [(c["seq"], c["kind"], c["window_start_ms"]) for c in payload["changes"]],
            [
                (1, "upsert", 0),
                (2, "upsert", 2000),
                (3, "final", 0),
            ],
        )
        self.assertEqual(payload["changes"][2]["count"], 1)
        self.assertEqual(payload["changes"][2]["sum"], 5)

    def test_failed_events_do_not_consume_sequence_numbers(self):
        self.request("POST", "/tables", {"name": "t"})
        self.request("POST", "/tables/t/rows", {"key": "k1", "label": "L1"})
        self.create(
            "s", 1000, 0,
            dedup_retention_ms=5000,
            lookup_table="t",
            change_retention=100,
        )
        self.event(100, 5, event_id="e1", lookup_key="k1")
        self.assertEqual(self.changes()[1]["latest_seq"], 1)
        # exact duplicate
        _, payload = self.event(100, 5, event_id="e1", lookup_key="k1")
        self.assertEqual(payload["duplicate"], True)
        # conflict
        status, _ = self.event(100, 6, event_id="e1", lookup_key="k1")
        self.assertEqual(status, 409)
        # unknown lookup key
        status, _ = self.event(200, 1, event_id="e2", lookup_key="nope")
        self.assertEqual(status, 409)
        # validation failure
        status, _ = self.request(
            "POST", "/streams/s/events", {"timestamp_ms": "x", "value": 1}
        )
        self.assertEqual(status, 422)
        self.assertEqual(self.changes()[1]["latest_seq"], 1)
        # too-late drop
        self.watermark(5000)
        _, payload = self.event(100, 1, event_id="e3", lookup_key="k1")
        self.assertEqual(payload["dropped"], True)
        # only the final record of window [0,1000) was added
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 2)
        self.assertEqual(payload["changes"][1]["kind"], "final")

    def test_joined_stream_publishes_base_window_changes_only(self):
        self.request("POST", "/tables", {"name": "t"})
        self.request("POST", "/tables/t/rows", {"key": "a", "label": "A"})
        self.request("POST", "/tables/t/rows", {"key": "b", "label": "B"})
        self.create("s", 1000, 0, lookup_table="t", change_retention=100)
        self.event(100, 1, lookup_key="a")
        self.event(200, 2, lookup_key="b")
        _, payload = self.changes()
        self.assertEqual(payload["latest_seq"], 2)
        self.assertEqual(
            [c["window_start_ms"] for c in payload["changes"]], [0, 0]
        )
        self.assertEqual(payload["changes"][1]["count"], 2)
        self.assertEqual(payload["changes"][1]["sum"], 3)
        # grouped results are unaffected and carry no change fields
        _, joined = self.request("GET", "/streams/s/joined-results")
        self.assertEqual(joined, {"stream": "s", "results": []})


class ChangeFeedReadTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create("s", 1000, 0, change_retention=100)
        for i in range(5):
            self.event(i * 1000, i + 1)

    def test_cursor_and_limit(self):
        status, payload = self.changes(after_seq=2, limit=2)
        self.assertEqual(status, 200)
        self.assertEqual([c["seq"] for c in payload["changes"]], [3, 4])
        self.assertEqual(payload["latest_seq"], 5)
        # reading at the tip is a valid empty page
        status, payload = self.changes(after_seq=5)
        self.assertEqual(status, 200)
        self.assertEqual(payload["changes"], [])

    def test_unknown_stream_is_404(self):
        status, payload = self.changes(stream="ghost")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "stream_not_found")

    def test_cursor_ahead_is_409(self):
        status, payload = self.changes(after_seq=6)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "change_cursor_ahead")

    def test_query_parameter_validation(self):
        bad_paths = [
            "/streams/s/changes",                          # missing both
            "/streams/s/changes?after_seq=0",              # missing limit
            "/streams/s/changes?limit=10",                 # missing after_seq
            "/streams/s/changes?after_seq=0&limit=10&x=1", # unknown
            "/streams/s/changes?after_seq=0&after_seq=1&limit=10",  # duplicate
            "/streams/s/changes?after_seq=-1&limit=10",    # negative
            "/streams/s/changes?after_seq=1.5&limit=10",   # non-integer
            "/streams/s/changes?after_seq=&limit=10",      # blank
            "/streams/s/changes?after_seq=0&limit=0",      # below range
            "/streams/s/changes?after_seq=0&limit=1001",   # above range
            "/streams/s/changes?after_seq=0&limit=-5",
            "/streams/s/changes?after_seq=0&limit=abc",
        ]
        for path in bad_paths:
            status, payload = self.request("GET", path)
            self.assertEqual(status, 422, path)
            self.assertEqual(payload["error"]["code"], "invalid_request", path)

    def test_limit_boundaries_are_accepted(self):
        self.assertEqual(self.changes(after_seq=0, limit=1)[0], 200)
        self.assertEqual(self.changes(after_seq=0, limit=1000)[0], 200)


class ChangeFeedRetentionTest(HttpTestCase):
    def test_trimming_keeps_latest_and_latest_seq_never_regresses(self):
        self.create("s", 1000, 0, change_retention=3)
        for i in range(5):
            self.event(i * 1000, 1)
        # cursor exactly at the trim boundary reads the retained suffix
        status, payload = self.changes(after_seq=2)
        self.assertEqual(status, 200)
        self.assertEqual(payload["latest_seq"], 5)
        self.assertEqual([c["seq"] for c in payload["changes"]], [3, 4, 5])
        # cursor behind the retained records is expired
        status, payload = self.changes(after_seq=1)
        self.assertEqual(status, 410)
        self.assertEqual(payload["error"]["code"], "change_cursor_expired")
        status, payload = self.changes(after_seq=0)
        self.assertEqual(status, 410)

    def test_expired_check_only_when_records_retained(self):
        self.create("s", 1000, 0, change_retention=2)
        # no records at all: any in-range cursor is fine, none is expired
        status, _ = self.changes(after_seq=0)
        self.assertEqual(status, 200)


class ChangeFeedSnapshotTest(HttpTestCase):
    def _build(self):
        self.create("plain", 1000, 0)
        self.create("feed", 1000, 0, change_retention=10)
        self.event(100, 5, stream="feed")
        self.event(200, 7, stream="feed")
        self.event(1500, 1, stream="feed")
        self.watermark(1100, stream="feed")  # finalizes [0,1000)

    def test_enabled_instance_exports_version_3(self):
        self._build()
        status, payload = self.snapshot()
        self.assertEqual(status, 200)
        self.assertEqual(payload["format_version"], 3)
        self.assertEqual(payload["tables"], [])
        streams = {s["name"]: s for s in payload["streams"]}
        feed = streams["feed"]
        self.assertEqual(feed["change_retention"], 10)
        self.assertEqual(feed["latest_seq"], 4)
        self.assertEqual(
            [(c["seq"], c["kind"]) for c in feed["changes"]],
            [(1, "upsert"), (2, "upsert"), (3, "upsert"), (4, "final")],
        )
        # the plain stream carries no change fields
        self.assertNotIn("change_retention", streams["plain"])
        self.assertNotIn("changes", streams["plain"])

    def test_disabled_instances_keep_original_versions(self):
        self.create("plain", 1000, 0)
        self.assertEqual(self.snapshot()[1]["format_version"], 1)
        self.request("POST", "/tables", {"name": "t"})
        self.assertEqual(self.snapshot()[1]["format_version"], 2)

    def test_round_trip_continues_cursor_and_sequence(self):
        self._build()
        _, snap = self.snapshot()
        before = self.changes("feed")[1]
        Handler.service = Service()
        status, payload = self.restore(snap)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 2})
        # identical feed state
        self.assertEqual(self.changes("feed")[1], before)
        # the next event continues the sequence without a gap
        self.event(1600, 2, stream="feed")
        _, payload = self.changes("feed", after_seq=4)
        self.assertEqual(
            payload["changes"],
            [{"seq": 5, "kind": "upsert", "window_start_ms": 1000,
              "window_end_ms": 2000, "count": 2, "sum": 3}],
        )
        self.assertEqual(payload["latest_seq"], 5)

    def test_reexport_without_writes_is_identical(self):
        self._build()
        _, first = self.snapshot()
        Handler.service = Service()
        self.restore(first)
        _, second = self.snapshot()
        self.assertEqual(
            json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True)
        )

    def test_trimmed_feed_round_trip(self):
        self.create("feed", 1000, 0, change_retention=2)
        for i in range(4):
            self.event(i * 1000, 1, stream="feed")
        _, snap = self.snapshot()
        Handler.service = Service()
        status, _ = self.restore(snap)
        self.assertEqual(status, 200)
        _, payload = self.changes("feed", after_seq=2)
        self.assertEqual(payload["latest_seq"], 4)
        self.assertEqual([c["seq"] for c in payload["changes"]], [3, 4])
        status, _ = self.changes("feed", after_seq=1)
        self.assertEqual(status, 410)

    def test_joined_feed_stream_round_trip(self):
        self.request("POST", "/tables", {"name": "t"})
        self.request("POST", "/tables/t/rows", {"key": "a", "label": "A"})
        self.create("j", 1000, 0, lookup_table="t", change_retention=10)
        self.event(100, 1, lookup_key="a", stream="j")
        _, snap = self.snapshot()
        self.assertEqual(snap["format_version"], 3)
        self.assertEqual(len(snap["tables"]), 1)
        Handler.service = Service()
        status, _ = self.restore(snap)
        self.assertEqual(status, 200)
        _, payload = self.changes("j")
        self.assertEqual(payload["latest_seq"], 1)


class ChangeFeedRestoreValidationTest(HttpTestCase):
    def _feed_stream(self, **overrides):
        entry = {
            "name": "s",
            "window_ms": 1000,
            "allowed_lateness_ms": 0,
            "dedup_retention_ms": None,
            "watermark_ms": None,
            "windows": [
                {"window_start_ms": 0, "window_end_ms": 1000,
                 "count": 2, "sum": 12},
            ],
            "finalized": [],
            "change_retention": 10,
            "latest_seq": 2,
            "changes": [
                {"seq": 1, "kind": "upsert", "window_start_ms": 0,
                 "window_end_ms": 1000, "count": 1, "sum": 5},
                {"seq": 2, "kind": "upsert", "window_start_ms": 0,
                 "window_end_ms": 1000, "count": 2, "sum": 12},
            ],
        }
        entry.update(overrides)
        return entry

    def doc(self, *streams, version=3):
        return {"format_version": version, "tables": [], "streams": list(streams)}

    def assert_invalid(self, doc):
        status, payload = self.restore(doc)
        self.assertEqual(status, 422, doc)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")
        # nothing was published
        _, current = self.snapshot()
        self.assertEqual(current, {"format_version": 1, "streams": []})

    def test_valid_document_restores(self):
        status, payload = self.restore(self.doc(self._feed_stream()))
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 1})

    def test_v3_requires_tables_field(self):
        doc = {"format_version": 3, "streams": []}
        self.assert_invalid(doc)

    def test_change_fields_rejected_in_older_versions(self):
        self.assert_invalid(self.doc(self._feed_stream(), version=1))
        self.assert_invalid(self.doc(self._feed_stream(), version=2))

    def test_change_fields_must_appear_together(self):
        entry = self._feed_stream()
        del entry["changes"]
        self.assert_invalid(self.doc(entry))
        entry = self._feed_stream()
        del entry["latest_seq"]
        self.assert_invalid(self.doc(entry))
        entry = self._feed_stream()
        del entry["change_retention"]
        self.assert_invalid(self.doc(entry))

    def test_config_validation(self):
        self.assert_invalid(self.doc(self._feed_stream(change_retention=0)))
        self.assert_invalid(self.doc(self._feed_stream(change_retention=True)))
        self.assert_invalid(self.doc(self._feed_stream(latest_seq=-1)))
        self.assert_invalid(self.doc(self._feed_stream(latest_seq="2")))
        self.assert_invalid(self.doc(self._feed_stream(changes={})))

    def test_retention_cap(self):
        self.assert_invalid(self.doc(self._feed_stream(change_retention=1)))

    def test_sequence_continuity(self):
        entry = self._feed_stream()
        entry["changes"] = [entry["changes"][1]]  # gap: seq 2 without seq 1
        entry["latest_seq"] = 2
        self.assert_invalid(self.doc(entry))
        entry = self._feed_stream()
        entry["changes"] = [entry["changes"][1], entry["changes"][0]]
        self.assert_invalid(self.doc(entry))
        # latest_seq ahead of the records
        self.assert_invalid(self.doc(self._feed_stream(latest_seq=3)))
        # records without a matching latest_seq
        entry = self._feed_stream(latest_seq=1)
        self.assert_invalid(self.doc(entry))
        # records exist but latest_seq is zero
        self.assert_invalid(self.doc(self._feed_stream(latest_seq=0)))
        # no records but latest_seq is non-zero
        self.assert_invalid(self.doc(self._feed_stream(latest_seq=5, changes=[])))

    def test_record_shape_validation(self):
        def with_record(**overrides):
            record = {
                "seq": 1, "kind": "upsert", "window_start_ms": 0,
                "window_end_ms": 1000, "count": 2, "sum": 12,
            }
            record.update(overrides)
            return self._feed_stream(latest_seq=1, changes=[record])

        self.assert_invalid(self.doc(with_record(seq=0)))
        self.assert_invalid(self.doc(with_record(seq=1.5)))
        self.assert_invalid(self.doc(with_record(kind="delete")))
        self.assert_invalid(self.doc(with_record(window_start_ms=500)))
        self.assert_invalid(self.doc(with_record(window_end_ms=999)))
        self.assert_invalid(self.doc(with_record(count=0)))
        self.assert_invalid(self.doc(with_record(sum="x")))
        self.assert_invalid(self.doc(with_record(extra=1)))
        recordless = with_record()
        del recordless["changes"][0]["kind"]
        self.assert_invalid(self.doc(recordless))

    def test_last_record_must_match_aggregate_state(self):
        # count/sum of the last retained record differs from the open window
        entry = self._feed_stream()
        entry["changes"][1]["count"] = 3
        self.assert_invalid(self.doc(entry))
        entry = self._feed_stream()
        entry["changes"][1]["sum"] = 13
        self.assert_invalid(self.doc(entry))
        # an open window's last record cannot be a final
        entry = self._feed_stream()
        entry["changes"][1]["kind"] = "final"
        self.assert_invalid(self.doc(entry))
        # record for a window the stream does not know
        entry = self._feed_stream()
        entry["changes"] = entry["changes"] + [
            {"seq": 3, "kind": "upsert", "window_start_ms": 1000,
             "window_end_ms": 2000, "count": 1, "sum": 1}
        ]
        entry["latest_seq"] = 3
        self.assert_invalid(self.doc(entry))

    def test_final_record_matches_finalized_window(self):
        entry = self._feed_stream(
            watermark_ms=1000,
            windows=[],
            finalized=[{
                "stream": "s", "window_start_ms": 0, "window_end_ms": 1000,
                "count": 2, "sum": 12,
            }],
            changes=[
                {"seq": 1, "kind": "upsert", "window_start_ms": 0,
                 "window_end_ms": 1000, "count": 1, "sum": 5},
                {"seq": 2, "kind": "final", "window_start_ms": 0,
                 "window_end_ms": 1000, "count": 2, "sum": 12},
            ],
        )
        status, _ = self.restore(self.doc(entry))
        self.assertEqual(status, 200)
        Handler.service = Service()
        # a finalized window's last record cannot be an upsert
        entry["changes"][1]["kind"] = "upsert"
        self.assert_invalid(self.doc(entry))
        # and the totals must match the finalized row
        entry["changes"][1]["kind"] = "final"
        entry["changes"][1]["sum"] = 11
        self.assert_invalid(self.doc(entry))


class ChangeFeedConcurrencyTest(HttpTestCase):
    def test_concurrent_writes_form_a_single_total_order(self):
        self.create("s", 1000, 0, change_retention=100000)
        writers = 4
        events_per_writer = 50
        errors = []

        def writer(base):
            try:
                for i in range(events_per_writer):
                    self.event(base + i, 1)
            except Exception as exc:  # pragma: no cover - diagnostic only
                errors.append(exc)

        threads = [
            threading.Thread(target=writer, args=(n * 100000,))
            for n in range(writers)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        self.assertFalse(errors)
        _, payload = self.changes()
        total = writers * events_per_writer
        self.assertEqual(payload["latest_seq"], total)
        seqs = [c["seq"] for c in payload["changes"]]
        self.assertEqual(seqs, list(range(1, total + 1)))


if __name__ == "__main__":
    unittest.main()
