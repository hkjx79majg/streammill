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


class BatchCreateTest(HttpTestCase):
    def test_create_echoes_batch_retention(self):
        status, body = self.create(batch_retention=5)
        self.assertEqual(status, 201)
        self.assertEqual(body["batch_retention"], 5)

    def test_create_without_batch_retention_unchanged(self):
        status, body = self.create()
        self.assertEqual(status, 201)
        self.assertNotIn("batch_retention", body)

    def test_create_rejects_invalid_batch_retention(self):
        for value in (0, -1, 1.5, "2", True):
            status, body = self.create(batch_retention=value)
            self.assertEqual(status, 422, value)
            self.assertEqual(body["error"]["code"], "invalid_request")

    def test_batches_disabled_without_retention(self):
        self.create()
        status, body = self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "batch_ingest_not_enabled")


class BatchValidationTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create(batch_retention=10)

    def test_invalid_json(self):
        status, body = self.request("POST", "/streams/s/batches", raw=b"{nope")
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "invalid_json")

    def test_unknown_stream(self):
        status, body = self.batch("b1", [{"timestamp_ms": 0, "value": 1}], stream="zz")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "stream_not_found")

    def test_unknown_stream_with_invalid_structure_still_422(self):
        status, body = self.request(
            "POST", "/streams/zz/batches", {"batch_id": "b1", "events": "nope"}
        )
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_request")

    def test_envelope_errors(self):
        bad = [
            {},
            {"batch_id": "b1"},
            {"events": [{"timestamp_ms": 0, "value": 1}]},
            {"batch_id": "", "events": [{"timestamp_ms": 0, "value": 1}]},
            {"batch_id": 7, "events": [{"timestamp_ms": 0, "value": 1}]},
            {"batch_id": "b1", "events": []},
            {"batch_id": "b1", "events": [{"timestamp_ms": 0, "value": 1}] * 1001},
            {"batch_id": "b1", "events": [{"timestamp_ms": 0, "value": 1}], "x": 1},
            {"batch_id": "b1", "events": [{"timestamp_ms": 0}]},
            {"batch_id": "b1", "events": [{"timestamp_ms": 0, "value": 1, "x": 1}]},
            {"batch_id": "b1", "events": [{"timestamp_ms": 0.5, "value": 1}]},
            {"batch_id": "b1", "events": [{"timestamp_ms": 0, "value": "1"}]},
            {"batch_id": "b1", "events": "nope"},
            "nope",
        ]
        for payload in bad:
            status, body = self.request("POST", "/streams/s/batches", payload)
            self.assertEqual(status, 422, payload)
            self.assertEqual(body["error"]["code"], "invalid_request")

    def test_event_fields_follow_stream_features(self):
        self.create("d", dedup_retention_ms=1000, batch_retention=5)
        # dedup stream requires event_id on every element
        status, body = self.batch("b1", [{"timestamp_ms": 0, "value": 1}], stream="d")
        self.assertEqual(status, 422)
        # plain stream rejects event_id as undeclared
        status, body = self.batch(
            "b2", [{"timestamp_ms": 0, "value": 1, "event_id": "e"}]
        )
        self.assertEqual(status, 422)


class BatchApplyTest(HttpTestCase):
    def test_success_response_and_aggregation(self):
        self.create(batch_retention=10)
        status, body = self.batch(
            "b1",
            [
                {"timestamp_ms": 0, "value": 1},
                {"timestamp_ms": 100, "value": 2},
                {"timestamp_ms": 1500, "value": 4},
            ],
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["stream"], "s")
        self.assertEqual(body["batch_id"], "b1")
        self.assertEqual(
            body["outcomes"],
            [{"dropped": False}, {"dropped": False}, {"dropped": False}],
        )
        self.watermark(1000)
        status, body = self.results()
        self.assertEqual(
            body["results"],
            [
                {
                    "stream": "s",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 2,
                    "sum": 3,
                }
            ],
        )

    def test_too_late_drop_is_successful_outcome(self):
        self.create(batch_retention=10)
        self.watermark(5000)
        status, body = self.batch(
            "b1",
            [{"timestamp_ms": 100, "value": 1}, {"timestamp_ms": 5100, "value": 2}],
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            body["outcomes"], [{"dropped": True}, {"dropped": False}]
        )

    def test_dedup_batch(self):
        self.create(dedup_retention_ms=10000, batch_retention=10)
        status, body = self.batch(
            "b1",
            [
                {"timestamp_ms": 0, "value": 1, "event_id": "a"},
                {"timestamp_ms": 0, "value": 1, "event_id": "a"},
                {"timestamp_ms": 100, "value": 2, "event_id": "b"},
            ],
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            body["outcomes"],
            [
                {"dropped": False, "duplicate": False},
                {"dropped": False, "duplicate": True},
                {"dropped": False, "duplicate": False},
            ],
        )
        self.watermark(1000)
        _, body = self.results()
        self.assertEqual(body["results"][0]["count"], 2)
        self.assertEqual(body["results"][0]["sum"], 3)

    def test_event_id_conflict_rolls_back_whole_batch(self):
        self.create(dedup_retention_ms=10000, batch_retention=10)
        self.event(0, 1, event_id="a")
        status, body = self.batch(
            "b1",
            [
                {"timestamp_ms": 100, "value": 5, "event_id": "fresh"},
                {"timestamp_ms": 200, "value": 9, "event_id": "a"},
            ],
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "event_id_conflict")
        # the first element of the failed batch left no trace
        self.watermark(1000)
        _, body = self.results()
        self.assertEqual(body["results"][0]["count"], 1)
        self.assertEqual(body["results"][0]["sum"], 1)
        # the fresh id was not registered either
        status, body = self.event(1500, 5, event_id="fresh")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"stream": "s", "dropped": False, "duplicate": False})

    def test_failed_batch_does_not_occupy_id(self):
        self.create(dedup_retention_ms=10000, batch_retention=10)
        self.event(0, 1, event_id="a")
        status, _ = self.batch(
            "b1", [{"timestamp_ms": 200, "value": 9, "event_id": "a"}]
        )
        self.assertEqual(status, 409)
        status, body = self.batch(
            "b1", [{"timestamp_ms": 300, "value": 3, "event_id": "c"}]
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["batch_id"], "b1")

    def test_lookup_key_not_found_rolls_back(self):
        self.request("POST", "/tables", {"name": "t"})
        self.request("POST", "/tables/t/rows", {"key": "k1", "label": "L1"})
        self.create(lookup_table="t", batch_retention=10)
        status, body = self.batch(
            "b1",
            [
                {"timestamp_ms": 0, "value": 1, "lookup_key": "k1"},
                {"timestamp_ms": 100, "value": 2, "lookup_key": "nope"},
            ],
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "lookup_key_not_found")
        self.watermark(1000)
        _, body = self.results()
        self.assertEqual(body["results"], [])

    def test_joined_batch_success(self):
        self.request("POST", "/tables", {"name": "t"})
        self.request("POST", "/tables/t/rows", {"key": "k1", "label": "L1"})
        self.create(lookup_table="t", batch_retention=10)
        status, body = self.batch(
            "b1",
            [
                {"timestamp_ms": 0, "value": 1, "lookup_key": "k1"},
                {"timestamp_ms": 100, "value": 2, "lookup_key": "k1"},
            ],
        )
        self.assertEqual(status, 200)
        self.watermark(1000)
        _, body = self.request("GET", "/streams/s/joined-results")
        self.assertEqual(len(body["results"]), 1)
        self.assertEqual(body["results"][0]["count"], 2)

    def test_auto_watermark_advances_per_element(self):
        self.create(auto_watermark_lag_ms=1000, batch_retention=10)
        status, body = self.batch(
            "b1",
            [
                {"timestamp_ms": 5000, "value": 1},
                {"timestamp_ms": 6000, "value": 2},
                {"timestamp_ms": 100, "value": 3},
            ],
        )
        self.assertEqual(status, 200)
        outcomes = body["outcomes"]
        self.assertEqual(outcomes[0]["watermark_ms"], 4000)
        self.assertEqual(outcomes[1]["watermark_ms"], 5000)
        # the third element is judged against the watermark after the
        # second element: 100 < 5000 - 0 (allowed_lateness_ms=0) -> dropped
        self.assertTrue(outcomes[2]["dropped"])
        self.assertEqual(outcomes[2]["watermark_ms"], 5000)

    def test_change_feed_records_batch_commits(self):
        self.create(change_retention=100, batch_retention=10)
        status, body = self.batch(
            "b1",
            [{"timestamp_ms": 0, "value": 1}, {"timestamp_ms": 100, "value": 2}],
        )
        self.assertEqual(status, 200)
        _, body = self.changes()
        self.assertEqual(body["latest_seq"], 2)
        self.assertEqual([r["kind"] for r in body["changes"]], ["upsert", "upsert"])

    def test_failed_batch_consumes_no_change_seq(self):
        self.create(
            dedup_retention_ms=10000, change_retention=100, batch_retention=10
        )
        self.event(0, 1, event_id="a")
        status, _ = self.batch(
            "b1",
            [
                {"timestamp_ms": 100, "value": 5, "event_id": "fresh"},
                {"timestamp_ms": 200, "value": 9, "event_id": "a"},
            ],
        )
        self.assertEqual(status, 409)
        _, body = self.changes()
        self.assertEqual(body["latest_seq"], 1)


class BatchReplayTest(HttpTestCase):
    def test_identical_retry_replays_response(self):
        self.create(batch_retention=10)
        events = [{"timestamp_ms": 0, "value": 1}, {"timestamp_ms": 100, "value": 2}]
        status, first = self.batch("b1", events)
        self.assertEqual(status, 200)
        status, second = self.batch("b1", events)
        self.assertEqual(status, 200)
        self.assertEqual(first, second)
        # no double write
        self.watermark(1000)
        _, body = self.results()
        self.assertEqual(body["results"][0]["count"], 2)

    def test_same_id_different_content_conflicts(self):
        self.create(batch_retention=10)
        self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        status, body = self.batch("b1", [{"timestamp_ms": 0, "value": 2}])
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "batch_id_conflict")
        status, body = self.batch(
            "b1",
            [{"timestamp_ms": 0, "value": 1}, {"timestamp_ms": 100, "value": 2}],
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "batch_id_conflict")

    def test_retention_eviction_allows_reuse(self):
        self.create(batch_retention=1)
        self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        self.batch("b2", [{"timestamp_ms": 100, "value": 2}])
        # b1 was evicted: the id may be reused with different content
        status, body = self.batch("b1", [{"timestamp_ms": 200, "value": 9}])
        self.assertEqual(status, 200)
        self.watermark(1000)
        _, body = self.results()
        self.assertEqual(body["results"][0]["count"], 3)
        self.assertEqual(body["results"][0]["sum"], 12)

    def test_replay_does_not_refresh_retention_order(self):
        self.create(batch_retention=2)
        self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        self.batch("b2", [{"timestamp_ms": 100, "value": 2}])
        # replay b1: must not move it behind b2 in the retention order
        self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        self.batch("b3", [{"timestamp_ms": 200, "value": 3}])
        # replay did not refresh, so the order is b1, b2, b3 -> b1 evicted,
        # b2 and b3 retained
        status, body = self.batch("b2", [{"timestamp_ms": 100, "value": 8}])
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "batch_id_conflict")
        status, body = self.batch("b1", [{"timestamp_ms": 300, "value": 4}])
        self.assertEqual(status, 200)


class BatchSnapshotTest(HttpTestCase):
    def test_snapshot_version_4_and_restore(self):
        self.create(batch_retention=10)
        self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        self.batch("b2", [{"timestamp_ms": 100, "value": 2}])
        status, snap = self.snapshot()
        self.assertEqual(status, 200)
        self.assertEqual(snap["format_version"], 4)
        entry = snap["streams"][0]
        self.assertEqual(entry["batch_retention"], 10)
        self.assertEqual([b["batch_id"] for b in entry["batches"]], ["b1", "b2"])

        Handler.service = Service()
        status, body = self.restore(snap)
        self.assertEqual(status, 200)
        self.assertEqual(body, {"restored_streams": 1})
        # replay hits the restored record instead of writing again
        status, body = self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        self.assertEqual(status, 200)
        self.assertEqual(body["outcomes"], [{"dropped": False}])
        self.watermark(1000)
        _, body = self.results()
        self.assertEqual(body["results"][0]["count"], 2)
        # conflict detection survives the restore as well
        status, body = self.batch("b2", [{"timestamp_ms": 100, "value": 7}])
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "batch_id_conflict")

    def test_auto_stream_batch_records_round_trip(self):
        self.create(auto_watermark_lag_ms=1000, batch_retention=5)
        status, first = self.batch(
            "b1",
            [{"timestamp_ms": 5000, "value": 1}, {"timestamp_ms": 6000, "value": 2}],
        )
        self.assertEqual(status, 200)
        _, snap = self.snapshot()
        self.assertEqual(snap["format_version"], 4)
        Handler.service = Service()
        status, _ = self.restore(snap)
        self.assertEqual(status, 200)
        # the replayed response is byte-identical to the first one
        status, second = self.batch(
            "b1",
            [{"timestamp_ms": 5000, "value": 1}, {"timestamp_ms": 6000, "value": 2}],
        )
        self.assertEqual(status, 200)
        self.assertEqual(first, second)
        # and the restored stream keeps its automatic watermark state:
        # 4500 is behind the restored watermark of 5000
        status, body = self.event(4500, 9)
        self.assertEqual(status, 200)
        self.assertTrue(body["dropped"])
        self.assertEqual(body["watermark_ms"], 5000)

    def test_concurrent_same_batch_id_commits_once(self):
        self.create(batch_retention=10)
        events = [{"timestamp_ms": 0, "value": 1}]
        outcomes = []

        def submit():
            outcomes.append(self.batch("b1", events))

        threads = [threading.Thread(target=submit) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertTrue(all(status == 200 for status, _ in outcomes))
        self.assertEqual(len({json.dumps(body, sort_keys=True) for _, body in outcomes}), 1)
        self.watermark(1000)
        _, body = self.results()
        self.assertEqual(body["results"][0]["count"], 1)

    def test_eviction_order_survives_restore(self):
        self.create(batch_retention=1)
        self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        self.batch("b2", [{"timestamp_ms": 100, "value": 2}])
        _, snap = self.snapshot()
        self.assertEqual(len(snap["streams"][0]["batches"]), 1)
        Handler.service = Service()
        self.restore(snap)
        # b2 is retained: conflicts on different content
        status, body = self.batch("b2", [{"timestamp_ms": 100, "value": 9}])
        self.assertEqual(status, 409)
        # b1 was already evicted before the snapshot: reusable
        status, _ = self.batch("b1", [{"timestamp_ms": 200, "value": 3}])
        self.assertEqual(status, 200)

    def test_snapshot_without_batch_streams_keeps_old_versions(self):
        self.create()
        _, snap = self.snapshot()
        self.assertEqual(snap["format_version"], 1)
        Handler.service = Service()
        self.create(change_retention=5)
        _, snap = self.snapshot()
        self.assertEqual(snap["format_version"], 3)

    def test_restore_rejects_invalid_batch_records(self):
        self.create(batch_retention=2)
        self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        _, snap = self.snapshot()
        entry = snap["streams"][0]

        def broken(mutate):
            doc = json.loads(json.dumps(snap))
            mutate(doc["streams"][0])
            Handler.service = Service()
            return self.restore(doc)

        # duplicate identifiers
        status, body = broken(lambda e: e["batches"].append(e["batches"][0]))
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "invalid_snapshot")
        # more records than the retention allows
        status, body = broken(
            lambda e: e["batches"].extend(
                [
                    dict(e["batches"][0], batch_id="b2"),
                    dict(e["batches"][0], batch_id="b3"),
                ]
            )
        )
        self.assertEqual(status, 422)
        # malformed request shape
        status, body = broken(
            lambda e: e["batches"][0]["request"].pop("events")
        )
        self.assertEqual(status, 422)
        # malformed response shape
        status, body = broken(
            lambda e: e["batches"][0]["response"].update(outcomes=[])
        )
        self.assertEqual(status, 422)
        # batches without batch_retention
        status, body = broken(lambda e: e.pop("batch_retention"))
        self.assertEqual(status, 422)
        # batch fields in an older document version
        doc = json.loads(json.dumps(snap))
        doc["format_version"] = 3
        Handler.service = Service()
        status, body = self.restore(doc)
        self.assertEqual(status, 422)
        # a failed restore publishes nothing
        _, body = self.request("GET", "/snapshot")
        self.assertEqual(body["streams"], [])

    def test_restore_old_versions_still_work(self):
        self.create()
        self.event(0, 1)
        _, snap = self.snapshot()
        self.assertEqual(snap["format_version"], 1)
        Handler.service = Service()
        status, body = self.restore(snap)
        self.assertEqual(status, 200)
        _, body = self.results()
        self.assertEqual(body["stream"], "s")


if __name__ == "__main__":
    unittest.main()
