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

    def restore(self, body=None, raw=None):
        return self.request("POST", "/snapshot/restore", body=body, raw=raw)


class BatchCreateTest(HttpTestCase):
    def test_create_echoes_batch_retention(self):
        status, payload = self.create(batch_retention=50)
        self.assertEqual(status, 201)
        self.assertEqual(payload["batch_retention"], 50)

    def test_create_without_batch_retention_keeps_shape(self):
        status, payload = self.create()
        self.assertEqual(status, 201)
        self.assertNotIn("batch_retention", payload)

    def test_invalid_batch_retention_rejected(self):
        for bad in (0, -3, 1.5, "10", True, None):
            status, payload = self.create(batch_retention=bad)
            self.assertEqual(status, 422, bad)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        status, _ = self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        self.assertEqual(status, 404)

    def test_batches_not_enabled(self):
        self.create()
        status, payload = self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "batch_ingest_not_enabled")

    def test_unknown_stream(self):
        status, payload = self.batch("b1", [{"timestamp_ms": 0, "value": 1}])
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "stream_not_found")


class BatchValidationTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create(batch_retention=10)

    def assert_invalid(self, body=None, raw=None):
        status, payload = self.request(
            "POST", "/streams/s/batches", body=body, raw=raw
        )
        self.assertEqual(status, 422, body)
        self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_non_json_body(self):
        status, payload = self.request(
            "POST", "/streams/s/batches", raw=b"{not json"
        )
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_json")

    def test_body_must_be_object(self):
        self.assert_invalid(body=[1, 2])
        self.assert_invalid(body="batch")

    def test_missing_fields(self):
        self.assert_invalid(body={"events": [{"timestamp_ms": 0, "value": 1}]})
        self.assert_invalid(body={"batch_id": "b1"})

    def test_extra_fields(self):
        self.assert_invalid(
            body={
                "batch_id": "b1",
                "events": [{"timestamp_ms": 0, "value": 1}],
                "extra": 1,
            }
        )

    def test_invalid_batch_id(self):
        for bad in ("", 1, None, True, ["b"]):
            self.assert_invalid(
                body={"batch_id": bad, "events": [{"timestamp_ms": 0, "value": 1}]}
            )

    def test_invalid_events_container(self):
        for bad in (None, {}, "x", [], [{"timestamp_ms": 0, "value": 1}] * 1001):
            self.assert_invalid(body={"batch_id": "b1", "events": bad})

    def test_invalid_event_elements(self):
        for bad in (
            [1],
            [{}],
            [{"timestamp_ms": 0}],
            [{"value": 1}],
            [{"timestamp_ms": 0.5, "value": 1}],
            [{"timestamp_ms": 0, "value": "x"}],
            [{"timestamp_ms": 0, "value": 1, "event_id": "e"}],
            [{"timestamp_ms": 0, "value": 1, "lookup_key": "k"}],
        ):
            self.assert_invalid(body={"batch_id": "b1", "events": bad})

    def test_whole_batch_validated_before_any_write(self):
        status, payload = self.batch(
            "b1",
            [
                {"timestamp_ms": 0, "value": 1},
                {"timestamp_ms": "bad", "value": 2},
            ],
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        # The structurally valid first element was not applied either.
        status, payload = self.batch("b2", [{"timestamp_ms": 0, "value": 1}])
        self.assertEqual(status, 200)
        self.assertEqual(payload["outcomes"], [{"dropped": False}])
        status, payload = self.watermark(1000)
        self.assertEqual(payload["finalized"][0]["count"], 1)

    def test_unknown_stream_validates_against_base_shape(self):
        status, payload = self.request(
            "POST",
            "/streams/ghost/batches",
            body={"batch_id": "b1", "events": [{"timestamp_ms": 0}]},
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        status, payload = self.batch(
            "b1", [{"timestamp_ms": 0, "value": 1}], stream="ghost"
        )
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "stream_not_found")

    def test_dedup_stream_requires_event_id(self):
        self.create("d", 1000, 0, dedup_retention_ms=100, batch_retention=5)
        status, payload = self.batch(
            "b1", [{"timestamp_ms": 0, "value": 1}], stream="d"
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_join_stream_requires_lookup_key(self):
        self.request("POST", "/tables", {"name": "t"})
        self.create("j", 1000, 0, lookup_table="t", batch_retention=5)
        status, payload = self.batch(
            "b1", [{"timestamp_ms": 0, "value": 1}], stream="j"
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")


class BatchSemanticsTest(HttpTestCase):
    def test_success_response_shape_and_order(self):
        self.create(batch_retention=10)
        status, payload = self.batch(
            "b1",
            [
                {"timestamp_ms": 100, "value": 1},
                {"timestamp_ms": 200, "value": 2.5},
                {"timestamp_ms": 1500, "value": 3},
            ],
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            payload,
            {
                "stream": "s",
                "batch_id": "b1",
                "outcomes": [
                    {"dropped": False},
                    {"dropped": False},
                    {"dropped": False},
                ],
            },
        )
        status, payload = self.watermark(1500)
        self.assertEqual(payload["finalized"][0]["count"], 2)
        self.assertEqual(payload["finalized"][0]["sum"], 3.5)

    def test_too_late_drop_is_successful_outcome(self):
        self.create(batch_retention=10)
        self.watermark(5000)
        status, payload = self.batch(
            "b1",
            [
                {"timestamp_ms": 100, "value": 1},
                {"timestamp_ms": 6000, "value": 2},
            ],
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            payload["outcomes"], [{"dropped": True}, {"dropped": False}]
        )

    def test_dedup_outcomes_and_duplicate(self):
        self.create(batch_retention=10, dedup_retention_ms=10000)
        self.event(100, 1, event_id="e1")
        status, payload = self.batch(
            "b1",
            [
                {"timestamp_ms": 100, "value": 1, "event_id": "e1"},
                {"timestamp_ms": 200, "value": 2, "event_id": "e2"},
            ],
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            payload["outcomes"],
            [
                {"dropped": False, "duplicate": True},
                {"dropped": False, "duplicate": False},
            ],
        )

    def test_event_id_conflict_rolls_back_everything(self):
        self.create(batch_retention=10, dedup_retention_ms=10000, change_retention=10)
        self.event(100, 1, event_id="e1")
        _, before = self.changes()
        status, payload = self.batch(
            "b1",
            [
                {"timestamp_ms": 200, "value": 2, "event_id": "e2"},
                {"timestamp_ms": 100, "value": 9, "event_id": "e1"},
            ],
        )
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")
        # No sequence numbers were consumed by the failed batch.
        _, after = self.changes()
        self.assertEqual(after, before)
        # e2 was neither aggregated nor remembered.
        status, payload = self.event(200, 2, event_id="e2")
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"stream": "s", "dropped": False, "duplicate": False})

    def test_lookup_key_not_found_rolls_back_everything(self):
        self.request("POST", "/tables", {"name": "t"})
        self.request("POST", "/tables/t/rows", {"key": "k1", "label": "L1"})
        self.create("j", 1000, 0, lookup_table="t", batch_retention=10)
        status, payload = self.batch(
            "b1",
            [
                {"timestamp_ms": 100, "value": 1, "lookup_key": "k1"},
                {"timestamp_ms": 200, "value": 2, "lookup_key": "ghost"},
            ],
            stream="j",
        )
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "lookup_key_not_found")
        # The first element left no aggregates behind.
        status, payload = self.watermark(1000, stream="j")
        self.assertEqual(payload["finalized"], [])

    def test_failed_batch_does_not_claim_the_id(self):
        self.create(batch_retention=10, dedup_retention_ms=10000)
        self.event(100, 1, event_id="e1")
        status, _ = self.batch(
            "b1", [{"timestamp_ms": 100, "value": 9, "event_id": "e1"}]
        )
        self.assertEqual(status, 409)
        status, payload = self.batch(
            "b1", [{"timestamp_ms": 300, "value": 3, "event_id": "e3"}]
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            payload["outcomes"], [{"dropped": False, "duplicate": False}]
        )

    def test_auto_watermark_advances_per_element(self):
        self.create(batch_retention=10, auto_watermark_lag_ms=0)
        status, payload = self.batch(
            "b1",
            [
                {"timestamp_ms": 1000, "value": 1},
                # After the first element the watermark is 1000, so this
                # older element is too late (allowed lateness 0).
                {"timestamp_ms": 500, "value": 2},
                {"timestamp_ms": 2500, "value": 3},
            ],
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            payload["outcomes"],
            [
                {"dropped": False, "watermark_ms": 1000, "finalized": []},
                {"dropped": True, "watermark_ms": 1000, "finalized": []},
                {
                    "dropped": False,
                    "watermark_ms": 2500,
                    "finalized": [
                        {
                            "stream": "s",
                            "window_start_ms": 1000,
                            "window_end_ms": 2000,
                            "count": 1,
                            "sum": 1,
                        },
                    ],
                },
            ],
        )

    def test_batch_publishes_change_records_in_order(self):
        self.create(batch_retention=10, change_retention=50, auto_watermark_lag_ms=0)
        status, _ = self.batch(
            "b1",
            [
                {"timestamp_ms": 100, "value": 1},
                {"timestamp_ms": 200, "value": 2},
                {"timestamp_ms": 1100, "value": 3},
            ],
        )
        self.assertEqual(status, 200)
        status, payload = self.changes()
        self.assertEqual(status, 200)
        kinds = [record["kind"] for record in payload["changes"]]
        # upsert, upsert, upsert (window 1000), final (window 0).
        self.assertEqual(kinds, ["upsert", "upsert", "upsert", "final"])
        self.assertEqual(payload["latest_seq"], 4)
        seqs = [record["seq"] for record in payload["changes"]]
        self.assertEqual(seqs, [1, 2, 3, 4])

    def test_concurrent_same_batch_id_commits_once(self):
        self.create(batch_retention=10)
        events = [{"timestamp_ms": 100, "value": 1}]
        results = []

        def submit():
            results.append(self.batch("b1", events))

        threads = [threading.Thread(target=submit) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(results), 8)
        first = results[0]
        self.assertEqual(first[0], 200)
        for result in results:
            self.assertEqual(result, first)
        # The events were applied exactly once.
        status, payload = self.watermark(1000)
        self.assertEqual(payload["finalized"][0]["count"], 1)


class BatchIdempotencyTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create(batch_retention=2)

    def test_identical_retry_returns_first_response_without_rewriting(self):
        events = [{"timestamp_ms": 100, "value": 1}]
        status, first = self.batch("b1", events)
        self.assertEqual(status, 200)
        status, replay = self.batch("b1", events)
        self.assertEqual(status, 200)
        self.assertEqual(replay, first)
        status, payload = self.watermark(1000)
        self.assertEqual(payload["finalized"][0]["count"], 1)

    def test_same_id_different_content_conflicts(self):
        self.batch("b1", [{"timestamp_ms": 100, "value": 1}])
        for different in (
            [{"timestamp_ms": 100, "value": 2}],
            [{"timestamp_ms": 100, "value": 1}, {"timestamp_ms": 200, "value": 3}],
            [{"timestamp_ms": 200, "value": 1}],
        ):
            status, payload = self.batch("b1", different)
            self.assertEqual(status, 409, different)
            self.assertEqual(payload["error"]["code"], "batch_id_conflict")

    def test_retention_evicts_oldest_and_ids_can_be_reused(self):
        self.batch("b1", [{"timestamp_ms": 100, "value": 1}])
        self.batch("b2", [{"timestamp_ms": 200, "value": 2}])
        self.batch("b3", [{"timestamp_ms": 300, "value": 3}])
        # b2 is still retained and replays its first response.
        status, payload = self.batch("b2", [{"timestamp_ms": 200, "value": 2}])
        self.assertEqual(status, 200)
        self.assertEqual(payload["outcomes"], [{"dropped": False}])
        # b1 was evicted: resubmitting it processes as a brand-new batch.
        status, payload = self.batch("b1", [{"timestamp_ms": 100, "value": 1}])
        self.assertEqual(status, 200)
        status, payload = self.watermark(1000)
        self.assertEqual(payload["finalized"][0]["count"], 4)
        self.assertEqual(payload["finalized"][0]["sum"], 7)

    def test_replay_does_not_refresh_eviction_order(self):
        self.batch("b1", [{"timestamp_ms": 100, "value": 1}])
        self.batch("b2", [{"timestamp_ms": 200, "value": 2}])
        # Replaying b1 must not protect it from eviction.
        self.batch("b1", [{"timestamp_ms": 100, "value": 1}])
        self.batch("b3", [{"timestamp_ms": 300, "value": 3}])
        # b2 is still retained and replays without aggregating again.
        status, payload = self.batch("b2", [{"timestamp_ms": 200, "value": 2}])
        self.assertEqual(status, 200)
        # b1 (the oldest commit) was evicted and reprocesses as new.
        status, payload = self.batch("b1", [{"timestamp_ms": 100, "value": 1}])
        self.assertEqual(status, 200)
        status, payload = self.watermark(1000)
        self.assertEqual(payload["finalized"][0]["count"], 4)


class BatchSnapshotTest(HttpTestCase):
    def test_snapshot_without_batch_streams_keeps_version(self):
        self.create()
        status, payload = self.snapshot()
        self.assertEqual(payload["format_version"], 1)
        self.assertNotIn("batch_retention", payload["streams"][0])

    def test_snapshot_with_batch_stream_exports_version_4(self):
        self.create(batch_retention=10)
        self.batch("b1", [{"timestamp_ms": 100, "value": 1}])
        self.batch("b2", [{"timestamp_ms": 200, "value": 2}])
        status, payload = self.snapshot()
        self.assertEqual(status, 200)
        self.assertEqual(payload["format_version"], 4)
        self.assertEqual(payload["tables"], [])
        stream = payload["streams"][0]
        self.assertEqual(stream["batch_retention"], 10)
        self.assertEqual([record["batch_id"] for record in stream["batches"]], ["b1", "b2"])
        self.assertEqual(
            stream["batches"][0],
            {
                "batch_id": "b1",
                "events": [{"timestamp_ms": 100, "value": 1}],
                "outcomes": [{"dropped": False}],
            },
        )

    def test_restore_roundtrip_continues_replay_and_eviction(self):
        self.create(batch_retention=2)
        _, first = self.batch("b1", [{"timestamp_ms": 100, "value": 1}])
        self.batch("b2", [{"timestamp_ms": 200, "value": 2}])
        _, document = self.snapshot()

        Handler.service = Service()
        status, payload = self.restore(body=document)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 1})
        # Retained records replay exactly as before the snapshot.
        status, replay = self.batch("b1", [{"timestamp_ms": 100, "value": 1}])
        self.assertEqual(status, 200)
        self.assertEqual(replay, first)
        # Eviction continues from the restored commit order: committing
        # b3 evicts b1 (the oldest) while b2 survives and keeps replaying.
        self.batch("b3", [{"timestamp_ms": 300, "value": 3}])
        status, payload = self.batch("b2", [{"timestamp_ms": 200, "value": 2}])
        self.assertEqual(status, 200)
        status, payload = self.batch("b1", [{"timestamp_ms": 100, "value": 1}])
        self.assertEqual(status, 200)
        status, payload = self.watermark(1000)
        self.assertEqual(payload["finalized"][0]["count"], 4)

    def test_restore_reexport_is_identical(self):
        self.create(
            batch_retention=5, dedup_retention_ms=1000, auto_watermark_lag_ms=10
        )
        self.batch("b1", [{"timestamp_ms": 100, "value": 1, "event_id": "e1"}])
        _, document = self.snapshot()
        Handler.service = Service()
        status, _ = self.restore(body=document)
        self.assertEqual(status, 200)
        _, again = self.snapshot()
        self.assertEqual(again, document)

    def test_restore_rejects_invalid_batch_records(self):
        self.create(batch_retention=2)
        self.batch("b1", [{"timestamp_ms": 100, "value": 1}])
        _, document = self.snapshot()
        stream = document["streams"][0]
        record = stream["batches"][0]

        cases = []
        duplicate = json.loads(json.dumps(document))
        duplicate["streams"][0]["batches"].append(record)
        cases.append(duplicate)
        overflow = json.loads(json.dumps(document))
        overflow["streams"][0]["batches"] = [
            record,
            dict(record, batch_id="b2"),
            dict(record, batch_id="b3"),
        ]
        cases.append(overflow)
        bad_event = json.loads(json.dumps(document))
        bad_event["streams"][0]["batches"][0]["events"] = []
        cases.append(bad_event)
        bad_outcome = json.loads(json.dumps(document))
        bad_outcome["streams"][0]["batches"][0]["outcomes"] = [{"dropped": "yes"}]
        cases.append(bad_outcome)
        missing_outcomes = json.loads(json.dumps(document))
        del missing_outcomes["streams"][0]["batches"][0]["outcomes"]
        cases.append(missing_outcomes)
        no_retention = json.loads(json.dumps(document))
        del no_retention["streams"][0]["batch_retention"]
        cases.append(no_retention)
        no_batches = json.loads(json.dumps(document))
        del no_batches["streams"][0]["batches"]
        cases.append(no_batches)

        for document in cases:
            Handler.service = Service()
            status, payload = self.restore(body=document)
            self.assertEqual(status, 422, json.dumps(document)[:120])
            self.assertEqual(payload["error"]["code"], "invalid_snapshot")
            status, payload = self.snapshot()
            self.assertEqual(payload, {"format_version": 1, "streams": []})

    def test_version_3_document_rejects_batch_fields(self):
        self.create(change_retention=5)
        _, document = self.snapshot()
        self.assertEqual(document["format_version"], 3)
        document["streams"][0]["batch_retention"] = 5
        Handler.service = Service()
        status, payload = self.restore(body=document)
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")

    def test_older_versions_still_restore(self):
        for version, extra in ((1, {}), (2, {"tables": []}), (3, {"tables": []})):
            Handler.service = Service()
            status, payload = self.restore(
                body={"format_version": version, "streams": [], **extra}
            )
            self.assertEqual(status, 200, version)
            self.assertEqual(payload, {"restored_streams": 0})

    def test_restore_conflict_on_non_empty_instance(self):
        self.create(batch_retention=2)
        status, payload = self.restore(body={"format_version": 4, "tables": [], "streams": []})
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "restore_conflict")


class BatchCompatibilityTest(HttpTestCase):
    def test_single_event_and_watermark_surfaces_unchanged(self):
        self.create(batch_retention=5)
        status, payload = self.event(100, 1)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"stream": "s", "dropped": False})
        status, payload = self.watermark(1000)
        self.assertEqual(status, 200)
        self.assertEqual(
            payload["finalized"],
            [
                {
                    "stream": "s",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 1,
                    "sum": 1,
                }
            ],
        )
        status, payload = self.results()
        self.assertEqual(
            payload,
            {
                "stream": "s",
                "results": [
                    {
                        "stream": "s",
                        "window_start_ms": 0,
                        "window_end_ms": 1000,
                        "count": 1,
                        "sum": 1,
                    }
                ],
            },
        )

    def test_healthz_unchanged(self):
        status, payload = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")


if __name__ == "__main__":
    unittest.main()
